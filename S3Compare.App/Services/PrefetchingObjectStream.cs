using System.Runtime.ExceptionServices;
using System.Threading.Channels;
using S3Compare.App.Models;

namespace S3Compare.App.Services;

/// <summary>Running totals shared by every stream of one bucket (all partitions), updated on consumption.</summary>
public sealed class StreamReadCounters
{
    private long _objects;
    private long _bytes;

    public long Objects => Interlocked.Read(ref _objects);
    public long Bytes => Interlocked.Read(ref _bytes);

    public void Add(long objects, long bytes)
    {
        Interlocked.Add(ref _objects, objects);
        Interlocked.Add(ref _bytes, bytes);
    }
}

/// <summary>
/// Lists one bucket (or one key range of it) with a background producer that requests page N+1 as soon as page N
/// arrives, buffering up to <c>prefetchPages</c> pages in a bounded channel. ListObjectsV2 pages within one listing
/// are inherently sequential (opaque continuation token), so prefetching hides the latency of the next page behind
/// the comparison of the current one.
/// </summary>
/// <remarks>
/// The range is <c>(startAfter, upper]</c>: listing starts after the full key <c>startAfter</c> and stops at the first
/// key whose prefix-relative form is greater than <c>upperRelativeInclusive</c> (null = to the end).
/// Producer exceptions surface from <see cref="MoveNextAsync"/>; <see cref="DisposeAsync"/> cancels and awaits the producer.
/// </remarks>
public sealed class PrefetchingObjectStream : IS3ObjectStream
{
    public const int DefaultPageSize = 1000;

    private readonly IS3Lister _lister;
    private readonly string _bucketName;
    private readonly string _prefix;
    private readonly string? _startAfter;
    private readonly string? _upper;
    private readonly int _pageSize;
    private readonly TokenBucketRateLimiter? _rateLimiter;
    private readonly Action<List<S3ObjectSummary>>? _onPageConsumed;
    private readonly StreamReadCounters? _sharedCounters;

    private readonly Channel<List<S3ObjectSummary>> _pages;
    private readonly CancellationTokenSource _producerCts;
    private readonly Task _producer;
    private Exception? _producerError;
    private volatile bool _producerCanceled;

    private List<S3ObjectSummary>? _page;
    private int _index;
    private S3ObjectSummary? _current;
    private long _totalObjectsRead;
    private long _totalBytesRead;
    private bool _finished;
    private int _disposed;

    public PrefetchingObjectStream(
        IS3Lister lister,
        string bucketName,
        string? prefix,
        string? startAfter = null,
        string? upperRelativeInclusive = null,
        int prefetchPages = ScanPerformanceOptions.DefaultPrefetchPages,
        TokenBucketRateLimiter? rateLimiter = null,
        Action<List<S3ObjectSummary>>? onPageConsumed = null,
        StreamReadCounters? sharedCounters = null,
        int pageSize = DefaultPageSize,
        CancellationToken cancellationToken = default)
    {
        _lister = lister ?? throw new ArgumentNullException(nameof(lister));
        _bucketName = bucketName ?? throw new ArgumentNullException(nameof(bucketName));
        _prefix = prefix ?? string.Empty;
        _startAfter = string.IsNullOrEmpty(startAfter) ? null : startAfter;
        _upper = upperRelativeInclusive;
        _pageSize = Math.Clamp(pageSize, 1, 1000);
        _rateLimiter = rateLimiter;
        _onPageConsumed = onPageConsumed;
        _sharedCounters = sharedCounters;

        _pages = Channel.CreateBounded<List<S3ObjectSummary>>(new BoundedChannelOptions(Math.Max(1, prefetchPages))
        {
            SingleReader = true,
            SingleWriter = true,
            FullMode = BoundedChannelFullMode.Wait
        });

        // Both buckets' producers start as soon as the streams exist, so A and B are listed concurrently.
        _producerCts = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken);
        _producer = Task.Run(() => ProduceAsync(_producerCts.Token));
    }

    public S3ObjectSummary? Current => _current;
    public long TotalObjectsRead => _totalObjectsRead;
    public long TotalBytesRead => _totalBytesRead;
    public bool HasMore => !_finished;

    private async Task ProduceAsync(CancellationToken cancellationToken)
    {
        try
        {
            string? continuationToken = null;
            bool first = true;
            while (true)
            {
                await _rateLimiter.WaitIfLimitedAsync(1, cancellationToken).ConfigureAwait(false);

                var page = await _lister.ListPageAsync(
                    _bucketName,
                    _prefix,
                    first ? _startAfter : null,
                    continuationToken,
                    delimiter: null,
                    _pageSize,
                    cancellationToken).ConfigureAwait(false);
                first = false;

                if (page.IsTruncated && string.IsNullOrEmpty(page.NextContinuationToken))
                {
                    throw new InvalidOperationException(
                        $"S3 reported a truncated listing for bucket '{_bucketName}' but returned no continuation token; aborting to avoid re-reading the first page.");
                }

                var items = page.Objects;
                bool reachedUpper = false;
                if (_upper != null)
                {
                    int cut = FirstIndexAboveUpper(items);
                    if (cut >= 0)
                    {
                        items = items.GetRange(0, cut);
                        reachedUpper = true;
                    }
                }

                if (items.Count > 0)
                {
                    await _pages.Writer.WriteAsync(items, cancellationToken).ConfigureAwait(false);
                }

                if (reachedUpper || !page.IsTruncated)
                {
                    break;
                }
                continuationToken = page.NextContinuationToken;
            }
        }
        catch (OperationCanceledException) when (cancellationToken.IsCancellationRequested)
        {
            _producerCanceled = true;
        }
        catch (Exception ex)
        {
            _producerError = ex;
        }
        finally
        {
            _pages.Writer.TryComplete();
        }
    }

    private int FirstIndexAboveUpper(List<S3ObjectSummary> items)
    {
        // Pages are sorted, so only the last item needs checking in the common case
        if (items.Count == 0 || Utf8KeyComparer.Instance.Compare(Relative(items[^1].Key), _upper) <= 0)
            return -1;

        int lo = 0, hi = items.Count - 1;
        while (lo < hi)
        {
            int mid = (lo + hi) / 2;
            if (Utf8KeyComparer.Instance.Compare(Relative(items[mid].Key), _upper) > 0) hi = mid;
            else lo = mid + 1;
        }
        return lo;
    }

    private string Relative(string key) => BucketComparisonEngine.ToRelativeKey(key, _prefix);

    public async Task<bool> MoveNextAsync(CancellationToken cancellationToken = default)
    {
        while (true)
        {
            if (_page != null && ++_index < _page.Count)
            {
                _current = _page[_index];
                return true;
            }
            _page = null;

            if (await _pages.Reader.WaitToReadAsync(cancellationToken).ConfigureAwait(false))
            {
                if (_pages.Reader.TryRead(out var page))
                {
                    _page = page;
                    _index = -1;

                    long bytes = 0;
                    foreach (var item in page) bytes += item.Size;
                    _totalObjectsRead += page.Count;
                    _totalBytesRead += bytes;
                    _sharedCounters?.Add(page.Count, bytes);

                    try
                    {
                        _onPageConsumed?.Invoke(page);
                    }
                    catch
                    {
                        // A misbehaving subscriber must never abort the listing.
                    }
                }
                continue;
            }

            // The producer finished: rethrow its failure, never report a failed or cancelled listing as the end of the range
            if (_producerError != null)
            {
                ExceptionDispatchInfo.Capture(_producerError).Throw();
            }
            if (_producerCanceled)
            {
                throw new OperationCanceledException("The listing was cancelled.", _producerCts.Token);
            }

            _current = null;
            _finished = true;
            return false;
        }
    }

    public async ValueTask DisposeAsync()
    {
        if (Interlocked.Exchange(ref _disposed, 1) != 0) return;

        try
        {
            _producerCts.Cancel();
        }
        catch (ObjectDisposedException)
        {
        }

        try
        {
            await _producer.ConfigureAwait(false);
        }
        catch
        {
            // ProduceAsync records its own failures
        }
        _producerCts.Dispose();
        _page = null;
    }
}
