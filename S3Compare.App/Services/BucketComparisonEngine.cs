using System.Collections.Concurrent;
using System.Diagnostics;
using System.IO;
using System.Runtime.ExceptionServices;
using System.Threading.Channels;
using Amazon.Runtime;
using Amazon.S3;
using Amazon.S3.Model;
using S3Compare.App.Models;

namespace S3Compare.App.Services;

/// <summary>
/// Streaming two-way merge diff of two buckets, run as independent merge-joins over key ranges.
/// </summary>
/// <remarks>
/// <para>The keyspace (relative to each bucket's own prefix) is split into ranges by <see cref="PartitionPlanner"/>; up to
/// <see cref="ScanPerformanceOptions.MaxParallelPartitions"/> workers pull ranges from a queue. Each worker lists both
/// buckets for its range through <see cref="PrefetchingObjectStream"/>s, so pages are fetched ahead and A and B are
/// listed concurrently.</para>
/// <para>Workers never touch shared output directly. They send <see cref="CommitBatch"/>es (the differences of a run of fully
/// processed keys, the counters for those keys and the range's new last completed key) to a single writer task, which
/// raises <see cref="DifferenceFound"/>, appends to the live CSV, aggregates statistics and saves checkpoints. Because
/// one thread applies whole batches, every checkpoint it takes is consistent: a range's LastCompletedKey is saved only
/// together with the CSV length that contains every difference for keys up to it.</para>
/// <para>Each range has its own <see cref="SequenceGapDetector"/>, so a numeric gap that straddles a range boundary is not
/// reported.</para>
/// </remarks>
public class BucketComparisonEngine
{
    /// <summary>Keys a worker processes before committing a batch (also committed every <see cref="CommitIntervalMs"/>).</summary>
    private const int CommitEveryKeys = 256;
    private const int CommitIntervalMs = 200;
    private const int PageEventIntervalMs = 250;

    private readonly S3BucketConfig? _configA;
    private readonly S3BucketConfig? _configB;
    private readonly IS3ObjectStream? _providedStreamA;
    private readonly IS3ObjectStream? _providedStreamB;
    private readonly IS3Lister? _injectedListerA;
    private readonly IS3Lister? _injectedListerB;
    private readonly ComparisonMode _mode;
    private readonly string? _liveReportCsvPath;
    private readonly ScanCheckpoint? _resumeCheckpoint;
    private readonly bool _detectSequenceGaps;
    private readonly int _maxSequenceGapThreshold;
    private readonly string _prefixA;
    private readonly string _prefixB;
    private readonly ScanPerformanceOptions _options;

    private readonly Stopwatch _stopwatch = new();
    private readonly Stopwatch _pauseWatch = new();
    private long _differenceIdCounter;
    private int _providedStreamsTaken;

    // Owned by the writer task while the scan runs (and by RunAsync before it starts / after it ends)
    private readonly ScanCounters _totals = new();
    private List<ScanPartition> _ranges = new();
    private string _phase = "Idle";
    private long _lastProgressMs;
    private long _lastCheckpointMs;
    private long _itemsSinceCheckpoint;
    private string _lastCheckpointDisplay = "-";
    private DateTime? _lastCheckpointTime;
    private Exception? _writerError;
    private bool _checkpointUnsafe;

    private FileStream? _csvStream;
    private StreamWriter? _csvWriter;

    // Shared by workers
    private readonly object _pauseLock = new();
    private volatile TaskCompletionSource? _pauseGate;
    private volatile string _currentKeyA = "-";
    private volatile string _currentKeyB = "-";
    private int _activePartitions;
    private readonly StreamReadCounters _readA = new();
    private readonly StreamReadCounters _readB = new();
    private readonly SemaphoreSlim _contentChecks;
    private readonly TokenBucketRateLimiter? _objectLimiter;
    private readonly int _objectBatch;
    private TokenBucketRateLimiter? _limiterA;
    private TokenBucketRateLimiter? _limiterB;
    private IAmazonS3? _clientA;
    private IAmazonS3? _clientB;
    private long _lastPageEventA;
    private long _lastPageEventB;

    public event Action<ComparisonStatistics>? ProgressUpdated;
    public event Action<ObjectDifference>? DifferenceFound;
    public event Action<List<S3ObjectSummary>>? PageLoadedA;
    public event Action<List<S3ObjectSummary>>? PageLoadedB;

    /// <summary>
    /// Optional override for FullData byte comparison of an object pair (A item, B item). Intended for tests;
    /// when null the engine downloads both objects through its S3 clients.
    /// </summary>
    public Func<S3ObjectSummary, S3ObjectSummary, CancellationToken, Task<bool>>? ContentComparer { get; set; }

    /// <summary>Base directory for checkpoints (default: <see cref="AppPaths.DataDirectory"/>). Intended for tests.</summary>
    public string? CheckpointBaseDirectory { get; set; }

    /// <summary>Uses this plan instead of planning a fresh scan. Must tile the keyspace. Intended for tests.</summary>
    public IReadOnlyList<ScanPartition>? PartitionPlanOverride { get; set; }

    /// <summary>MaxKeys for range listings (S3 caps it at 1000). Intended for tests.</summary>
    public int ListPageSize { get; set; } = PrefetchingObjectStream.DefaultPageSize;

    /// <summary>The partition plan of the current (or last) run, with progress. Snapshot copy.</summary>
    public IReadOnlyList<ScanPartition> CurrentPlan => _ranges.Select(r => r.Clone()).ToList();

    public BucketComparisonEngine(
        S3BucketConfig configA,
        S3BucketConfig configB,
        ComparisonMode mode,
        string? liveReportCsvPath = null,
        ScanCheckpoint? resumeCheckpoint = null,
        bool detectSequenceGaps = true,
        int maxSequenceGapThreshold = 25,
        ScanPerformanceOptions? performance = null)
        : this(configA, configB, null, null, mode, liveReportCsvPath, resumeCheckpoint, detectSequenceGaps, maxSequenceGapThreshold, performance)
    {
    }

    /// <summary>Compares through the given listers (for example in-memory fakes) instead of creating S3 clients.</summary>
    public BucketComparisonEngine(
        S3BucketConfig configA,
        S3BucketConfig configB,
        IS3Lister? listerA,
        IS3Lister? listerB,
        ComparisonMode mode,
        string? liveReportCsvPath = null,
        ScanCheckpoint? resumeCheckpoint = null,
        bool detectSequenceGaps = true,
        int maxSequenceGapThreshold = 25,
        ScanPerformanceOptions? performance = null)
    {
        _configA = configA ?? throw new ArgumentNullException(nameof(configA));
        _configB = configB ?? throw new ArgumentNullException(nameof(configB));
        if ((listerA == null) != (listerB == null))
            throw new ArgumentException("Provide listers for both buckets or for neither.");
        _injectedListerA = listerA;
        _injectedListerB = listerB;
        _prefixA = configA.Prefix ?? string.Empty;
        _prefixB = configB.Prefix ?? string.Empty;
        _mode = mode;
        _liveReportCsvPath = liveReportCsvPath;
        _resumeCheckpoint = resumeCheckpoint;
        _detectSequenceGaps = detectSequenceGaps;
        _maxSequenceGapThreshold = maxSequenceGapThreshold;
        _options = ValidatedOptions(performance);

        _contentChecks = new SemaphoreSlim(_options.MaxConcurrentContentChecks, _options.MaxConcurrentContentChecks);
        _objectLimiter = TokenBucketRateLimiter.Create(_options.MaxObjectsPerSecond, burstSeconds: 0.1);
        _objectBatch = Math.Clamp(_options.MaxObjectsPerSecond / 20, 1, 100);

        if (resumeCheckpoint != null)
        {
            InitializeFromCheckpoint(resumeCheckpoint);
        }
    }

    /// <summary>Compares two pre-built object streams as a single range (no partitioning, no checkpoints).</summary>
    public BucketComparisonEngine(
        IS3ObjectStream streamA,
        IS3ObjectStream streamB,
        ComparisonMode mode,
        string? liveReportCsvPath = null,
        bool detectSequenceGaps = true,
        int maxSequenceGapThreshold = 25,
        string prefixA = "",
        string prefixB = "",
        ScanCheckpoint? resumeCheckpoint = null)
    {
        _providedStreamA = streamA ?? throw new ArgumentNullException(nameof(streamA));
        _providedStreamB = streamB ?? throw new ArgumentNullException(nameof(streamB));
        _prefixA = prefixA ?? string.Empty;
        _prefixB = prefixB ?? string.Empty;
        _mode = mode;
        _liveReportCsvPath = liveReportCsvPath;
        _resumeCheckpoint = resumeCheckpoint;
        _detectSequenceGaps = detectSequenceGaps;
        _maxSequenceGapThreshold = maxSequenceGapThreshold;
        _options = new ScanPerformanceOptions { MaxParallelPartitions = 1 };

        _contentChecks = new SemaphoreSlim(_options.MaxConcurrentContentChecks, _options.MaxConcurrentContentChecks);
        _objectBatch = 1;

        if (resumeCheckpoint != null)
        {
            InitializeFromCheckpoint(resumeCheckpoint);
        }
    }

    private static ScanPerformanceOptions ValidatedOptions(ScanPerformanceOptions? performance)
    {
        var options = performance?.Clone() ?? new ScanPerformanceOptions();
        var errors = options.Validate();
        if (errors.Count > 0)
        {
            throw new ArgumentException("Invalid performance settings: " + string.Join(" ", errors), nameof(performance));
        }
        return options;
    }

    /// <summary>Strips the bucket's prefix so keys from buckets with different prefixes can be compared.</summary>
    public static string ToRelativeKey(string key, string prefix)
    {
        return !string.IsNullOrEmpty(prefix) && key.StartsWith(prefix, StringComparison.Ordinal)
            ? key.Substring(prefix.Length)
            : key;
    }

    /// <summary>Builds the S3 StartAfter value for a bucket from a relative key.</summary>
    public static string? BuildStartAfter(string prefix, string? relativeKey)
    {
        return string.IsNullOrEmpty(relativeKey) ? null : (prefix ?? string.Empty) + relativeKey;
    }

    private void InitializeFromCheckpoint(ScanCheckpoint cp)
    {
        _totals.ObjectsCompared = cp.ObjectsCompared;
        _totals.MatchedCount = cp.MatchedCount;
        _totals.TotalDifferences = cp.TotalDifferences;
        _totals.MissingInBucketBCount = cp.MissingInBucketBCount;
        _totals.MissingInBucketACount = cp.MissingInBucketACount;
        _totals.SizeMismatchCount = cp.SizeMismatchCount;
        _totals.ETagMismatchCount = cp.ETagMismatchCount;
        _totals.ContentMismatchCount = cp.ContentMismatchCount;
        _totals.ContentCheckErrorCount = cp.ContentCheckErrorCount;
        _totals.SequenceGapCount = cp.SequenceGapCount;
        _differenceIdCounter = Math.Max(cp.LastDifferenceId, cp.TotalDifferences);
    }

    public void Pause()
    {
        lock (_pauseLock)
        {
            if (_pauseGate != null) return;
            _pauseGate = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
            _pauseWatch.Start();
        }
    }

    public void Resume()
    {
        TaskCompletionSource? gate;
        lock (_pauseLock)
        {
            gate = _pauseGate;
            if (gate == null) return;
            _pauseGate = null;
            _pauseWatch.Stop();
        }
        gate.TrySetResult();
    }

    public bool IsPaused => _pauseGate != null;

    private bool CanCheckpoint => _configA != null && _configB != null;

    public async Task<ComparisonStatistics> RunAsync(CancellationToken cancellationToken = default)
    {
        _stopwatch.Restart();
        lock (_pauseLock)
        {
            _pauseWatch.Reset();
            if (_pauseGate != null) _pauseWatch.Start();
        }
        _lastProgressMs = 0;
        _lastCheckpointMs = 0;
        _itemsSinceCheckpoint = 0;
        _writerError = null;
        _checkpointUnsafe = false;

        using var scanCts = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken);
        ScanDiagnosticsLogger? diagnosticsLogger = null;
        Channel<CommitBatch>? commits = null;
        Task? writerTask = null;
        bool completed = false;

        await OpenLiveCsvAsync().ConfigureAwait(false);

        try
        {
            IS3Lister? listerA = _injectedListerA;
            IS3Lister? listerB = _injectedListerB;
            if (_providedStreamA == null)
            {
                _limiterA = TokenBucketRateLimiter.Create(_options.MaxListRequestsPerSecond);
                _limiterB = TokenBucketRateLimiter.Create(_options.MaxListRequestsPerSecond);

                if (listerA == null)
                {
                    // Every partition keeps one LIST in flight per bucket, plus content downloads
                    int connections = _options.RequiredConnectionsPerServer + 4;
                    _clientA = S3ClientFactory.CreateClient(_configA!, connections);
                    _clientB = S3ClientFactory.CreateClient(_configB!, connections);
                    listerA = new AmazonS3Lister(_clientA);
                    listerB = new AmazonS3Lister(_clientB);

                    diagnosticsLogger = new ScanDiagnosticsLogger();
                    diagnosticsLogger.LogSessionConfig(_configA!, _configB!, _mode, _resumeCheckpoint == null ? null : "resume " + _resumeCheckpoint.DescribeProgress());
                }
            }

            _phase = "Planning";
            RaiseProgress();
            _ranges = await BuildPlanAsync(listerA, listerB, scanCts.Token).ConfigureAwait(false);

            if (diagnosticsLogger != null)
            {
                await diagnosticsLogger.LogInfoAsync(
                    $"Partition plan: {_ranges.Count} range(s), {_ranges.Count(r => r.Completed)} already complete; " +
                    $"MaxParallelPartitions={_options.MaxParallelPartitions}, PrefetchPages={_options.PrefetchPages}, " +
                    $"MaxListRequestsPerSecond={_options.MaxListRequestsPerSecond}, MaxObjectsPerSecond={_options.MaxObjectsPerSecond}, " +
                    $"MaxConcurrentContentChecks={_options.MaxConcurrentContentChecks}").ConfigureAwait(false);
            }

            commits = Channel.CreateBounded<CommitBatch>(new BoundedChannelOptions(1024)
            {
                SingleReader = true,
                FullMode = BoundedChannelFullMode.Wait
            });
            var commitChannel = commits;
            writerTask = Task.Run(() => RunWriterAsync(commitChannel, diagnosticsLogger, scanCts));

            _phase = "Scanning";
            var pending = new ConcurrentQueue<int>(Enumerable.Range(0, _ranges.Count).Where(i => !_ranges[i].Completed));
            var plan = _ranges.Select(r => r.Clone()).ToArray();
            int workerCount = Math.Min(_options.MaxParallelPartitions, pending.Count);
            Exception? firstError = null;

            var workers = new Task[workerCount];
            for (int w = 0; w < workerCount; w++)
            {
                workers[w] = Task.Run(async () =>
                {
                    while (pending.TryDequeue(out int index))
                    {
                        try
                        {
                            await ProcessRangeAsync(index, plan[index], listerA, listerB, commitChannel.Writer, scanCts.Token).ConfigureAwait(false);
                        }
                        catch (Exception ex) when (ex is not OperationCanceledException || !scanCts.IsCancellationRequested)
                        {
                            // A real failure in one range stops the whole scan
                            Interlocked.CompareExchange(ref firstError, ex, null);
                            try { scanCts.Cancel(); } catch (ObjectDisposedException) { }
                            return;
                        }
                    }
                });
            }

            try
            {
                await Task.WhenAll(workers).ConfigureAwait(false);
            }
            catch (OperationCanceledException) when (scanCts.IsCancellationRequested)
            {
                // Stopped: every worker has committed the keys it fully processed
            }

            commits.Writer.TryComplete();
            await writerTask.ConfigureAwait(false);
            writerTask = null;

            if (_writerError != null) ExceptionDispatchInfo.Capture(_writerError).Throw();
            if (firstError != null) ExceptionDispatchInfo.Capture(firstError).Throw();
            cancellationToken.ThrowIfCancellationRequested();

            _phase = "Completed";
            if (CanCheckpoint)
            {
                CheckpointManager.DeleteCheckpoint(CreateCheckpointSnapshot(), CheckpointBaseDirectory);
            }
            if (diagnosticsLogger != null)
            {
                await diagnosticsLogger.LogInfoAsync($"Scan completed successfully. Total objects: {_totals.ObjectsCompared:N0}, Matched: {_totals.MatchedCount:N0}, Differences: {_totals.TotalDifferences:N0}").ConfigureAwait(false);
            }
            completed = true;
        }
        catch (Exception ex)
        {
            if (diagnosticsLogger != null && ex is not OperationCanceledException)
            {
                await diagnosticsLogger.LogErrorAsync("ScanExecution", ex).ConfigureAwait(false);
            }
            throw;
        }
        finally
        {
            if (writerTask != null)
            {
                // Failed before the workers were collected: let the writer drain whatever was committed
                commits!.Writer.TryComplete();
                try
                {
                    await writerTask.ConfigureAwait(false);
                }
                catch
                {
                    // Recorded in _writerError
                }
            }

            _stopwatch.Stop();
            lock (_pauseLock)
            {
                _pauseWatch.Stop();
            }

            // Stopped or failed: persist where we got to (also flushes the live CSV to match the checkpoint)
            if (!completed)
            {
                _phase = cancellationToken.IsCancellationRequested ? "Stopped" : "Failed";
                await SaveCheckpointIfNeededAsync(diagnosticsLogger).ConfigureAwait(false);
            }

            RaiseProgress();

            if (_csvWriter != null)
            {
                try
                {
                    await _csvWriter.FlushAsync().ConfigureAwait(false);
                }
                catch
                {
                    // Disk errors were already reported through the writer
                }
                finally
                {
                    await _csvWriter.DisposeAsync().ConfigureAwait(false);
                    _csvWriter = null;
                    _csvStream = null;
                }
            }

            if (_providedStreamA != null) await _providedStreamA.DisposeAsync().ConfigureAwait(false);
            if (_providedStreamB != null) await _providedStreamB.DisposeAsync().ConfigureAwait(false);
            _clientA?.Dispose();
            _clientB?.Dispose();
            _clientA = null;
            _clientB = null;

            if (diagnosticsLogger != null)
            {
                await diagnosticsLogger.DisposeAsync().ConfigureAwait(false);
            }
        }

        return CloneStats();
    }

    private async Task OpenLiveCsvAsync()
    {
        if (string.IsNullOrWhiteSpace(_liveReportCsvPath)) return;

        var dir = Path.GetDirectoryName(_liveReportCsvPath);
        if (!string.IsNullOrEmpty(dir) && !Directory.Exists(dir))
        {
            Directory.CreateDirectory(dir);
        }

        if (_resumeCheckpoint != null)
        {
            // Resume: keep rows written up to the checkpoint and drop anything after it (those items are re-processed).
            _csvStream = new FileStream(_liveReportCsvPath, FileMode.OpenOrCreate, FileAccess.Write, FileShare.Read);
            long keep = _resumeCheckpoint.LiveReportCsvBytes;
            if (keep > 0 && _csvStream.Length > keep)
            {
                _csvStream.SetLength(keep);
            }
            _csvStream.Seek(0, SeekOrigin.End);
        }
        else
        {
            // Fresh scan: never append to a previous scan's report.
            _csvStream = new FileStream(_liveReportCsvPath, FileMode.Create, FileAccess.Write, FileShare.Read);
        }

        _csvWriter = new StreamWriter(_csvStream, CsvReport.Utf8WithBom, bufferSize: 64 * 1024);

        if (_csvStream.Length == 0)
        {
            await _csvWriter.WriteLineAsync(CsvReport.Header).ConfigureAwait(false);
            await _csvWriter.FlushAsync().ConfigureAwait(false);
        }
    }

    private async Task<List<ScanPartition>> BuildPlanAsync(IS3Lister? listerA, IS3Lister? listerB, CancellationToken cancellationToken)
    {
        if (_providedStreamA != null)
        {
            // Pre-built streams cannot be restarted at a range boundary
            return new List<ScanPartition> { new() { LastCompletedKey = NullIfEmpty(_resumeCheckpoint?.LastEvaluatedKey) } };
        }

        if (PartitionPlanOverride != null)
        {
            var custom = PartitionPlanOverride.Select(p => p.Clone()).ToList();
            if (!PartitionPlanner.TilesKeyspace(custom))
                throw new InvalidOperationException("The partition plan override does not tile the keyspace.");
            return custom;
        }

        if (_resumeCheckpoint != null)
        {
            if (_resumeCheckpoint.Partitions is { Count: > 0 } saved)
            {
                var plan = saved.Select(p => p.Clone()).ToList();
                if (!PartitionPlanner.TilesKeyspace(plan))
                    throw new InvalidOperationException("The checkpoint's partition plan is damaged (its ranges do not cover the keyspace). Discard the checkpoint and start a new scan.");
                return plan;
            }

            // Version 1 checkpoint: one key for the whole scan, so resume as a single range
            return new List<ScanPartition> { new() { LastCompletedKey = NullIfEmpty(_resumeCheckpoint.LastEvaluatedKey) } };
        }

        if (_options.MaxParallelPartitions <= 1)
        {
            return new List<ScanPartition> { new() };
        }

        return await PartitionPlanner.PlanAsync(
            new PlannerBucket(listerA!, _configA!.BucketName, _prefixA, _limiterA),
            new PlannerBucket(listerB!, _configB!.BucketName, _prefixB, _limiterB),
            _options.TargetPartitions,
            _options.MaxDiscoveryRequests,
            cancellationToken).ConfigureAwait(false);
    }

    private static string? NullIfEmpty(string? s) => string.IsNullOrEmpty(s) ? null : s;

    private (IS3ObjectStream A, IS3ObjectStream B) CreateStreams(ScanPartition range, IS3Lister? listerA, IS3Lister? listerB, CancellationToken cancellationToken)
    {
        if (_providedStreamA != null)
        {
            if (Interlocked.Exchange(ref _providedStreamsTaken, 1) != 0)
                throw new InvalidOperationException("Pre-built streams can only be compared once.");
            return (_providedStreamA, _providedStreamB!);
        }

        string? resumeAfter = range.LastCompletedKey ?? range.Lower;
        var streamA = new PrefetchingObjectStream(
            listerA!, _configA!.BucketName, _prefixA, BuildStartAfter(_prefixA, resumeAfter), range.Upper,
            _options.PrefetchPages, _limiterA, page => RaisePageLoaded(PageLoadedA, ref _lastPageEventA, page), _readA, ListPageSize, cancellationToken);
        var streamB = new PrefetchingObjectStream(
            listerB!, _configB!.BucketName, _prefixB, BuildStartAfter(_prefixB, resumeAfter), range.Upper,
            _options.PrefetchPages, _limiterB, page => RaisePageLoaded(PageLoadedB, ref _lastPageEventB, page), _readB, ListPageSize, cancellationToken);
        return (streamA, streamB);
    }

    /// <summary>With many partitions pages arrive far faster than a UI can show them; forward at most one every 250ms.</summary>
    private static void RaisePageLoaded(Action<List<S3ObjectSummary>>? handler, ref long lastTick, List<S3ObjectSummary> page)
    {
        if (handler == null) return;
        long now = Environment.TickCount64;
        long last = Interlocked.Read(ref lastTick);
        if (now - last < PageEventIntervalMs) return;
        if (Interlocked.CompareExchange(ref lastTick, now, last) != last) return;
        SafeRaise(handler, page);
    }

    /// <summary>Merge-join of one key range. Each fully processed key is committed atomically (see <see cref="CommitBatch"/>).</summary>
    private async Task ProcessRangeAsync(
        int rangeIndex,
        ScanPartition range,
        IS3Lister? listerA,
        IS3Lister? listerB,
        ChannelWriter<CommitBatch> commits,
        CancellationToken cancellationToken)
    {
        Interlocked.Increment(ref _activePartitions);
        var work = new RangeWork(rangeIndex, range.LastCompletedKey);
        IS3ObjectStream? enumA = null;
        IS3ObjectStream? enumB = null;
        bool ownsStreams = _providedStreamA == null;
        try
        {
            (enumA, enumB) = CreateStreams(range, listerA, listerB, cancellationToken);
            var gapDetector = _detectSequenceGaps ? new SequenceGapDetector(_maxSequenceGapThreshold) : null;

            bool hasA = await enumA.MoveNextAsync(cancellationToken).ConfigureAwait(false);
            bool hasB = await enumB.MoveNextAsync(cancellationToken).ConfigureAwait(false);

            while (hasA || hasB)
            {
                await CheckPauseAsync(work, commits, cancellationToken).ConfigureAwait(false);
                cancellationToken.ThrowIfCancellationRequested();

                S3ObjectSummary? itemA = hasA ? enumA.Current : null;
                S3ObjectSummary? itemB = hasB ? enumB.Current : null;
                string? relA = itemA != null ? ToRelativeKey(itemA.Key, _prefixA) : null;
                string? relB = itemB != null ? ToRelativeKey(itemB.Key, _prefixB) : null;
                if (itemA != null) work.CurrentKeyA = itemA.Key;
                if (itemB != null) work.CurrentKeyB = itemB.Key;

                // cmp < 0: only in A (so far); cmp > 0: only in B; cmp == 0: in both
                int cmp = itemB == null ? -1
                    : itemA == null ? 1
                    : Utf8KeyComparer.Instance.Compare(relA, relB);

                string processedRelativeKey;
                if (cmp == 0)
                {
                    processedRelativeKey = relA!;
                    await CompareSameKeyAsync(work, itemA!, itemB!, relA!, cancellationToken).ConfigureAwait(false);
                }
                else if (cmp < 0)
                {
                    processedRelativeKey = relA!;
                    work.KeyDiffs.Add(new ObjectDifference
                    {
                        Id = NextDifferenceId(),
                        Key = DisplayKey(itemA!, relA!),
                        Type = DifferenceType.MissingInBucketB,
                        Details = $"Object exists in Bucket A but is missing in Bucket B. Size: {itemA!.FormattedSize}, ETag: {itemA.ETag}, StorageClass: {itemA.StorageClass}, LastModified: {itemA.LastModified:yyyy-MM-dd HH:mm:ss}",
                        BucketASize = itemA.Size,
                        BucketAETag = itemA.ETag,
                        BucketALastModified = itemA.LastModified,
                        BucketAStorageClass = itemA.StorageClass
                    });
                    work.KeyCounters.MissingInBucketBCount++;
                    work.KeyCounters.TotalDifferences++;
                }
                else
                {
                    processedRelativeKey = relB!;
                    work.KeyDiffs.Add(new ObjectDifference
                    {
                        Id = NextDifferenceId(),
                        Key = DisplayKey(itemB!, relB!),
                        Type = DifferenceType.MissingInBucketA,
                        Details = $"Object exists in Bucket B but is missing in Bucket A. Size: {itemB!.FormattedSize}, ETag: {itemB.ETag}, StorageClass: {itemB.StorageClass}, LastModified: {itemB.LastModified:yyyy-MM-dd HH:mm:ss}",
                        BucketBSize = itemB.Size,
                        BucketBETag = itemB.ETag,
                        BucketBLastModified = itemB.LastModified,
                        BucketBStorageClass = itemB.StorageClass
                    });
                    work.KeyCounters.MissingInBucketACount++;
                    work.KeyCounters.TotalDifferences++;
                }

                CheckSequenceGap(work, gapDetector, processedRelativeKey);

                // The key is fully processed: from here on a checkpoint may skip it
                work.CompleteKey(processedRelativeKey);

                if (cmp <= 0) hasA = await enumA.MoveNextAsync(cancellationToken).ConfigureAwait(false);
                if (cmp >= 0) hasB = await enumB.MoveNextAsync(cancellationToken).ConfigureAwait(false);

                if (work.ShouldCommit(CommitEveryKeys, CommitIntervalMs))
                {
                    await CommitAsync(work, commits, completed: false, cancellationToken).ConfigureAwait(false);
                }

                if (_objectLimiter != null && ++work.SinceObjectTokens >= _objectBatch)
                {
                    work.SinceObjectTokens = 0;
                    await _objectLimiter.WaitAsync(_objectBatch, cancellationToken).ConfigureAwait(false);
                }
            }

            await CommitAsync(work, commits, completed: true, cancellationToken).ConfigureAwait(false);
        }
        catch
        {
            // Keys fully processed before the stop are real progress; commit them so a resume skips them.
            // Differences of a half-processed key were never moved out of KeyDiffs, so they are dropped and redone.
            await TryCommitAfterFailureAsync(work, commits).ConfigureAwait(false);
            throw;
        }
        finally
        {
            if (ownsStreams)
            {
                if (enumA != null) await enumA.DisposeAsync().ConfigureAwait(false);
                if (enumB != null) await enumB.DisposeAsync().ConfigureAwait(false);
            }
            Interlocked.Decrement(ref _activePartitions);
        }
    }

    private long NextDifferenceId() => Interlocked.Increment(ref _differenceIdCounter);

    private async Task CommitAsync(RangeWork work, ChannelWriter<CommitBatch> commits, bool completed, CancellationToken cancellationToken)
    {
        if (!completed && !work.HasUncommitted) return;

        var batch = work.BuildBatch(completed);
        await commits.WriteAsync(batch, cancellationToken).ConfigureAwait(false);
        // Only forget the batch once the writer has it; if the write was cancelled the failure path re-sends it
        work.MarkCommitted();
        _currentKeyA = work.CurrentKeyA;
        _currentKeyB = work.CurrentKeyB;
    }

    private static async Task TryCommitAfterFailureAsync(RangeWork work, ChannelWriter<CommitBatch> commits)
    {
        if (!work.HasUncommitted) return;
        try
        {
            await commits.WriteAsync(work.BuildBatch(completed: false), CancellationToken.None).ConfigureAwait(false);
            work.MarkCommitted();
        }
        catch
        {
            // The writer has failed (channel closed); those keys will simply be re-processed on resume
        }
    }

    private async Task CheckPauseAsync(RangeWork work, ChannelWriter<CommitBatch> commits, CancellationToken cancellationToken)
    {
        var gate = _pauseGate;
        if (gate == null) return;

        // Publish finished keys before idling so the UI and any checkpoint reflect them
        await CommitAsync(work, commits, completed: false, cancellationToken).ConfigureAwait(false);
        await gate.Task.WaitAsync(cancellationToken).ConfigureAwait(false);
    }

    /// <summary>
    /// Key shown to the user. With a shared prefix this is the full key; with different prefixes the
    /// keys are only comparable relative to their prefixes, so the relative key is shown.
    /// </summary>
    private string DisplayKey(S3ObjectSummary item, string relativeKey)
        => _prefixA == _prefixB ? item.Key : relativeKey;

    private static void SafeRaise<T>(Action<T>? handler, T arg)
    {
        if (handler == null) return;
        try
        {
            handler(arg);
        }
        catch
        {
            // A misbehaving subscriber (e.g. a closing window) must never abort the scan.
        }
    }

    private void CheckSequenceGap(RangeWork work, SequenceGapDetector? detector, string relativeKey)
    {
        if (detector == null) return;

        long unusedIds = 0;
        var gaps = detector.CheckKey(relativeKey, ref unusedIds);
        foreach (var gap in gaps)
        {
            gap.Id = NextDifferenceId();
            if (_prefixA == _prefixB)
            {
                gap.Key = _prefixA + gap.Key;
            }
            work.KeyDiffs.Add(gap);
            work.KeyCounters.SequenceGapCount++;
            work.KeyCounters.TotalDifferences++;
        }
    }

    /// <summary>Single consumer of worker commits: CSV, events, statistics and checkpoints all happen here, in order.</summary>
    private async Task RunWriterAsync(Channel<CommitBatch> commits, ScanDiagnosticsLogger? diagnosticsLogger, CancellationTokenSource scanCts)
    {
        try
        {
            await foreach (var batch in commits.Reader.ReadAllAsync().ConfigureAwait(false))
            {
                if (batch.Differences != null)
                {
                    foreach (var diff in batch.Differences)
                    {
                        SafeRaise(DifferenceFound, diff);
                        if (_csvWriter != null)
                        {
                            await _csvWriter.WriteLineAsync(CsvReport.BuildLine(diff)).ConfigureAwait(false);
                        }
                    }
                }

                _totals.Add(batch.Delta);
                var range = _ranges[batch.RangeIndex];
                if (batch.LastCompletedKey != null) range.LastCompletedKey = batch.LastCompletedKey;
                if (batch.Completed) range.Completed = true;
                _itemsSinceCheckpoint += batch.Delta.ObjectsCompared;

                await TickAsync(diagnosticsLogger).ConfigureAwait(false);
            }
        }
        catch (Exception ex)
        {
            // e.g. the disk is full: the CSV no longer matches the ranges' progress, so stop and keep the last good checkpoint
            _writerError = ex;
            _checkpointUnsafe = true;
            commits.Writer.TryComplete(ex);
            try { scanCts.Cancel(); } catch (ObjectDisposedException) { }
        }
    }

    /// <summary>Periodic work on the writer: UI progress (~200ms) and checkpoints (~5s).</summary>
    private async Task TickAsync(ScanDiagnosticsLogger? diagnosticsLogger)
    {
        long elapsedMs = ActiveElapsedMs();

        if (elapsedMs - _lastCheckpointMs > 5000 || (_itemsSinceCheckpoint >= 10000 && elapsedMs - _lastCheckpointMs > 1000))
        {
            _lastCheckpointMs = elapsedMs;
            _itemsSinceCheckpoint = 0;
            await SaveCheckpointIfNeededAsync(diagnosticsLogger).ConfigureAwait(false);
        }

        if (elapsedMs - _lastProgressMs > 200)
        {
            _lastProgressMs = elapsedMs;
            var stats = CloneStats();
            if (diagnosticsLogger != null)
            {
                await diagnosticsLogger.LogHeartbeatAsync(
                    stats.ObjectsCompared,
                    stats.ItemsPerSecond,
                    stats.CurrentKeyA,
                    stats.CurrentKeyB,
                    stats.TotalDifferences,
                    stats.ElapsedTime).ConfigureAwait(false);
            }
            SafeRaise(ProgressUpdated, stats);
        }
    }

    private void RaiseProgress() => SafeRaise(ProgressUpdated, CloneStats());

    private long ActiveElapsedMs() => (long)(_stopwatch.Elapsed - _pauseWatch.Elapsed).TotalMilliseconds;

    private async Task SaveCheckpointIfNeededAsync(ScanDiagnosticsLogger? diagnosticsLogger)
    {
        if (!CanCheckpoint || _checkpointUnsafe || !_ranges.Any(r => r.Completed || r.LastCompletedKey != null))
            return;

        try
        {
            // The CSV must be on disk before the checkpoint records its length
            if (_csvWriter != null)
            {
                await _csvWriter.FlushAsync().ConfigureAwait(false);
            }

            var cp = CreateCheckpointSnapshot();
            string path = await CheckpointManager.SaveCheckpointAsync(cp, CheckpointBaseDirectory).ConfigureAwait(false);
            _lastCheckpointDisplay = cp.DescribeProgress();
            _lastCheckpointTime = DateTime.Now;

            if (diagnosticsLogger != null)
            {
                await diagnosticsLogger.LogCheckpointAsync(path, _lastCheckpointDisplay, cp.ObjectsCompared).ConfigureAwait(false);
            }
        }
        catch
        {
            // Never break comparison on checkpoint write failure
        }
    }

    private ScanCheckpoint CreateCheckpointSnapshot()
    {
        var partitions = _ranges.Select(r => r.Clone()).ToList();
        bool singleUnbounded = partitions.Count == 1 && partitions[0].Lower == null && partitions[0].Upper == null;

        return new ScanCheckpoint
        {
            BucketNameA = _configA?.BucketName ?? "",
            EndpointA = _configA?.EndpointUrl ?? "",
            RegionA = _configA?.Region ?? "",
            PrefixA = _prefixA,
            BucketNameB = _configB?.BucketName ?? "",
            EndpointB = _configB?.EndpointUrl ?? "",
            RegionB = _configB?.Region ?? "",
            PrefixB = _prefixB,
            Mode = _mode,
            LastEvaluatedKey = singleUnbounded ? partitions[0].LastCompletedKey ?? "" : "",
            Partitions = partitions,
            LastDifferenceId = Interlocked.Read(ref _differenceIdCounter),
            LiveReportCsvPath = _liveReportCsvPath,
            LiveReportCsvBytes = _csvStream?.Length ?? 0,
            ObjectsCompared = _totals.ObjectsCompared,
            MatchedCount = _totals.MatchedCount,
            TotalDifferences = _totals.TotalDifferences,
            MissingInBucketBCount = _totals.MissingInBucketBCount,
            MissingInBucketACount = _totals.MissingInBucketACount,
            SizeMismatchCount = _totals.SizeMismatchCount,
            ETagMismatchCount = _totals.ETagMismatchCount,
            ContentMismatchCount = _totals.ContentMismatchCount,
            ContentCheckErrorCount = _totals.ContentCheckErrorCount,
            SequenceGapCount = _totals.SequenceGapCount,
            ElapsedSeconds = TotalElapsedSeconds(),
            DetectSequenceGaps = _detectSequenceGaps,
            MaxSequenceGapThreshold = _maxSequenceGapThreshold
        };
    }

    /// <summary>Elapsed scan time excluding time spent paused (plus time accumulated before a resume).</summary>
    private double TotalElapsedSeconds()
    {
        double totalSeconds = (_stopwatch.Elapsed - _pauseWatch.Elapsed).TotalSeconds;
        if (_resumeCheckpoint != null)
        {
            totalSeconds += _resumeCheckpoint.ElapsedSeconds;
        }
        return Math.Max(0, totalSeconds);
    }

    private ObjectDifference BuildPairDifference(S3ObjectSummary itemA, S3ObjectSummary itemB, string relativeKey, DifferenceType type, string details)
    {
        return new ObjectDifference
        {
            Id = NextDifferenceId(),
            Key = DisplayKey(itemA, relativeKey),
            Type = type,
            Details = details,
            BucketASize = itemA.Size,
            BucketBSize = itemB.Size,
            BucketAETag = itemA.ETag,
            BucketBETag = itemB.ETag,
            BucketALastModified = itemA.LastModified,
            BucketBLastModified = itemB.LastModified,
            BucketAStorageClass = itemA.StorageClass,
            BucketBStorageClass = itemB.StorageClass
        };
    }

    private async Task CompareSameKeyAsync(
        RangeWork work,
        S3ObjectSummary itemA,
        S3ObjectSummary itemB,
        string relativeKey,
        CancellationToken cancellationToken)
    {
        // 1. Check size
        if (itemA.Size != itemB.Size)
        {
            work.KeyDiffs.Add(BuildPairDifference(itemA, itemB, relativeKey, DifferenceType.SizeMismatch,
                $"Size mismatch: A has {itemA.FormattedSize} ({itemA.Size} B), B has {itemB.FormattedSize} ({itemB.Size} B)"));
            work.KeyCounters.SizeMismatchCount++;
            work.KeyCounters.TotalDifferences++;
            return;
        }

        // 2. Check ETags
        if (string.Equals(itemA.ETag, itemB.ETag, StringComparison.OrdinalIgnoreCase))
        {
            work.KeyCounters.MatchedCount++;
            return;
        }

        // ETags differ
        bool canDownload = ContentComparer != null || (_clientA != null && _clientB != null);

        if (_mode == ComparisonMode.ListingOnly || !canDownload)
        {
            work.KeyDiffs.Add(BuildPairDifference(itemA, itemB, relativeKey, DifferenceType.ETagMismatch,
                $"ETags differ (A: {itemA.ETag}, B: {itemB.ETag}). Size is identical ({itemA.FormattedSize}). Note: multipart uploads have different ETags even if content matches."));
            work.KeyCounters.ETagMismatchCount++;
            work.KeyCounters.TotalDifferences++;
            return;
        }

        // Full Data mode
        // Always download: single-part ETags are not MD5 content hashes for SSE-KMS/SSE-C objects,
        // so differing ETags alone do not prove the content differs.
        bool contentMatches;
        await _contentChecks.WaitAsync(cancellationToken).ConfigureAwait(false);
        try
        {
            // Downloads count against each bucket's request budget
            await _limiterA.WaitIfLimitedAsync(1, cancellationToken).ConfigureAwait(false);
            await _limiterB.WaitIfLimitedAsync(1, cancellationToken).ConfigureAwait(false);

            contentMatches = ContentComparer != null
                ? await ContentComparer(itemA, itemB, cancellationToken).ConfigureAwait(false)
                : await StreamCompareObjectsAsync(_clientA!, _clientB!, itemA.Key, itemB.Key, cancellationToken).ConfigureAwait(false);
        }
        catch (AmazonServiceException ex)
        {
            // Glacier InvalidObjectState, 404 races, AccessDenied, ...: record the object and keep scanning.
            work.KeyDiffs.Add(BuildPairDifference(itemA, itemB, relativeKey, DifferenceType.ContentCheckError,
                $"Content could not be verified: {(string.IsNullOrEmpty(ex.ErrorCode) ? ex.GetType().Name : ex.ErrorCode)}: {ex.Message}"));
            work.KeyCounters.ContentCheckErrorCount++;
            work.KeyCounters.TotalDifferences++;
            return;
        }
        finally
        {
            _contentChecks.Release();
        }

        if (contentMatches)
        {
            work.KeyCounters.MatchedCount++;
        }
        else
        {
            work.KeyDiffs.Add(BuildPairDifference(itemA, itemB, relativeKey, DifferenceType.ContentMismatch,
                $"Content byte mismatch! Size is identical ({itemA.FormattedSize}) but data stream differs."));
            work.KeyCounters.ContentMismatchCount++;
            work.KeyCounters.TotalDifferences++;
        }
    }

    private async Task<bool> StreamCompareObjectsAsync(
        IAmazonS3 clientA,
        IAmazonS3 clientB,
        string keyA,
        string keyB,
        CancellationToken cancellationToken)
    {
        if (_configA == null || _configB == null) return false;

        var getA = new GetObjectRequest { BucketName = _configA.BucketName, Key = keyA };
        var getB = new GetObjectRequest { BucketName = _configB.BucketName, Key = keyB };

        // Open both streams concurrently
        var taskA = clientA.GetObjectAsync(getA, cancellationToken);
        var taskB = clientB.GetObjectAsync(getB, cancellationToken);
        try
        {
            await Task.WhenAll(taskA, taskB).ConfigureAwait(false);
        }
        catch
        {
            // WhenAll waits for both; release whichever request did succeed
            if (taskA.Status == TaskStatus.RanToCompletion) taskA.Result.Dispose();
            if (taskB.Status == TaskStatus.RanToCompletion) taskB.Result.Dispose();
            throw;
        }

        using var respA = taskA.Result;
        using var respB = taskB.Result;

        await using var streamA = respA.ResponseStream;
        await using var streamB = respB.ResponseStream;

        byte[] bufA = new byte[65536];
        byte[] bufB = new byte[65536];

        while (true)
        {
            int bytesReadA = await ReadFullBufferAsync(streamA, bufA, cancellationToken).ConfigureAwait(false);
            int bytesReadB = await ReadFullBufferAsync(streamB, bufB, cancellationToken).ConfigureAwait(false);

            if (bytesReadA != bytesReadB)
                return false;

            if (bytesReadA == 0)
                return true;

            if (!bufA.AsSpan(0, bytesReadA).SequenceEqual(bufB.AsSpan(0, bytesReadB)))
                return false;
        }
    }

    private static async Task<int> ReadFullBufferAsync(Stream stream, byte[] buffer, CancellationToken cancellationToken)
    {
        int totalRead = 0;
        while (totalRead < buffer.Length)
        {
            int read = await stream.ReadAsync(buffer.AsMemory(totalRead, buffer.Length - totalRead), cancellationToken).ConfigureAwait(false);
            if (read == 0)
                break;
            totalRead += read;
        }
        return totalRead;
    }

    private ComparisonStatistics CloneStats()
    {
        double elapsedSeconds = TotalElapsedSeconds();
        var stats = new ComparisonStatistics
        {
            ElapsedTime = TimeSpan.FromSeconds(elapsedSeconds),
            ItemsPerSecond = elapsedSeconds > 0 ? _totals.ObjectsCompared / elapsedSeconds : 0,
            CurrentKeyA = _currentKeyA,
            CurrentKeyB = _currentKeyB,
            // Cheap, allocation-free memory probes (Process.GetCurrentProcess() costs ~20ms and leaks a handle)
            WorkingSetMB = Environment.WorkingSet / (1024.0 * 1024.0),
            GCTotalMemoryMB = GC.GetTotalMemory(false) / (1024.0 * 1024.0),
            LastCheckpointKey = _lastCheckpointDisplay,
            LastCheckpointTime = _lastCheckpointTime,
            Phase = _phase,
            PartitionsTotal = _ranges.Count,
            PartitionsCompleted = _ranges.Count(r => r.Completed),
            ActivePartitions = Volatile.Read(ref _activePartitions)
        };
        _totals.CopyTo(stats);

        if (_providedStreamA != null && _providedStreamB != null)
        {
            stats.BucketAObjectsRead = _providedStreamA.TotalObjectsRead;
            stats.BucketABytesRead = _providedStreamA.TotalBytesRead;
            stats.BucketBObjectsRead = _providedStreamB.TotalObjectsRead;
            stats.BucketBBytesRead = _providedStreamB.TotalBytesRead;
        }
        else
        {
            stats.BucketAObjectsRead = _readA.Objects;
            stats.BucketABytesRead = _readA.Bytes;
            stats.BucketBObjectsRead = _readB.Objects;
            stats.BucketBBytesRead = _readB.Bytes;
        }
        return stats;
    }

    /// <summary>Difference counters; a worker's per-key and per-batch tallies, and the writer's scan totals.</summary>
    private sealed class ScanCounters
    {
        public long ObjectsCompared;
        public long MatchedCount;
        public long TotalDifferences;
        public long MissingInBucketBCount;
        public long MissingInBucketACount;
        public long SizeMismatchCount;
        public long ETagMismatchCount;
        public long ContentMismatchCount;
        public long ContentCheckErrorCount;
        public long SequenceGapCount;

        public void Add(ScanCounters other)
        {
            ObjectsCompared += other.ObjectsCompared;
            MatchedCount += other.MatchedCount;
            TotalDifferences += other.TotalDifferences;
            MissingInBucketBCount += other.MissingInBucketBCount;
            MissingInBucketACount += other.MissingInBucketACount;
            SizeMismatchCount += other.SizeMismatchCount;
            ETagMismatchCount += other.ETagMismatchCount;
            ContentMismatchCount += other.ContentMismatchCount;
            ContentCheckErrorCount += other.ContentCheckErrorCount;
            SequenceGapCount += other.SequenceGapCount;
        }

        public void CopyTo(ComparisonStatistics stats)
        {
            stats.ObjectsCompared = ObjectsCompared;
            stats.MatchedCount = MatchedCount;
            stats.TotalDifferences = TotalDifferences;
            stats.MissingInBucketBCount = MissingInBucketBCount;
            stats.MissingInBucketACount = MissingInBucketACount;
            stats.SizeMismatchCount = SizeMismatchCount;
            stats.ETagMismatchCount = ETagMismatchCount;
            stats.ContentMismatchCount = ContentMismatchCount;
            stats.ContentCheckErrorCount = ContentCheckErrorCount;
            stats.SequenceGapCount = SequenceGapCount;
        }

        public void Reset() => CopyFrom(new ScanCounters());

        private void CopyFrom(ScanCounters other)
        {
            ObjectsCompared = other.ObjectsCompared;
            MatchedCount = other.MatchedCount;
            TotalDifferences = other.TotalDifferences;
            MissingInBucketBCount = other.MissingInBucketBCount;
            MissingInBucketACount = other.MissingInBucketACount;
            SizeMismatchCount = other.SizeMismatchCount;
            ETagMismatchCount = other.ETagMismatchCount;
            ContentMismatchCount = other.ContentMismatchCount;
            ContentCheckErrorCount = other.ContentCheckErrorCount;
            SequenceGapCount = other.SequenceGapCount;
        }

        public ScanCounters Clone()
        {
            var copy = new ScanCounters();
            copy.CopyFrom(this);
            return copy;
        }
    }

    /// <summary>
    /// What one worker hands to the writer: the differences of a run of fully processed keys in one range, the counters
    /// for exactly those keys, and the range's new last completed key. The writer applies a batch as a unit, so a
    /// checkpoint never separates a key from its differences.
    /// </summary>
    private sealed class CommitBatch
    {
        public int RangeIndex { get; init; }
        public List<ObjectDifference>? Differences { get; init; }
        public ScanCounters Delta { get; init; } = new();
        public string? LastCompletedKey { get; init; }
        public bool Completed { get; init; }
    }

    /// <summary>A worker's private state for one range.</summary>
    private sealed class RangeWork
    {
        private readonly int _rangeIndex;
        private readonly ScanCounters _batchCounters = new();
        private List<ObjectDifference> _batchDiffs = new();
        private int _keysSinceCommit;
        private long _lastCommitTick = Environment.TickCount64;

        /// <summary>Effects of the key being processed; moved into the batch only once the key is complete.</summary>
        public ScanCounters KeyCounters { get; } = new();
        public List<ObjectDifference> KeyDiffs { get; } = new();

        public string? LastCompletedKey { get; private set; }
        public string CurrentKeyA { get; set; } = "-";
        public string CurrentKeyB { get; set; } = "-";
        public int SinceObjectTokens { get; set; }

        public RangeWork(int rangeIndex, string? lastCompletedKey)
        {
            _rangeIndex = rangeIndex;
            LastCompletedKey = lastCompletedKey;
        }

        public bool HasUncommitted => _keysSinceCommit > 0 || _batchDiffs.Count > 0;

        public void CompleteKey(string relativeKey)
        {
            KeyCounters.ObjectsCompared++;
            _batchCounters.Add(KeyCounters);
            KeyCounters.Reset();
            if (KeyDiffs.Count > 0)
            {
                _batchDiffs.AddRange(KeyDiffs);
                KeyDiffs.Clear();
            }
            LastCompletedKey = relativeKey;
            _keysSinceCommit++;
        }

        public bool ShouldCommit(int everyKeys, int intervalMs)
            => _keysSinceCommit >= everyKeys
               || _batchDiffs.Count >= everyKeys
               || (_keysSinceCommit > 0 && Environment.TickCount64 - _lastCommitTick >= intervalMs);

        public CommitBatch BuildBatch(bool completed) => new()
        {
            RangeIndex = _rangeIndex,
            Differences = _batchDiffs.Count > 0 ? _batchDiffs : null,
            Delta = _batchCounters.Clone(),
            LastCompletedKey = LastCompletedKey,
            Completed = completed
        };

        public void MarkCommitted()
        {
            _batchCounters.Reset();
            if (_batchDiffs.Count > 0) _batchDiffs = new List<ObjectDifference>();
            _keysSinceCommit = 0;
            _lastCommitTick = Environment.TickCount64;
        }
    }
}
