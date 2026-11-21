using System.Collections.Concurrent;
using S3Compare.App.Models;
using S3Compare.App.Services;

namespace S3Compare.Tests;

public sealed record FakeListCall(int Number, string Bucket, string? Prefix, string? StartAfter, string? ContinuationToken, string? Delimiter, int MaxKeys);

/// <summary>
/// In-memory ListObjectsV2 over any number of buckets, honouring Prefix, StartAfter, ContinuationToken, Delimiter and
/// MaxKeys with S3's UTF-8 key order. Optional per-call latency, fault injection and call hooks.
/// </summary>
public sealed class FakeS3Lister : IS3Lister
{
    private const string TokenPrefix = "fake-token:";

    private readonly ConcurrentDictionary<string, List<S3ObjectSummary>> _buckets = new();
    private int _callCount;
    private int _inFlight;
    private int _maxInFlight;

    /// <summary>Delay applied to every call (cancellable).</summary>
    public TimeSpan Latency { get; set; }

    /// <summary>Return an exception to make that call fail.</summary>
    public Func<FakeListCall, Exception?>? Fault { get; set; }

    /// <summary>Invoked at the start of every call (e.g. to cancel a scan at a given point).</summary>
    public Action<FakeListCall>? OnCall { get; set; }

    public ConcurrentQueue<FakeListCall> Calls { get; } = new();
    public int CallCount => Volatile.Read(ref _callCount);
    public int MaxInFlight => Volatile.Read(ref _maxInFlight);

    public FakeS3Lister SetBucket(string bucket, IEnumerable<S3ObjectSummary> objects)
    {
        _buckets[bucket] = objects
            .GroupBy(o => o.Key, StringComparer.Ordinal).Select(g => g.Last())
            .OrderBy(o => o.Key, Utf8KeyComparer.Instance)
            .ToList();
        return this;
    }

    public int CallsFor(string bucket) => Calls.Count(c => c.Bucket == bucket);

    public async Task<S3ListPage> ListPageAsync(
        string bucketName,
        string? prefix,
        string? startAfter,
        string? continuationToken,
        string? delimiter,
        int maxKeys,
        CancellationToken cancellationToken = default)
    {
        var call = new FakeListCall(Interlocked.Increment(ref _callCount), bucketName, prefix, startAfter, continuationToken, delimiter, maxKeys);
        Calls.Enqueue(call);
        int inFlight = Interlocked.Increment(ref _inFlight);
        int seen;
        while (inFlight > (seen = Volatile.Read(ref _maxInFlight)) && Interlocked.CompareExchange(ref _maxInFlight, inFlight, seen) != seen)
        {
        }

        try
        {
            OnCall?.Invoke(call);
            if (Latency > TimeSpan.Zero)
            {
                await Task.Delay(Latency, cancellationToken).ConfigureAwait(false);
            }
            cancellationToken.ThrowIfCancellationRequested();

            var fault = Fault?.Invoke(call);
            if (fault != null) throw fault;

            return BuildPage(bucketName, prefix ?? "", startAfter, continuationToken, delimiter, Math.Clamp(maxKeys, 1, 1000));
        }
        finally
        {
            Interlocked.Decrement(ref _inFlight);
        }
    }

    private S3ListPage BuildPage(string bucketName, string prefix, string? startAfter, string? continuationToken, string? delimiter, int maxKeys)
    {
        var keys = _buckets.TryGetValue(bucketName, out var list) ? list : new List<S3ObjectSummary>();

        // As in S3, a continuation token takes precedence over StartAfter
        string? marker = continuationToken != null ? DecodeToken(continuationToken) : startAfter;
        string? rolledUpMarker = marker != null && !string.IsNullOrEmpty(delimiter) && marker.EndsWith(delimiter, StringComparison.Ordinal) ? marker : null;

        int index = Math.Max(FirstIndexAfter(keys, marker), FirstIndexAtOrAfter(keys, prefix));
        var objects = new List<S3ObjectSummary>();
        var commonPrefixes = new List<string>();
        string? lastEntry = null;
        bool truncated = false;

        for (; index < keys.Count; index++)
        {
            var obj = keys[index];
            if (!obj.Key.StartsWith(prefix, StringComparison.Ordinal))
            {
                if (Utf8KeyComparer.Instance.Compare(obj.Key, prefix) > 0) break;
                continue;
            }
            if (rolledUpMarker != null && obj.Key.StartsWith(rolledUpMarker, StringComparison.Ordinal)) continue;

            string entry = obj.Key;
            bool isCommonPrefix = false;
            if (!string.IsNullOrEmpty(delimiter))
            {
                int at = obj.Key.IndexOf(delimiter, prefix.Length, StringComparison.Ordinal);
                if (at >= 0)
                {
                    entry = obj.Key.Substring(0, at + delimiter.Length);
                    isCommonPrefix = true;
                    if (entry == lastEntry) continue;
                }
            }

            if (objects.Count + commonPrefixes.Count == maxKeys)
            {
                truncated = true;
                break;
            }

            if (isCommonPrefix) commonPrefixes.Add(entry);
            else objects.Add(Copy(obj));
            lastEntry = entry;
        }

        return new S3ListPage
        {
            Objects = objects,
            CommonPrefixes = commonPrefixes,
            IsTruncated = truncated,
            NextContinuationToken = truncated ? TokenPrefix + lastEntry : null
        };
    }

    private static int FirstIndexAfter(List<S3ObjectSummary> keys, string? marker)
    {
        if (marker == null) return 0;
        int lo = 0, hi = keys.Count;
        while (lo < hi)
        {
            int mid = (lo + hi) / 2;
            if (Utf8KeyComparer.Instance.Compare(keys[mid].Key, marker) > 0) hi = mid;
            else lo = mid + 1;
        }
        return lo;
    }

    private static int FirstIndexAtOrAfter(List<S3ObjectSummary> keys, string value)
    {
        int lo = 0, hi = keys.Count;
        while (lo < hi)
        {
            int mid = (lo + hi) / 2;
            if (Utf8KeyComparer.Instance.Compare(keys[mid].Key, value) >= 0) hi = mid;
            else lo = mid + 1;
        }
        return lo;
    }

    private static string DecodeToken(string token)
    {
        if (!token.StartsWith(TokenPrefix, StringComparison.Ordinal))
            throw new ArgumentException("Invalid continuation token", nameof(token));
        return token.Substring(TokenPrefix.Length);
    }

    private static S3ObjectSummary Copy(S3ObjectSummary o) => new()
    {
        Key = o.Key,
        Size = o.Size,
        ETag = o.ETag,
        LastModified = o.LastModified,
        StorageClass = o.StorageClass
    };
}
