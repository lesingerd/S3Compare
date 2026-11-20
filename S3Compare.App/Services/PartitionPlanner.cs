using S3Compare.App.Models;

namespace S3Compare.App.Services;

/// <summary>A bucket as seen by the planner: its lister, name, prefix and request throttle.</summary>
public sealed record PlannerBucket(IS3Lister Lister, string BucketName, string Prefix, TokenBucketRateLimiter? RateLimiter = null);

/// <summary>
/// Splits the prefix-relative keyspace shared by two buckets into contiguous ranges that can be compared independently.
/// S3 lists keys in UTF-8 byte order, so any set of boundary keys yields ranges whose merge-joins together see every key
/// exactly once. Planning quality only affects how evenly work is spread, never correctness.
/// </summary>
/// <remarks>
/// 1. Discovery: Delimiter="/" listings under each bucket's prefix, descending up to <see cref="MaxDiscoveryDepth"/>
///    levels, collecting the relative CommonPrefixes of both buckets until there are enough candidates.
/// 2. Flat-namespace fallback: probes with MaxKeys=1 and StartAfter = prefix + each printable ASCII character (then
///    two-character probes under populated first characters), using the returned keys as candidates. A probe whose
///    answer is already known (it falls between a previous probe and that probe's result) is skipped.
/// 3. Boundaries are chosen evenly from the sorted, de-duplicated candidates.
/// Every LIST call counts against a shared budget (MaxDiscoveryRequests).
/// </remarks>
public static class PartitionPlanner
{
    public const int MaxDiscoveryDepth = 3;
    public const string Delimiter = "/";

    /// <summary>Pages read from one folder's delimiter listing; folders holding millions of objects are not paged through.</summary>
    private const int MaxPagesPerFolder = 4;

    private const char FirstProbeChar = ' ';
    private const char LastProbeChar = '~';

    public static async Task<List<ScanPartition>> PlanAsync(
        PlannerBucket bucketA,
        PlannerBucket bucketB,
        int targetPartitions,
        int maxDiscoveryRequests,
        CancellationToken cancellationToken = default)
    {
        if (targetPartitions <= 1 || maxDiscoveryRequests <= 0)
        {
            return BuildRanges(Array.Empty<string>(), 1);
        }

        var budget = new RequestBudget(maxDiscoveryRequests);

        // 1. Hierarchical discovery, one level at a time on both buckets concurrently, until the union has enough candidates
        var candidates = new HashSet<string>(StringComparer.Ordinal);
        var levelA = new List<string> { string.Empty };
        var levelB = new List<string> { string.Empty };
        for (int depth = 1; depth <= MaxDiscoveryDepth && (levelA.Count > 0 || levelB.Count > 0); depth++)
        {
            var nextA = ListFoldersAsync(bucketA, levelA, budget, cancellationToken);
            var nextB = ListFoldersAsync(bucketB, levelB, budget, cancellationToken);
            await Task.WhenAll(nextA, nextB).ConfigureAwait(false);

            candidates.UnionWith(nextA.Result);
            candidates.UnionWith(nextB.Result);
            if (candidates.Count >= targetPartitions || budget.Remaining <= 0) break;

            levelA = nextA.Result;
            levelB = nextB.Result;
        }

        // 2. Flat namespace (or too few folders): probe the keyspace by leading characters
        if (candidates.Count < targetPartitions && budget.Remaining > 0)
        {
            var probeA = ProbeKeyspaceAsync(bucketA, targetPartitions, budget, cancellationToken);
            var probeB = ProbeKeyspaceAsync(bucketB, targetPartitions, budget, cancellationToken);
            await Task.WhenAll(probeA, probeB).ConfigureAwait(false);
            candidates.UnionWith(probeA.Result);
            candidates.UnionWith(probeB.Result);
        }

        // 3. Evenly spaced boundaries
        return BuildRanges(candidates, targetPartitions);
    }

    /// <summary>
    /// Turns candidate boundary keys into at most <paramref name="targetPartitions"/> ranges that tile the keyspace:
    /// (null, b0], (b0, b1], ..., (bn, null].
    /// </summary>
    public static List<ScanPartition> BuildRanges(IEnumerable<string> candidates, int targetPartitions)
    {
        var sorted = candidates
            .Where(c => !string.IsNullOrEmpty(c))
            .Distinct(StringComparer.Ordinal)
            .OrderBy(c => c, Utf8KeyComparer.Instance)
            .ToList();

        int rangeCount = Math.Max(1, Math.Min(targetPartitions, sorted.Count + 1));
        var boundaries = new List<string>(rangeCount);
        int lastIndex = -1;
        for (int i = 1; i < rangeCount; i++)
        {
            if (lastIndex + 1 >= sorted.Count) break;
            int index = (int)((long)i * sorted.Count / rangeCount);
            index = Math.Clamp(index, lastIndex + 1, sorted.Count - 1);
            boundaries.Add(sorted[index]);
            lastIndex = index;
        }

        var ranges = new List<ScanPartition>(boundaries.Count + 1);
        string? lower = null;
        foreach (var boundary in boundaries)
        {
            ranges.Add(new ScanPartition { Lower = lower, Upper = boundary });
            lower = boundary;
        }
        ranges.Add(new ScanPartition { Lower = lower, Upper = null });
        return ranges;
    }

    /// <summary>True when the relative key falls in (Lower, Upper].</summary>
    public static bool Contains(ScanPartition range, string relativeKey)
    {
        return (range.Lower == null || Utf8KeyComparer.Instance.Compare(relativeKey, range.Lower) > 0)
            && (range.Upper == null || Utf8KeyComparer.Instance.Compare(relativeKey, range.Upper) <= 0);
    }

    /// <summary>True when the ranges are ordered, contiguous and unbounded at both ends, i.e. tile the keyspace exactly.</summary>
    public static bool TilesKeyspace(IReadOnlyList<ScanPartition> ranges)
    {
        if (ranges.Count == 0 || ranges[0].Lower != null || ranges[^1].Upper != null) return false;
        for (int i = 0; i < ranges.Count - 1; i++)
        {
            var upper = ranges[i].Upper;
            if (upper == null || !string.Equals(upper, ranges[i + 1].Lower, StringComparison.Ordinal)) return false;
            if (ranges[i].Lower != null && Utf8KeyComparer.Instance.Compare(ranges[i].Lower, upper) >= 0) return false;
        }
        return true;
    }

    /// <summary>Delimiter listings of the given folders (relative); returns their relative sub-folders.</summary>
    private static async Task<List<string>> ListFoldersAsync(PlannerBucket bucket, List<string> folders, RequestBudget budget, CancellationToken cancellationToken)
    {
        var found = new List<string>();
        foreach (var folder in folders)
        {
            string? continuationToken = null;
            for (int pageNo = 0; pageNo < MaxPagesPerFolder; pageNo++)
            {
                if (!budget.TryTake()) return found;

                await bucket.RateLimiter.WaitIfLimitedAsync(1, cancellationToken).ConfigureAwait(false);
                var page = await bucket.Lister.ListPageAsync(
                    bucket.BucketName, bucket.Prefix + folder, null, continuationToken, Delimiter, 1000, cancellationToken).ConfigureAwait(false);

                foreach (var commonPrefix in page.CommonPrefixes)
                {
                    found.Add(BucketComparisonEngine.ToRelativeKey(commonPrefix, bucket.Prefix));
                }

                if (!page.IsTruncated || string.IsNullOrEmpty(page.NextContinuationToken)) break;
                continuationToken = page.NextContinuationToken;
            }
        }
        return found;
    }

    private static async Task<List<string>> ProbeKeyspaceAsync(PlannerBucket bucket, int target, RequestBudget budget, CancellationToken cancellationToken)
    {
        var found = new List<string>();

        // Level 1: one probe per printable ASCII character
        var firstLevel = await ProbeAsync(bucket, ProbeChars().Select(c => c.ToString()), budget, cancellationToken).ConfigureAwait(false);
        found.AddRange(firstLevel);
        if (found.Distinct(StringComparer.Ordinal).Count() >= target) return found;

        // Level 2: two-character probes under every first character that holds keys
        var populated = firstLevel
            .Where(k => k.Length > 0 && k[0] >= FirstProbeChar && k[0] <= LastProbeChar)
            .Select(k => k[0])
            .Distinct()
            .OrderBy(c => c)
            .ToList();

        var secondLevelPoints = populated.SelectMany(c => ProbeChars().Select(d => string.Concat(c, d)));
        found.AddRange(await ProbeAsync(bucket, secondLevelPoints, budget, cancellationToken).ConfigureAwait(false));
        return found;
    }

    /// <summary>
    /// Issues MaxKeys=1 probes at ascending relative positions, returning the first key after each. Positions that fall
    /// before the previous probe's answer would return the same key, so they are skipped.
    /// </summary>
    private static async Task<List<string>> ProbeAsync(PlannerBucket bucket, IEnumerable<string> ascendingPoints, RequestBudget budget, CancellationToken cancellationToken)
    {
        var found = new List<string>();
        string? lastResult = null;
        foreach (var point in ascendingPoints)
        {
            if (lastResult != null && Utf8KeyComparer.Instance.Compare(point, lastResult) < 0) continue;
            if (!budget.TryTake()) break;

            await bucket.RateLimiter.WaitIfLimitedAsync(1, cancellationToken).ConfigureAwait(false);
            var page = await bucket.Lister.ListPageAsync(
                bucket.BucketName, bucket.Prefix, bucket.Prefix + point, null, null, 1, cancellationToken).ConfigureAwait(false);

            if (page.Objects.Count == 0)
            {
                // Nothing after this point: later (larger) points would find nothing either
                break;
            }

            lastResult = BucketComparisonEngine.ToRelativeKey(page.Objects[0].Key, bucket.Prefix);
            found.Add(lastResult);
        }
        return found;
    }

    private static IEnumerable<char> ProbeChars()
    {
        for (char c = FirstProbeChar; c <= LastProbeChar; c++) yield return c;
    }

    private sealed class RequestBudget
    {
        private int _remaining;

        public RequestBudget(int total) => _remaining = total;

        public int Remaining => Volatile.Read(ref _remaining);

        public bool TryTake() => Interlocked.Decrement(ref _remaining) >= 0;
    }
}
