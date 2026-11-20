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
/// 2. Flat-namespace fallback: rounds of MaxKeys=1 probes. Round one probes StartAfter = prefix + each printable ASCII
///    character; every later round probes one character deeper under each populated prefix found so far. Keys with a
///    long shared prefix and no "/" (file_00001.dat, IMG_0001.jpg) therefore descend through the shared part until they
///    branch, without a depth cap. Descending through a prefix with a single child costs only a few probes, because a
///    probe whose answer is already known (it falls between a previous probe and that probe's result) is not sent.
///    Probes run coarse to fine so a small budget still spans the whole range, and each bucket gets half of the
///    remaining budget.
/// 3. Boundaries are chosen evenly from the sorted, de-duplicated candidates.
/// Every LIST call counts against a shared budget (MaxDiscoveryRequests) and waits while the scan is paused.
/// </remarks>
public static class PartitionPlanner
{
    public const int MaxDiscoveryDepth = 3;
    public const string Delimiter = "/";

    /// <summary>Pages read from one folder's delimiter listing; folders holding millions of objects are not paged through.</summary>
    private const int MaxPagesPerFolder = 4;

    /// <summary>Safety stop for probe descent (S3 keys are at most 1024 bytes).</summary>
    private const int MaxProbeRounds = 1024;

    private const char FirstProbeChar = ' ';
    private const char LastProbeChar = '~';

    /// <param name="afterRelativeKey">
    /// When set (resuming a legacy checkpoint), only the keyspace after this key is planned: every boundary is greater
    /// than it and the first range starts with it as its LastCompletedKey.
    /// </param>
    /// <param name="waitIfPaused">Awaited before every LIST request so a paused scan does not keep planning.</param>
    public static async Task<List<ScanPartition>> PlanAsync(
        PlannerBucket bucketA,
        PlannerBucket bucketB,
        int targetPartitions,
        int maxDiscoveryRequests,
        CancellationToken cancellationToken = default,
        string? afterRelativeKey = null,
        Func<CancellationToken, Task>? waitIfPaused = null)
    {
        string? after = string.IsNullOrEmpty(afterRelativeKey) ? null : afterRelativeKey;
        var context = new PlanContext(after, waitIfPaused);

        var candidates = new HashSet<string>(StringComparer.Ordinal);
        if (targetPartitions > 1 && maxDiscoveryRequests > 0)
        {
            var budget = new RequestBudget(maxDiscoveryRequests);

            // 1. Hierarchical discovery, one level at a time on both buckets concurrently, until the union has enough candidates
            var levelA = new List<string> { string.Empty };
            var levelB = new List<string> { string.Empty };
            for (int depth = 1; depth <= MaxDiscoveryDepth && (levelA.Count > 0 || levelB.Count > 0); depth++)
            {
                var nextA = ListFoldersAsync(bucketA, levelA, budget, context, cancellationToken);
                var nextB = ListFoldersAsync(bucketB, levelB, budget, context, cancellationToken);
                await Task.WhenAll(nextA, nextB).ConfigureAwait(false);

                candidates.UnionWith(nextA.Result);
                candidates.UnionWith(nextB.Result);
                if (context.CountUsable(candidates) >= targetPartitions || budget.Remaining <= 0) break;

                levelA = nextA.Result;
                levelB = nextB.Result;
            }

            // 2. Flat namespace (or too few folders): probe the keyspace character by character
            if (context.CountUsable(candidates) < targetPartitions && budget.Remaining > 0)
            {
                // Each bucket gets its own half of what is left, so one bucket cannot starve the other
                int remaining = Math.Max(0, budget.Remaining);
                var probeA = ProbeKeyspaceAsync(bucketA, targetPartitions, budget.Split(remaining / 2), context, cancellationToken);
                var probeB = ProbeKeyspaceAsync(bucketB, targetPartitions, budget.Split(remaining - remaining / 2), context, cancellationToken);
                await Task.WhenAll(probeA, probeB).ConfigureAwait(false);
                candidates.UnionWith(probeA.Result);
                candidates.UnionWith(probeB.Result);
            }
        }

        // 3. Evenly spaced boundaries (after the resume key, if any)
        var ranges = BuildRanges(candidates.Where(context.IsUsable), Math.Max(1, targetPartitions));
        ranges[0].LastCompletedKey = after;
        return ranges;
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
    private static async Task<List<string>> ListFoldersAsync(PlannerBucket bucket, List<string> folders, RequestBudget budget, PlanContext context, CancellationToken cancellationToken)
    {
        var found = new List<string>();
        // Coarse to fine, so a budget that runs out still samples folders across the whole level
        foreach (var folder in CoarseToFine(folders.Where(context.MayHoldKeysAfter).ToList()))
        {
            string? continuationToken = null;
            for (int pageNo = 0; pageNo < MaxPagesPerFolder; pageNo++)
            {
                if (!budget.TryTake()) return found;

                await context.BeforeRequestAsync(bucket, cancellationToken).ConfigureAwait(false);
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

    /// <summary>
    /// Probe rounds: each round probes every populated prefix one character deeper, and the next round continues under
    /// each distinct (prefix + next character) seen in the answers. Stops when enough candidates are found, the budget
    /// runs out or no prefix branches further.
    /// </summary>
    private static async Task<List<string>> ProbeKeyspaceAsync(PlannerBucket bucket, int target, RequestBudget budget, PlanContext context, CancellationToken cancellationToken)
    {
        var found = new HashSet<string>(StringComparer.Ordinal);
        var known = new ProbeResults();
        var frontier = new List<string> { string.Empty };

        for (int round = 0; round < MaxProbeRounds && frontier.Count > 0 && budget.Remaining > 0; round++)
        {
            // All points of the round in key order, then coarse to fine, so a small budget spreads over every prefix
            var points = frontier
                .SelectMany(p => ProbeChars().Select(c => p + c))
                .Where(context.MayHoldKeysAfter)
                .OrderBy(p => p, Utf8KeyComparer.Instance)
                .ToList();
            var answers = await ProbeAsync(bucket, CoarseToFine(points), known, budget, context, cancellationToken).ConfigureAwait(false);
            found.UnionWith(answers);
            if (context.CountUsable(found) >= target) break;

            // Descend one character under every prefix the answers show to be populated
            var next = new HashSet<string>(StringComparer.Ordinal);
            foreach (var answer in answers.Distinct(StringComparer.Ordinal))
            {
                foreach (var prefix in frontier)
                {
                    if (answer.Length > prefix.Length && answer.StartsWith(prefix, StringComparison.Ordinal))
                    {
                        next.Add(SafePrefix(answer, prefix.Length + 1));
                    }
                }
            }
            frontier = next.OrderBy(p => p, Utf8KeyComparer.Instance).ToList();
        }
        return found.ToList();
    }

    /// <summary>First <paramref name="length"/> chars, extended so a surrogate pair is never split.</summary>
    private static string SafePrefix(string key, int length)
    {
        if (length < key.Length && char.IsHighSurrogate(key[length - 1])) length++;
        return key[..length];
    }

    /// <summary>
    /// Issues MaxKeys=1 probes at relative positions (in any order), returning the first key after each, including
    /// answers already known. A position whose answer follows from an earlier probe is not sent: if the nearest probed
    /// position below it found key K and the position is before K, the answer is K again; if that probe found nothing,
    /// nothing follows this one either.
    /// </summary>
    private static async Task<List<string>> ProbeAsync(PlannerBucket bucket, IEnumerable<string> points, ProbeResults known, RequestBudget budget, PlanContext context, CancellationToken cancellationToken)
    {
        var found = new List<string>();
        foreach (var rawPoint in points)
        {
            // Resuming after a key: nothing before it matters
            string point = context.After != null && Utf8KeyComparer.Instance.Compare(rawPoint, context.After) < 0 ? context.After : rawPoint;

            if (known.TryGetAnswer(point, out var knownResult))
            {
                if (knownResult != null) found.Add(knownResult);
                continue;
            }
            if (!budget.TryTake()) break;

            await context.BeforeRequestAsync(bucket, cancellationToken).ConfigureAwait(false);
            var page = await bucket.Lister.ListPageAsync(
                bucket.BucketName, bucket.Prefix, bucket.Prefix + point, null, null, 1, cancellationToken).ConfigureAwait(false);

            string? result = page.Objects.Count == 0 ? null : BucketComparisonEngine.ToRelativeKey(page.Objects[0].Key, bucket.Prefix);
            known.Add(point, result);
            if (result != null) found.Add(result);
        }
        return found;
    }

    /// <summary>
    /// Reorders items so every prefix of the sequence is spread evenly over the whole list: indices 0, n/2, n/4, 3n/4, ...
    /// A budget that runs out part-way then leaves gaps everywhere instead of skipping the end entirely.
    /// </summary>
    public static IEnumerable<T> CoarseToFine<T>(IReadOnlyList<T> items)
    {
        if (items.Count == 0) yield break;
        var emitted = new bool[items.Count];
        int stride = 1;
        while (stride * 2 <= items.Count) stride *= 2;
        for (; stride >= 1; stride /= 2)
        {
            for (int i = 0; i < items.Count; i += stride)
            {
                if (emitted[i]) continue;
                emitted[i] = true;
                yield return items[i];
            }
        }
    }

    private static IEnumerable<char> ProbeChars()
    {
        for (char c = FirstProbeChar; c <= LastProbeChar; c++) yield return c;
    }

    /// <summary>Resume key, pause hook and throttling shared by all planner requests.</summary>
    private sealed class PlanContext
    {
        private readonly Func<CancellationToken, Task>? _waitIfPaused;

        public PlanContext(string? after, Func<CancellationToken, Task>? waitIfPaused)
        {
            After = after;
            _waitIfPaused = waitIfPaused;
        }

        public string? After { get; }

        /// <summary>A boundary must lie after the resume key.</summary>
        public bool IsUsable(string candidate)
            => !string.IsNullOrEmpty(candidate) && (After == null || Utf8KeyComparer.Instance.Compare(candidate, After) > 0);

        public int CountUsable(IEnumerable<string> candidates) => candidates.Count(IsUsable);

        /// <summary>True when keys starting with <paramref name="prefix"/> can sort after the resume key.</summary>
        public bool MayHoldKeysAfter(string prefix)
            => After == null || After.StartsWith(prefix, StringComparison.Ordinal) || Utf8KeyComparer.Instance.Compare(prefix, After) > 0;

        public async Task BeforeRequestAsync(PlannerBucket bucket, CancellationToken cancellationToken)
        {
            if (_waitIfPaused != null) await _waitIfPaused(cancellationToken).ConfigureAwait(false);
            await bucket.RateLimiter.WaitIfLimitedAsync(1, cancellationToken).ConfigureAwait(false);
        }
    }

    /// <summary>Probe answers sorted by position (UTF-8 order).</summary>
    private sealed class ProbeResults
    {
        private readonly List<string> _points = new();
        private readonly List<string?> _results = new();

        public void Add(string point, string? result)
        {
            int index = LowerBound(point);
            if (index < _points.Count && Utf8KeyComparer.Instance.Compare(_points[index], point) == 0) return;
            _points.Insert(index, point);
            _results.Insert(index, result);
        }

        /// <summary>The first key after <paramref name="point"/> (null: none), when it follows from earlier probes.</summary>
        public bool TryGetAnswer(string point, out string? result)
        {
            result = null;
            int index = LowerBound(point);
            if (index < _points.Count && Utf8KeyComparer.Instance.Compare(_points[index], point) == 0)
            {
                result = _results[index];
                return true;
            }
            if (index == 0) return false;

            string? belowResult = _results[index - 1];
            if (belowResult == null) return true;
            if (Utf8KeyComparer.Instance.Compare(point, belowResult) < 0)
            {
                result = belowResult;
                return true;
            }
            return false;
        }

        private int LowerBound(string point)
        {
            int lo = 0, hi = _points.Count;
            while (lo < hi)
            {
                int mid = (lo + hi) / 2;
                if (Utf8KeyComparer.Instance.Compare(_points[mid], point) < 0) lo = mid + 1;
                else hi = mid;
            }
            return lo;
        }
    }

    private sealed class RequestBudget
    {
        private int _remaining;

        public RequestBudget(int total) => _remaining = total;

        public int Remaining => Volatile.Read(ref _remaining);

        public bool TryTake() => Interlocked.Decrement(ref _remaining) >= 0;

        /// <summary>Moves up to <paramref name="amount"/> requests into a separate budget.</summary>
        public RequestBudget Split(int amount)
        {
            int taken = 0;
            while (taken < amount && TryTake()) taken++;
            return new RequestBudget(taken);
        }
    }
}
