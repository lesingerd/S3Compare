using System.Collections.Concurrent;
using System.Diagnostics;
using S3Compare.App.Models;

namespace S3Compare.App.Services;

/// <summary>A bucket as seen by the planner: its lister, name, prefix, request throttle and a short label ("A"/"B") for diagnostics.</summary>
public sealed record PlannerBucket(IS3Lister Lister, string BucketName, string Prefix, TokenBucketRateLimiter? RateLimiter = null, string Label = "");

/// <summary>The ranges of a plan and the sorted boundary candidates (relative keys) they were built from.</summary>
public sealed record PlanResult(List<ScanPartition> Ranges, IReadOnlyList<string> Candidates);

/// <summary>Limits, concurrency and reporting hooks of one planning run. The defaults apply no time box.</summary>
public sealed class PlannerOptions
{
    public static readonly TimeSpan DefaultRequestTimeout = TimeSpan.FromSeconds(15);
    public const int DefaultConcurrency = 8;

    /// <summary>Planning stops sending requests after this much (unpaused) time; zero = unlimited.</summary>
    public TimeSpan MaxDuration { get; init; } = TimeSpan.Zero;

    /// <summary>One planner request is abandoned after this long, even if the lister ignores cancellation.</summary>
    public TimeSpan RequestTimeout { get; init; } = DefaultRequestTimeout;

    /// <summary>Independent planner requests in flight per bucket (the request budget and the rate limiter still apply).</summary>
    public int Concurrency { get; init; } = DefaultConcurrency;

    /// <summary>Folder levels listed at most (0 = none: probes only).</summary>
    public int MaxDiscoveryDepth { get; init; } = PartitionPlanner.MaxDiscoveryDepth;

    /// <summary>Called a few times per second (and on every state change of note) from a background thread.</summary>
    public Action<PlannerProgress>? OnProgress { get; init; }

    /// <summary>Diagnostic log lines (called from several threads).</summary>
    public Action<string>? Log { get; init; }

    /// <summary>Classified issues (a skipped request, a disabled technique, a time limit); called from several threads.</summary>
    public Action<IssueOutcome, string>? OnIssue { get; init; }
}

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
/// Every LIST call counts against a shared budget (MaxDiscoveryRequests), waits while the scan is paused and goes through the
/// bucket's rate limiter. Independent requests (the folders of one level, a wave of probes) run concurrently, bounded by
/// <see cref="PlannerOptions.Concurrency"/>.
///
/// Robustness: planning never fails because of a request. Each request has its own timeout
/// (<see cref="PlannerOptions.RequestTimeout"/>); a failed or timed-out request is logged and skipped. A failing delimiter
/// listing switches that bucket to probes for the rest of the plan, and probing is switched off for a bucket after
/// <see cref="MaxConsecutiveProbeFailures"/> failures in a row. When <see cref="PlannerOptions.MaxDuration"/> is exceeded
/// the planner stops sending requests and builds ranges from the candidates found so far. Only cancellation of the scan
/// itself propagates. The planner uses only ordinary StartAfter values (printable ASCII appended to the prefix); there is
/// no U+10FFFF style sentinel.
/// </remarks>
public static class PartitionPlanner
{
    public const int MaxDiscoveryDepth = 3;
    public const string Delimiter = "/";
    public const int MaxConsecutiveProbeFailures = 3;

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
        Func<CancellationToken, Task>? waitIfPaused = null,
        PlannerOptions? options = null)
        => (await PlanWithCandidatesAsync(bucketA, bucketB, targetPartitions, maxDiscoveryRequests, cancellationToken, afterRelativeKey, waitIfPaused, options).ConfigureAwait(false)).Ranges;

    /// <summary>
    /// <see cref="PlanAsync"/> that also returns the distinct boundary candidates (relative keys, sorted, after the resume key)
    /// the ranges were built from. <see cref="RangeSplitter"/> uses them.
    /// </summary>
    public static async Task<PlanResult> PlanWithCandidatesAsync(
        PlannerBucket bucketA,
        PlannerBucket bucketB,
        int targetPartitions,
        int maxDiscoveryRequests,
        CancellationToken cancellationToken = default,
        string? afterRelativeKey = null,
        Func<CancellationToken, Task>? waitIfPaused = null,
        PlannerOptions? options = null)
    {
        string? after = string.IsNullOrEmpty(afterRelativeKey) ? null : afterRelativeKey;
        options ??= new PlannerOptions();
        bucketA = bucketA with { Label = string.IsNullOrEmpty(bucketA.Label) ? "A" : bucketA.Label };
        bucketB = bucketB with { Label = string.IsNullOrEmpty(bucketB.Label) ? "B" : bucketB.Label };
        using var context = new PlanContext(after, waitIfPaused, options, Math.Max(0, maxDiscoveryRequests), cancellationToken);

        var candidates = new HashSet<string>(StringComparer.Ordinal);
        Task? ticker = null;
        try
        {
            if (targetPartitions > 1 && maxDiscoveryRequests > 0)
            {
                context.Log($"Planning started: target {targetPartitions} ranges, budget {maxDiscoveryRequests:N0} requests, " +
                            $"time limit {(options.MaxDuration > TimeSpan.Zero ? options.MaxDuration.TotalSeconds.ToString("N0") + " s" : "none")}, " +
                            $"request timeout {options.RequestTimeout.TotalSeconds:N0} s, concurrency {Math.Max(1, options.Concurrency)}");
                ticker = context.RunTickerAsync();
                var budget = new RequestBudget(maxDiscoveryRequests);

                // 1. Hierarchical discovery, one level at a time on both buckets concurrently, until the union has enough candidates
                var levelA = new List<string> { string.Empty };
                var levelB = new List<string> { string.Empty };
                int maxDepth = Math.Max(0, options.MaxDiscoveryDepth);
                for (int depth = 1; depth <= maxDepth && (levelA.Count > 0 || levelB.Count > 0) && !context.Stopped; depth++)
                {
                    var nextA = ListFoldersAsync(bucketA, levelA, depth, maxDepth, budget, context);
                    var nextB = ListFoldersAsync(bucketB, levelB, depth, maxDepth, budget, context);
                    await Task.WhenAll(nextA, nextB).ConfigureAwait(false);

                    candidates.UnionWith(nextA.Result);
                    candidates.UnionWith(nextB.Result);
                    context.AddCandidates(nextA.Result);
                    context.AddCandidates(nextB.Result);
                    if (budget.Remaining <= 0) break;
                    // Keep descending even when there are enough candidates: the ranges are spaced evenly over the candidates, so a
                    // folder with thousands of subfolders must contribute them, or one range ends up holding most of the bucket (a few
                    // near-empty top folders each with one big subfolder). Only stop early when listing the next level (one request
                    // per folder found, on the bucket that has it) would take more than half of the remaining budget: a partly listed
                    // level would skew the plan towards its first folders, and the probe fallback keeps the other half. Uneven ranges
                    // that remain are split while scanning (see RangeSplitter).
                    if (context.CountUsable(candidates) >= targetPartitions && (long)(nextA.Result.Count + nextB.Result.Count) > budget.Remaining / 2) break;

                    levelA = nextA.Result;
                    levelB = nextB.Result;
                }

                // 2. Flat namespace (or too few folders): probe the keyspace character by character
                if (context.CountUsable(candidates) < targetPartitions && budget.Remaining > 0 && !context.Stopped)
                {
                    // Each bucket gets its own half of what is left, so one bucket cannot starve the other
                    int remaining = Math.Max(0, budget.Remaining);
                    var probeA = ProbeKeyspaceAsync(bucketA, targetPartitions, budget.Split(remaining / 2), context);
                    var probeB = ProbeKeyspaceAsync(bucketB, targetPartitions, budget.Split(remaining - remaining / 2), context);
                    await Task.WhenAll(probeA, probeB).ConfigureAwait(false);
                    candidates.UnionWith(probeA.Result);
                    candidates.UnionWith(probeB.Result);
                    context.AddCandidates(probeA.Result);
                    context.AddCandidates(probeB.Result);
                }
            }

            // 3. Evenly spaced boundaries (after the resume key, if any). Always tiles, even from a partial candidate set.
            var ranges = BuildRanges(candidates.Where(context.IsUsable), Math.Max(1, targetPartitions));
            ranges[0].LastCompletedKey = after;
            context.SetStep("Building ranges");
            context.Finish(ranges.Count, targetPartitions);
            return new PlanResult(ranges, candidates.Where(context.IsUsable).OrderBy(c => c, Utf8KeyComparer.Instance).ToList());
        }
        finally
        {
            context.StopTicker();
            if (ticker != null)
            {
                try { await ticker.ConfigureAwait(false); } catch (OperationCanceledException) { }
            }
        }
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

    /// <summary>Delimiter listings of the given folders (relative), several at a time; returns their relative sub-folders.</summary>
    private static async Task<List<string>> ListFoldersAsync(PlannerBucket bucket, List<string> folders, int depth, int maxDepth, RequestBudget budget, PlanContext context)
    {
        var found = new ConcurrentBag<string>();
        // Coarse to fine, so a budget that runs out still samples folders across the whole level
        var ordered = CoarseToFine(folders.Where(context.MayHoldKeysAfter).ToList()).ToList();
        if (ordered.Count == 0) return new List<string>();

        context.SetStep(bucket, $"delimiter depth {depth}/{maxDepth} ({ordered.Count:N0} folders)");
        using var gate = new SemaphoreSlim(context.Concurrency);
        var tasks = new List<Task>();
        foreach (var folder in ordered)
        {
            try
            {
                await gate.WaitAsync(context.PlanToken).ConfigureAwait(false);
            }
            catch (OperationCanceledException) when (!context.IsScanCancelled)
            {
                break; // the time box ended while waiting for a free slot
            }
            if (budget.Remaining <= 0 || context.Stopped || context.DelimiterDisabled(bucket))
            {
                gate.Release();
                break;
            }

            tasks.Add(Task.Run(async () =>
            {
                try
                {
                    string? continuationToken = null;
                    for (int pageNo = 0; pageNo < MaxPagesPerFolder; pageNo++)
                    {
                        if (context.Stopped || context.DelimiterDisabled(bucket) || !budget.TryTake()) return;

                        var token = continuationToken;
                        var page = await context.RequestAsync(bucket, "delimiter listing",
                            ct => bucket.Lister.ListPageAsync(bucket.BucketName, bucket.Prefix + folder, null, token, Delimiter, 1000, ct)).ConfigureAwait(false);
                        if (page == null)
                        {
                            // Failed (not merely timed out): do not keep using a technique this endpoint rejects
                            if (!context.Stopped) context.DisableDelimiter(bucket);
                            return;
                        }

                        foreach (var commonPrefix in page.CommonPrefixes)
                        {
                            found.Add(BucketComparisonEngine.ToRelativeKey(commonPrefix, bucket.Prefix));
                        }
                        context.AddCandidates(page.CommonPrefixes.Select(p => BucketComparisonEngine.ToRelativeKey(p, bucket.Prefix)));

                        if (!page.IsTruncated || string.IsNullOrEmpty(page.NextContinuationToken)) return;
                        continuationToken = page.NextContinuationToken;
                    }
                }
                finally
                {
                    gate.Release();
                }
            }, CancellationToken.None));
        }
        await Task.WhenAll(tasks).ConfigureAwait(false);
        return found.ToList();
    }

    /// <summary>
    /// Probe rounds: each round probes every populated prefix one character deeper, and the next round continues under
    /// each distinct (prefix + next character) seen in the answers. Stops when enough candidates are found, the budget
    /// runs out or no prefix branches further.
    /// </summary>
    private static async Task<List<string>> ProbeKeyspaceAsync(PlannerBucket bucket, int target, RequestBudget budget, PlanContext context)
    {
        var found = new HashSet<string>(StringComparer.Ordinal);
        var known = new ProbeResults();
        var frontier = new List<string> { string.Empty };

        for (int round = 0; round < MaxProbeRounds && frontier.Count > 0 && budget.Remaining > 0 && !context.Stopped && !context.ProbingDisabled(bucket); round++)
        {
            context.SetStep(bucket, round == 0
                ? "probe round 1 (first characters)"
                : $"probe round {round + 1}: shared-prefix jump to depth {frontier[0].Length} ('{Clip(frontier[0])}', {frontier.Count:N0} prefixes)");

            // All points of the round in key order, then coarse to fine, so a small budget spreads over every prefix
            var points = frontier
                .SelectMany(p => ProbeChars().Select(c => p + c))
                .Where(context.MayHoldKeysAfter)
                .OrderBy(p => p, Utf8KeyComparer.Instance)
                .ToList();
            var answers = await ProbeAsync(bucket, CoarseToFine(points).ToList(), known, budget, context).ConfigureAwait(false);
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

    private static string Clip(string value) => value.Length <= 24 ? value : value[..21] + "...";

    /// <summary>First <paramref name="length"/> chars, extended so a surrogate pair is never split.</summary>
    private static string SafePrefix(string key, int length)
    {
        if (length < key.Length && char.IsHighSurrogate(key[length - 1])) length++;
        return key[..length];
    }

    /// <summary>
    /// Issues MaxKeys=1 probes at relative positions, in waves of up to <see cref="PlanContext.Concurrency"/> concurrent
    /// requests, returning the first key after each, including answers already known. A position whose answer follows from an
    /// earlier probe is not sent: if the nearest probed position below it found key K and the position is before K, the answer
    /// is K again; if that probe found nothing, nothing follows this one either. (Answers within one wave are not yet known to
    /// each other; the points of a wave are far apart because the order is coarse to fine.) A failed probe is skipped.
    /// </summary>
    private static async Task<List<string>> ProbeAsync(PlannerBucket bucket, List<string> points, ProbeResults known, RequestBudget budget, PlanContext context)
    {
        var found = new List<string>();
        int next = 0;
        int waveSize = 1; // grows while probes keep finding new keys, falls back to 1 when answers repeat (a single-child prefix)
        var seen = new HashSet<string>(StringComparer.Ordinal);
        while (next < points.Count && !context.Stopped && !context.ProbingDisabled(bucket))
        {
            var wave = new List<string>();
            var inWave = new HashSet<string>(StringComparer.Ordinal);
            while (next < points.Count && wave.Count < Math.Min(waveSize, context.Concurrency))
            {
                string rawPoint = points[next++];
                // Resuming after a key: nothing before it matters
                string point = context.After != null && Utf8KeyComparer.Instance.Compare(rawPoint, context.After) < 0 ? context.After : rawPoint;

                if (known.TryGetAnswer(point, out var knownResult))
                {
                    if (knownResult != null) found.Add(knownResult);
                    continue;
                }
                if (inWave.Contains(point)) continue;
                if (!budget.TryTake())
                {
                    next = points.Count;
                    break;
                }
                inWave.Add(point);
                wave.Add(point);
            }
            if (wave.Count == 0) continue;

            var pages = await Task.WhenAll(wave.Select(point => context.RequestAsync(bucket, "probe",
                ct => bucket.Lister.ListPageAsync(bucket.BucketName, bucket.Prefix, bucket.Prefix + point, null, null, 1, ct)))).ConfigureAwait(false);

            bool anyNew = false;
            for (int i = 0; i < wave.Count; i++)
            {
                var page = pages[i];
                if (page == null)
                {
                    if (!context.Stopped) context.ProbeFailed(bucket);
                    continue;
                }
                context.ProbeSucceeded(bucket);
                string? result = page.Objects.Count == 0 ? null : BucketComparisonEngine.ToRelativeKey(page.Objects[0].Key, bucket.Prefix);
                known.Add(wave[i], result);
                if (result != null)
                {
                    if (seen.Add(result)) anyNew = true;
                    found.Add(result);
                    context.AddCandidates(new[] { result });
                }
            }
            waveSize = anyNew ? Math.Min(context.Concurrency, waveSize * 2) : 1;
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

    /// <summary>Resume key, pause hook, throttling, time box, failure policy and progress shared by all planner requests.</summary>
    private sealed class PlanContext : IDisposable
    {
        private readonly Func<CancellationToken, Task>? _waitIfPaused;
        private readonly PlannerOptions _options;
        private readonly int _budgetTotal;
        private readonly CancellationToken _scanToken;
        private readonly CancellationTokenSource _timeBox;
        private readonly CancellationTokenSource _tickerStop = new();
        private readonly Stopwatch _clock = Stopwatch.StartNew();
        private readonly ConcurrentDictionary<string, byte> _candidates = new(StringComparer.Ordinal);
        private readonly ConcurrentDictionary<string, string> _steps = new();
        private readonly ConcurrentDictionary<string, byte> _disabled = new();
        private readonly ConcurrentDictionary<string, int> _probeFailureStreak = new();
        private readonly object _errorLock = new();
        private int _requestsSent;
        private int _failed;
        private int _pauseWaiters;
        private volatile bool _timedOut;
        private bool _finished;
        private string? _lastError;
        private TimeSpan _paused;
        private TimeSpan _lastTick;
        private long _lastProgressMs = -1000;

        public PlanContext(string? after, Func<CancellationToken, Task>? waitIfPaused, PlannerOptions options, int budgetTotal, CancellationToken scanToken)
        {
            After = after;
            _waitIfPaused = waitIfPaused;
            _options = options;
            _budgetTotal = budgetTotal;
            _scanToken = scanToken;
            _timeBox = CancellationTokenSource.CreateLinkedTokenSource(scanToken);
            PlanToken = _timeBox.Token;
        }

        public string? After { get; }
        public int Concurrency => Math.Max(1, _options.Concurrency);

        /// <summary>Cancelled by the scan stopping or by the time box.</summary>
        public CancellationToken PlanToken { get; }

        public bool IsScanCancelled => _scanToken.IsCancellationRequested;

        public bool Stopped => _timedOut || _scanToken.IsCancellationRequested;

        /// <summary>A boundary must lie after the resume key.</summary>
        public bool IsUsable(string candidate)
            => !string.IsNullOrEmpty(candidate) && (After == null || Utf8KeyComparer.Instance.Compare(candidate, After) > 0);

        public int CountUsable(IEnumerable<string> candidates) => candidates.Count(IsUsable);

        /// <summary>True when keys starting with <paramref name="prefix"/> can sort after the resume key.</summary>
        public bool MayHoldKeysAfter(string prefix)
            => After == null || After.StartsWith(prefix, StringComparison.Ordinal) || Utf8KeyComparer.Instance.Compare(prefix, After) > 0;

        public void Log(string message)
        {
            try { _options.Log?.Invoke(message); } catch { /* a logging failure must not affect planning */ }
        }

        public void AddCandidates(IEnumerable<string> keys)
        {
            foreach (var key in keys)
            {
                if (!string.IsNullOrEmpty(key)) _candidates.TryAdd(key, 0);
            }
        }

        public void SetStep(PlannerBucket bucket, string text)
        {
            _steps[bucket.Label] = text;
            RaiseProgress(force: true);
        }

        public void SetStep(string text)
        {
            _steps.Clear();
            _steps["-"] = text;
            RaiseProgress(force: true);
        }

        public bool DelimiterDisabled(PlannerBucket bucket) => _disabled.ContainsKey(bucket.Label + ": delimiter listing");
        public bool ProbingDisabled(PlannerBucket bucket) => _disabled.ContainsKey(bucket.Label + ": key probing");

        public void DisableDelimiter(PlannerBucket bucket)
        {
            if (!_disabled.TryAdd(bucket.Label + ": delimiter listing", 0)) return;
            Log($"Planner: bucket {bucket.Label} delimiter listing failed; disabled for the rest of this plan, continuing with probes. Last error: {LastError}");
            Issue(IssueOutcome.Skipped, $"delimiter listing disabled for bucket {bucket.Label} after a failed request — planning continued with probes; results unaffected, only split quality may be lower");
            RaiseProgress(force: true);
        }

        public void ProbeFailed(PlannerBucket bucket)
        {
            int streak = _probeFailureStreak.AddOrUpdate(bucket.Label, 1, (_, n) => n + 1);
            if (streak >= MaxConsecutiveProbeFailures && _disabled.TryAdd(bucket.Label + ": key probing", 0))
            {
                Log($"Planner: bucket {bucket.Label} probes failed {streak} times in a row; probing disabled for the rest of this plan. Last error: {LastError}");
                Issue(IssueOutcome.Skipped, $"key probing disabled for bucket {bucket.Label} after {streak} failed probes in a row — planning continued; results unaffected, only split quality may be lower");
                RaiseProgress(force: true);
            }
        }

        public void ProbeSucceeded(PlannerBucket bucket) => _probeFailureStreak[bucket.Label] = 0;

        private void Issue(IssueOutcome outcome, string detail)
        {
            try { _options.OnIssue?.Invoke(outcome, detail); } catch { /* reporting must not affect planning */ }
        }

        private string? LastError
        {
            get { lock (_errorLock) return _lastError; }
        }

        private void RecordFailure(PlannerBucket bucket, string kind, string message)
        {
            int count = Interlocked.Increment(ref _failed);
            string detail = $"planner request {bucket.Label} {kind}: {message} — planning continued; results unaffected, only split quality may be lower";
            string text = IssueEntry.Format(DateTime.Now, IssueOutcome.Skipped, detail);
            lock (_errorLock) _lastError = text;
            Issue(IssueOutcome.Skipped, detail);
            // Every failure of the first few, then occasional ones, so a flood of failures cannot flood the log
            if (count <= 10 || count % 25 == 0) Log($"Planner request failed ({count:N0} so far): {text}");
            RaiseProgress(force: true);
        }

        /// <summary>
        /// Pause hook and rate limiter, then one request with its own timeout. Returns null when the request failed, timed out or
        /// the time box ended; throws only when the scan itself is cancelled.
        /// </summary>
        public async Task<S3ListPage?> RequestAsync(PlannerBucket bucket, string kind, Func<CancellationToken, Task<S3ListPage>> call)
        {
            if (Stopped) return null;
            try
            {
                if (_waitIfPaused != null)
                {
                    Interlocked.Increment(ref _pauseWaiters);
                    try { await _waitIfPaused(PlanToken).ConfigureAwait(false); }
                    finally { Interlocked.Decrement(ref _pauseWaiters); }
                }
                await bucket.RateLimiter.WaitIfLimitedAsync(1, PlanToken).ConfigureAwait(false);
                if (Stopped) return null;

                Interlocked.Increment(ref _requestsSent);
                using var requestCts = CancellationTokenSource.CreateLinkedTokenSource(PlanToken);
                requestCts.CancelAfter(_options.RequestTimeout);
                return await AbandonableTask.WithCancellation(call(requestCts.Token), requestCts.Token).ConfigureAwait(false);
            }
            catch (OperationCanceledException) when (_scanToken.IsCancellationRequested)
            {
                throw;
            }
            catch (OperationCanceledException) when (_timedOut)
            {
                return null;
            }
            catch (OperationCanceledException)
            {
                RecordFailure(bucket, kind, $"no answer within {_options.RequestTimeout.TotalSeconds:N0} s (request abandoned)");
                return null;
            }
            catch (Exception ex)
            {
                RecordFailure(bucket, kind, $"{ex.GetType().Name}: {ex.Message}");
                return null;
            }
        }

        /// <summary>Background loop: progress reports (elapsed time keeps moving while requests hang) and the time box.</summary>
        public async Task RunTickerAsync()
        {
            RaiseProgress(force: true);
            try
            {
                while (!_tickerStop.IsCancellationRequested)
                {
                    await Task.Delay(100, _tickerStop.Token).ConfigureAwait(false);
                    Tick();
                }
            }
            catch (OperationCanceledException)
            {
            }
        }

        public void StopTicker()
        {
            try { _tickerStop.Cancel(); } catch (ObjectDisposedException) { }
        }

        private void Tick()
        {
            var now = _clock.Elapsed;
            if (_pauseWaiters > 0) _paused += now - _lastTick;
            _lastTick = now;

            if (!_timedOut && _options.MaxDuration > TimeSpan.Zero && ActiveElapsed >= _options.MaxDuration)
            {
                _timedOut = true;
                Log($"Planner: time limit of {_options.MaxDuration.TotalSeconds:N0} s reached after {_requestsSent:N0} requests and {_candidates.Count:N0} candidates; " +
                    "stopping planner requests and building ranges from what was found.");
                Issue(IssueOutcome.Degraded, $"planning time limit of {_options.MaxDuration.TotalSeconds:N0} s reached after {_requestsSent:N0} requests and {_candidates.Count:N0} candidates — ranges built from what was found; results unaffected, the scan may use fewer parallel ranges (speed, not correctness)");
                try { _timeBox.Cancel(); } catch (ObjectDisposedException) { }
                RaiseProgress(force: true);
                return;
            }
            RaiseProgress(force: false);
        }

        private TimeSpan ActiveElapsed => _clock.Elapsed - _paused;

        public void Finish(int rangeCount, int targetPartitions)
        {
            _finished = true;
            RaiseProgress(force: true);
            var p = Snapshot();
            if (!p.TimedOut && p.FailedRequests > 0 && targetPartitions > 1 && rangeCount < targetPartitions)
            {
                Issue(IssueOutcome.Degraded, $"planning built {rangeCount:N0} range(s) instead of {targetPartitions:N0} because {p.FailedRequests:N0} request(s) failed — results unaffected, the scan may be slower (speed, not correctness)");
            }
            Log($"Planning finished: {rangeCount:N0} range(s) from {p.Candidates:N0} candidates, {p.RequestsUsed:N0}/{p.RequestBudget:N0} requests, " +
                $"{p.FailedRequests:N0} failed, {p.Elapsed.TotalSeconds:N1} s{(p.TimedOut ? ", TIME LIMIT REACHED" : "")}" +
                $"{(p.DisabledTechniques.Length > 0 ? ", disabled: " + p.DisabledTechniques : "")}");
        }

        public PlannerProgress Snapshot() => new()
        {
            Step = string.Join(" | ", _steps.OrderBy(kv => kv.Key, StringComparer.Ordinal).Select(kv => kv.Key == "-" ? kv.Value : $"{kv.Key}: {kv.Value}")),
            RequestsUsed = Volatile.Read(ref _requestsSent),
            RequestBudget = _budgetTotal,
            Candidates = _candidates.Count,
            Elapsed = ActiveElapsed,
            TimeLimit = _options.MaxDuration,
            FailedRequests = Volatile.Read(ref _failed),
            LastError = LastError,
            TimedOut = _timedOut,
            Finished = _finished,
            DisabledTechniques = string.Join(", ", _disabled.Keys.OrderBy(k => k, StringComparer.Ordinal))
        };

        private void RaiseProgress(bool force)
        {
            if (_options.OnProgress == null) return;
            long now = _clock.ElapsedMilliseconds;
            long last = Interlocked.Read(ref _lastProgressMs);
            if (!force && now - last < 250) return;
            Interlocked.Exchange(ref _lastProgressMs, now);
            try { _options.OnProgress(Snapshot()); } catch { /* a reporting failure must not affect planning */ }
        }

        public void Dispose()
        {
            _tickerStop.Dispose();
            _timeBox.Dispose();
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
