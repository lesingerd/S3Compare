using System.Diagnostics;
using S3Compare.App.Models;
using S3Compare.App.Services;

namespace S3Compare.Tests;

/// <summary>The range splitter (work stealing) and the planner's descent into big folders.</summary>
public class RangeSplitterTests
{
    private static S3ObjectSummary Obj(string key) => new() { Key = key, Size = 1, ETag = "e" };

    private static RangeSplitter Splitter(IEnumerable<string> keys, string prefix = "")
    {
        var lister = new FakeS3Lister()
            .SetBucket("a", keys.Select(k => Obj(prefix + k)))
            .SetBucket("b", keys.Select(k => Obj(prefix + k)));
        return new RangeSplitter(new RangeSplitter.Side(lister, "a", prefix, null), new RangeSplitter.Side(lister, "b", prefix, null));
    }

    /// <summary>A few near-empty top folders and one with hundreds of subfolders (the shape that left one range with most keys).</summary>
    internal static List<string> Lopsided()
    {
        var keys = new List<string>();
        foreach (var r in new[] { "a", "b", "c", "e", "f", "g", "h" }) keys.Add($"{r}/CV/_DIRECTORY_HOLDER_");
        for (int v = 0; v < 300; v++)
            for (int c = 0; c < 3; c++)
                keys.Add($"d/CV/V_{v:000}/CHUNK_{c}/data");
        return keys;
    }

    [Fact]
    public async Task FolderWalk_PicksTheMiddleSubfolderAhead()
    {
        var s = Splitter(Lopsided(), "pre/");
        var key = await s.FindSplitAsync("d/CV/V_010/CHUNK_1/data", "e/CV/", CancellationToken.None);
        Assert.Equal("folder", s.Method);
        Assert.NotNull(key);
        Assert.StartsWith("d/CV/V_", key);
        int v = int.Parse(key.Substring("d/CV/V_".Length, 3));
        Assert.InRange(v, 140, 170);
    }

    [Fact]
    public async Task FolderWalk_CutsInsideTheBigFolder_EvenWithSmallTopFoldersAhead()
    {
        var s = Splitter(Lopsided());
        var key = await s.FindSplitAsync("d/CV/V_010/CHUNK_1/data", null, CancellationToken.None);
        Assert.NotNull(key);
        Assert.StartsWith("d/CV/V_", key); // the few small top folders after the big one must not pull the cut out of it
    }

    [Fact]
    public async Task FolderWalk_StaysInsideTheUpperBound()
    {
        var s = Splitter(Lopsided());
        var key = await s.FindSplitAsync("d/CV/V_010/CHUNK_1/data", "d/CV/V_050/", CancellationToken.None);
        Assert.NotNull(key);
        Assert.True(Utf8KeyComparer.Instance.Compare(key, "d/CV/V_010/CHUNK_1/data") > 0, key);
        Assert.True(Utf8KeyComparer.Instance.Compare(key, "d/CV/V_050/") < 0, key);
    }

    [Fact]
    public async Task FlatNamespace_UsesProbes()
    {
        var keys = Enumerable.Range(0, 5000).Select(i => $"file_{i:00000}.dat").ToList();
        var s = Splitter(keys);
        var key = await s.FindSplitAsync("file_00100.dat", null, CancellationToken.None);
        Assert.Equal("probe", s.Method);
        Assert.NotNull(key);
        Assert.True(Utf8KeyComparer.Instance.Compare(key, "file_00100.dat") > 0, key);

        var bounded = await s.FindSplitAsync("file_00100.dat", "file_03000.dat", CancellationToken.None);
        Assert.NotNull(bounded);
        Assert.True(Utf8KeyComparer.Instance.Compare(bounded, "file_00100.dat") > 0 && Utf8KeyComparer.Instance.Compare(bounded, "file_03000.dat") < 0, bounded);
    }

    [Fact]
    public async Task NothingAhead_ReturnsNull()
    {
        var keys = Enumerable.Range(0, 50).Select(i => $"x/{i:00}").ToList();
        var s = Splitter(keys);
        Assert.Null(await s.FindSplitAsync("x/49", null, CancellationToken.None));
    }

    [Fact]
    public async Task FailingLister_ReturnsNull_AndCountsTheErrors()
    {
        var bad = new FakeS3Lister { Fault = _ => new InvalidOperationException("SlowDown (simulated)") }
            .SetBucket("a", Lopsided().Select(Obj)).SetBucket("b", Lopsided().Select(Obj));
        var s = new RangeSplitter(new RangeSplitter.Side(bad, "a", "", null), new RangeSplitter.Side(bad, "b", "", null), TimeSpan.FromSeconds(2));
        Assert.Null(await s.FindSplitAsync("d/CV/V_010/CHUNK_1/data", null, CancellationToken.None));
        Assert.True(s.RequestErrors > 0);
        Assert.NotNull(s.LastError);
    }

    [Fact]
    public async Task Planner_DescendsIntoTheBigFolder_WhenTheTopLevelIsLopsided()
    {
        var keys = Lopsided();
        var lister = new FakeS3Lister().SetBucket("a", keys.Select(Obj)).SetBucket("b", keys.Select(Obj));
        var plan = await PartitionPlanner.PlanWithCandidatesAsync(new PlannerBucket(lister, "a", ""), new PlannerBucket(lister, "b", ""), 16, 500);

        Assert.Equal(16, plan.Ranges.Count);
        int inBig = plan.Ranges.Count(r => r.Upper != null && r.Upper.StartsWith("d/CV/V_", StringComparison.Ordinal));
        Assert.True(inBig >= 12, $"{inBig} of 15 boundaries inside the big folder: {string.Join(" ", plan.Ranges)}");
        Assert.True(PartitionPlanner.TilesKeyspace(plan.Ranges));
        Assert.Equal(plan.Candidates.OrderBy(c => c, Utf8KeyComparer.Instance), plan.Candidates);
        Assert.Contains("d/CV/V_150/", plan.Candidates);
    }

    [Fact]
    public async Task Planner_StopsDescending_WhenTheNextLevelIsTooExpensive()
    {
        // 300 top folders: with enough candidates and a next level that would take more than half of the budget, planning stops.
        var keys = Enumerable.Range(0, 300).Select(i => $"t{i:000}/sub/file").ToList();
        var lister = new FakeS3Lister().SetBucket("a", keys.Select(Obj)).SetBucket("b", keys.Select(Obj));
        var ranges = await PartitionPlanner.PlanAsync(new PlannerBucket(lister, "a", ""), new PlannerBucket(lister, "b", ""), 16, 100);
        Assert.Equal(2, lister.CallCount);
        Assert.Equal(16, ranges.Count);
    }
}

/// <summary>Work stealing in the engine: one big or lopsided range is split by idle workers and the result is unchanged.</summary>
public class RangeSplittingEngineTests
{
    private static S3ObjectSummary Obj(string key, long size = 100, string etag = "e") =>
        new() { Key = key, Size = size, ETag = etag, LastModified = new DateTime(2026, 1, 1, 0, 0, 0, DateTimeKind.Utc), StorageClass = "STANDARD" };

    private static S3BucketConfig Config(string bucket) =>
        new() { BucketName = bucket, Prefix = "", EndpointUrl = "https://fake.invalid", Region = "us-east-1" };

    /// <summary>Same keys in both buckets, with objects only in A, only in B and size or ETag mismatches sprinkled in.</summary>
    private static BucketPair Generate(int seed, IEnumerable<string> keys)
    {
        var rnd = new Random(seed);
        var data = new BucketPair();
        foreach (var key in keys)
        {
            int roll = rnd.Next(100);
            if (roll < 5) data.A.Add(Obj(key));
            else if (roll < 10) data.B.Add(Obj(key));
            else if (roll < 14) { data.A.Add(Obj(key, 5)); data.B.Add(Obj(key, 6)); }
            else if (roll < 18) { data.A.Add(Obj(key, 5, "e1")); data.B.Add(Obj(key, 5, "e2")); }
            else { data.A.Add(Obj(key)); data.B.Add(Obj(key)); }
        }
        return data;
    }

    private static IEnumerable<string> HierarchicalKeys(int folders, int perFolder) =>
        Enumerable.Range(0, folders).SelectMany(d => Enumerable.Range(0, perFolder).Select(i => $"dir{d:000}/file_{i:0000}.dat"));

    private static IEnumerable<string> FlatKeys(int count) => Enumerable.Range(0, count).Select(i => $"file_{i:00000}.dat");

    /// <summary>One range (no planning) unless a plan is given; short split timings so tests do not wait seconds.</summary>
    private static BucketComparisonEngine Engine(
        BucketPair data, IS3Lister lister, TempDir dir, Action<ScanPerformanceOptions>? tune = null,
        string? csvPath = null, ScanCheckpoint? resume = null, IReadOnlyList<ScanPartition>? plan = null, bool detectGaps = false)
    {
        var options = new ScanPerformanceOptions
        {
            MaxParallelPartitions = 4,
            MaxListRequestsPerSecond = 0,
            MaxDiscoveryRequests = 0, // no plan: the whole keyspace is one range, so only splitting can parallelise it
            MinRangeAgeForSplit = TimeSpan.FromMilliseconds(30),
            SplitIdlePoll = TimeSpan.FromMilliseconds(10)
        };
        tune?.Invoke(options);
        return new BucketComparisonEngine(Config("bucket-a"), Config("bucket-b"), lister, lister, ComparisonMode.ListingOnly, csvPath, resume, detectGaps, 25, options)
        {
            CheckpointBaseDirectory = dir.Path,
            PartitionPlanOverride = plan,
            ListPageSize = 10,
        };
    }

    private static async Task<(ComparisonStatistics Stats, List<ObjectDifference> Diffs)> RunSingleStreamAsync(BucketPair data, bool detectGaps = false)
    {
        var diffs = new List<ObjectDifference>();
        var engine = new BucketComparisonEngine(new MockS3ObjectStream(data.A), new MockS3ObjectStream(data.B), ComparisonMode.ListingOnly, detectSequenceGaps: detectGaps);
        engine.DifferenceFound += diffs.Add;
        return (await engine.RunAsync(), diffs);
    }

    private static async Task<(ComparisonStatistics Stats, List<ObjectDifference> Diffs, BucketComparisonEngine Engine)> RunSplitAsync(
        BucketPair data, FakeS3Lister lister, TempDir dir, Action<ScanPerformanceOptions>? tune = null,
        IReadOnlyList<ScanPartition>? plan = null, bool detectGaps = false)
    {
        var diffs = new List<ObjectDifference>();
        var engine = Engine(data, lister, dir, tune, plan: plan, detectGaps: detectGaps);
        engine.DifferenceFound += diffs.Add;
        var stats = await engine.RunAsync().WaitAsync(TimeSpan.FromSeconds(60));
        return (stats, diffs, engine);
    }

    private static string Fingerprint(ObjectDifference d) =>
        $"{d.Type}|{d.Key}|{d.BucketASize}|{d.BucketBSize}|{d.BucketAETag}|{d.BucketBETag}|{d.Details}";

    private static List<string> Fingerprints(IEnumerable<ObjectDifference> diffs) =>
        diffs.Select(Fingerprint).OrderBy(s => s, StringComparer.Ordinal).ToList();

    private static void AssertSameCounters(ComparisonStatistics expected, ComparisonStatistics actual)
    {
        Assert.Equal(expected.ObjectsCompared, actual.ObjectsCompared);
        Assert.Equal(expected.MatchedCount, actual.MatchedCount);
        Assert.Equal(expected.TotalDifferences, actual.TotalDifferences);
        Assert.Equal(expected.MissingInBucketACount, actual.MissingInBucketACount);
        Assert.Equal(expected.MissingInBucketBCount, actual.MissingInBucketBCount);
        Assert.Equal(expected.SizeMismatchCount, actual.SizeMismatchCount);
        Assert.Equal(expected.ETagMismatchCount, actual.ETagMismatchCount);
        Assert.Equal(expected.SequenceGapCount, actual.SequenceGapCount);
    }

    private static string Describe(IEnumerable<ScanPartition> plan) => string.Join(" ", plan);

    [Fact]
    public async Task OneBigRange_IsSplitByIdleWorkers_AndMatchesSingleStream()
    {
        using var dir = new TempDir();
        var data = Generate(71, HierarchicalKeys(40, 60));
        var expected = await RunSingleStreamAsync(data);

        var lister = data.ToLister(TimeSpan.FromMilliseconds(2));
        var actual = await RunSplitAsync(data, lister, dir);

        var plan = actual.Engine.CurrentPlan;
        Assert.True(plan.Count > 1, "idle workers split the single range");
        Assert.True(PartitionPlanner.TilesKeyspace(plan), Describe(plan));
        Assert.All(plan, r => Assert.True(r.Completed));
        Assert.True(lister.MaxInFlight > 2, "the halves were listed at the same time");
        Assert.Equal(plan.Count - 1, actual.Stats.PartitionSplits);
        Assert.Equal(plan.Count, actual.Stats.PartitionsTotal);
        Assert.Equal(plan.Count, actual.Stats.PartitionsCompleted);
        AssertSameCounters(expected.Stats, actual.Stats);
        Assert.Equal(Fingerprints(expected.Diffs), Fingerprints(actual.Diffs));
        Assert.Contains("split by idle workers", DiagnosticsFormatter.Format(actual.Stats));
    }

    [Fact]
    public async Task OneBigFlatRange_IsSplitByProbing_AndMatchesSingleStream()
    {
        using var dir = new TempDir();
        var data = Generate(72, FlatKeys(6000));
        var expected = await RunSingleStreamAsync(data);

        var actual = await RunSplitAsync(data, data.ToLister(TimeSpan.FromMilliseconds(2)), dir);

        var plan = actual.Engine.CurrentPlan;
        Assert.True(plan.Count > 1, "a flat namespace is split too (probe fallback)");
        Assert.True(PartitionPlanner.TilesKeyspace(plan), Describe(plan));
        AssertSameCounters(expected.Stats, actual.Stats);
        Assert.Equal(Fingerprints(expected.Diffs), Fingerprints(actual.Diffs));
    }

    [Fact]
    public async Task LopsidedPlan_IsBalancedBySplitting_AndMatchesSingleStream()
    {
        using var dir = new TempDir();
        var data = Generate(75, RangeSplitterTests.Lopsided());
        var expected = await RunSingleStreamAsync(data);

        // The old shallow plan: boundaries at the top folders only, so one range holds the big folder
        var shallow = PartitionPlanner.BuildRanges(new[] { "a/", "b/", "c/", "d/", "e/", "f/", "g/", "h/" }, 16);
        var actual = await RunSplitAsync(data, data.ToLister(TimeSpan.FromMilliseconds(2)), dir, plan: shallow);

        var plan = actual.Engine.CurrentPlan;
        Assert.True(plan.Count > shallow.Count, "the big folder was split: " + Describe(plan));
        Assert.Contains(plan, r => (r.Lower ?? string.Empty).StartsWith("d/CV/V_", StringComparison.Ordinal));
        Assert.True(PartitionPlanner.TilesKeyspace(plan), Describe(plan));
        AssertSameCounters(expected.Stats, actual.Stats);
        Assert.Equal(Fingerprints(expected.Diffs), Fingerprints(actual.Diffs));
    }

    [Fact]
    public async Task SplitDisabled_KeepsOneRange()
    {
        using var dir = new TempDir();
        var data = Generate(73, HierarchicalKeys(20, 40));
        var actual = await RunSplitAsync(data, data.ToLister(TimeSpan.FromMilliseconds(2)), dir, o => o.EnableRangeSplitting = false);

        Assert.Single(actual.Engine.CurrentPlan);
        Assert.Equal(0, actual.Stats.PartitionSplits);
        Assert.DoesNotContain("split by idle workers", DiagnosticsFormatter.Format(actual.Stats));
    }

    [Fact]
    public async Task CancelAfterSplits_ThenResume_GivesTheUninterruptedResult()
    {
        using var dir = new TempDir();
        var data = Generate(74, HierarchicalKeys(40, 60));
        var expected = await RunSingleStreamAsync(data);
        string csv = dir.File("live.csv");

        var lister = data.ToLister(TimeSpan.FromMilliseconds(2));
        using var cts = new CancellationTokenSource();
        var engine1 = Engine(data, lister, dir, csvPath: csv);
        int found = 0;
        engine1.DifferenceFound += _ =>
        {
            // Cancel once the range has been split more than once and about half of the differences are in.
            if (Interlocked.Increment(ref found) >= expected.Stats.TotalDifferences / 2 && engine1.CurrentPlan.Count > 2 && !cts.IsCancellationRequested)
                cts.Cancel();
        };
        await Assert.ThrowsAnyAsync<OperationCanceledException>(() => engine1.RunAsync(cts.Token).WaitAsync(TimeSpan.FromSeconds(60)));

        var checkpoint = await CheckpointManager.FindCheckpointAsync(
            "bucket-a", "https://fake.invalid", "", "bucket-b", "https://fake.invalid", "", ComparisonMode.ListingOnly, dir.Path);
        Assert.NotNull(checkpoint);
        Assert.NotNull(checkpoint.Partitions);
        Assert.True(checkpoint.Partitions.Count > 2, "the checkpoint holds the split plan");
        Assert.True(PartitionPlanner.TilesKeyspace(checkpoint.Partitions), Describe(checkpoint.Partitions));
        Assert.Contains(checkpoint.Partitions, p => !p.Completed);

        var resumed = await Engine(data, lister, dir, csvPath: csv, resume: checkpoint).RunAsync().WaitAsync(TimeSpan.FromSeconds(60));

        AssertSameCounters(expected.Stats, resumed);
        Assert.Equal(Fingerprints(expected.Diffs), Fingerprints(CsvReport.ReadDifferences(csv)));
    }

    [Fact]
    public async Task SplitWithSequenceGaps_MatchesSingleStream()
    {
        using var dir = new TempDir();
        var keys = Enumerable.Range(0, 8000).Where(i => i % 97 != 5).Select(i => $"logs/file_{i:00000}.dat").ToList();
        var data = new BucketPair();
        data.A.AddRange(keys.Select(k => Obj(k)));
        data.B.AddRange(keys.Select(k => Obj(k)));
        var expected = await RunSingleStreamAsync(data, detectGaps: true);
        Assert.True(expected.Stats.SequenceGapCount > 10);

        var actual = await RunSplitAsync(data, data.ToLister(TimeSpan.FromMilliseconds(2)), dir, detectGaps: true);

        Assert.True(actual.Engine.CurrentPlan.Count > 1);
        AssertSameCounters(expected.Stats, actual.Stats);
        Assert.Equal(expected.Stats.SequenceGapCount, actual.Stats.SequenceGapCount);
        Assert.Equal(Fingerprints(expected.Diffs), Fingerprints(actual.Diffs));
    }

    [Fact]
    public async Task ScanEndsPromptly_WhenTheLastRangeFinishesWhileWorkersAreIdle()
    {
        // Idle workers must be woken when the last busy worker leaves, not after a poll interval (here an hour).
        using var dir = new TempDir();
        var data = Generate(76, HierarchicalKeys(4, 30));
        var lister = data.ToLister(TimeSpan.FromMilliseconds(2));
        var sw = Stopwatch.StartNew();
        var actual = await RunSplitAsync(data, lister, dir, o =>
        {
            o.MinRangeAgeForSplit = TimeSpan.FromHours(1); // nothing is ever eligible for a split
            o.SplitIdlePoll = TimeSpan.FromHours(1);
        });
        Assert.Single(actual.Engine.CurrentPlan);
        Assert.True(sw.Elapsed < TimeSpan.FromSeconds(20), $"the scan took {sw.Elapsed.TotalSeconds:F1} s");
    }
}
