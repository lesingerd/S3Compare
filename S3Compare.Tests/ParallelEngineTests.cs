using System.Diagnostics;
using S3Compare.App.Models;
using S3Compare.App.Services;

namespace S3Compare.Tests;

/// <summary>Two buckets' contents (full keys) plus their prefixes.</summary>
public sealed class BucketPair
{
    public string PrefixA { get; init; } = "";
    public string PrefixB { get; init; } = "";
    public List<S3ObjectSummary> A { get; } = new();
    public List<S3ObjectSummary> B { get; } = new();

    public FakeS3Lister ToLister(TimeSpan latency = default) =>
        new FakeS3Lister { Latency = latency }.SetBucket("bucket-a", A).SetBucket("bucket-b", B);

    public PlannerBucket PlannerA(IS3Lister lister) => new(lister, "bucket-a", PrefixA);

    /// <summary>Keeps one object per key (the last added), as a bucket would.</summary>
    public BucketPair Dedupe()
    {
        foreach (var list in new[] { A, B })
        {
            var unique = list.GroupBy(o => o.Key, StringComparer.Ordinal).Select(g => g.Last()).ToList();
            list.Clear();
            list.AddRange(unique);
        }
        return this;
    }
    public PlannerBucket PlannerB(IS3Lister lister) => new(lister, "bucket-b", PrefixB);
}

public sealed class TempDir : IDisposable
{
    public string Path { get; } = System.IO.Path.Combine(System.IO.Path.GetTempPath(), "S3CompareTests_" + Guid.NewGuid().ToString("N"));

    public TempDir() => Directory.CreateDirectory(Path);

    public string File(string name) => System.IO.Path.Combine(Path, name);

    public void Dispose()
    {
        try { Directory.Delete(Path, recursive: true); } catch { }
    }
}

public class ParallelEngineTests
{
    private static S3ObjectSummary Obj(string key, long size = 100, string etag = "e") =>
        new() { Key = key, Size = size, ETag = etag, LastModified = new DateTime(2026, 1, 1, 0, 0, 0, DateTimeKind.Utc), StorageClass = "STANDARD" };

    private static S3BucketConfig Config(string bucket, string prefix) =>
        new() { BucketName = bucket, Prefix = prefix, EndpointUrl = "https://fake.invalid", Region = "us-east-1" };

    private static BucketComparisonEngine PartitionedEngine(
        BucketPair data,
        IS3Lister lister,
        TempDir dir,
        int partitions,
        string? csvPath = null,
        ScanCheckpoint? resume = null,
        IReadOnlyList<ScanPartition>? plan = null,
        int pageSize = 50,
        bool detectGaps = false,
        ComparisonMode mode = ComparisonMode.ListingOnly,
        Action<ScanPerformanceOptions>? tune = null)
    {
        var options = new ScanPerformanceOptions { MaxParallelPartitions = partitions, MaxListRequestsPerSecond = 0 };
        tune?.Invoke(options);
        return new BucketComparisonEngine(
            Config("bucket-a", data.PrefixA), Config("bucket-b", data.PrefixB), lister, lister,
            mode, csvPath, resume, detectGaps, 25, options)
        {
            CheckpointBaseDirectory = dir.Path,
            PartitionPlanOverride = plan,
            ListPageSize = pageSize
        };
    }

    /// <summary>Reference result: the classic single merge-join over two plain streams.</summary>
    private static async Task<(ComparisonStatistics Stats, List<ObjectDifference> Diffs)> RunSingleStreamAsync(BucketPair data, bool detectGaps = false)
    {
        var diffs = new List<ObjectDifference>();
        var engine = new BucketComparisonEngine(
            new MockS3ObjectStream(data.A), new MockS3ObjectStream(data.B), ComparisonMode.ListingOnly,
            detectSequenceGaps: detectGaps, prefixA: data.PrefixA, prefixB: data.PrefixB);
        engine.DifferenceFound += diffs.Add;
        var stats = await engine.RunAsync();
        return (stats, diffs);
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

    /// <summary>
    /// Random mix of folders, flat keys and unicode, with objects only in A, only in B, size and ETag mismatches,
    /// and runs of consecutive keys missing from one side.
    /// </summary>
    private static BucketPair RandomData(int seed, int count, string prefixA = "", string prefixB = "")
    {
        var rnd = new Random(seed);
        var relative = new HashSet<string>(StringComparer.Ordinal);
        string[] tops = { "alpha", "beta", "gamma", "delta", "logs", "img", "é", "中文", "\U0001F600", "zz" };
        while (relative.Count < count)
        {
            int shape = rnd.Next(10);
            string key = shape switch
            {
                < 6 => $"{tops[rnd.Next(tops.Length)]}/{rnd.Next(8)}/file_{rnd.Next(100000):D5}.bin",
                < 8 => $"{tops[rnd.Next(tops.Length)]}/{rnd.Next(100000):D5}",
                _ => $"flat_{rnd.Next(1000000):D6}"
            };
            relative.Add(key);
        }

        var sorted = relative.OrderBy(k => k, Utf8KeyComparer.Instance).ToList();
        var data = new BucketPair { PrefixA = prefixA, PrefixB = prefixB };
        int missingRunB = 0, missingRunA = 0;
        foreach (var key in sorted)
        {
            if (missingRunA == 0 && missingRunB == 0)
            {
                int roll = rnd.Next(1000);
                if (roll < 4) missingRunB = rnd.Next(5, 40);
                else if (roll < 8) missingRunA = rnd.Next(5, 40);
            }

            long size = rnd.Next(1, 1000);
            if (missingRunB > 0) { missingRunB--; data.A.Add(Obj(prefixA + key, size)); continue; }
            if (missingRunA > 0) { missingRunA--; data.B.Add(Obj(prefixB + key, size)); continue; }

            int kind = rnd.Next(100);
            if (kind < 4) data.A.Add(Obj(prefixA + key, size));
            else if (kind < 8) data.B.Add(Obj(prefixB + key, size));
            else
            {
                data.A.Add(Obj(prefixA + key, size, "e1"));
                long sizeB = kind < 11 ? size + 1 : size;
                string etagB = kind is >= 11 and < 14 ? "e2" : "e1";
                data.B.Add(Obj(prefixB + key, sizeB, etagB));
            }
        }
        return data;
    }

    /// <summary>Puts differences exactly on and right around every plan boundary, and missing runs straddling them.</summary>
    private static void PlantBoundaryDifferences(BucketPair data, IReadOnlyList<ScanPartition> plan, int seed)
    {
        var rnd = new Random(seed);
        foreach (var boundary in plan.Where(p => p.Upper != null).Select(p => p.Upper!))
        {
            switch (rnd.Next(4))
            {
                case 0:
                    // A key exactly equal to the boundary, only in A
                    data.A.Add(Obj(data.PrefixA + boundary, 7));
                    break;
                case 1:
                    // Exactly on the boundary in both, but different sizes; plus the next key only in B
                    data.A.Add(Obj(data.PrefixA + boundary, 7));
                    data.B.Add(Obj(data.PrefixB + boundary, 8));
                    data.B.Add(Obj(data.PrefixB + boundary + " ", 1));
                    break;
                case 2:
                    // A run missing from B that spans the boundary
                    foreach (var suffix in new[] { "", "0", "1", "2" })
                        data.A.Add(Obj(data.PrefixA + boundary + suffix, 3));
                    data.A.Add(Obj(data.PrefixA + boundary.Substring(0, boundary.Length - 1) + " ", 3));
                    break;
                default:
                    // A run missing from A that spans the boundary
                    foreach (var suffix in new[] { "", "a", "b" })
                        data.B.Add(Obj(data.PrefixB + boundary + suffix, 3));
                    data.B.Add(Obj(data.PrefixB + boundary.Substring(0, boundary.Length - 1) + " ", 3));
                    break;
            }
        }
    }

    [Theory]
    [InlineData(1)]
    [InlineData(3)]
    [InlineData(17)]
    public async Task Partitioned_MatchesSingleStream_WithDifferencesOnBoundaries(int partitions)
    {
        using var dir = new TempDir();
        var data = RandomData(seed: 1000 + partitions, count: 4000);
        var lister = data.ToLister();

        // Plan on the base data, then plant differences right on the chosen boundaries and run with that plan
        var plan = partitions == 1
            ? new List<ScanPartition> { new() }
            : await PartitionPlanner.PlanAsync(data.PlannerA(lister), data.PlannerB(lister), partitions * 4, 500);
        PlantBoundaryDifferences(data, plan, seed: partitions);
        data.Dedupe();
        lister = data.ToLister();

        var expected = await RunSingleStreamAsync(data);

        var diffs = new List<ObjectDifference>();
        var engine = PartitionedEngine(data, lister, dir, partitions, plan: plan, pageSize: 37);
        engine.DifferenceFound += diffs.Add;
        var stats = await engine.RunAsync();

        AssertSameCounters(expected.Stats, stats);
        Assert.Equal(Fingerprints(expected.Diffs), Fingerprints(diffs));
        Assert.Equal(diffs.Count, diffs.Select(d => d.Id).Distinct().Count());
        Assert.Equal(plan.Count, stats.PartitionsTotal);
        Assert.Equal(plan.Count, stats.PartitionsCompleted);
        Assert.Equal(data.A.Count, stats.BucketAObjectsRead);
        Assert.Equal(data.B.Count, stats.BucketBObjectsRead);
    }

    [Theory]
    [InlineData(3, "", "")]
    [InlineData(17, "", "")]
    [InlineData(8, "src/", "backup/2026/")]
    public async Task Partitioned_WithItsOwnPlan_MatchesSingleStream(int partitions, string prefixA, string prefixB)
    {
        using var dir = new TempDir();
        var data = RandomData(seed: 77 + partitions, count: 3000, prefixA, prefixB);
        // Objects outside the prefixes must be ignored by both
        data.A.Add(Obj("unrelated/x"));
        data.B.Add(Obj("aaa-outside"));

        var expected = await RunSingleStreamAsync(new BucketPair
        {
            PrefixA = prefixA,
            PrefixB = prefixB
        }.With(data.A.Where(o => o.Key.StartsWith(prefixA)), data.B.Where(o => o.Key.StartsWith(prefixB))));

        var diffs = new List<ObjectDifference>();
        var engine = PartitionedEngine(data, data.ToLister(), dir, partitions);
        engine.DifferenceFound += diffs.Add;
        var stats = await engine.RunAsync();

        Assert.True(stats.PartitionsTotal > 1, "expected the planner to split the keyspace");
        AssertSameCounters(expected.Stats, stats);
        Assert.Equal(Fingerprints(expected.Diffs), Fingerprints(diffs));
    }

    [Fact]
    public async Task SequenceGaps_AreStillFoundWithinRanges()
    {
        using var dir = new TempDir();
        var data = new BucketPair();
        foreach (var folder in new[] { "a", "b", "c", "d" })
        {
            foreach (int i in new[] { 1, 2, 4, 5 })
            {
                data.A.Add(Obj($"{folder}/part_{i:D2}.csv"));
                data.B.Add(Obj($"{folder}/part_{i:D2}.csv"));
            }
        }

        var diffs = new List<ObjectDifference>();
        var plan = PartitionPlanner.BuildRanges(new[] { "a/", "b/", "c/", "d/" }, 10);
        var engine = PartitionedEngine(data, data.ToLister(), dir, partitions: 2, detectGaps: true, plan: plan);
        engine.DifferenceFound += diffs.Add;
        var stats = await engine.RunAsync();

        Assert.Equal(5, stats.PartitionsTotal);
        Assert.Equal(4, stats.SequenceGapCount);
        Assert.Equal(new[] { "a/part_03.csv", "b/part_03.csv", "c/part_03.csv", "d/part_03.csv" }, diffs.Select(d => d.Key).OrderBy(k => k));
    }

    [Theory]
    [InlineData(40)]
    [InlineData(120)]
    public async Task CancelWithPartitionsInFlight_ThenResume_GivesSameCsvAsUninterruptedRun(int cancelAtCall)
    {
        using var dir = new TempDir();
        var data = RandomData(seed: 4242, count: 4000);
        string interruptedCsv = dir.File("interrupted.csv");
        string referenceCsv = dir.File("reference.csv");

        // Uninterrupted reference
        var referenceStats = await PartitionedEngine(data, data.ToLister(), dir, 4, referenceCsv, pageSize: 25).RunAsync();

        // Run 1: cancelled from inside the listing while several partitions are mid-range
        using var cts = new CancellationTokenSource();
        var slow = data.ToLister(TimeSpan.FromMilliseconds(2));
        slow.OnCall = call => { if (call.Number == cancelAtCall) cts.Cancel(); };
        await Assert.ThrowsAnyAsync<OperationCanceledException>(() =>
            PartitionedEngine(data, slow, dir, 4, interruptedCsv, pageSize: 25).RunAsync(cts.Token));

        var checkpoint = await CheckpointManager.FindCheckpointAsync(
            "bucket-a", "https://fake.invalid", "", "bucket-b", "https://fake.invalid", "", ComparisonMode.ListingOnly, dir.Path);
        Assert.NotNull(checkpoint);
        Assert.Equal(2, checkpoint.Version);
        Assert.NotNull(checkpoint.Partitions);
        Assert.True(checkpoint.Partitions.Count > 1);
        Assert.True(checkpoint.HasProgress);
        Assert.Contains(checkpoint.Partitions, p => !p.Completed);
        Assert.True(PartitionPlanner.TilesKeyspace(checkpoint.Partitions));

        // Run 2: resume the persisted plan (with different performance settings, which must not matter)
        var resumed = PartitionedEngine(data, data.ToLister(), dir, 2, interruptedCsv, resume: checkpoint, pageSize: 40);
        var resumedStats = await resumed.RunAsync();

        AssertSameCounters(referenceStats, resumedStats);
        var interruptedRows = CsvReport.ReadDifferences(interruptedCsv).ToList();
        var referenceRows = CsvReport.ReadDifferences(referenceCsv).ToList();
        Assert.Equal(Fingerprints(referenceRows), Fingerprints(interruptedRows));
        Assert.Equal(interruptedRows.Count, interruptedRows.Select(Fingerprint).Distinct().Count());
        Assert.Equal(interruptedRows.Count, interruptedRows.Select(d => d.Id).Distinct().Count());

        // A completed scan removes its checkpoint
        Assert.Null(await CheckpointManager.FindCheckpointAsync(
            "bucket-a", "https://fake.invalid", "", "bucket-b", "https://fake.invalid", "", ComparisonMode.ListingOnly, dir.Path));
    }

    [Fact]
    public async Task VersionOneCheckpoint_IsReplannedAfterItsKey_AndResumesInParallel()
    {
        using var dir = new TempDir();
        var data = new BucketPair();
        data.A.AddRange(Enumerable.Range(0, 1000).Select(i => Obj($"k{i:D4}")));

        var oldCheckpoint = new ScanCheckpoint
        {
            Version = 1,
            BucketNameA = "bucket-a",
            BucketNameB = "bucket-b",
            LastEvaluatedKey = "k0499",
            ObjectsCompared = 500,
            MissingInBucketBCount = 500,
            TotalDifferences = 500
        };

        var diffs = new List<ObjectDifference>();
        var engine = PartitionedEngine(data, data.ToLister(), dir, partitions: 8, resume: oldCheckpoint);
        engine.DifferenceFound += diffs.Add;
        var stats = await engine.RunAsync();

        Assert.True(stats.PartitionsTotal >= 8, $"expected the rest of the keyspace to be split, got {stats.PartitionsTotal} range(s)");
        Assert.Equal(stats.PartitionsTotal, stats.PartitionsCompleted);
        Assert.Equal(1000, stats.ObjectsCompared);
        Assert.Equal(1000, stats.MissingInBucketBCount);
        Assert.Equal(500, diffs.Count);
        Assert.All(diffs, d => Assert.True(string.CompareOrdinal(d.Key, "k0499") > 0));
        Assert.All(diffs, d => Assert.True(d.Id > 500));
    }

    [Fact]
    public async Task DamagedCheckpointPlan_IsRejectedWithAMessage()
    {
        using var dir = new TempDir();
        var data = new BucketPair();
        var broken = new ScanCheckpoint
        {
            BucketNameA = "bucket-a",
            BucketNameB = "bucket-b",
            Partitions = new List<ScanPartition> { new() { Upper = "m" }, new() { Lower = "n" } }   // gap between m and n
        };

        var ex = await Assert.ThrowsAsync<InvalidOperationException>(() =>
            PartitionedEngine(data, data.ToLister(), dir, 4, resume: broken).RunAsync());
        Assert.Contains("Discard the checkpoint", ex.Message);
    }

    [Fact]
    public async Task ListingFailureInOnePartition_FailsTheScan_AndKeepsACheckpoint()
    {
        using var dir = new TempDir();
        var data = RandomData(seed: 9, count: 2000);
        var lister = data.ToLister(TimeSpan.FromMilliseconds(1));
        lister.Fault = call => call.Number == 40 ? new IOException("connection reset") : null;

        var ex = await Assert.ThrowsAsync<IOException>(() =>
            PartitionedEngine(data, lister, dir, 4, dir.File("live.csv"), pageSize: 20).RunAsync().WaitAsync(TimeSpan.FromSeconds(30)));
        Assert.Equal("connection reset", ex.Message);

        var checkpoint = await CheckpointManager.FindCheckpointAsync(
            "bucket-a", "https://fake.invalid", "", "bucket-b", "https://fake.invalid", "", ComparisonMode.ListingOnly, dir.Path);
        Assert.NotNull(checkpoint);
        Assert.True(checkpoint.HasProgress);
    }

    [Fact]
    public async Task PrefetchAndPartitions_AreSeveralTimesFasterThanOneStream_WithPageLatency()
    {
        using var dir = new TempDir();
        var data = new BucketPair();
        for (int d = 0; d < 40; d++)
        {
            for (int f = 0; f < 250; f++)
            {
                data.A.Add(Obj($"dir{d:D2}/file{f:D4}"));
                data.B.Add(Obj($"dir{d:D2}/file{f:D4}"));
            }
        }
        // Plenty of latency headroom so CPU contention on a slow CI agent cannot hide the effect
        var latency = TimeSpan.FromMilliseconds(40);

        var sw = Stopwatch.StartNew();
        var single = await PartitionedEngine(data, data.ToLister(latency), dir, partitions: 1, pageSize: 250).RunAsync();
        var singleTime = sw.Elapsed;

        sw.Restart();
        var parallel = await PartitionedEngine(data, data.ToLister(latency), dir, partitions: 8, pageSize: 250).RunAsync();
        var parallelTime = sw.Elapsed;

        Assert.Equal(10000, single.MatchedCount);
        Assert.Equal(10000, parallel.MatchedCount);
        Assert.True(parallelTime.TotalMilliseconds * 2.0 < singleTime.TotalMilliseconds,
            $"8 partitions took {parallelTime.TotalMilliseconds:F0} ms vs {singleTime.TotalMilliseconds:F0} ms for one");
    }

    [Fact]
    public async Task SinglePartition_ListsBothBucketsConcurrently()
    {
        using var dir = new TempDir();
        var data = new BucketPair();
        data.A.AddRange(Enumerable.Range(0, 200).Select(i => Obj($"k{i:D4}")));
        data.B.AddRange(Enumerable.Range(0, 200).Select(i => Obj($"k{i:D4}")));
        var lister = data.ToLister(TimeSpan.FromMilliseconds(30));

        await PartitionedEngine(data, lister, dir, partitions: 1, pageSize: 20).RunAsync();

        Assert.True(lister.MaxInFlight >= 2, $"max in-flight was {lister.MaxInFlight}");
    }

    [Fact]
    public async Task MaxObjectsPerSecond_IsRoughlyRespected()
    {
        using var dir = new TempDir();
        var data = new BucketPair();
        data.A.AddRange(Enumerable.Range(0, 3000).Select(i => Obj($"d{i % 10}/k{i:D5}")));
        data.B.AddRange(data.A.Select(o => Obj(o.Key)));

        var sw = Stopwatch.StartNew();
        var stats = await PartitionedEngine(data, data.ToLister(), dir, partitions: 4, tune: o => o.MaxObjectsPerSecond = 5000).RunAsync();

        // 3000 objects at 5000/s with a 0.1 s burst: ~0.5 s
        Assert.Equal(3000, stats.ObjectsCompared);
        Assert.InRange(sw.Elapsed.TotalSeconds, 0.3, 10);
    }

    [Fact]
    public async Task MaxListRequestsPerSecond_IsSharedAcrossPartitions()
    {
        using var dir = new TempDir();
        var data = new BucketPair();
        data.A.AddRange(Enumerable.Range(0, 600).Select(i => Obj($"d{i % 6}/k{i:D4}")));
        data.B.AddRange(data.A.Select(o => Obj(o.Key)));
        var lister = data.ToLister();

        var sw = Stopwatch.StartNew();
        await PartitionedEngine(data, lister, dir, partitions: 4, pageSize: 10, tune: o => o.MaxListRequestsPerSecond = 40).RunAsync();

        // ~62 requests per bucket against a 40/s budget with a 1 s burst
        int requestsA = lister.CallsFor("bucket-a");
        Assert.True(requestsA > 60, $"requests {requestsA}");
        Assert.InRange(sw.Elapsed.TotalSeconds, (requestsA - 40) / 40.0 * 0.6, 30);
    }

    [Fact]
    public async Task FullData_ContentChecksAcrossPartitions_AreBoundedGlobally()
    {
        using var dir = new TempDir();
        var data = new BucketPair();
        data.A.AddRange(Enumerable.Range(0, 64).Select(i => Obj($"d{i % 16:D2}/k{i:D3}", 10, "a-1")));
        data.B.AddRange(Enumerable.Range(0, 64).Select(i => Obj($"d{i % 16:D2}/k{i:D3}", 10, "b-1")));

        int inFlight = 0, maxInFlight = 0;
        var engine = PartitionedEngine(data, data.ToLister(), dir, partitions: 8, mode: ComparisonMode.FullData,
            tune: o => o.MaxConcurrentContentChecks = 2);
        engine.ContentComparer = async (_, _, ct) =>
        {
            int now = Interlocked.Increment(ref inFlight);
            InterlockedMax(ref maxInFlight, now);
            await Task.Delay(5, ct);
            Interlocked.Decrement(ref inFlight);
            return true;
        };

        var stats = await engine.RunAsync();

        Assert.Equal(64, stats.MatchedCount);
        Assert.InRange(maxInFlight, 1, 2);
    }

    [Fact]
    public async Task PauseAffectsAllPartitions()
    {
        using var dir = new TempDir();
        var data = RandomData(seed: 5, count: 3000);
        var lister = data.ToLister(TimeSpan.FromMilliseconds(2));
        var engine = PartitionedEngine(data, lister, dir, partitions: 4, pageSize: 10);
        long lastCompared = 0;
        engine.ProgressUpdated += s => Interlocked.Exchange(ref lastCompared, s.ObjectsCompared);

        engine.Pause();
        var run = engine.RunAsync();
        await Task.Delay(400);
        Assert.False(run.IsCompleted);
        Assert.Equal(0, Interlocked.Read(ref lastCompared));
        Assert.Equal(0, lister.CallCount);   // not even the planner lists while paused

        engine.Resume();
        var stats = await run.WaitAsync(TimeSpan.FromSeconds(30));
        Assert.True(stats.ObjectsCompared > 0);
    }

    [Fact]
    public async Task FolderMarkerKey_EqualToPrefix_IsProcessedOnce_AndNotRepeatedOnResume()
    {
        using var dir = new TempDir();
        var data = new BucketPair { PrefixA = "data/", PrefixB = "data/" };
        data.A.AddRange(new[] { Obj("data/"), Obj("data/a"), Obj("data/b") });

        // Fresh partitioned run: the marker (relative key "") is compared exactly once
        var fresh = new List<ObjectDifference>();
        var engine = PartitionedEngine(data, data.ToLister(), dir, partitions: 4);
        engine.DifferenceFound += fresh.Add;
        await engine.RunAsync();
        Assert.Equal(new[] { "data/", "data/a", "data/b" }, fresh.Select(d => d.Key).OrderBy(k => k, StringComparer.Ordinal));

        // Resume right after the marker: StartAfter must be "data/", not "start from the beginning"
        var checkpoint = new ScanCheckpoint
        {
            BucketNameA = "bucket-a", BucketNameB = "bucket-b", PrefixA = "data/", PrefixB = "data/",
            Partitions = new List<ScanPartition> { new() { LastCompletedKey = "" } },
            ObjectsCompared = 1, MissingInBucketBCount = 1, TotalDifferences = 1, LastDifferenceId = 1
        };
        var resumedDiffs = new List<ObjectDifference>();
        var resumed = PartitionedEngine(data, data.ToLister(), dir, partitions: 4, resume: checkpoint);
        resumed.DifferenceFound += resumedDiffs.Add;
        var stats = await resumed.RunAsync();

        Assert.Equal(new[] { "data/a", "data/b" }, resumedDiffs.Select(d => d.Key).OrderBy(k => k, StringComparer.Ordinal));
        Assert.Equal(3, stats.ObjectsCompared);
        Assert.Equal(3, stats.MissingInBucketBCount);
    }

    [Fact]
    public async Task ResumedRange_ContinuesTheSequenceGapSeriesOfItsLastKey()
    {
        using var dir = new TempDir();
        var data = new BucketPair();
        foreach (var n in new[] { 1, 2, 4, 5 })
        {
            data.A.Add(Obj($"part_{n:D2}"));
            data.B.Add(Obj($"part_{n:D2}"));
        }
        var checkpoint = new ScanCheckpoint
        {
            BucketNameA = "bucket-a", BucketNameB = "bucket-b",
            Partitions = new List<ScanPartition> { new() { LastCompletedKey = "part_02" } },
            ObjectsCompared = 2, MatchedCount = 2
        };

        var diffs = new List<ObjectDifference>();
        var engine = PartitionedEngine(data, data.ToLister(), dir, partitions: 4, resume: checkpoint, detectGaps: true);
        engine.DifferenceFound += diffs.Add;
        var stats = await engine.RunAsync();

        // Same as an uninterrupted run: part_03 is missing between part_02 (before the stop) and part_04 (after)
        Assert.Equal(1, stats.SequenceGapCount);
        Assert.Equal("part_03", Assert.Single(diffs).Key);
    }

    [Fact]
    public async Task MaxObjectsPerSecond_StillApplies_WithManySmallRanges()
    {
        using var dir = new TempDir();
        var data = new BucketPair();
        data.A.AddRange(Enumerable.Range(0, 3000).Select(i => Obj($"k{i:D5}")));
        data.B.AddRange(data.A.Select(o => Obj(o.Key)));

        // 75 ranges of 40 keys, each smaller than the limiter's batch (5000 / 20, capped at 100 objects)
        var plan = PartitionPlanner.BuildRanges(Enumerable.Range(1, 74).Select(i => $"k{i * 40:D5}"), 1000);
        Assert.Equal(75, plan.Count);

        var sw = Stopwatch.StartNew();
        var stats = await PartitionedEngine(data, data.ToLister(), dir, partitions: 4, plan: plan, tune: o => o.MaxObjectsPerSecond = 5000).RunAsync();

        Assert.Equal(3000, stats.ObjectsCompared);
        Assert.InRange(sw.Elapsed.TotalSeconds, 0.3, 15);
    }

    [Fact]
    public async Task UserStop_IsReportedAsCancellation_EvenWhenRequestsFailBecauseOfIt()
    {
        using var dir = new TempDir();
        var data = RandomData(seed: 31, count: 2000);
        using var cts = new CancellationTokenSource();
        var lister = data.ToLister(TimeSpan.FromMilliseconds(1));
        lister.OnCall = call => { if (call.Number == 40) cts.Cancel(); };
        // Requests aborted by the stop fail with a transport error rather than OperationCanceledException
        lister.Fault = call => call.Number >= 40 ? new IOException("connection aborted") : null;

        await Assert.ThrowsAnyAsync<OperationCanceledException>(() =>
            PartitionedEngine(data, lister, dir, 4, pageSize: 20).RunAsync(cts.Token).WaitAsync(TimeSpan.FromSeconds(30)));
    }

    [Fact]
    public async Task CrashBetweenCsvFlushAndCheckpointSave_ResumeTruncatesTheExtraRows()
    {
        using var dir = new TempDir();
        var data = RandomData(seed: 2718, count: 4000);
        string interruptedCsv = dir.File("interrupted.csv");
        string referenceCsv = dir.File("reference.csv");

        var referenceStats = await PartitionedEngine(data, data.ToLister(), dir, 4, referenceCsv, pageSize: 25).RunAsync();

        using var cts = new CancellationTokenSource();
        var slow = data.ToLister(TimeSpan.FromMilliseconds(2));
        slow.OnCall = call => { if (call.Number == 80) cts.Cancel(); };
        await Assert.ThrowsAnyAsync<OperationCanceledException>(() =>
            PartitionedEngine(data, slow, dir, 4, interruptedCsv, pageSize: 25).RunAsync(cts.Token));

        var checkpoint = await CheckpointManager.FindCheckpointAsync(
            "bucket-a", "https://fake.invalid", "", "bucket-b", "https://fake.invalid", "", ComparisonMode.ListingOnly, dir.Path);
        Assert.NotNull(checkpoint);
        Assert.Equal(checkpoint.LiveReportCsvBytes, new FileInfo(interruptedCsv).Length);

        // Simulate rows that reached the CSV after the last checkpoint was saved (the process died in between):
        // copies of rows already there, plus a torn final line
        var existing = await File.ReadAllLinesAsync(interruptedCsv);
        var replay = existing.Skip(Math.Max(1, existing.Length - 15)).ToList();
        await File.AppendAllTextAsync(interruptedCsv, string.Join("\n", replay) + "\n" + "999999,\"torn", CsvReport.Utf8WithBom);
        Assert.True(new FileInfo(interruptedCsv).Length > checkpoint.LiveReportCsvBytes);

        var resumedStats = await PartitionedEngine(data, data.ToLister(), dir, 4, interruptedCsv, resume: checkpoint, pageSize: 25).RunAsync();

        AssertSameCounters(referenceStats, resumedStats);
        var rows = CsvReport.ReadDifferences(interruptedCsv).ToList();
        Assert.Equal(Fingerprints(CsvReport.ReadDifferences(referenceCsv)), Fingerprints(rows));
        Assert.Equal(rows.Count, rows.Select(d => d.Id).Distinct().Count());
    }

    [Fact]
    public async Task WriterFailure_StopsAllPartitions_AndSurfacesTheError()
    {
        using var dir = new TempDir();
        var data = RandomData(seed: 12, count: 3000);
        var engine = PartitionedEngine(data, data.ToLister(TimeSpan.FromMilliseconds(1)), dir, 8, dir.File("live.csv"), pageSize: 10);
        engine.BeforeWriterBatch = n => { if (n == 5) throw new IOException("disk full"); };

        var ex = await Assert.ThrowsAsync<IOException>(() => engine.RunAsync().WaitAsync(TimeSpan.FromSeconds(30)));
        Assert.Equal("disk full", ex.Message);

        // The CSV no longer matches the ranges' progress, so no checkpoint is written for this failure
        Assert.Null(await CheckpointManager.FindCheckpointAsync(
            "bucket-a", "https://fake.invalid", "", "bucket-b", "https://fake.invalid", "", ComparisonMode.ListingOnly, dir.Path));
    }

    [Fact]
    public void InvalidPerformanceOptions_AreRejected()
    {
        Assert.Throws<ArgumentException>(() => new BucketComparisonEngine(
            Config("a", ""), Config("b", ""), ComparisonMode.ListingOnly,
            performance: new ScanPerformanceOptions { MaxParallelPartitions = 0 }));
    }

    private static void InterlockedMax(ref int target, int value)
    {
        int seen;
        while (value > (seen = Volatile.Read(ref target)) && Interlocked.CompareExchange(ref target, value, seen) != seen)
        {
        }
    }
}

internal static class BucketPairExtensions
{
    public static BucketPair With(this BucketPair pair, IEnumerable<S3ObjectSummary> a, IEnumerable<S3ObjectSummary> b)
    {
        pair.A.AddRange(a);
        pair.B.AddRange(b);
        return pair;
    }
}
