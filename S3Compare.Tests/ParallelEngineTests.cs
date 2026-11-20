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
    public async Task VersionOneCheckpoint_ResumesAsSingleRange()
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

        Assert.Equal(1, stats.PartitionsTotal);
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
            for (int f = 0; f < 500; f++)
            {
                data.A.Add(Obj($"dir{d:D2}/file{f:D4}"));
                data.B.Add(Obj($"dir{d:D2}/file{f:D4}"));
            }
        }
        var latency = TimeSpan.FromMilliseconds(20);

        var sw = Stopwatch.StartNew();
        var single = await PartitionedEngine(data, data.ToLister(latency), dir, partitions: 1, pageSize: 500).RunAsync();
        var singleTime = sw.Elapsed;

        sw.Restart();
        var parallel = await PartitionedEngine(data, data.ToLister(latency), dir, partitions: 8, pageSize: 500).RunAsync();
        var parallelTime = sw.Elapsed;

        Assert.Equal(20000, single.MatchedCount);
        Assert.Equal(20000, parallel.MatchedCount);
        Assert.True(parallelTime.TotalMilliseconds * 2.5 < singleTime.TotalMilliseconds,
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
        var engine = PartitionedEngine(data, data.ToLister(TimeSpan.FromMilliseconds(2)), dir, partitions: 4, pageSize: 10);
        long lastCompared = 0;
        engine.ProgressUpdated += s => Interlocked.Exchange(ref lastCompared, s.ObjectsCompared);

        engine.Pause();
        var run = engine.RunAsync();
        await Task.Delay(400);
        Assert.False(run.IsCompleted);
        Assert.Equal(0, Interlocked.Read(ref lastCompared));

        engine.Resume();
        var stats = await run.WaitAsync(TimeSpan.FromSeconds(30));
        Assert.True(stats.ObjectsCompared > 0);
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
