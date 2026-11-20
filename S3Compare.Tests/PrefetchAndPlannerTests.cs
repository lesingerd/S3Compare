using System.Diagnostics;
using S3Compare.App.Models;
using S3Compare.App.Services;

namespace S3Compare.Tests;

public class PrefetchingObjectStreamTests
{
    private static List<S3ObjectSummary> Keys(int count, string format = "k{0:D5}") =>
        Enumerable.Range(0, count).Select(i => new S3ObjectSummary { Key = string.Format(format, i), Size = i, ETag = "e" }).ToList();

    private static async Task<List<string>> DrainAsync(IS3ObjectStream stream)
    {
        var keys = new List<string>();
        while (await stream.MoveNextAsync()) keys.Add(stream.Current!.Key);
        return keys;
    }

    [Fact]
    public async Task YieldsEveryKeyInOrder_AcrossPages_AndCountsOnConsumption()
    {
        var lister = new FakeS3Lister().SetBucket("b", Keys(2500));
        var counters = new StreamReadCounters();
        await using var stream = new PrefetchingObjectStream(lister, "b", "", pageSize: 100, sharedCounters: counters);

        var keys = await DrainAsync(stream);

        Assert.Equal(Keys(2500).Select(k => k.Key), keys);
        Assert.Equal(2500, stream.TotalObjectsRead);
        Assert.Equal(2500, counters.Objects);
        Assert.Equal(Enumerable.Range(0, 2500).Sum(i => (long)i), stream.TotalBytesRead);
        Assert.False(stream.HasMore);
        Assert.Equal(25, lister.CallCount);
    }

    [Fact]
    public async Task HonoursStartAfterAndInclusiveUpperBound_AndStopsListingAtTheBound()
    {
        var lister = new FakeS3Lister().SetBucket("b", Keys(100, "data/k{0:D3}"));
        await using var stream = new PrefetchingObjectStream(lister, "b", "data/", startAfter: "data/k010", upperRelativeInclusive: "k020", pageSize: 5);

        var keys = await DrainAsync(stream);

        Assert.Equal(Enumerable.Range(11, 10).Select(i => $"data/k{i:D3}"), keys);
        Assert.InRange(lister.CallCount, 2, 4);   // not the 18 pages that remain after k010
    }

    [Fact]
    public async Task RequestsNextPagesBeforeTheConsumerAsks_BoundedByPrefetchPages()
    {
        var lister = new FakeS3Lister { Latency = TimeSpan.FromMilliseconds(10) }.SetBucket("b", Keys(500));
        await using var stream = new PrefetchingObjectStream(lister, "b", "", prefetchPages: 2, pageSize: 10);

        Assert.True(await stream.MoveNextAsync());
        await Task.Delay(400);

        // Page 1 consumed, 2 buffered, and one more fetched and waiting for room in the channel
        Assert.InRange(lister.CallCount, 3, 4);
    }

    [Fact]
    public async Task ProducerException_SurfacesToConsumer_AfterEarlierPages()
    {
        var lister = new FakeS3Lister
        {
            Fault = call => call.Number == 3 ? new InvalidOperationException("listing exploded") : null
        }.SetBucket("b", Keys(100));
        await using var stream = new PrefetchingObjectStream(lister, "b", "", pageSize: 10);

        int consumed = 0;
        var ex = await Assert.ThrowsAsync<InvalidOperationException>(async () =>
        {
            while (await stream.MoveNextAsync()) consumed++;
        });

        Assert.Equal("listing exploded", ex.Message);
        Assert.Equal(20, consumed);
    }

    [Fact]
    public async Task DisposeWhileProducerIsBlocked_DoesNotHang()
    {
        // Producer stuck in a slow request
        var slow = new FakeS3Lister { Latency = TimeSpan.FromSeconds(30) }.SetBucket("b", Keys(10));
        var stream1 = new PrefetchingObjectStream(slow, "b", "");
        await Task.Delay(50);
        await stream1.DisposeAsync().AsTask().WaitAsync(TimeSpan.FromSeconds(5));

        // Producer stuck on a full channel nobody reads
        var fast = new FakeS3Lister().SetBucket("b", Keys(1000));
        var stream2 = new PrefetchingObjectStream(fast, "b", "", prefetchPages: 1, pageSize: 10);
        await Task.Delay(100);
        await stream2.DisposeAsync().AsTask().WaitAsync(TimeSpan.FromSeconds(5));
        Assert.InRange(fast.CallCount, 1, 3);
    }

    [Fact]
    public async Task CancellingMoveNext_ThrowsPromptly()
    {
        var lister = new FakeS3Lister { Latency = TimeSpan.FromSeconds(30) }.SetBucket("b", Keys(10));
        await using var stream = new PrefetchingObjectStream(lister, "b", "");
        using var cts = new CancellationTokenSource(TimeSpan.FromMilliseconds(100));

        await Assert.ThrowsAnyAsync<OperationCanceledException>(() => stream.MoveNextAsync(cts.Token).WaitAsync(TimeSpan.FromSeconds(5)));
    }

    [Fact]
    public async Task CancelledListing_IsNeverReportedAsEndOfStream()
    {
        var lister = new FakeS3Lister { Latency = TimeSpan.FromMilliseconds(20) }.SetBucket("b", Keys(1000));
        using var scanCts = new CancellationTokenSource();
        await using var stream = new PrefetchingObjectStream(lister, "b", "", pageSize: 10, cancellationToken: scanCts.Token);

        Assert.True(await stream.MoveNextAsync());
        scanCts.Cancel();

        // The consumer's own token is not cancelled, but the listing was: it must not look complete
        await Assert.ThrowsAnyAsync<OperationCanceledException>(async () =>
        {
            while (await stream.MoveNextAsync()) { }
        });
    }

    [Fact]
    public async Task TruncatedPageWithoutToken_Throws()
    {
        var lister = new TruncatedWithoutTokenLister();
        await using var stream = new PrefetchingObjectStream(lister, "b", "");
        await Assert.ThrowsAsync<InvalidOperationException>(async () => await DrainAsync(stream));
    }

    private sealed class TruncatedWithoutTokenLister : IS3Lister
    {
        public Task<S3ListPage> ListPageAsync(string bucketName, string? prefix, string? startAfter, string? continuationToken, string? delimiter, int maxKeys, CancellationToken cancellationToken = default)
            => Task.FromResult(new S3ListPage { Objects = { new S3ObjectSummary { Key = "a" } }, IsTruncated = true });
    }
}

public class TokenBucketRateLimiterTests
{
    [Fact]
    public async Task LongRunRate_IsRoughlyRespected()
    {
        var limiter = new TokenBucketRateLimiter(ratePerSecond: 100, capacity: 10);
        var sw = Stopwatch.StartNew();
        for (int i = 0; i < 40; i++)
        {
            await limiter.WaitAsync();
        }
        // 10 free (burst) + 30 at 100/s = ~300ms
        Assert.InRange(sw.ElapsedMilliseconds, 200, 3000);
    }

    [Fact]
    public async Task BatchesLargerThanCapacity_WaitForTheirDebt()
    {
        var limiter = new TokenBucketRateLimiter(ratePerSecond: 1000, capacity: 10);
        var sw = Stopwatch.StartNew();
        await limiter.WaitAsync(10);    // burst
        await limiter.WaitAsync(300);   // ~300ms of debt
        Assert.InRange(sw.ElapsedMilliseconds, 200, 3000);
    }

    [Fact]
    public void ZeroRate_MeansUnlimited()
    {
        Assert.Null(TokenBucketRateLimiter.Create(0));
        Assert.True(((TokenBucketRateLimiter?)null).WaitIfLimitedAsync(1000).IsCompletedSuccessfully);
    }
}

public class PartitionPlannerTests
{
    private static S3ObjectSummary Obj(string key) => new() { Key = key, Size = 1, ETag = "e" };

    private static void AssertTilesAndCovers(IReadOnlyList<ScanPartition> ranges, IEnumerable<string> relativeKeys)
    {
        Assert.True(PartitionPlanner.TilesKeyspace(ranges), "ranges must tile the keyspace: " + string.Join(" ", ranges));
        foreach (var key in relativeKeys)
        {
            Assert.Equal(1, ranges.Count(r => PartitionPlanner.Contains(r, key)));
        }
    }

    [Fact]
    public void BuildRanges_DedupesSortsAndTiles()
    {
        var single = PartitionPlanner.BuildRanges(Array.Empty<string>(), 32);
        Assert.Single(single);
        Assert.Null(single[0].Lower);
        Assert.Null(single[0].Upper);

        var ranges = PartitionPlanner.BuildRanges(new[] { "b", "a", "a", "", "c" }, 10);
        Assert.Equal(new[] { "(-inf, a]", "(a, b]", "(b, c]", "(c, +inf]" }, ranges.Select(r => r.ToString()));

        var even = PartitionPlanner.BuildRanges(Enumerable.Range(0, 1000).Select(i => $"k{i:D4}"), 8);
        Assert.Equal(8, even.Count);
        Assert.True(PartitionPlanner.TilesKeyspace(even));
    }

    [Fact]
    public async Task Hierarchical_UsesCommonPrefixesOfBothBuckets()
    {
        var keysA = Enumerable.Range(0, 20).SelectMany(d => Enumerable.Range(0, 5).Select(f => $"d{d:D2}/f{f}")).ToList();
        var keysB = Enumerable.Range(20, 20).SelectMany(d => Enumerable.Range(0, 5).Select(f => $"d{d:D2}/f{f}")).ToList();
        var lister = new FakeS3Lister().SetBucket("a", keysA.Select(Obj)).SetBucket("b", keysB.Select(Obj));

        var ranges = await PartitionPlanner.PlanAsync(new PlannerBucket(lister, "a", ""), new PlannerBucket(lister, "b", ""), 32, 500);

        Assert.Equal(32, ranges.Count);
        AssertTilesAndCovers(ranges, keysA.Concat(keysB));
        Assert.All(ranges.Where(r => r.Upper != null), r => Assert.EndsWith("/", r.Upper));
        Assert.Equal(2, lister.CallCount);   // one delimiter listing per bucket was enough
    }

    [Fact]
    public async Task Hierarchical_DescendsWhenTopLevelHasTooFewFolders()
    {
        var keys = new[] { "x", "y" }.SelectMany(top => Enumerable.Range(0, 20).Select(i => $"{top}/sub{i:D2}/file")).ToList();
        var lister = new FakeS3Lister().SetBucket("a", keys.Select(Obj)).SetBucket("b", keys.Select(Obj));

        var ranges = await PartitionPlanner.PlanAsync(new PlannerBucket(lister, "a", ""), new PlannerBucket(lister, "b", ""), 16, 500);

        Assert.Equal(16, ranges.Count);
        AssertTilesAndCovers(ranges, keys);
        Assert.Contains(ranges, r => r.Upper != null && r.Upper.StartsWith("y/sub"));
    }

    [Fact]
    public async Task FlatNamespace_FallsBackToProbes()
    {
        var keys = Enumerable.Range(0, 5000).Select(i => (i * 7919 % 100000).ToString("D5")).Distinct().ToList();
        var lister = new FakeS3Lister().SetBucket("a", keys.Select(Obj)).SetBucket("b", keys.Select(Obj));

        var ranges = await PartitionPlanner.PlanAsync(new PlannerBucket(lister, "a", ""), new PlannerBucket(lister, "b", ""), 32, 500);

        Assert.True(ranges.Count >= 16, $"expected the probes to find many boundaries, got {ranges.Count}");
        AssertTilesAndCovers(ranges, keys);
        Assert.Contains(lister.Calls, c => c.MaxKeys == 1 && c.StartAfter != null);
        Assert.True(lister.CallCount <= 500);
    }

    [Fact]
    public async Task DiscoveryBudget_IsNeverExceeded_AndPlanStillTiles()
    {
        var keys = Enumerable.Range(0, 3000).Select(i => $"{(char)('a' + i % 26)}{i:D5}").ToList();
        var lister = new FakeS3Lister().SetBucket("a", keys.Select(Obj)).SetBucket("b", keys.Select(Obj));

        var ranges = await PartitionPlanner.PlanAsync(new PlannerBucket(lister, "a", ""), new PlannerBucket(lister, "b", ""), 64, 5);

        Assert.True(lister.CallCount <= 5);
        AssertTilesAndCovers(ranges, keys);
    }

    [Fact]
    public async Task EmptyBuckets_GiveOneUnboundedRange()
    {
        var lister = new FakeS3Lister().SetBucket("a", Array.Empty<S3ObjectSummary>()).SetBucket("b", Array.Empty<S3ObjectSummary>());

        var ranges = await PartitionPlanner.PlanAsync(new PlannerBucket(lister, "a", ""), new PlannerBucket(lister, "b", ""), 32, 500);

        var only = Assert.Single(ranges);
        Assert.Null(only.Lower);
        Assert.Null(only.Upper);
    }

    [Fact]
    public async Task DifferentPrefixes_ProduceRelativeBoundaries()
    {
        var relative = Enumerable.Range(0, 30).SelectMany(d => new[] { $"d{d:D2}/a", $"d{d:D2}/b" }).ToList();
        var lister = new FakeS3Lister()
            .SetBucket("a", relative.Select(k => Obj("in/" + k)).Append(Obj("other/zzz")))
            .SetBucket("b", relative.Select(k => Obj("out/x/" + k)).Append(Obj("aaa")));

        var ranges = await PartitionPlanner.PlanAsync(new PlannerBucket(lister, "a", "in/"), new PlannerBucket(lister, "b", "out/x/"), 16, 500);

        Assert.True(ranges.Count > 4);
        AssertTilesAndCovers(ranges, relative);
        Assert.All(ranges.Where(r => r.Upper != null), r => Assert.Matches("^d\\d\\d/$", r.Upper));
    }

    [Fact]
    public async Task UnicodeKeys_AreSplitInUtf8Order()
    {
        var tops = new[] { "alpha", "é", "ü", "中文", "日本", "\U0001F600", "！", "zeta", "Ω", "ß" };
        var keys = tops.SelectMany(t => Enumerable.Range(0, 3).Select(i => $"{t}/{i}")).ToList();
        var lister = new FakeS3Lister().SetBucket("a", keys.Select(Obj)).SetBucket("b", keys.Select(Obj));

        var ranges = await PartitionPlanner.PlanAsync(new PlannerBucket(lister, "a", ""), new PlannerBucket(lister, "b", ""), 8, 500);

        Assert.Equal(8, ranges.Count);
        AssertTilesAndCovers(ranges, keys);
    }

    [Fact]
    public void PerformanceOptions_Validate()
    {
        Assert.Empty(new ScanPerformanceOptions().Validate());
        var bad = new ScanPerformanceOptions { MaxParallelPartitions = 0, PrefetchPages = 0, MaxListRequestsPerSecond = -1, MaxObjectsPerSecond = -5, MaxConcurrentContentChecks = 0, MaxDiscoveryRequests = -1 };
        Assert.Equal(6, bad.Validate().Count);
        Assert.Equal(32, new ScanPerformanceOptions().TargetPartitions);
        Assert.Equal(24, new ScanPerformanceOptions().RequiredConnectionsPerServer);
    }
}
