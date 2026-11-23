using System.Collections.Concurrent;
using S3Compare.App.Converters;
using S3Compare.App.Models;
using S3Compare.App.Services;
using S3Compare.App.ViewModels;

namespace S3Compare.Tests;

/// <summary>Wraps a lister; the listed calls report a failed first attempt (as AmazonS3Lister does when it retries) and then succeed.</summary>
public sealed class RetryingLister : IS3Lister
{
    private readonly IS3Lister _inner;
    private readonly Func<int, bool> _retryOnCall;
    private int _calls;

    public RetryingLister(IS3Lister inner, Func<int, bool> retryOnCall)
    {
        _inner = inner;
        _retryOnCall = retryOnCall;
    }

    public Task<S3ListPage> ListPageAsync(string bucketName, string? prefix, string? startAfter, string? continuationToken,
        string? delimiter, int maxKeys, CancellationToken cancellationToken = default)
    {
        if (_retryOnCall(Interlocked.Increment(ref _calls)))
            InFlightOperations.Current?.NoteFailedAttempt("TimeoutException: LIST request timed out after 30 s");
        return _inner.ListPageAsync(bucketName, prefix, startAfter, continuationToken, delimiter, maxKeys, cancellationToken);
    }
}

public class IssueLogTests
{
    [Fact]
    public void Summary_CountsEachOutcome_AndFormatsTheLine()
    {
        var log = new IssueLog();
        log.Add(IssueOutcome.Recovered, "a");
        log.Add(IssueOutcome.Recovered, "b");
        log.Add(IssueOutcome.Skipped, "c");

        var s = log.Snapshot();
        Assert.Equal("Issues: 0 failed \u00B7 2 recovered \u00B7 1 skipped \u00B7 0 degraded \u00B7 0 recorded", s.Line);
        Assert.Equal(3, s.Total);
    }

    [Fact]
    public void MostSevere_FollowsSeverity_ThenRecency()
    {
        var log = new IssueLog();
        log.Add(IssueOutcome.Failed, "old failure");
        log.Add(IssueOutcome.Recovered, "newer but harmless");
        Assert.Equal("old failure", log.Snapshot().MostSevere!.Detail);

        log.Add(IssueOutcome.Failed, "newer failure");
        Assert.Equal("newer failure", log.Snapshot().MostSevere!.Detail);

        var order = new IssueLog();
        order.Add(IssueOutcome.Recovered, "r");
        order.Add(IssueOutcome.Skipped, "s");
        Assert.Equal(IssueOutcome.Skipped, order.Snapshot().MostSevere!.Outcome);
        order.Add(IssueOutcome.Recorded, "rec");
        Assert.Equal(IssueOutcome.Recorded, order.Snapshot().MostSevere!.Outcome);
        order.Add(IssueOutcome.Degraded, "d");
        Assert.Equal(IssueOutcome.Degraded, order.Snapshot().MostSevere!.Outcome);
        order.Add(IssueOutcome.Failed, "f");
        Assert.Equal(IssueOutcome.Failed, order.Snapshot().MostSevere!.Outcome);
    }

    [Fact]
    public void Recent_IsNewestFirst_AndKeepsTenWhileCountsStayExact()
    {
        var log = new IssueLog();
        for (int i = 1; i <= 25; i++) log.Add(IssueOutcome.Skipped, $"n{i}");

        var s = log.Snapshot();
        Assert.Equal(25, s.Skipped);
        Assert.Equal(10, s.Recent.Count);
        Assert.Equal("n25", s.Recent[0].Detail);
        Assert.Equal("n16", s.Recent[^1].Detail);
    }

    [Fact]
    public void Display_StartsWithTheOutcomeWord_AfterTimeAndMarker()
    {
        var entry = new IssueLog().Add(IssueOutcome.Recovered, "x");
        Assert.Matches("^\\[\\d\\d:\\d\\d:\\d\\d\\] \u2713 RECOVERED x$", entry.Display);
        Assert.Contains("SKIPPED", new IssueLog().Add(IssueOutcome.Skipped, "x").Display);
        Assert.Contains("DEGRADED", new IssueLog().Add(IssueOutcome.Degraded, "x").Display);
        Assert.Contains("RECORDED", new IssueLog().Add(IssueOutcome.Recorded, "x").Display);
        Assert.Contains("FAILED", new IssueLog().Add(IssueOutcome.Failed, "x").Display);
    }

    [Fact]
    public void OnlyFailed_IsRed()
    {
        Assert.Same(IssueBrushes.Failed, IssueBrushes.For(IssueOutcome.Failed));
        Assert.Same(IssueBrushes.Warning, IssueBrushes.For(IssueOutcome.Degraded));
        Assert.Same(IssueBrushes.Warning, IssueBrushes.For(IssueOutcome.Recorded));
        Assert.NotSame(IssueBrushes.Failed, IssueBrushes.For(IssueOutcome.Skipped));
        Assert.NotSame(IssueBrushes.Failed, IssueBrushes.For(IssueOutcome.Recovered));
        Assert.NotSame(IssueBrushes.Warning, IssueBrushes.For(IssueOutcome.Recovered));
    }

    [Fact]
    public void Formatter_ShowsSummary_MostSevere_RecentIssues_AndBucketRetries()
    {
        var log = new IssueLog();
        log.Add(IssueOutcome.Recovered, "A LIST: recovered on attempt 2 \u2014 no impact");
        log.Add(IssueOutcome.Skipped, "planner request skipped");
        var stats = new ComparisonStatistics
        {
            Phase = "Scanning",
            Issues = log.Snapshot(),
            BucketAList = new ListRequestStats { Requests = 10, Retried = 2, Recovered = 1, Failures = 1 }
        };

        string text = DiagnosticsFormatter.Format(stats);

        Assert.Contains("Issues: 0 failed \u00B7 1 recovered \u00B7 1 skipped \u00B7 0 degraded \u00B7 0 recorded", text);
        Assert.Contains("Most severe      : [", text);
        Assert.Contains("SKIPPED planner request skipped", text);
        Assert.Contains("Recent issues", text);
        Assert.Contains("retried 2 (recovered 1) \u00B7 failed 1", text);
        Assert.DoesNotContain("Recent issues", DiagnosticsFormatter.Format(stats, includeIssues: false));
    }

    [Fact]
    public void ViewModel_ShowsIssues_AndOnlyAFailedStatusIsRed()
    {
        Exception? exception = null;
        var thread = new Thread(() =>
        {
            try
            {
                var vm = new MainViewModel();
                var log = new IssueLog();
                log.Add(IssueOutcome.Recovered, "LIST A: recovered on attempt 2 \u2014 no impact");
                vm.ApplyStatistics(new ComparisonStatistics { Phase = "Scanning", Issues = log.Snapshot() });

                Assert.True(vm.HasIssues);
                Assert.Contains("0 failed \u00B7 1 recovered", vm.IssueStatusText);
                Assert.Contains("RECOVERED", vm.IssueStatusText);
                Assert.Same(IssueBrushes.Recovered, vm.IssueBrush);
                Assert.Single(vm.RecentIssues);

                log.Add(IssueOutcome.Failed, "range 3: IOException \u2014 the scan was stopped");
                vm.ApplyStatistics(new ComparisonStatistics { Phase = "Scanning", Issues = log.Snapshot() });
                Assert.Same(IssueBrushes.Failed, vm.IssueBrush);
                Assert.Contains("FAILED", vm.IssueStatusText);

                vm.StatusMessage = "FAILED: the comparison was stopped by an error";
                Assert.Same(IssueBrushes.Failed, vm.StatusBrush);
                vm.StatusMessage = "Comparison Complete!";
                Assert.NotSame(IssueBrushes.Failed, vm.StatusBrush);
            }
            catch (Exception ex)
            {
                exception = ex;
            }
        });
        thread.SetApartmentState(ApartmentState.STA);
        thread.Start();
        thread.Join();
        if (exception != null) throw new Exception(exception.Message, exception);
    }
}

public class IssueClassificationTests
{
    private static S3ObjectSummary Obj(string key, long size = 100, string etag = "e") =>
        new() { Key = key, Size = size, ETag = etag, LastModified = new DateTime(2026, 1, 1, 0, 0, 0, DateTimeKind.Utc), StorageClass = "STANDARD" };

    private static S3BucketConfig Config(string bucket) =>
        new() { BucketName = bucket, Prefix = "", EndpointUrl = "https://fake.invalid", Region = "us-east-1" };

    private static BucketComparisonEngine Engine(IS3Lister lister, TempDir dir, int partitions)
    {
        var options = new ScanPerformanceOptions { MaxParallelPartitions = partitions, PrefetchPages = 1, MaxListRequestsPerSecond = 0 };
        return new BucketComparisonEngine(Config("bucket-a"), Config("bucket-b"), lister, lister, ComparisonMode.ListingOnly, null, null, false, 25, options)
        {
            CheckpointBaseDirectory = dir.Path,
            ListPageSize = 10,
            DiagnosticsLogDirectory = dir.Path
        };
    }

    private static FakeS3Lister Data(int count)
    {
        var keys = Enumerable.Range(0, count).Select(i => Obj($"k{i:D5}")).ToList();
        return new FakeS3Lister().SetBucket("bucket-a", keys).SetBucket("bucket-b", keys);
    }

    private static string ReadLog(TempDir dir)
    {
        var file = Directory.GetFiles(dir.Path, "scan_diagnostics_*.log").Single();
        using var stream = new FileStream(file, FileMode.Open, FileAccess.Read, FileShare.ReadWrite);
        using var reader = new StreamReader(stream);
        return reader.ReadToEnd();
    }

    [Fact]
    public async Task ListRetrySuccess_IsRecovered_NotAFailure()
    {
        using var dir = new TempDir();
        var lister = new RetryingLister(Data(200), call => call == 3);
        var engine = Engine(lister, dir, partitions: 1);

        var final = await engine.RunAsync().WaitAsync(TimeSpan.FromSeconds(30));

        Assert.Equal(200, final.ObjectsCompared);
        Assert.Equal(0, final.Issues.Failed);
        Assert.Equal(1, final.Issues.Recovered);
        var issue = Assert.Single(final.Issues.Recent);
        Assert.Equal(IssueOutcome.Recovered, issue.Outcome);
        Assert.Contains("RECOVERED", issue.Display);
        Assert.Contains("recovered on attempt 2 \u2014 no impact", issue.Display);
        Assert.Contains("timed out after 30 s", issue.Display);

        long retried = final.BucketAList.Retried + final.BucketBList.Retried;
        long recovered = final.BucketAList.Recovered + final.BucketBList.Recovered;
        Assert.Equal(1, retried);
        Assert.Equal(1, recovered);
        Assert.Equal(0, final.BucketAList.Failures + final.BucketBList.Failures);

        string text = DiagnosticsFormatter.Format(final);
        Assert.Contains("Issues: 0 failed \u00B7 1 recovered \u00B7 0 skipped \u00B7 0 degraded \u00B7 0 recorded", text);
        Assert.Contains("retried 1 (recovered 1) \u00B7 failed 0", text);
        Assert.Contains("RECOVERED", ReadLog(dir));
    }

    [Fact]
    public async Task PlannerRequestFailure_IsSkipped_ScanCompletes()
    {
        using var dir = new TempDir();
        var lister = Data(1500);
        // Planner probes use MaxKeys=1; the scan's own listings (page size 10) are unaffected
        lister.Fault = call => call.MaxKeys == 1 && call.Number % 3 == 0 ? new IOException("connection reset") : null;
        var engine = Engine(lister, dir, partitions: 4);

        var final = await engine.RunAsync().WaitAsync(TimeSpan.FromSeconds(60));

        Assert.Equal(1500, final.ObjectsCompared);
        Assert.Equal(0, final.Issues.Failed);
        Assert.True(final.Issues.Skipped > 0, "expected skipped planner requests");
        var skipped = final.Issues.Recent.First(i => i.Outcome == IssueOutcome.Skipped);
        Assert.Contains("SKIPPED", skipped.Display);
        Assert.Contains("planning continued; results unaffected, only split quality may be lower", skipped.Display);
        Assert.Contains("IOException", skipped.Display);
        Assert.NotEqual(IssueOutcome.Failed, final.Issues.MostSevere!.Outcome);
        Assert.Contains("SKIPPED", ReadLog(dir));
    }

    [Fact]
    public async Task DisabledDelimiter_IsSkipped_PerBucket()
    {
        var keys = Enumerable.Range(0, 2000).Select(i => Obj($"dir{i % 40:D2}/file{i:D5}.dat")).ToList();
        var lister = new FakeS3Lister().SetBucket("a", keys).SetBucket("b", keys);
        lister.Fault = call => call.Delimiter != null ? new InvalidOperationException("delimiter not supported") : null;
        var issues = new ConcurrentQueue<(IssueOutcome, string)>();

        await PartitionPlanner.PlanAsync(new PlannerBucket(lister, "a", "", null, "A"), new PlannerBucket(lister, "b", "", null, "B"), 16, 500,
            options: new PlannerOptions { OnIssue = (o, d) => issues.Enqueue((o, d)) });

        Assert.All(issues, i => Assert.Equal(IssueOutcome.Skipped, i.Item1));
        Assert.Equal(2, issues.Count(i => i.Item2.Contains("delimiter listing disabled for bucket")));
    }

    [Fact]
    public async Task PlanningTimeLimit_IsDegraded_NotFailed()
    {
        var keys = Enumerable.Range(0, 3000).Select(i => Obj($"file_{i:D5}.dat")).ToList();
        var lister = new FakeS3Lister { Latency = TimeSpan.FromMilliseconds(40) }.SetBucket("a", keys).SetBucket("b", keys);
        var issues = new ConcurrentQueue<(IssueOutcome, string)>();

        await PartitionPlanner.PlanAsync(new PlannerBucket(lister, "a", ""), new PlannerBucket(lister, "b", ""), 64, 100_000,
            options: new PlannerOptions { MaxDuration = TimeSpan.FromMilliseconds(300), Concurrency = 1, OnIssue = (o, d) => issues.Enqueue((o, d)) })
            .WaitAsync(TimeSpan.FromSeconds(10));

        var degraded = Assert.Single(issues, i => i.Item1 == IssueOutcome.Degraded);
        Assert.Contains("time limit", degraded.Item2);
        Assert.DoesNotContain(issues, i => i.Item1 == IssueOutcome.Failed);
    }

    [Fact]
    public async Task FatalListFailure_IsFailed_AndLogged()
    {
        using var dir = new TempDir();
        var lister = Data(2000);
        lister.Fault = call => call.Number >= 20 && call.MaxKeys == 10 ? new IOException("connection aborted") : null;
        var engine = Engine(lister, dir, partitions: 1);

        await Assert.ThrowsAsync<IOException>(() => engine.RunAsync().WaitAsync(TimeSpan.FromSeconds(30)));

        string log = ReadLog(dir);
        Assert.Contains("FAILED range 0: IOException: connection aborted \u2014 the scan was stopped", log);
        Assert.Contains("Issue: [", log);
    }

    [Fact]
    public async Task FullData_ContentRetry_IsRecovered_AndPerObjectErrors_AreRecorded()
    {
        var itemsA = new[] { Obj("a-flaky", 10, "x"), Obj("b-io", 10, "x"), Obj("e-ok", 10, "x") };
        var itemsB = itemsA.Select(o => Obj(o.Key, 10, "y")).ToArray();
        int flakyAttempts = 0;
        var engine = new BucketComparisonEngine(
            new MockS3ObjectStream(itemsA), new MockS3ObjectStream(itemsB), ComparisonMode.FullData, detectSequenceGaps: false)
        {
            ContentComparer = (a, b, ct) => a.Key switch
            {
                "a-flaky" when Interlocked.Increment(ref flakyAttempts) == 1 => throw new IOException("connection reset"),
                "b-io" => throw new IOException("connection reset"),
                _ => Task.FromResult(true)
            }
        };

        var stats = await engine.RunAsync();

        Assert.Equal(0, stats.Issues.Failed);
        Assert.Equal(1, stats.Issues.Recovered);
        Assert.Equal(1, stats.Issues.Recorded);
        var recorded = stats.Issues.Recent.Single(i => i.Outcome == IssueOutcome.Recorded);
        Assert.Contains("RECORDED as Content Check Error for 'b-io'; scan continued", recorded.Display);
        Assert.Equal(IssueOutcome.Recorded, stats.Issues.MostSevere!.Outcome);
        var recovered = stats.Issues.Recent.Single(i => i.Outcome == IssueOutcome.Recovered);
        Assert.Contains("a-flaky", recovered.Display);
        Assert.Contains("no impact", recovered.Display);
    }
}
