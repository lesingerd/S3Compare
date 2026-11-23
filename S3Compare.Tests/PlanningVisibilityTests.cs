using System.Collections.Concurrent;
using System.Diagnostics;
using S3Compare.App.Models;
using S3Compare.App.Services;
using S3Compare.App.ViewModels;

namespace S3Compare.Tests;

/// <summary>A lister whose calls, after <see cref="HangAfter"/> of them, never return and never look at the cancellation token.</summary>
public sealed class IgnoringCancellationLister : IS3Lister
{
    private readonly IS3Lister _inner;
    private readonly TaskCompletionSource<S3ListPage> _never = new(TaskCreationOptions.RunContinuationsAsynchronously);
    private int _calls;

    public IgnoringCancellationLister(IS3Lister inner, int hangAfter)
    {
        _inner = inner;
        HangAfter = hangAfter;
    }

    public int HangAfter { get; set; }
    public int HungCalls => _calls > HangAfter ? _calls - HangAfter : 0;

    public Task<S3ListPage> ListPageAsync(string bucketName, string? prefix, string? startAfter, string? continuationToken,
        string? delimiter, int maxKeys, CancellationToken cancellationToken = default)
    {
        if (Interlocked.Increment(ref _calls) > HangAfter) return _never.Task; // ignores cancellationToken on purpose
        return _inner.ListPageAsync(bucketName, prefix, startAfter, continuationToken, delimiter, maxKeys, cancellationToken);
    }

    public void Release() => _never.TrySetException(new IOException("released"));
}

public class PlannerResilienceTests
{
    private static S3ObjectSummary Obj(string key) =>
        new() { Key = key, Size = 1, ETag = "e", LastModified = new DateTime(2026, 1, 1, 0, 0, 0, DateTimeKind.Utc), StorageClass = "STANDARD" };

    private static FakeS3Lister FlatLister(int count = 3000, TimeSpan latency = default)
    {
        var keys = Enumerable.Range(0, count).Select(i => $"file_{i:D5}.dat").ToList();
        return new FakeS3Lister { Latency = latency }.SetBucket("a", keys.Select(Obj)).SetBucket("b", keys.Select(Obj));
    }

    private static PlannerBucket A(IS3Lister l) => new(l, "a", "");
    private static PlannerBucket B(IS3Lister l) => new(l, "b", "");

    [Fact]
    public async Task TimeBox_StopsPlanning_AndStillYieldsTiledRanges()
    {
        var lister = FlatLister(latency: TimeSpan.FromMilliseconds(40));
        var progress = new ConcurrentQueue<PlannerProgress>();
        var logs = new ConcurrentQueue<string>();
        var options = new PlannerOptions
        {
            MaxDuration = TimeSpan.FromMilliseconds(300),
            Concurrency = 1,
            OnProgress = progress.Enqueue,
            Log = logs.Enqueue
        };

        var watch = Stopwatch.StartNew();
        var ranges = await PartitionPlanner.PlanAsync(A(lister), B(lister), 64, 100_000, options: options).WaitAsync(TimeSpan.FromSeconds(10));
        watch.Stop();

        Assert.True(PartitionPlanner.TilesKeyspace(ranges));
        Assert.True(watch.Elapsed < TimeSpan.FromSeconds(5), $"planning took {watch.Elapsed}");
        var last = progress.Last();
        Assert.True(last.TimedOut);
        Assert.True(last.Finished);
        Assert.Contains(logs, l => l.Contains("time limit", StringComparison.OrdinalIgnoreCase));
        Assert.True(lister.CallCount < 100, $"{lister.CallCount} calls despite the time box");
    }

    [Fact]
    public async Task TimeBox_ZeroMeansUnlimited()
    {
        var lister = FlatLister();
        var progress = new ConcurrentQueue<PlannerProgress>();
        var ranges = await PartitionPlanner.PlanAsync(A(lister), B(lister), 16, 500, options: new PlannerOptions { OnProgress = progress.Enqueue });

        Assert.False(progress.Last().TimedOut);
        Assert.True(ranges.Count >= 8);
    }

    [Fact]
    public async Task ProbesRejectedForUnusualStartAfter_StillGiveAMultiRangePlan()
    {
        var lister = FlatLister();
        // The endpoint rejects some unusual StartAfter values: every fourth probe fails
        lister.Fault = call => call.StartAfter != null && call.Number % 4 == 0
            ? new InvalidOperationException("InvalidArgument: StartAfter")
            : null;
        var progress = new ConcurrentQueue<PlannerProgress>();

        var ranges = await PartitionPlanner.PlanAsync(A(lister), B(lister), 16, 500, options: new PlannerOptions { OnProgress = progress.Enqueue });

        Assert.True(PartitionPlanner.TilesKeyspace(ranges));
        Assert.True(ranges.Count >= 8, $"got {ranges.Count} ranges");
        var last = progress.Last();
        Assert.True(last.FailedRequests > 0);
        Assert.Contains("InvalidArgument", last.LastError);
    }

    [Fact]
    public async Task DelimiterListingRejected_IsLoggedOnce_AndPlanningContinuesWithProbes()
    {
        var keys = Enumerable.Range(0, 2000).Select(i => $"dir{i % 40:D2}/file{i:D5}.dat").ToList();
        var lister = new FakeS3Lister().SetBucket("a", keys.Select(Obj)).SetBucket("b", keys.Select(Obj));
        lister.Fault = call => call.Delimiter != null ? new InvalidOperationException("delimiter not supported") : null;
        var logs = new ConcurrentQueue<string>();
        var progress = new ConcurrentQueue<PlannerProgress>();

        var ranges = await PartitionPlanner.PlanAsync(A(lister), B(lister), 16, 500,
            options: new PlannerOptions { Log = logs.Enqueue, OnProgress = progress.Enqueue });

        Assert.True(ranges.Count >= 8, $"got {ranges.Count} ranges");
        Assert.True(PartitionPlanner.TilesKeyspace(ranges));
        // once per bucket, not once per failed request
        Assert.Equal(2, logs.Count(l => l.Contains("delimiter listing failed")));
        Assert.Contains("delimiter listing", progress.Last().DisabledTechniques);
        Assert.True(lister.Calls.Count(c => c.Delimiter != null) <= 4, "the disabled technique kept being used");
    }

    [Fact]
    public async Task ProbingThatAlwaysFails_IsDisabled_AndPlanningStillCompletes()
    {
        var lister = FlatLister();
        lister.Fault = call => call.StartAfter != null ? new InvalidOperationException("nope") : null;
        var progress = new ConcurrentQueue<PlannerProgress>();

        var ranges = await PartitionPlanner.PlanAsync(A(lister), B(lister), 16, 500, options: new PlannerOptions { OnProgress = progress.Enqueue });

        Assert.True(PartitionPlanner.TilesKeyspace(ranges));
        Assert.Contains("key probing", progress.Last().DisabledTechniques);
        Assert.True(lister.CallCount < 40, $"{lister.CallCount} calls after probing was disabled");
    }

    [Fact]
    public async Task HungRequests_AreAbandonedAfterTheRequestTimeout()
    {
        var inner = FlatLister();
        var hung = new IgnoringCancellationLister(inner, hangAfter: 0);
        var progress = new ConcurrentQueue<PlannerProgress>();
        var options = new PlannerOptions { RequestTimeout = TimeSpan.FromMilliseconds(150), OnProgress = progress.Enqueue };

        var watch = Stopwatch.StartNew();
        var ranges = await PartitionPlanner.PlanAsync(A(hung), B(hung), 8, 6, options: options).WaitAsync(TimeSpan.FromSeconds(15));
        watch.Stop();

        Assert.True(watch.Elapsed < TimeSpan.FromSeconds(8), $"planning took {watch.Elapsed}");
        Assert.True(PartitionPlanner.TilesKeyspace(ranges));
        Assert.Single(ranges);
        var last = progress.Last();
        Assert.True(last.FailedRequests > 0);
        Assert.Contains("no answer within", last.LastError);
    }

    [Fact]
    public async Task ScanCancellation_StillPropagates_EvenFromAHungRequest()
    {
        var hung = new IgnoringCancellationLister(FlatLister(), hangAfter: 0);
        using var cts = new CancellationTokenSource();
        var planning = PartitionPlanner.PlanAsync(A(hung), B(hung), 8, 100, cts.Token);
        await Task.Delay(150);
        Assert.False(planning.IsCompleted);

        cts.Cancel();
        await Assert.ThrowsAnyAsync<OperationCanceledException>(() => planning.WaitAsync(TimeSpan.FromSeconds(5)));
    }

    [Fact]
    public async Task Progress_IsReportedWhilePlanning()
    {
        var lister = FlatLister(latency: TimeSpan.FromMilliseconds(20));
        var progress = new ConcurrentQueue<PlannerProgress>();

        await PartitionPlanner.PlanAsync(A(lister), B(lister), 16, 500, options: new PlannerOptions { OnProgress = progress.Enqueue });

        var all = progress.ToList();
        Assert.True(all.Count >= 3, $"only {all.Count} progress reports");
        Assert.All(all, p => Assert.Equal(500, p.RequestBudget));
        Assert.Contains(all, p => p.RequestsUsed > 0 && p.Candidates > 0);
        Assert.Contains(all, p => p.Step.Contains("probe round"));
        Assert.Contains(all, p => p.Elapsed > TimeSpan.Zero);
        var last = all[^1];
        Assert.True(last.Finished);
        Assert.False(last.TimedOut);
        Assert.Equal(lister.CallCount, last.RequestsUsed);
    }

    [Fact]
    public async Task IndependentRequests_RunConcurrently_WithinTheBudget()
    {
        var lister = FlatLister(latency: TimeSpan.FromMilliseconds(30));
        await PartitionPlanner.PlanAsync(A(lister), B(lister), 16, 500, options: new PlannerOptions { Concurrency = 8 });
        Assert.True(lister.MaxInFlight >= 4, $"max in flight {lister.MaxInFlight}");
        Assert.True(lister.CallCount <= 500);

        var serial = FlatLister(latency: TimeSpan.FromMilliseconds(5));
        await PartitionPlanner.PlanAsync(A(serial), B(serial), 16, 500, options: new PlannerOptions { Concurrency = 1 });
        Assert.True(serial.MaxInFlight <= 2, $"max in flight {serial.MaxInFlight} with concurrency 1 (one per bucket)");
    }

    [Fact]
    public void MaxPlanningSeconds_DefaultsTo60_AndIsValidated()
    {
        Assert.Equal(60, new ScanPerformanceOptions().MaxPlanningSeconds);
        Assert.Empty(new ScanPerformanceOptions { MaxPlanningSeconds = 0 }.Validate());
        Assert.Single(new ScanPerformanceOptions { MaxPlanningSeconds = -1 }.Validate());
    }
}

public class EngineDiagnosticsTests
{
    private static S3ObjectSummary Obj(string key, long size = 100) =>
        new() { Key = key, Size = size, ETag = "e", LastModified = new DateTime(2026, 1, 1, 0, 0, 0, DateTimeKind.Utc), StorageClass = "STANDARD" };

    private static S3BucketConfig Config(string bucket) =>
        new() { BucketName = bucket, Prefix = "", EndpointUrl = "https://fake.invalid", Region = "us-east-1" };

    private static BucketComparisonEngine Engine(IS3Lister lister, TempDir dir, int partitions, string? csv = null, int pageSize = 10, Action<ScanPerformanceOptions>? tune = null)
    {
        var options = new ScanPerformanceOptions { MaxParallelPartitions = partitions, PrefetchPages = 1, MaxListRequestsPerSecond = 0 };
        tune?.Invoke(options);
        return new BucketComparisonEngine(Config("bucket-a"), Config("bucket-b"), lister, lister, ComparisonMode.ListingOnly, csv, null, false, 25, options)
        {
            CheckpointBaseDirectory = dir.Path,
            ListPageSize = pageSize,
            DiagnosticsLogDirectory = dir.Path
        };
    }

    private static FakeS3Lister Data(int count, int missingInB = 0, TimeSpan latency = default)
    {
        var a = Enumerable.Range(0, count).Select(i => Obj($"k{i:D5}"));
        var b = Enumerable.Range(missingInB, count - missingInB).Select(i => Obj($"k{i:D5}"));
        return new FakeS3Lister { Latency = latency }.SetBucket("bucket-a", a).SetBucket("bucket-b", b);
    }

    private static string ReadLog(TempDir dir)
    {
        var file = Directory.GetFiles(dir.Path, "scan_diagnostics_*.log").Single();
        using var stream = new FileStream(file, FileMode.Open, FileAccess.Read, FileShare.ReadWrite);
        using var reader = new StreamReader(stream);
        return reader.ReadToEnd();
    }

    [Fact]
    public async Task Scan_ReportsPhases_PlannerProgress_RequestStats_AndPartitions()
    {
        using var dir = new TempDir();
        var lister = Data(1500, missingInB: 10, latency: TimeSpan.FromMilliseconds(15));
        var engine = Engine(lister, dir, partitions: 4);
        var seen = new ConcurrentQueue<ComparisonStatistics>();
        engine.ProgressUpdated += seen.Enqueue;

        var final = await engine.RunAsync();

        Assert.Equal(10, final.MissingInBucketBCount);
        var phases = seen.Select(s => s.Phase).Distinct().ToList();
        Assert.Contains("Planning", phases);
        Assert.Contains("Scanning", phases);
        Assert.Equal("Completed", final.Phase);

        // Planner progress is visible while planning and ends finished
        Assert.Contains(seen, s => s.Phase == "Planning" && s.Planner != null && s.Planner.Step.Length > 0);
        Assert.NotNull(final.Planner);
        Assert.True(final.Planner!.Finished);
        Assert.True(final.Planner.RequestsUsed > 0);
        Assert.Equal(engine.CurrentPlan.Count, final.PartitionsTotal);

        // Per-bucket and per-partition numbers
        Assert.True(final.BucketAList.Requests > 0 && final.BucketBList.Requests > 0);
        Assert.True(final.BucketAList.AverageLatencyMs >= 10, $"avg latency {final.BucketAList.AverageLatencyMs}");
        Assert.True(final.BucketAList.LastLatencyMs > 0);
        Assert.Equal(final.BucketAList.Requests + final.BucketBList.Requests, final.TotalListRequests);
        Assert.Equal(lister.CallCount, final.TotalListRequests);
        Assert.NotEmpty(final.Partitions);
        Assert.All(final.Partitions, p => Assert.Equal("Done", p.State));
        Assert.Equal(final.ObjectsCompared, final.Partitions.Sum(p => p.ObjectsCompared));
        Assert.Contains(seen, s => s.Phase == "Scanning" && s.Partitions.Any(p => p.State == "Running"));
    }

    [Fact]
    public async Task Scan_WritesPhaseSteps_AndPlannerSummary_ToTheDiagnosticsLog()
    {
        using var dir = new TempDir();
        var engine = Engine(Data(800), dir, partitions: 4);

        await engine.RunAsync();

        string log = ReadLog(dir);
        Assert.Contains("Step: Opening the live CSV report", log);
        Assert.Contains("Step: Planning key-range partitions", log);
        Assert.Contains("Planning started:", log);
        Assert.Contains("Planning finished:", log);
        Assert.Contains("Step: Scanning", log);
        Assert.Contains("Step: Finalizing", log);
        Assert.Contains("Step: Completed", log);
    }

    [Fact]
    public async Task PlanningTimeBox_FromSettings_ScanStillFindsEveryDifference()
    {
        using var dir = new TempDir();
        var engine = Engine(Data(400, missingInB: 25, latency: TimeSpan.FromMilliseconds(200)), dir, partitions: 8,
            tune: o => { o.MaxPlanningSeconds = 1; o.MaxDiscoveryRequests = 5000; });

        var final = await engine.RunAsync();

        Assert.True(final.Planner!.TimedOut);
        Assert.Equal(25, final.MissingInBucketBCount);
        Assert.Equal(400, final.ObjectsCompared);
        Assert.True(PartitionPlanner.TilesKeyspace(engine.CurrentPlan.ToList()));
        Assert.Contains("TIME LIMIT REACHED", ReadLog(dir));
    }

    [Fact]
    public async Task Stop_IsHonoured_EvenWhenTheListerIgnoresCancellation()
    {
        using var dir = new TempDir();
        var hung = new IgnoringCancellationLister(Data(500), hangAfter: 6);
        var engine = Engine(hung, dir, partitions: 1);
        var last = new ConcurrentQueue<ComparisonStatistics>();
        engine.ProgressUpdated += last.Enqueue;
        using var cts = new CancellationTokenSource();

        var run = Task.Run(() => engine.RunAsync(cts.Token));

        // The stalled request shows up in the in-flight list with a growing age
        var deadline = DateTime.UtcNow.AddSeconds(15);
        ComparisonStatistics? stalled = null;
        while (DateTime.UtcNow < deadline && stalled == null)
        {
            await Task.Delay(200);
            stalled = last.Where(s => s.InFlight.Any(o => o.AgeSeconds > 0.5 && o.Description.StartsWith("LIST"))).LastOrDefault();
        }
        Assert.NotNull(stalled);
        Assert.Contains("bucket", stalled!.InFlight[0].Description);
        Assert.Contains("startAfter=", stalled.InFlight[0].Description);
        Assert.False(run.IsCompleted);

        cts.Cancel();
        await Assert.ThrowsAnyAsync<OperationCanceledException>(() => run.WaitAsync(TimeSpan.FromSeconds(10)));
        hung.Release();
    }

    [Fact]
    public async Task Abandon_WhenFinalizingHangs_SavesAConsistentCheckpoint_AndTheLateEngineChangesNothing()
    {
        using var dir = new TempDir();
        string csv = dir.File("report.csv");
        var engine = Engine(Data(300, missingInB: 30), dir, partitions: 1, csv: csv);
        var entered = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var release = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        engine.FinalizeHook = async () =>
        {
            entered.TrySetResult();
            await release.Task; // ignores cancellation
        };
        using var cts = new CancellationTokenSource();
        var run = Task.Run(() => engine.RunAsync(cts.Token));
        await entered.Task.WaitAsync(TimeSpan.FromSeconds(10));

        cts.Cancel();
        await Task.Delay(300);
        Assert.False(run.IsCompleted); // stuck, as the watchdog scenario

        string? checkpoint = await engine.AbandonAsync("test");

        Assert.NotNull(checkpoint);
        Assert.True(File.Exists(checkpoint));
        string log = ReadLog(dir);
        Assert.Contains("ABANDON requested", log);
        Assert.Contains("checkpoint saved", log);
        var stats = (await Task.Run(() => engine.CurrentPlan)).ToList();
        Assert.All(stats, p => Assert.True(p.Completed || p.LastCompletedKey != null));

        long csvBytesAtAbandon = new FileInfo(csv).Length;
        release.SetResult();
        await Assert.ThrowsAnyAsync<OperationCanceledException>(() => run.WaitAsync(TimeSpan.FromSeconds(10)));
        Assert.True(File.Exists(checkpoint), "an abandoned engine must not delete or rewrite the checkpoint");
        Assert.Equal(csvBytesAtAbandon, new FileInfo(csv).Length);
    }

    [Fact]
    public async Task Abandon_WhenTheWriterIsStuck_StopsFurtherCsvRows_AndReportsTheStuckWriter()
    {
        using var dir = new TempDir();
        string csv = dir.File("report.csv");
        var engine = Engine(Data(200, missingInB: 200 - 1), dir, partitions: 1, csv: csv); // nearly everything only in A
        var entered = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var release = new ManualResetEventSlim();
        engine.BeforeWriterBatch = _ =>
        {
            entered.TrySetResult();
            release.Wait(); // a disk write that never returns
        };
        int events = 0;
        engine.DifferenceFound += _ => Interlocked.Increment(ref events);
        using var cts = new CancellationTokenSource();
        var run = Task.Run(() => engine.RunAsync(cts.Token));
        await entered.Task.WaitAsync(TimeSpan.FromSeconds(10));

        cts.Cancel();
        await Task.Delay(300);
        Assert.False(run.IsCompleted);

        var watch = Stopwatch.StartNew();
        string? checkpoint = await engine.AbandonAsync("test", gateTimeout: TimeSpan.FromMilliseconds(300));
        Assert.True(watch.Elapsed < TimeSpan.FromSeconds(5));
        Assert.Null(checkpoint); // the writer holds the gate: no checkpoint can be consistent with the CSV right now
        Assert.Contains("writer is stuck", ReadLog(dir));

        release.Set();
        await Assert.ThrowsAnyAsync<OperationCanceledException>(() => run.WaitAsync(TimeSpan.FromSeconds(10)));
        Assert.Equal(0, events);
        Assert.Single(File.ReadAllLines(csv)); // header only
    }

    [Fact]
    public async Task Pause_IsReportedAsPausedPhase()
    {
        using var dir = new TempDir();
        var engine = Engine(Data(2000, latency: TimeSpan.FromMilliseconds(20)), dir, partitions: 2);
        var seen = new ConcurrentQueue<ComparisonStatistics>();
        engine.ProgressUpdated += seen.Enqueue;
        engine.Pause();

        var run = Task.Run(() => engine.RunAsync());
        await Task.Delay(1500);
        Assert.Contains(seen, s => s.IsPaused && DiagnosticsFormatter.DisplayPhase(s) == "Paused");
        engine.Resume();
        await run.WaitAsync(TimeSpan.FromSeconds(30));
    }

    [Fact]
    public void StatusMessage_NamesThePhase_AndPlannerProgress_AndAStalledRequest()
    {
        var planning = new ComparisonStatistics
        {
            Phase = "Planning",
            Planner = new PlannerProgress { Step = "A: delimiter depth 2/3 (4 folders)", RequestsUsed = 12, RequestBudget = 500, Candidates = 7, Elapsed = TimeSpan.FromSeconds(9), TimeLimit = TimeSpan.FromSeconds(60) }
        };
        string text = MainViewModel.BuildStatusMessage(planning);
        Assert.StartsWith("[PLANNING]", text);
        Assert.Contains("12/500", text);
        Assert.Contains("candidates 7", text);
        Assert.Contains("9s of 60s", text);

        var scanning = new ComparisonStatistics { Phase = "Scanning", ObjectsCompared = 1234, PartitionsTotal = 8, PartitionsCompleted = 2, ActivePartitions = 4,
            InFlight = new() { new InFlightOperationInfo { Description = "LIST A bucket 'x'", AgeSeconds = 31, Attempt = 2 } } };
        text = MainViewModel.BuildStatusMessage(scanning);
        Assert.StartsWith("[SCANNING]", text);
        Assert.Contains("waiting on: LIST A bucket 'x' | age 31.0s | attempt 2", text);

        Assert.StartsWith("[PAUSED]", MainViewModel.BuildStatusMessage(new ComparisonStatistics { Phase = "Scanning", IsPaused = true }));
        Assert.StartsWith("[FINALIZING]", MainViewModel.BuildStatusMessage(new ComparisonStatistics { Phase = "Finalizing", LatestStep = "draining" }));
    }

    [Fact]
    public async Task MeteredLister_CountsLatency_AndHonoursCancellationOfAHungInnerCall()
    {
        var metrics = new ListMetrics();
        var inFlight = new InFlightOperations();
        var hung = new IgnoringCancellationLister(Data(10), hangAfter: 1);
        var metered = new MeteredLister(hung, "A", metrics, inFlight);

        await metered.ListPageAsync("bucket-a", "", null, null, null, 5);
        var stats = metrics.Snapshot(0);
        Assert.Equal(1, stats.Requests);

        using var cts = new CancellationTokenSource(TimeSpan.FromMilliseconds(200));
        var call = metered.ListPageAsync("bucket-a", "p/", "p/x", null, "/", 5, cts.Token);
        await Task.Delay(50);
        var op = Assert.Single(inFlight.Snapshot());
        Assert.Contains("prefix='p/'", op.Description);
        Assert.Contains("startAfter='p/x'", op.Description);
        Assert.Contains("delimiter='/'", op.Description);

        await Assert.ThrowsAnyAsync<OperationCanceledException>(() => call.WaitAsync(TimeSpan.FromSeconds(5)));
        Assert.Empty(inFlight.Snapshot());
    }
}
