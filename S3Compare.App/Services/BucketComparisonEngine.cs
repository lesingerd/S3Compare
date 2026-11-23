using System.Collections.Concurrent;
using System.Diagnostics;
using System.IO;
using System.Runtime.ExceptionServices;
using System.Threading.Channels;
using Amazon.Runtime;
using Amazon.S3;
using Amazon.S3.Model;
using S3Compare.App.Models;

namespace S3Compare.App.Services;

/// <summary>
/// Streaming two-way merge diff of two buckets, run as independent merge-joins over key ranges.
/// </summary>
/// <remarks>
/// <para>The keyspace (relative to each bucket's own prefix) is split into ranges by <see cref="PartitionPlanner"/>; up to
/// <see cref="ScanPerformanceOptions.MaxParallelPartitions"/> workers pull ranges from a queue. Each worker lists both
/// buckets for its range through <see cref="PrefetchingObjectStream"/>s, so pages are fetched ahead and A and B are
/// listed concurrently.</para>
/// <para>Workers never touch shared output directly. They send <see cref="CommitBatch"/>es (the differences of a run of fully
/// processed keys, the counters for those keys and the range's new last completed key) to a single writer task, which
/// raises <see cref="DifferenceFound"/>, appends to the live CSV, aggregates statistics and saves checkpoints. Because
/// one thread applies whole batches, every checkpoint it takes is consistent: a range's LastCompletedKey is saved only
/// together with the CSV length that contains every difference for keys up to it.</para>
/// <para>Each range has its own <see cref="SequenceGapDetector"/>, so a numeric gap that straddles a range boundary is not
/// reported.</para>
/// </remarks>
public class BucketComparisonEngine
{
    /// <summary>Keys a worker processes before committing a batch (also committed every <see cref="CommitIntervalMs"/>).</summary>
    private const int CommitEveryKeys = 256;
    private const int CommitIntervalMs = 200;
    private const int PageEventIntervalMs = 250;

    private readonly S3BucketConfig? _configA;
    private readonly S3BucketConfig? _configB;
    private readonly IS3ObjectStream? _providedStreamA;
    private readonly IS3ObjectStream? _providedStreamB;
    private readonly IS3Lister? _injectedListerA;
    private readonly IS3Lister? _injectedListerB;
    private readonly ComparisonMode _mode;
    private readonly string? _liveReportCsvPath;
    private readonly ScanCheckpoint? _resumeCheckpoint;
    private readonly bool _detectSequenceGaps;
    private readonly int _maxSequenceGapThreshold;
    private readonly string _prefixA;
    private readonly string _prefixB;
    private readonly ScanPerformanceOptions _options;

    private readonly Stopwatch _stopwatch = new();
    private readonly Stopwatch _pauseWatch = new();
    private long _differenceIdCounter;
    private int _providedStreamsTaken;

    // Owned by the writer task while the scan runs (and by RunAsync before it starts / after it ends)
    private readonly ScanCounters _totals = new();
    private List<ScanPartition> _ranges = new();
    private string _phase = "Idle";
    private long _lastProgressMs;
    private long _lastCheckpointMs;
    private long _itemsSinceCheckpoint;
    private string _lastCheckpointDisplay = "-";
    private DateTime? _lastCheckpointTime;
    private Exception? _writerError;
    private bool _checkpointUnsafe;

    private FileStream? _csvStream;
    private StreamWriter? _csvWriter;

    // Shared by workers
    private readonly object _pauseLock = new();
    private volatile TaskCompletionSource? _pauseGate;
    private volatile string _currentKeyA = "-";
    private volatile string _currentKeyB = "-";
    private int _activePartitions;
    private readonly object _stateLock = new(); // guards _ranges and the live state of its ranges (Upper, Active, InFlightKey, ...)
    private int _splitCount;
    private readonly StreamReadCounters _readA = new();
    private readonly StreamReadCounters _readB = new();
    private readonly SemaphoreSlim _contentChecks;
    private readonly TokenBucketRateLimiter? _objectLimiter;
    private readonly int _objectBatch;
    private TokenBucketRateLimiter? _limiterA;
    private TokenBucketRateLimiter? _limiterB;
    private IAmazonS3? _clientA;
    private IAmazonS3? _clientB;
    private long _lastPageEventA;
    private long _lastPageEventB;

    // Diagnostics
    private readonly InFlightOperations _inFlight = new();
    private readonly ListMetrics _metricsA = new();
    private readonly ListMetrics _metricsB = new();
    private volatile PlannerProgress? _plannerProgress;
    private volatile string _latestStep = "-";
    private readonly IssueLog _issues = new();
    private volatile ScanDiagnosticsLogger? _diag;
    private long _lastHeartbeatLogMs;

    // Abandon support: the writer applies a batch under _writerGate; once abandoned it applies nothing more
    private readonly SemaphoreSlim _writerGate = new(1, 1);
    private volatile bool _abandoned;

    public event Action<ComparisonStatistics>? ProgressUpdated;
    public event Action<ObjectDifference>? DifferenceFound;
    public event Action<List<S3ObjectSummary>>? PageLoadedA;
    public event Action<List<S3ObjectSummary>>? PageLoadedB;

    /// <summary>
    /// Optional override for FullData byte comparison of an object pair (A item, B item). Intended for tests;
    /// when null the engine downloads both objects through its S3 clients.
    /// </summary>
    public Func<S3ObjectSummary, S3ObjectSummary, CancellationToken, Task<bool>>? ContentComparer { get; set; }

    /// <summary>Base directory for checkpoints (default: <see cref="AppPaths.DataDirectory"/>). Intended for tests.</summary>
    public string? CheckpointBaseDirectory { get; set; }

    /// <summary>Uses this plan instead of planning a fresh scan. Must tile the keyspace. Intended for tests.</summary>
    public IReadOnlyList<ScanPartition>? PartitionPlanOverride { get; set; }

    /// <summary>MaxKeys for range listings (S3 caps it at 1000). Intended for tests.</summary>
    public int ListPageSize { get; set; } = PrefetchingObjectStream.DefaultPageSize;

    /// <summary>Test hook: runs on the writer before each commit batch (1-based number); throwing simulates a disk failure.</summary>
    internal Action<int>? BeforeWriterBatch { get; set; }

    /// <summary>Test hook: awaited (ignoring cancellation) after the workers finish, simulating a hang while finalizing.</summary>
    internal Func<Task>? FinalizeHook { get; set; }

    /// <summary>The partition plan of the current (or last) run, with progress. Snapshot copy.</summary>
    public IReadOnlyList<ScanPartition> CurrentPlan { get { lock (_stateLock) return _ranges.Select(r => r.Clone()).ToList(); } }

    public BucketComparisonEngine(
        S3BucketConfig configA,
        S3BucketConfig configB,
        ComparisonMode mode,
        string? liveReportCsvPath = null,
        ScanCheckpoint? resumeCheckpoint = null,
        bool detectSequenceGaps = true,
        int maxSequenceGapThreshold = 25,
        ScanPerformanceOptions? performance = null)
        : this(configA, configB, null, null, mode, liveReportCsvPath, resumeCheckpoint, detectSequenceGaps, maxSequenceGapThreshold, performance)
    {
    }

    /// <summary>Compares through the given listers (for example in-memory fakes) instead of creating S3 clients.</summary>
    public BucketComparisonEngine(
        S3BucketConfig configA,
        S3BucketConfig configB,
        IS3Lister? listerA,
        IS3Lister? listerB,
        ComparisonMode mode,
        string? liveReportCsvPath = null,
        ScanCheckpoint? resumeCheckpoint = null,
        bool detectSequenceGaps = true,
        int maxSequenceGapThreshold = 25,
        ScanPerformanceOptions? performance = null)
    {
        _configA = configA ?? throw new ArgumentNullException(nameof(configA));
        _configB = configB ?? throw new ArgumentNullException(nameof(configB));
        if ((listerA == null) != (listerB == null))
            throw new ArgumentException("Provide listers for both buckets or for neither.");
        _injectedListerA = listerA;
        _injectedListerB = listerB;
        _prefixA = configA.Prefix ?? string.Empty;
        _prefixB = configB.Prefix ?? string.Empty;
        _mode = mode;
        _liveReportCsvPath = liveReportCsvPath;
        _resumeCheckpoint = resumeCheckpoint;
        _detectSequenceGaps = detectSequenceGaps;
        _maxSequenceGapThreshold = maxSequenceGapThreshold;
        _options = ValidatedOptions(performance);

        _contentChecks = new SemaphoreSlim(_options.MaxConcurrentContentChecks, _options.MaxConcurrentContentChecks);
        _objectLimiter = TokenBucketRateLimiter.Create(_options.MaxObjectsPerSecond, burstSeconds: 0.1);
        _objectBatch = Math.Clamp(_options.MaxObjectsPerSecond / 20, 1, 100);

        if (resumeCheckpoint != null)
        {
            InitializeFromCheckpoint(resumeCheckpoint);
        }
    }

    /// <summary>Compares two pre-built object streams as a single range (no partitioning, no checkpoints).</summary>
    public BucketComparisonEngine(
        IS3ObjectStream streamA,
        IS3ObjectStream streamB,
        ComparisonMode mode,
        string? liveReportCsvPath = null,
        bool detectSequenceGaps = true,
        int maxSequenceGapThreshold = 25,
        string prefixA = "",
        string prefixB = "",
        ScanCheckpoint? resumeCheckpoint = null)
    {
        _providedStreamA = streamA ?? throw new ArgumentNullException(nameof(streamA));
        _providedStreamB = streamB ?? throw new ArgumentNullException(nameof(streamB));
        _prefixA = prefixA ?? string.Empty;
        _prefixB = prefixB ?? string.Empty;
        _mode = mode;
        _liveReportCsvPath = liveReportCsvPath;
        _resumeCheckpoint = resumeCheckpoint;
        _detectSequenceGaps = detectSequenceGaps;
        _maxSequenceGapThreshold = maxSequenceGapThreshold;
        _options = new ScanPerformanceOptions { MaxParallelPartitions = 1 };

        _contentChecks = new SemaphoreSlim(_options.MaxConcurrentContentChecks, _options.MaxConcurrentContentChecks);
        _objectBatch = 1;

        if (resumeCheckpoint != null)
        {
            InitializeFromCheckpoint(resumeCheckpoint);
        }
    }

    private static ScanPerformanceOptions ValidatedOptions(ScanPerformanceOptions? performance)
    {
        var options = performance?.Clone() ?? new ScanPerformanceOptions();
        var errors = options.Validate();
        if (errors.Count > 0)
        {
            throw new ArgumentException("Invalid performance settings: " + string.Join(" ", errors), nameof(performance));
        }
        return options;
    }

    /// <summary>Strips the bucket's prefix so keys from buckets with different prefixes can be compared.</summary>
    public static string ToRelativeKey(string key, string prefix)
    {
        return !string.IsNullOrEmpty(prefix) && key.StartsWith(prefix, StringComparison.Ordinal)
            ? key.Substring(prefix.Length)
            : key;
    }

    /// <summary>
    /// Builds the S3 StartAfter value for a bucket from a relative key; null means "from the start of the prefix".
    /// An empty relative key is a real key (the folder marker object whose key equals the prefix), so it yields
    /// StartAfter = prefix, which excludes that marker. With no prefix an empty key cannot exist (S3 keys are never
    /// empty), so nothing needs skipping and null is returned.
    /// </summary>
    public static string? BuildStartAfter(string prefix, string? relativeKey)
    {
        if (relativeKey == null) return null;
        string startAfter = (prefix ?? string.Empty) + relativeKey;
        return startAfter.Length == 0 ? null : startAfter;
    }

    private void InitializeFromCheckpoint(ScanCheckpoint cp)
    {
        _totals.ObjectsCompared = cp.ObjectsCompared;
        _totals.MatchedCount = cp.MatchedCount;
        _totals.TotalDifferences = cp.TotalDifferences;
        _totals.MissingInBucketBCount = cp.MissingInBucketBCount;
        _totals.MissingInBucketACount = cp.MissingInBucketACount;
        _totals.SizeMismatchCount = cp.SizeMismatchCount;
        _totals.ETagMismatchCount = cp.ETagMismatchCount;
        _totals.ContentMismatchCount = cp.ContentMismatchCount;
        _totals.ContentCheckErrorCount = cp.ContentCheckErrorCount;
        _totals.SequenceGapCount = cp.SequenceGapCount;
        _differenceIdCounter = Math.Max(cp.LastDifferenceId, cp.TotalDifferences);
    }

    public void Pause()
    {
        lock (_pauseLock)
        {
            if (_pauseGate != null) return;
            _pauseGate = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
            _pauseWatch.Start();
        }
        RaiseProgress();
    }

    public void Resume()
    {
        TaskCompletionSource? gate;
        lock (_pauseLock)
        {
            gate = _pauseGate;
            if (gate == null) return;
            _pauseGate = null;
            _pauseWatch.Stop();
        }
        gate.TrySetResult();
        RaiseProgress();
    }

    public bool IsPaused => _pauseGate != null;

    private bool CanCheckpoint => _configA != null && _configB != null;

    public async Task<ComparisonStatistics> RunAsync(CancellationToken cancellationToken = default)
    {
        _stopwatch.Restart();
        lock (_pauseLock)
        {
            _pauseWatch.Reset();
            if (_pauseGate != null) _pauseWatch.Start();
        }
        _lastProgressMs = 0;
        _lastCheckpointMs = 0;
        _itemsSinceCheckpoint = 0;
        _writerError = null;
        _checkpointUnsafe = false;

        using var scanCts = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken);
        ScanDiagnosticsLogger? diagnosticsLogger = null;
        Channel<CommitBatch>? commits = null;
        Task? writerTask = null;
        bool completed = false;
        using var heartbeatCts = new CancellationTokenSource();
        Task? heartbeatTask = null;

        _abandoned = false;
        _phase = "Starting";
        _plannerProgress = null;
        _issues.Clear();
        _issues.Added = LogIssue;
        SetStep("Opening the live CSV report");
        await OpenLiveCsvAsync().ConfigureAwait(false);

        try
        {
            IS3Lister? listerA = _injectedListerA;
            IS3Lister? listerB = _injectedListerB;
            if (_providedStreamA == null)
            {
                _limiterA = TokenBucketRateLimiter.Create(_options.MaxListRequestsPerSecond);
                _limiterB = TokenBucketRateLimiter.Create(_options.MaxListRequestsPerSecond);

                if (listerA == null)
                {
                    SetStep("Creating the S3 clients");
                    // Every partition keeps one LIST in flight per bucket, plus content downloads. The clients do not retry
                    // by themselves (MaxErrorRetry 0): LIST retries are done, visibly, by AmazonS3Lister and content
                    // downloads by the engine (ContentCheckAttempts).
                    int connections = _options.RequiredConnectionsPerServer + 4;
                    _clientA = S3ClientFactory.CreateClient(_configA!, connections, maxErrorRetry: 0);
                    _clientB = S3ClientFactory.CreateClient(_configB!, connections, maxErrorRetry: 0);
                    listerA = new AmazonS3Lister(_clientA);
                    listerB = new AmazonS3Lister(_clientB);
                }

                if (listerA == null || DiagnosticsLogDirectory != null)
                {
                    diagnosticsLogger = new ScanDiagnosticsLogger(DiagnosticsLogDirectory);
                    _diag = diagnosticsLogger;
                    diagnosticsLogger.LogSessionConfig(_configA!, _configB!, _mode, _resumeCheckpoint == null ? null : "resume " + _resumeCheckpoint.DescribeProgress());
                    diagnosticsLogger.Log($"Step: {_latestStep}");
                }

                // Every request, from the planner and the scan, is metered, tracked while in flight and cancellable
                listerA = new MeteredLister(listerA!, "A", _metricsA, _inFlight, _issues);
                listerB = new MeteredLister(listerB!, "B", _metricsB, _inFlight, _issues);
            }

            heartbeatTask = Task.Run(() => RunHeartbeatAsync(heartbeatCts.Token));

            _phase = "Planning";
            SetStep("Planning key-range partitions");
            RaiseProgress();
            var plannedRanges = await BuildPlanAsync(listerA, listerB, scanCts.Token).ConfigureAwait(false);
            lock (_stateLock)
            {
                _ranges = plannedRanges;
                _splitCount = 0;
            }
            SetStep($"Plan ready: {plannedRanges.Count:N0} range(s), {plannedRanges.Count(r => r.Completed):N0} already complete");

            if (diagnosticsLogger != null)
            {
                await diagnosticsLogger.LogInfoAsync(
                    $"Partition plan: {plannedRanges.Count} range(s), {plannedRanges.Count(r => r.Completed)} already complete; " +
                    $"MaxParallelPartitions={_options.MaxParallelPartitions}, PrefetchPages={_options.PrefetchPages}, " +
                    $"MaxListRequestsPerSecond={_options.MaxListRequestsPerSecond}, MaxObjectsPerSecond={_options.MaxObjectsPerSecond}, " +
                    $"MaxConcurrentContentChecks={_options.MaxConcurrentContentChecks}").ConfigureAwait(false);
            }

            commits = Channel.CreateBounded<CommitBatch>(new BoundedChannelOptions(1024)
            {
                SingleReader = true,
                FullMode = BoundedChannelFullMode.Wait
            });
            var commitChannel = commits;
            writerTask = Task.Run(() => RunWriterAsync(commitChannel, diagnosticsLogger, scanCts));

            _phase = "Scanning";
            SetStep($"Scanning: starting up to {_options.MaxParallelPartitions} worker(s)");
            ConcurrentQueue<ScanPartition> pending;
            lock (_stateLock) pending = new ConcurrentQueue<ScanPartition>(_ranges.Where(r => !r.Completed));
            // With work stealing every configured worker runs even when fewer ranges are left (a resumed plan, a lopsided
            // plan): the idle ones split the running ranges.
            bool splitting = SplittingEnabled;
            int workerCount = pending.IsEmpty ? 0 : Math.Min(_options.MaxParallelPartitions, splitting ? int.MaxValue : pending.Count);
            if (splitting && workerCount > 0)
            {
                diagnosticsLogger?.Log($"Work stealing: {workerCount} worker(s) for {pending.Count} unfinished key range(s); idle workers split running ranges.");
            }
            Exception? firstError = null;
            int busyWorkers = 0; // workers holding (or about to hold, or splitting for) a range

            // Runs one range; false when it failed for real (the whole scan is then being stopped)
            async Task<bool> RunRangeGuardedAsync(ScanPartition range, ObjectTokenAccount objectTokens)
            {
                try
                {
                    await ProcessRangeAsync(range, listerA, listerB, commitChannel.Writer, objectTokens, scanCts.Token).ConfigureAwait(false);
                    return true;
                }
                catch (Exception ex) when (ex is not OperationCanceledException || !scanCts.IsCancellationRequested)
                {
                    // A real failure in one range stops the whole scan
                    int index;
                    lock (_stateLock) index = _ranges.IndexOf(range);
                    if (_writerError == null) _issues.Add(IssueOutcome.Failed, $"range {index}: {ex.GetType().Name}: {ex.Message} \u2014 the scan was stopped; a checkpoint is kept so it can be resumed");
                    diagnosticsLogger?.Log($"Worker failed on range {index}: {ex.GetType().Name}: {ex.Message}");
                    Interlocked.CompareExchange(ref firstError, ex, null);
                    try { scanCts.Cancel(); } catch (ObjectDisposedException) { }
                    return false;
                }
            }

            var workers = new Task[workerCount];
            for (int w = 0; w < workerCount; w++)
            {
                workers[w] = Task.Run(async () =>
                {
                    // Object-rate tokens are owed per worker, across ranges, so many small ranges are still throttled
                    var objectTokens = new ObjectTokenAccount();
                    while (true)
                    {
                        // Counted before the dequeue, so an idle worker never sees "nothing queued, nobody busy" while a range is
                        // being handed to a worker that has not marked it active yet. Idle workers are woken only after the count
                        // drops, or they could see this worker still busy, go back to sleep and delay the end of the scan.
                        bool ran = false;
                        bool failed = false;
                        Interlocked.Increment(ref busyWorkers);
                        try
                        {
                            if (pending.TryDequeue(out var range))
                            {
                                ran = true;
                                if (await RunRangeGuardedAsync(range, objectTokens).ConfigureAwait(false)) continue;
                                failed = true;
                            }
                        }
                        finally
                        {
                            // Wake idle workers after a range, and whenever nobody holds one any more (the last check of an idle
                            // worker may be what kept the count up while the last range ended).
                            if (Interlocked.Decrement(ref busyWorkers) == 0 || ran) SignalRangeEnded();
                        }
                        if (failed) return;
                        if (!splitting) break;

                        // Nothing queued: take the second half of a running range, wait for one to become splittable, or finish
                        // when no worker holds a range any more (then no new range can appear). Looking for a split does not
                        // count as busy: the range being split is held by its own worker until it ends.
                        await WaitWhilePausedAsync(scanCts.Token).ConfigureAwait(false);
                        var wake = Volatile.Read(ref _rangeEnded).Task; // read before the checks, so a range ending meanwhile is not missed
                        var (stolen, attempted) = await TrySplitRangeAsync(listerA!, listerB!, scanCts.Token).ConfigureAwait(false);
                        if (stolen != null)
                        {
                            Interlocked.Increment(ref busyWorkers);
                            bool ok;
                            try
                            {
                                ok = await RunRangeGuardedAsync(stolen, objectTokens).ConfigureAwait(false);
                            }
                            finally
                            {
                                Interlocked.Decrement(ref busyWorkers);
                                SignalRangeEnded();
                            }
                            if (!ok) return;
                            continue;
                        }
                        if (attempted) SignalRangeEnded(); // a failed attempt backs its range off; others may try a different one
                        if (Volatile.Read(ref busyWorkers) == 0 && pending.IsEmpty) break;
                        await Task.WhenAny(wake, Task.Delay(_options.SplitIdlePoll, scanCts.Token)).ConfigureAwait(false);
                        scanCts.Token.ThrowIfCancellationRequested();
                    }

                    // Charge what is left below a full batch
                    if (_objectLimiter != null && objectTokens.Owed > 0)
                    {
                        await _objectLimiter.WaitAsync(objectTokens.Owed, scanCts.Token).ConfigureAwait(false);
                        objectTokens.Owed = 0;
                    }
                });
            }

            try
            {
                await Task.WhenAll(workers).ConfigureAwait(false);
            }
            catch (OperationCanceledException) when (scanCts.IsCancellationRequested)
            {
                // Stopped: every worker has committed the keys it fully processed
            }

            _phase = "Finalizing";
            SetStep("Finalizing: all workers finished, draining the writer");
            if (FinalizeHook != null) await FinalizeHook().ConfigureAwait(false);
            commits.Writer.TryComplete();
            await writerTask.ConfigureAwait(false);
            writerTask = null;

            // A stop requested by the caller wins: errors raised while workers were being torn down are side effects of it
            cancellationToken.ThrowIfCancellationRequested();
            if (_writerError != null) ExceptionDispatchInfo.Capture(_writerError).Throw();
            if (firstError != null) ExceptionDispatchInfo.Capture(firstError).Throw();

            SetStep("Finalizing: scan complete, removing the checkpoint");
            _phase = "Completed";
            if (CanCheckpoint && !_abandoned)
            {
                CheckpointManager.DeleteCheckpoint(CreateCheckpointSnapshot(), CheckpointBaseDirectory);
            }
            if (diagnosticsLogger != null)
            {
                await diagnosticsLogger.LogInfoAsync($"Scan completed successfully. Total objects: {_totals.ObjectsCompared:N0}, Matched: {_totals.MatchedCount:N0}, Differences: {_totals.TotalDifferences:N0}").ConfigureAwait(false);
            }
            completed = true;
        }
        catch (Exception ex) when (cancellationToken.IsCancellationRequested && ex is not OperationCanceledException)
        {
            // E.g. planning failed because its requests were aborted by the stop: report a stop, not a failure
            throw new OperationCanceledException("The scan was stopped.", ex, cancellationToken);
        }
        catch (Exception ex)
        {
            if (ex is not OperationCanceledException && !_issues.HasFailed)
                _issues.Add(IssueOutcome.Failed, $"scan failed: {ex.GetType().Name}: {ex.Message} \u2014 the scan was stopped");
            if (diagnosticsLogger != null && ex is not OperationCanceledException)
            {
                await diagnosticsLogger.LogErrorAsync("ScanExecution", ex).ConfigureAwait(false);
            }
            throw;
        }
        finally
        {
            if (writerTask != null)
            {
                // Failed before the workers were collected: let the writer drain whatever was committed
                commits!.Writer.TryComplete();
                try
                {
                    await writerTask.ConfigureAwait(false);
                }
                catch
                {
                    // Recorded in _writerError
                }
            }

            heartbeatCts.Cancel();
            if (heartbeatTask != null)
            {
                try { await heartbeatTask.ConfigureAwait(false); } catch (OperationCanceledException) { }
            }

            _stopwatch.Stop();
            lock (_pauseLock)
            {
                _pauseWatch.Stop();
            }

            // Stopped or failed: persist where we got to (also flushes the live CSV to match the checkpoint)
            if (!completed)
            {
                _phase = cancellationToken.IsCancellationRequested ? "Stopped" : "Failed";
                SetStep(_abandoned ? "Stopped (engine was abandoned by the stop watchdog)" : $"{_phase}: saving the final checkpoint");
                if (!_abandoned) await SaveCheckpointIfNeededAsync(diagnosticsLogger).ConfigureAwait(false);
            }

            SetStep(_phase == "Completed" ? "Completed" : $"{_phase}: engine finished");
            RaiseProgress();

            if (_csvWriter != null)
            {
                try
                {
                    await _csvWriter.FlushAsync().ConfigureAwait(false);
                }
                catch
                {
                    // Disk errors were already reported through the writer
                }
                finally
                {
                    await _csvWriter.DisposeAsync().ConfigureAwait(false);
                    _csvWriter = null;
                    _csvStream = null;
                }
            }

            if (_providedStreamA != null) await _providedStreamA.DisposeAsync().ConfigureAwait(false);
            if (_providedStreamB != null) await _providedStreamB.DisposeAsync().ConfigureAwait(false);
            _clientA?.Dispose();
            _clientB?.Dispose();
            _clientA = null;
            _clientB = null;

            if (diagnosticsLogger != null)
            {
                await diagnosticsLogger.DisposeAsync().ConfigureAwait(false);
            }
        }

        return CloneStats();
    }

    private async Task OpenLiveCsvAsync()
    {
        if (string.IsNullOrWhiteSpace(_liveReportCsvPath)) return;

        var dir = Path.GetDirectoryName(_liveReportCsvPath);
        if (!string.IsNullOrEmpty(dir) && !Directory.Exists(dir))
        {
            Directory.CreateDirectory(dir);
        }

        if (_resumeCheckpoint != null)
        {
            // Resume: keep rows written up to the checkpoint and drop anything after it (those items are re-processed).
            _csvStream = new FileStream(_liveReportCsvPath, FileMode.OpenOrCreate, FileAccess.Write, FileShare.Read);
            long keep = _resumeCheckpoint.LiveReportCsvBytes;
            if (keep > 0 && _csvStream.Length > keep)
            {
                _csvStream.SetLength(keep);
            }
            _csvStream.Seek(0, SeekOrigin.End);
        }
        else
        {
            // Fresh scan: never append to a previous scan's report.
            _csvStream = new FileStream(_liveReportCsvPath, FileMode.Create, FileAccess.Write, FileShare.Read);
        }

        _csvWriter = new StreamWriter(_csvStream, CsvReport.Utf8WithBom, bufferSize: 64 * 1024);

        if (_csvStream.Length == 0)
        {
            await _csvWriter.WriteLineAsync(CsvReport.Header).ConfigureAwait(false);
            await _csvWriter.FlushAsync().ConfigureAwait(false);
        }
    }

    private async Task<List<ScanPartition>> BuildPlanAsync(IS3Lister? listerA, IS3Lister? listerB, CancellationToken cancellationToken)
    {
        if (_providedStreamA != null)
        {
            // Pre-built streams cannot be restarted at a range boundary
            return new List<ScanPartition> { new() { LastCompletedKey = NullIfEmpty(_resumeCheckpoint?.LastEvaluatedKey) } };
        }

        if (PartitionPlanOverride != null)
        {
            var custom = PartitionPlanOverride.Select(p => p.Clone()).ToList();
            if (!PartitionPlanner.TilesKeyspace(custom))
                throw new InvalidOperationException("The partition plan override does not tile the keyspace.");
            return custom;
        }

        if (_resumeCheckpoint != null)
        {
            if (_resumeCheckpoint.Partitions is { Count: > 0 } saved)
            {
                var plan = saved.Select(p => p.Clone()).ToList();
                if (!PartitionPlanner.TilesKeyspace(plan))
                    throw new InvalidOperationException("The checkpoint's partition plan is damaged (its ranges do not cover the keyspace). Discard the checkpoint and start a new scan.");
                return plan;
            }

            // Version 1 checkpoint: one key for the whole scan. Plan the keyspace after it so the rest still runs in parallel.
            string? resumeKey = NullIfEmpty(_resumeCheckpoint.LastEvaluatedKey);
            if (_options.MaxParallelPartitions <= 1 || listerA == null || listerB == null)
            {
                return new List<ScanPartition> { new() { LastCompletedKey = resumeKey } };
            }
            return await PlanWithPlannerAsync(listerA, listerB, resumeKey, cancellationToken).ConfigureAwait(false);
        }

        if (_options.MaxParallelPartitions <= 1)
        {
            return new List<ScanPartition> { new() };
        }

        return await PlanWithPlannerAsync(listerA!, listerB!, null, cancellationToken).ConfigureAwait(false);
    }

    private Task<List<ScanPartition>> PlanWithPlannerAsync(IS3Lister listerA, IS3Lister listerB, string? afterRelativeKey, CancellationToken cancellationToken)
    {
        var plannerOptions = new PlannerOptions
        {
            MaxDuration = TimeSpan.FromSeconds(_options.MaxPlanningSeconds),
            MaxDiscoveryDepth = _options.MaxDiscoveryDepth,
            OnProgress = progress =>
            {
                _plannerProgress = progress;
                if (!progress.Finished) _latestStep = $"Planning: {progress.Step}";
                RaiseProgress();
            },
            Log = message => _diag?.Log(message),
            OnIssue = (outcome, detail) => _issues.Add(outcome, detail)
        };

        return PartitionPlanner.PlanAsync(
            new PlannerBucket(listerA, _configA!.BucketName, _prefixA, _limiterA, "A"),
            new PlannerBucket(listerB, _configB!.BucketName, _prefixB, _limiterB, "B"),
            _options.TargetPartitions,
            _options.MaxDiscoveryRequests,
            cancellationToken,
            afterRelativeKey,
            WaitWhilePausedAsync,
            plannerOptions);
    }

    /// <summary>Directory for the diagnostics log. When set, a log is written even with injected listers. Intended for tests.</summary>
    public string? DiagnosticsLogDirectory { get; set; }

    /// <summary>Records the latest engine phase step: shown in the UI and written to the diagnostics log.</summary>
    private void SetStep(string step)
    {
        _latestStep = step;
        _diag?.Log($"Step: {step}");
    }

    /// <summary>Every second: refreshes the UI (elapsed time, latency, in-flight ages) even when no batch arrives, and logs a diagnostics block every 10 s.</summary>
    private async Task RunHeartbeatAsync(CancellationToken cancellationToken)
    {
        try
        {
            while (!cancellationToken.IsCancellationRequested)
            {
                await Task.Delay(1000, cancellationToken).ConfigureAwait(false);
                var stats = CloneStats();
                SafeRaise(ProgressUpdated, stats);

                long now = Environment.TickCount64;
                if (_diag != null && now - Interlocked.Read(ref _lastHeartbeatLogMs) >= 10_000)
                {
                    Interlocked.Exchange(ref _lastHeartbeatLogMs, now);
                    _diag.LogBlock("periodic", DiagnosticsFormatter.Format(stats));
                }
            }
        }
        catch (OperationCanceledException)
        {
        }
    }

    /// <summary>
    /// Last resort when <see cref="RunAsync"/> does not return after its token was cancelled (a request or a disk write that
    /// ignores cancellation): logs what is stuck, saves the checkpoint that is consistent with the live CSV, and from then on
    /// stops the engine from writing CSV rows, raising difference events or saving or deleting checkpoints. The abandoned
    /// RunAsync task may still finish later; it has no further effect. Safe to call more than once.
    /// </summary>
    /// <returns>The checkpoint file path, or null when none could be saved.</returns>
    public async Task<string?> AbandonAsync(string reason, TimeSpan? gateTimeout = null)
    {
        var stuck = _inFlight.Snapshot();
        if (_diag != null)
        {
            _diag.Log($"ABANDON requested: {reason}. Phase {_phase}, step '{_latestStep}'. {stuck.Count} operation(s) in flight:");
            foreach (var op in stuck) _diag.Log($"  stuck: {op}");
            _diag.LogBlock("at abandon", DiagnosticsFormatter.Format(CloneStats(), includePartitions: true));
        }

        // Wait for the writer to finish its current batch (it applies batches under the gate), then freeze it
        bool gotGate = await _writerGate.WaitAsync(gateTimeout ?? TimeSpan.FromSeconds(3)).ConfigureAwait(false);
        bool alreadyAbandoned = _abandoned;
        _abandoned = true;
        string? path = null;
        try
        {
            if (gotGate && !alreadyAbandoned && CanCheckpoint && !_checkpointUnsafe && HasRangeProgress())
            {
                try
                {
                    if (_csvWriter != null) await _csvWriter.FlushAsync().ConfigureAwait(false);
                    var cp = CreateCheckpointSnapshot();
                    path = await CheckpointManager.SaveCheckpointAsync(cp, CheckpointBaseDirectory).ConfigureAwait(false);
                    _lastCheckpointDisplay = cp.DescribeProgress();
                    _lastCheckpointTime = DateTime.Now;
                    _diag?.Log($"ABANDON: checkpoint saved ({_lastCheckpointDisplay}) -> {Path.GetFileName(path)}");
                }
                catch (Exception ex)
                {
                    _diag?.Log($"ABANDON: could not save a checkpoint: {ex.GetType().Name}: {ex.Message}");
                }
            }
            else if (!gotGate)
            {
                _diag?.Log("ABANDON: the writer is stuck inside a batch (disk?); no new checkpoint written, the last periodic one stays valid.");
            }
        }
        finally
        {
            if (gotGate) _writerGate.Release();
        }

        SetStep("Abandoned: the engine did not stop in time; it can no longer write results");
        _phase = "Stopped";
        RaiseProgress();
        return path;
    }

    /// <summary>Waits while the scan is paused (used between planner requests).</summary>
    private Task WaitWhilePausedAsync(CancellationToken cancellationToken)
    {
        var gate = _pauseGate;
        return gate == null ? Task.CompletedTask : gate.Task.WaitAsync(cancellationToken);
    }

    private static string? NullIfEmpty(string? s) => string.IsNullOrEmpty(s) ? null : s;

    private bool SplittingEnabled => _options.EnableRangeSplitting && _providedStreamA == null && _options.MaxParallelPartitions > 1;

    /// <summary>Split attempts that found no key in a row before a range is left alone for good.</summary>
    private const int MaxSplitFailures = 6;

    /// <summary>Completed (and replaced) whenever a worker leaves a range, so idle workers re-check at once instead of at the next poll.</summary>
    private TaskCompletionSource _rangeEnded = new(TaskCreationOptions.RunContinuationsAsynchronously);

    private void SignalRangeEnded() =>
        Interlocked.Exchange(ref _rangeEnded, new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously)).TrySetResult();

    /// <summary>
    /// Work stealing: picks the running range left alone longest (running, or last split, at least
    /// <see cref="ScanPerformanceOptions.MinRangeAgeForSplit"/> ago), finds a key ahead of its worker that cuts the rest in two, and - under the
    /// state lock, only if the worker has not reached that key yet - lowers the range's upper bound to it and inserts the
    /// second half as a new range right after it, which the calling worker then runs. Range is null when nothing was split;
    /// Attempted is false when no range was eligible (nothing was sent).
    /// A failed attempt backs off (10 s, doubling, up to 2 min) and after <see cref="MaxSplitFailures"/> in a row the range
    /// is left alone. Never throws except for cancellation: a split is an optimisation only.
    /// </summary>
    private async Task<(ScanPartition? Range, bool Attempted)> TrySplitRangeAsync(IS3Lister listerA, IS3Lister listerB, CancellationToken ct)
    {
        ScanPartition? victim;
        string? position, upper;
        long now = _stopwatch.ElapsedMilliseconds;
        long minAge = (long)_options.MinRangeAgeForSplit.TotalMilliseconds;
        lock (_stateLock)
        {
            victim = _ranges
                .Where(r => r.Active && !r.Completed && !r.Splitting && r.SplitFailures < MaxSplitFailures
                    && now >= r.NextSplitAttemptMs && now - r.ActiveSinceMs >= minAge)
                .OrderBy(r => r.ActiveSinceMs) // the range left alone longest: splits rotate over the running ranges
                .FirstOrDefault();
            if (victim == null) return (null, false);
            victim.Splitting = true;
            position = victim.InFlightKey ?? victim.LastCompletedKey ?? victim.Lower;
            upper = victim.Upper;
        }

        var sw = Stopwatch.StartNew();
        var splitter = new RangeSplitter(
            new RangeSplitter.Side(listerA, _configA!.BucketName, _prefixA, _limiterA),
            new RangeSplitter.Side(listerB, _configB!.BucketName, _prefixB, _limiterB),
            _options.SplitRequestTimeout,
            WaitWhilePausedAsync);
        string? key = null;
        try
        {
            key = await splitter.FindSplitAsync(position, upper, ct).ConfigureAwait(false);
        }
        catch (OperationCanceledException) when (ct.IsCancellationRequested)
        {
            lock (_stateLock) victim.Splitting = false;
            throw;
        }
        catch (Exception ex)
        {
            key = null;
            _issues.Add(IssueOutcome.Skipped, $"splitting a running key range failed ({ex.GetType().Name}: {ex.Message}); the range keeps running on one worker — results unaffected");
        }

        ScanPartition? created = null;
        string rejected = "no split key found";
        int index = -1;
        string? lower;
        long nextAttemptInMs = 0;
        bool givingUp = false;
        lock (_stateLock)
        {
            victim.Splitting = false;
            lower = victim.Lower;
            if (key != null)
            {
                // Re-check against the live state: the worker kept going while the split key was looked up.
                string? done = victim.InFlightKey ?? victim.LastCompletedKey ?? victim.Lower;
                if (victim.Completed || !victim.Active) rejected = "the range finished meanwhile";
                else if (done != null && Utf8KeyComparer.Instance.Compare(key, done) <= 0) rejected = "the worker passed the split key meanwhile";
                else if (victim.Lower != null && Utf8KeyComparer.Instance.Compare(key, victim.Lower) <= 0) rejected = "split key not inside the range";
                else if (victim.Upper != null && Utf8KeyComparer.Instance.Compare(key, victim.Upper) >= 0) rejected = "split key not inside the range";
                else
                {
                    index = _ranges.IndexOf(victim);
                    created = new ScanPartition { Lower = key, Upper = victim.Upper, CreatedBySplit = true };
                    victim.Upper = key;
                    _ranges.Insert(index + 1, created);
                    _splitCount++;
                    victim.SplitFailures = 0;
                    victim.ActiveSinceMs = _stopwatch.ElapsedMilliseconds; // both halves now wait MinRangeAgeForSplit before the next split
                }
            }
            if (created == null)
            {
                victim.SplitFailures++;
                long backoff = Math.Min(120_000, 10_000L << Math.Min(victim.SplitFailures - 1, 4));
                victim.NextSplitAttemptMs = _stopwatch.ElapsedMilliseconds + backoff;
                nextAttemptInMs = backoff;
                givingUp = victim.SplitFailures >= MaxSplitFailures;
            }
        }

        string cost = $"{splitter.RequestsUsed} request(s){(splitter.RequestErrors > 0 ? $", {splitter.RequestErrors} skipped" : string.Empty)}, {sw.ElapsedMilliseconds} ms";
        if (created != null)
        {
            _diag?.Log($"Split key range #{index}: an idle worker took ({Clip(key!)}, {Clip(created.Upper ?? "(end)")}] from the running range, which now ends at '{Clip(key!)}' (method {splitter.Method}, {cost}).");
        }
        else
        {
            if (splitter.RequestErrors > 0 && splitter.LastError != null)
                _issues.Add(IssueOutcome.Skipped, $"{splitter.LastError} — the running range was not split this time; results unaffected");
            _diag?.Log($"Split of key range ({Clip(lower ?? "(start)")}, ...] not done: {rejected} ({cost}); next attempt in {nextAttemptInMs / 1000} s{(givingUp ? " (giving up on this range)" : string.Empty)}.");
        }
        return (created, true);
    }

    private static string Clip(string value) => value.Length <= 40 ? value : value[..37] + "...";

    private (IS3ObjectStream A, IS3ObjectStream B) CreateStreams(string? resumeAfter, string? upper, IS3Lister? listerA, IS3Lister? listerB, CancellationToken cancellationToken)
    {
        if (_providedStreamA != null)
        {
            if (Interlocked.Exchange(ref _providedStreamsTaken, 1) != 0)
                throw new InvalidOperationException("Pre-built streams can only be compared once.");
            return (_providedStreamA, _providedStreamB!);
        }

        var streamA = new PrefetchingObjectStream(
            listerA!, _configA!.BucketName, _prefixA, BuildStartAfter(_prefixA, resumeAfter), upper,
            _options.PrefetchPages, _limiterA, page => RaisePageLoaded(PageLoadedA, ref _lastPageEventA, page), _readA, ListPageSize, cancellationToken);
        var streamB = new PrefetchingObjectStream(
            listerB!, _configB!.BucketName, _prefixB, BuildStartAfter(_prefixB, resumeAfter), upper,
            _options.PrefetchPages, _limiterB, page => RaisePageLoaded(PageLoadedB, ref _lastPageEventB, page), _readB, ListPageSize, cancellationToken);
        return (streamA, streamB);
    }

    /// <summary>With many partitions pages arrive far faster than a UI can show them; forward at most one every 250ms.</summary>
    private static void RaisePageLoaded(Action<List<S3ObjectSummary>>? handler, ref long lastTick, List<S3ObjectSummary> page)
    {
        if (handler == null) return;
        long now = Environment.TickCount64;
        long last = Interlocked.Read(ref lastTick);
        if (now - last < PageEventIntervalMs) return;
        if (Interlocked.CompareExchange(ref lastTick, now, last) != last) return;
        SafeRaise(handler, page);
    }

    /// <summary>Merge-join of one key range. Each fully processed key is committed atomically (see <see cref="CommitBatch"/>).</summary>
    private async Task ProcessRangeAsync(
        ScanPartition range,
        IS3Lister? listerA,
        IS3Lister? listerB,
        ChannelWriter<CommitBatch> commits,
        ObjectTokenAccount objectTokens,
        CancellationToken cancellationToken)
    {
        // Not active yet, so no split can change the range until it is marked active below
        string? startKey, lowerKey, upperKey;
        bool createdBySplit;
        lock (_stateLock)
        {
            startKey = range.LastCompletedKey;
            lowerKey = range.Lower;
            upperKey = range.Upper;
            createdBySplit = range.CreatedBySplit;
        }
        var work = new RangeWork(range, startKey);
        IS3ObjectStream? enumA = null;
        IS3ObjectStream? enumB = null;
        bool ownsStreams = _providedStreamA == null;
        bool marked = false;
        try
        {
            string? resumeAfter = startKey ?? lowerKey;
            (enumA, enumB) = CreateStreams(resumeAfter, upperKey, listerA, listerB, cancellationToken);
            var gapDetector = _detectSequenceGaps ? new SequenceGapDetector(_maxSequenceGapThreshold) : null;
            // A resumed range continues the numeric series of the key it stopped after, as an uninterrupted run would. The second
            // half of a split continues the series of the key where the first half now ends.
            string? primeKey = startKey ?? (createdBySplit ? lowerKey : null);
            if (gapDetector != null && primeKey != null)
            {
                long unusedIds = 0;
                gapDetector.CheckKey(primeKey, ref unusedIds);
            }

            lock (_stateLock)
            {
                range.Active = true;
                range.ActiveSinceMs = _stopwatch.ElapsedMilliseconds;
                range.InFlightKey = resumeAfter;
            }
            marked = true;
            Interlocked.Increment(ref _activePartitions);

            bool hasA = await enumA.MoveNextAsync(cancellationToken).ConfigureAwait(false);
            bool hasB = await enumB.MoveNextAsync(cancellationToken).ConfigureAwait(false);

            while (hasA || hasB)
            {
                await CheckPauseAsync(work, commits, cancellationToken).ConfigureAwait(false);
                cancellationToken.ThrowIfCancellationRequested();

                S3ObjectSummary? itemA = hasA ? enumA.Current : null;
                S3ObjectSummary? itemB = hasB ? enumB.Current : null;
                string? relA = itemA != null ? ToRelativeKey(itemA.Key, _prefixA) : null;
                string? relB = itemB != null ? ToRelativeKey(itemB.Key, _prefixB) : null;
                if (itemA != null) work.CurrentKeyA = itemA.Key;
                if (itemB != null) work.CurrentKeyB = itemB.Key;

                // cmp < 0: only in A (so far); cmp > 0: only in B; cmp == 0: in both
                int cmp = itemB == null ? -1
                    : itemA == null ? 1
                    : Utf8KeyComparer.Instance.Compare(relA, relB);

                // A split may have lowered the range's upper bound (the rest now belongs to another worker). The key about to be
                // processed is the smaller head, so once it is past the bound, both listings are: the range is finished. The
                // check and the in-flight key are set under the state lock, which is where a split checks its key against them.
                string nextKey = cmp <= 0 ? relA! : relB!;
                bool pastUpper;
                lock (_stateLock)
                {
                    pastUpper = range.Upper != null && Utf8KeyComparer.Instance.Compare(nextKey, range.Upper) > 0;
                    if (!pastUpper) range.InFlightKey = nextKey;
                }
                if (pastUpper) break;

                string processedRelativeKey;
                if (cmp == 0)
                {
                    processedRelativeKey = relA!;
                    await CompareSameKeyAsync(work, itemA!, itemB!, relA!, cancellationToken).ConfigureAwait(false);
                }
                else if (cmp < 0)
                {
                    processedRelativeKey = relA!;
                    work.KeyDiffs.Add(new ObjectDifference
                    {
                        Id = NextDifferenceId(),
                        Key = DisplayKey(itemA!, relA!),
                        Type = DifferenceType.MissingInBucketB,
                        Details = $"Object exists in Bucket A but is missing in Bucket B. Size: {itemA!.FormattedSize}, ETag: {itemA.ETag}, StorageClass: {itemA.StorageClass}, LastModified: {itemA.LastModified:yyyy-MM-dd HH:mm:ss}",
                        BucketASize = itemA.Size,
                        BucketAETag = itemA.ETag,
                        BucketALastModified = itemA.LastModified,
                        BucketAStorageClass = itemA.StorageClass
                    });
                    work.KeyCounters.MissingInBucketBCount++;
                    work.KeyCounters.TotalDifferences++;
                }
                else
                {
                    processedRelativeKey = relB!;
                    work.KeyDiffs.Add(new ObjectDifference
                    {
                        Id = NextDifferenceId(),
                        Key = DisplayKey(itemB!, relB!),
                        Type = DifferenceType.MissingInBucketA,
                        Details = $"Object exists in Bucket B but is missing in Bucket A. Size: {itemB!.FormattedSize}, ETag: {itemB.ETag}, StorageClass: {itemB.StorageClass}, LastModified: {itemB.LastModified:yyyy-MM-dd HH:mm:ss}",
                        BucketBSize = itemB.Size,
                        BucketBETag = itemB.ETag,
                        BucketBLastModified = itemB.LastModified,
                        BucketBStorageClass = itemB.StorageClass
                    });
                    work.KeyCounters.MissingInBucketACount++;
                    work.KeyCounters.TotalDifferences++;
                }

                CheckSequenceGap(work, gapDetector, processedRelativeKey);

                // The key is fully processed: from here on a checkpoint may skip it
                work.CompleteKey(processedRelativeKey);

                if (cmp <= 0) hasA = await enumA.MoveNextAsync(cancellationToken).ConfigureAwait(false);
                if (cmp >= 0) hasB = await enumB.MoveNextAsync(cancellationToken).ConfigureAwait(false);

                if (work.ShouldCommit(CommitEveryKeys, CommitIntervalMs))
                {
                    await CommitAsync(work, commits, completed: false, cancellationToken).ConfigureAwait(false);
                }

                // One token per compared key: a matched A/B pair is a single iteration, so it costs 1, like a one-sided key
                if (_objectLimiter != null && ++objectTokens.Owed >= _objectBatch)
                {
                    int owed = objectTokens.Owed;
                    objectTokens.Owed = 0;
                    await _objectLimiter.WaitAsync(owed, cancellationToken).ConfigureAwait(false);
                }
            }

            await CommitAsync(work, commits, completed: true, cancellationToken).ConfigureAwait(false);
        }
        catch
        {
            // Keys fully processed before the stop are real progress; commit them so a resume skips them.
            // Differences of a half-processed key were never moved out of KeyDiffs, so they are dropped and redone.
            await TryCommitAfterFailureAsync(work, commits).ConfigureAwait(false);
            throw;
        }
        finally
        {
            if (ownsStreams)
            {
                if (enumA != null) await enumA.DisposeAsync().ConfigureAwait(false);
                if (enumB != null) await enumB.DisposeAsync().ConfigureAwait(false);
            }
            if (marked)
            {
                Interlocked.Decrement(ref _activePartitions);
                lock (_stateLock) range.Active = false;
            }
        }
    }

    private long NextDifferenceId() => Interlocked.Increment(ref _differenceIdCounter);

    private async Task CommitAsync(RangeWork work, ChannelWriter<CommitBatch> commits, bool completed, CancellationToken cancellationToken)
    {
        if (!completed && !work.HasUncommitted) return;

        var batch = work.BuildBatch(completed);
        await commits.WriteAsync(batch, cancellationToken).ConfigureAwait(false);
        // Only forget the batch once the writer has it; if the write was cancelled the failure path re-sends it
        work.MarkCommitted();
        _currentKeyA = work.CurrentKeyA;
        _currentKeyB = work.CurrentKeyB;
    }

    private static async Task TryCommitAfterFailureAsync(RangeWork work, ChannelWriter<CommitBatch> commits)
    {
        if (!work.HasUncommitted) return;
        try
        {
            await commits.WriteAsync(work.BuildBatch(completed: false), CancellationToken.None).ConfigureAwait(false);
            work.MarkCommitted();
        }
        catch
        {
            // The writer has failed (channel closed); those keys will simply be re-processed on resume
        }
    }

    private async Task CheckPauseAsync(RangeWork work, ChannelWriter<CommitBatch> commits, CancellationToken cancellationToken)
    {
        var gate = _pauseGate;
        if (gate == null) return;

        // Publish finished keys before idling so the UI and any checkpoint reflect them
        await CommitAsync(work, commits, completed: false, cancellationToken).ConfigureAwait(false);
        await gate.Task.WaitAsync(cancellationToken).ConfigureAwait(false);
    }

    /// <summary>
    /// Key shown to the user. With a shared prefix this is the full key; with different prefixes the
    /// keys are only comparable relative to their prefixes, so the relative key is shown.
    /// </summary>
    private string DisplayKey(S3ObjectSummary item, string relativeKey)
        => _prefixA == _prefixB ? item.Key : relativeKey;

    private static void SafeRaise<T>(Action<T>? handler, T arg)
    {
        if (handler == null) return;
        try
        {
            handler(arg);
        }
        catch
        {
            // A misbehaving subscriber (e.g. a closing window) must never abort the scan.
        }
    }

    private void CheckSequenceGap(RangeWork work, SequenceGapDetector? detector, string relativeKey)
    {
        if (detector == null) return;

        long unusedIds = 0;
        var gaps = detector.CheckKey(relativeKey, ref unusedIds);
        foreach (var gap in gaps)
        {
            gap.Id = NextDifferenceId();
            if (_prefixA == _prefixB)
            {
                gap.Key = _prefixA + gap.Key;
            }
            work.KeyDiffs.Add(gap);
            work.KeyCounters.SequenceGapCount++;
            work.KeyCounters.TotalDifferences++;
        }
    }

    /// <summary>Single consumer of worker commits: CSV, events, statistics and checkpoints all happen here, in order.</summary>
    private async Task RunWriterAsync(Channel<CommitBatch> commits, ScanDiagnosticsLogger? diagnosticsLogger, CancellationTokenSource scanCts)
    {
        try
        {
            int batchNumber = 0;
            await foreach (var batch in commits.Reader.ReadAllAsync().ConfigureAwait(false))
            {
                // A batch is applied as a unit under the gate, so AbandonAsync can take a consistent checkpoint between batches
                await _writerGate.WaitAsync().ConfigureAwait(false);
                try
                {
                    if (_abandoned) continue; // abandoned: nothing may be written any more

                    BeforeWriterBatch?.Invoke(++batchNumber);
                    if (_abandoned) continue; // abandoned while the hook (or a stalled disk) held this batch

                    if (batch.Differences != null)
                    {
                        using var csvOp = _inFlight.Begin($"CSV writer: writing {batch.Differences.Count:N0} row(s) to the live report");
                        foreach (var diff in batch.Differences)
                        {
                            SafeRaise(DifferenceFound, diff);
                            if (_csvWriter != null)
                            {
                                await _csvWriter.WriteLineAsync(CsvReport.BuildLine(diff)).ConfigureAwait(false);
                            }
                        }
                    }

                    _totals.Add(batch.Delta);
                    lock (_stateLock)
                    {
                        var range = batch.Range;
                        if (batch.LastCompletedKey != null) range.LastCompletedKey = batch.LastCompletedKey;
                        if (batch.Completed) range.Completed = true;
                        range.ObjectsCompared += batch.Delta.ObjectsCompared;
                    }
                    _itemsSinceCheckpoint += batch.Delta.ObjectsCompared;

                    await TickAsync(diagnosticsLogger).ConfigureAwait(false);
                }
                finally
                {
                    _writerGate.Release();
                }
            }
        }
        catch (Exception ex)
        {
            // e.g. the disk is full: the CSV no longer matches the ranges' progress, so stop and keep the last good checkpoint
            _issues.Add(IssueOutcome.Failed, $"writer failed: {ex.GetType().Name}: {ex.Message} \u2014 the scan was stopped; the last good checkpoint is kept so it can be resumed");
            _writerError = ex;
            _checkpointUnsafe = true;
            commits.Writer.TryComplete(ex);
            try { scanCts.Cancel(); } catch (ObjectDisposedException) { }
        }
    }

    /// <summary>Periodic work on the writer: UI progress (~200ms) and checkpoints (~5s).</summary>
    private async Task TickAsync(ScanDiagnosticsLogger? diagnosticsLogger)
    {
        long elapsedMs = ActiveElapsedMs();

        if (elapsedMs - _lastCheckpointMs > 5000 || (_itemsSinceCheckpoint >= 10000 && elapsedMs - _lastCheckpointMs > 1000))
        {
            _lastCheckpointMs = elapsedMs;
            _itemsSinceCheckpoint = 0;
            await SaveCheckpointIfNeededAsync(diagnosticsLogger).ConfigureAwait(false);
        }

        if (elapsedMs - _lastProgressMs > 200)
        {
            _lastProgressMs = elapsedMs;
            var stats = CloneStats();
            if (diagnosticsLogger != null)
            {
                await diagnosticsLogger.LogHeartbeatAsync(
                    stats.ObjectsCompared,
                    stats.ItemsPerSecond,
                    stats.CurrentKeyA,
                    stats.CurrentKeyB,
                    stats.TotalDifferences,
                    stats.ElapsedTime).ConfigureAwait(false);
            }
            SafeRaise(ProgressUpdated, stats);
        }
    }

    private void RaiseProgress() => SafeRaise(ProgressUpdated, CloneStats());

    private long ActiveElapsedMs() => (long)(_stopwatch.Elapsed - _pauseWatch.Elapsed).TotalMilliseconds;

    private async Task SaveCheckpointIfNeededAsync(ScanDiagnosticsLogger? diagnosticsLogger)
    {
        if (!CanCheckpoint || _checkpointUnsafe || _abandoned || !HasRangeProgress())
            return;

        try
        {
            // The CSV must be on disk before the checkpoint records its length
            if (_csvWriter != null)
            {
                await _csvWriter.FlushAsync().ConfigureAwait(false);
            }

            var cp = CreateCheckpointSnapshot();
            string path = await CheckpointManager.SaveCheckpointAsync(cp, CheckpointBaseDirectory).ConfigureAwait(false);
            _lastCheckpointDisplay = cp.DescribeProgress();
            _lastCheckpointTime = DateTime.Now;

            if (diagnosticsLogger != null)
            {
                await diagnosticsLogger.LogCheckpointAsync(path, _lastCheckpointDisplay, cp.ObjectsCompared).ConfigureAwait(false);
            }
        }
        catch
        {
            // Never break comparison on checkpoint write failure
        }
    }

    private ScanCheckpoint CreateCheckpointSnapshot()
    {
        List<ScanPartition> partitions;
        lock (_stateLock) partitions = _ranges.Select(r => r.Clone()).ToList();
        bool singleUnbounded = partitions.Count == 1 && partitions[0].Lower == null && partitions[0].Upper == null;

        return new ScanCheckpoint
        {
            BucketNameA = _configA?.BucketName ?? "",
            EndpointA = _configA?.EndpointUrl ?? "",
            RegionA = _configA?.Region ?? "",
            PrefixA = _prefixA,
            BucketNameB = _configB?.BucketName ?? "",
            EndpointB = _configB?.EndpointUrl ?? "",
            RegionB = _configB?.Region ?? "",
            PrefixB = _prefixB,
            Mode = _mode,
            LastEvaluatedKey = singleUnbounded ? partitions[0].LastCompletedKey ?? "" : "",
            Partitions = partitions,
            LastDifferenceId = Interlocked.Read(ref _differenceIdCounter),
            LiveReportCsvPath = _liveReportCsvPath,
            LiveReportCsvBytes = _csvStream?.Length ?? 0,
            ObjectsCompared = _totals.ObjectsCompared,
            MatchedCount = _totals.MatchedCount,
            TotalDifferences = _totals.TotalDifferences,
            MissingInBucketBCount = _totals.MissingInBucketBCount,
            MissingInBucketACount = _totals.MissingInBucketACount,
            SizeMismatchCount = _totals.SizeMismatchCount,
            ETagMismatchCount = _totals.ETagMismatchCount,
            ContentMismatchCount = _totals.ContentMismatchCount,
            ContentCheckErrorCount = _totals.ContentCheckErrorCount,
            SequenceGapCount = _totals.SequenceGapCount,
            ElapsedSeconds = TotalElapsedSeconds(),
            DetectSequenceGaps = _detectSequenceGaps,
            MaxSequenceGapThreshold = _maxSequenceGapThreshold
        };
    }

    /// <summary>Elapsed scan time excluding time spent paused (plus time accumulated before a resume).</summary>
    private double TotalElapsedSeconds()
    {
        double totalSeconds = (_stopwatch.Elapsed - _pauseWatch.Elapsed).TotalSeconds;
        if (_resumeCheckpoint != null)
        {
            totalSeconds += _resumeCheckpoint.ElapsedSeconds;
        }
        return Math.Max(0, totalSeconds);
    }

    private ObjectDifference BuildPairDifference(S3ObjectSummary itemA, S3ObjectSummary itemB, string relativeKey, DifferenceType type, string details)
    {
        return new ObjectDifference
        {
            Id = NextDifferenceId(),
            Key = DisplayKey(itemA, relativeKey),
            Type = type,
            Details = details,
            BucketASize = itemA.Size,
            BucketBSize = itemB.Size,
            BucketAETag = itemA.ETag,
            BucketBETag = itemB.ETag,
            BucketALastModified = itemA.LastModified,
            BucketBLastModified = itemB.LastModified,
            BucketAStorageClass = itemA.StorageClass,
            BucketBStorageClass = itemB.StorageClass
        };
    }

    private async Task CompareSameKeyAsync(
        RangeWork work,
        S3ObjectSummary itemA,
        S3ObjectSummary itemB,
        string relativeKey,
        CancellationToken cancellationToken)
    {
        // 1. Check size
        if (itemA.Size != itemB.Size)
        {
            work.KeyDiffs.Add(BuildPairDifference(itemA, itemB, relativeKey, DifferenceType.SizeMismatch,
                $"Size mismatch: A has {itemA.FormattedSize} ({itemA.Size} B), B has {itemB.FormattedSize} ({itemB.Size} B)"));
            work.KeyCounters.SizeMismatchCount++;
            work.KeyCounters.TotalDifferences++;
            return;
        }

        // 2. Check ETags
        if (string.Equals(itemA.ETag, itemB.ETag, StringComparison.OrdinalIgnoreCase))
        {
            work.KeyCounters.MatchedCount++;
            return;
        }

        // ETags differ
        bool canDownload = ContentComparer != null || (_clientA != null && _clientB != null);

        if (_mode == ComparisonMode.ListingOnly || !canDownload)
        {
            work.KeyDiffs.Add(BuildPairDifference(itemA, itemB, relativeKey, DifferenceType.ETagMismatch,
                $"ETags differ (A: {itemA.ETag}, B: {itemB.ETag}). Size is identical ({itemA.FormattedSize}). Note: multipart uploads have different ETags even if content matches."));
            work.KeyCounters.ETagMismatchCount++;
            work.KeyCounters.TotalDifferences++;
            return;
        }

        // Full Data mode
        // Always download: single-part ETags are not MD5 content hashes for SSE-KMS/SSE-C objects,
        // so differing ETags alone do not prove the content differs.
        bool contentMatches;
        Exception? firstFailure = null;
        await _contentChecks.WaitAsync(cancellationToken).ConfigureAwait(false);
        try
        {
            for (int attempt = 1; ; attempt++)
            {
                try
                {
                    // Downloads count against each bucket's request budget
                    await _limiterA.WaitIfLimitedAsync(1, cancellationToken).ConfigureAwait(false);
                    await _limiterB.WaitIfLimitedAsync(1, cancellationToken).ConfigureAwait(false);

                    // Stop must not wait for a download that ignores cancellation
                    contentMatches = await AbandonableTask.WithCancellation(
                        ContentComparer != null
                            ? ContentComparer(itemA, itemB, cancellationToken)
                            : StreamCompareObjectsAsync(_clientA!, _clientB!, itemA.Key, itemB.Key, cancellationToken),
                        cancellationToken).ConfigureAwait(false);
                    break;
                }
                catch (Exception ex) when (IsPerObjectContentFailure(ex, cancellationToken))
                {
                    firstFailure ??= ex;
                    // Transport failures (reset connection, timeout, ...) get one more try; service errors are definitive
                    if (attempt < ContentCheckAttempts && ex is not AmazonServiceException) continue;

                    // Glacier InvalidObjectState, 404 races, AccessDenied, network failures...: record the object and keep scanning.
                    string reason = ex is AmazonServiceException svc && !string.IsNullOrEmpty(svc.ErrorCode) ? svc.ErrorCode : ex.GetType().Name;
                    string attempts = attempt > 1 ? $" (after {attempt} attempts)" : "";
                    work.KeyDiffs.Add(BuildPairDifference(itemA, itemB, relativeKey, DifferenceType.ContentCheckError,
                        $"Content could not be verified{attempts}: {reason}: {ex.Message}"));
                    _issues.Add(IssueOutcome.Recorded, $"as Content Check Error for '{relativeKey}'; scan continued ({reason}{(attempt > 1 ? $", after {attempt} attempts" : "")})");
                    work.KeyCounters.ContentCheckErrorCount++;
                    work.KeyCounters.TotalDifferences++;
                    return;
                }
            }
        }
        finally
        {
            _contentChecks.Release();
        }

        if (firstFailure != null)
        {
            _issues.Add(IssueOutcome.Recovered,
                $"content check of '{relativeKey}' succeeded on a retry — no impact (first attempt: {firstFailure.GetType().Name}: {firstFailure.Message})");
        }

        if (contentMatches)
        {
            work.KeyCounters.MatchedCount++;
        }
        else
        {
            work.KeyDiffs.Add(BuildPairDifference(itemA, itemB, relativeKey, DifferenceType.ContentMismatch,
                $"Content byte mismatch! Size is identical ({itemA.FormattedSize}) but data stream differs."));
            work.KeyCounters.ContentMismatchCount++;
            work.KeyCounters.TotalDifferences++;
        }
    }

    private const int ContentCheckAttempts = 2;

    /// <summary>
    /// Failures that concern one object pair and must not abort the scan. A cancellation counts only when it is not the
    /// scan being stopped (e.g. an HTTP timeout surfacing as TaskCanceledException).
    /// </summary>
    private static bool IsPerObjectContentFailure(Exception ex, CancellationToken scanToken) => ex switch
    {
        OperationCanceledException => !scanToken.IsCancellationRequested,
        AmazonServiceException or AmazonClientException or IOException or System.Net.Http.HttpRequestException => true,
        _ => false
    };

    private async Task<bool> StreamCompareObjectsAsync(
        IAmazonS3 clientA,
        IAmazonS3 clientB,
        string keyA,
        string keyB,
        CancellationToken cancellationToken)
    {
        if (_configA == null || _configB == null) return false;

        var getA = new GetObjectRequest { BucketName = _configA.BucketName, Key = keyA };
        var getB = new GetObjectRequest { BucketName = _configB.BucketName, Key = keyB };

        // Open both streams concurrently
        var taskA = clientA.GetObjectAsync(getA, cancellationToken);
        var taskB = clientB.GetObjectAsync(getB, cancellationToken);
        try
        {
            await Task.WhenAll(taskA, taskB).ConfigureAwait(false);
        }
        catch
        {
            // WhenAll waits for both; release whichever request did succeed
            if (taskA.Status == TaskStatus.RanToCompletion) taskA.Result.Dispose();
            if (taskB.Status == TaskStatus.RanToCompletion) taskB.Result.Dispose();
            throw;
        }

        using var respA = taskA.Result;
        using var respB = taskB.Result;

        await using var streamA = respA.ResponseStream;
        await using var streamB = respB.ResponseStream;

        byte[] bufA = new byte[65536];
        byte[] bufB = new byte[65536];

        while (true)
        {
            int bytesReadA = await ReadFullBufferAsync(streamA, bufA, cancellationToken).ConfigureAwait(false);
            int bytesReadB = await ReadFullBufferAsync(streamB, bufB, cancellationToken).ConfigureAwait(false);

            if (bytesReadA != bytesReadB)
                return false;

            if (bytesReadA == 0)
                return true;

            if (!bufA.AsSpan(0, bytesReadA).SequenceEqual(bufB.AsSpan(0, bytesReadB)))
                return false;
        }
    }

    private static async Task<int> ReadFullBufferAsync(Stream stream, byte[] buffer, CancellationToken cancellationToken)
    {
        int totalRead = 0;
        while (totalRead < buffer.Length)
        {
            int read = await stream.ReadAsync(buffer.AsMemory(totalRead, buffer.Length - totalRead), cancellationToken).ConfigureAwait(false);
            if (read == 0)
                break;
            totalRead += read;
        }
        return totalRead;
    }

    private void FillRequestDiagnostics(ComparisonStatistics stats)
    {
        double waitA = _limiterA?.TotalWaitSeconds ?? 0;
        double waitB = _limiterB?.TotalWaitSeconds ?? 0;
        stats.BucketAList = _metricsA.Snapshot(waitA);
        stats.BucketBList = _metricsB.Snapshot(waitB);
        stats.TotalListRequests = stats.BucketAList.Requests + stats.BucketBList.Requests;
        stats.RateLimiterWaitSeconds = waitA + waitB;

        stats.Issues = _issues.Snapshot();
    }

    /// <summary>Writes a classified issue to the diagnostics log (same wording as the UI). A flood of identical outcomes is thinned out; the counts stay exact.</summary>
    private void LogIssue(IssueEntry entry, int countOfOutcome)
    {
        bool important = entry.Outcome is IssueOutcome.Failed or IssueOutcome.Degraded;
        if (important || countOfOutcome <= 50 || countOfOutcome % 25 == 0)
            _diag?.Log($"Issue: {entry.Display}");
    }

    /// <summary>True when any range has completed keys (so a checkpoint is worth saving).</summary>
    private bool HasRangeProgress()
    {
        lock (_stateLock) return _ranges.Any(r => r.Completed || r.LastCompletedKey != null);
    }

    /// <summary>Plan with per-range state, for the diagnostics table (read under the state lock: a split inserts ranges while the scan runs).</summary>
    private List<PartitionStatus> SnapshotPartitions(out int rangeCount, out int rangesDone, out int splits)
    {
        lock (_stateLock)
        {
            rangeCount = _ranges.Count;
            rangesDone = _ranges.Count(r => r.Completed);
            splits = _splitCount;
            var list = new List<PartitionStatus>(_ranges.Count);
            for (int i = 0; i < _ranges.Count; i++)
            {
                var r = _ranges[i];
                list.Add(new PartitionStatus
                {
                    Index = i,
                    Lower = r.Lower ?? "(start)",
                    Upper = r.Upper ?? "(end)",
                    State = r.Completed ? "Done" : r.Active ? "Running" : "Pending",
                    LastCompletedKey = r.LastCompletedKey ?? "-",
                    ObjectsCompared = r.ObjectsCompared
                });
            }
            return list;
        }
    }

    private ComparisonStatistics CloneStats()
    {
        double elapsedSeconds = TotalElapsedSeconds();
        var partitionRows = SnapshotPartitions(out int rangeCount, out int rangesDone, out int splits);
        var stats = new ComparisonStatistics
        {
            ElapsedTime = TimeSpan.FromSeconds(elapsedSeconds),
            ItemsPerSecond = elapsedSeconds > 0 ? _totals.ObjectsCompared / elapsedSeconds : 0,
            CurrentKeyA = _currentKeyA,
            CurrentKeyB = _currentKeyB,
            // Cheap, allocation-free memory probes (Process.GetCurrentProcess() costs ~20ms and leaks a handle)
            WorkingSetMB = Environment.WorkingSet / (1024.0 * 1024.0),
            GCTotalMemoryMB = GC.GetTotalMemory(false) / (1024.0 * 1024.0),
            LastCheckpointKey = _lastCheckpointDisplay,
            LastCheckpointTime = _lastCheckpointTime,
            Phase = _phase,
            PartitionsTotal = rangeCount,
            PartitionsCompleted = rangesDone,
            PartitionSplits = splits,
            ActivePartitions = Volatile.Read(ref _activePartitions),
            IsPaused = IsPaused,
            LatestStep = _latestStep,
            Planner = _plannerProgress,
            InFlight = _inFlight.Snapshot(),
            Partitions = partitionRows
        };
        _totals.CopyTo(stats);
        FillRequestDiagnostics(stats);

        if (_providedStreamA != null && _providedStreamB != null)
        {
            stats.BucketAObjectsRead = _providedStreamA.TotalObjectsRead;
            stats.BucketABytesRead = _providedStreamA.TotalBytesRead;
            stats.BucketBObjectsRead = _providedStreamB.TotalObjectsRead;
            stats.BucketBBytesRead = _providedStreamB.TotalBytesRead;
        }
        else
        {
            stats.BucketAObjectsRead = _readA.Objects;
            stats.BucketABytesRead = _readA.Bytes;
            stats.BucketBObjectsRead = _readB.Objects;
            stats.BucketBBytesRead = _readB.Bytes;
        }
        return stats;
    }

    /// <summary>Difference counters; a worker's per-key and per-batch tallies, and the writer's scan totals.</summary>
    private sealed class ScanCounters
    {
        public long ObjectsCompared;
        public long MatchedCount;
        public long TotalDifferences;
        public long MissingInBucketBCount;
        public long MissingInBucketACount;
        public long SizeMismatchCount;
        public long ETagMismatchCount;
        public long ContentMismatchCount;
        public long ContentCheckErrorCount;
        public long SequenceGapCount;

        public void Add(ScanCounters other)
        {
            ObjectsCompared += other.ObjectsCompared;
            MatchedCount += other.MatchedCount;
            TotalDifferences += other.TotalDifferences;
            MissingInBucketBCount += other.MissingInBucketBCount;
            MissingInBucketACount += other.MissingInBucketACount;
            SizeMismatchCount += other.SizeMismatchCount;
            ETagMismatchCount += other.ETagMismatchCount;
            ContentMismatchCount += other.ContentMismatchCount;
            ContentCheckErrorCount += other.ContentCheckErrorCount;
            SequenceGapCount += other.SequenceGapCount;
        }

        public void CopyTo(ComparisonStatistics stats)
        {
            stats.ObjectsCompared = ObjectsCompared;
            stats.MatchedCount = MatchedCount;
            stats.TotalDifferences = TotalDifferences;
            stats.MissingInBucketBCount = MissingInBucketBCount;
            stats.MissingInBucketACount = MissingInBucketACount;
            stats.SizeMismatchCount = SizeMismatchCount;
            stats.ETagMismatchCount = ETagMismatchCount;
            stats.ContentMismatchCount = ContentMismatchCount;
            stats.ContentCheckErrorCount = ContentCheckErrorCount;
            stats.SequenceGapCount = SequenceGapCount;
        }

        public void Reset() => CopyFrom(new ScanCounters());

        private void CopyFrom(ScanCounters other)
        {
            ObjectsCompared = other.ObjectsCompared;
            MatchedCount = other.MatchedCount;
            TotalDifferences = other.TotalDifferences;
            MissingInBucketBCount = other.MissingInBucketBCount;
            MissingInBucketACount = other.MissingInBucketACount;
            SizeMismatchCount = other.SizeMismatchCount;
            ETagMismatchCount = other.ETagMismatchCount;
            ContentMismatchCount = other.ContentMismatchCount;
            ContentCheckErrorCount = other.ContentCheckErrorCount;
            SequenceGapCount = other.SequenceGapCount;
        }

        public ScanCounters Clone()
        {
            var copy = new ScanCounters();
            copy.CopyFrom(this);
            return copy;
        }
    }

    /// <summary>
    /// What one worker hands to the writer: the differences of a run of fully processed keys in one range, the counters
    /// for exactly those keys, and the range's new last completed key. The writer applies a batch as a unit, so a
    /// checkpoint never separates a key from its differences.
    /// </summary>
    private sealed class CommitBatch
    {
        public required ScanPartition Range { get; init; }
        public List<ObjectDifference>? Differences { get; init; }
        public ScanCounters Delta { get; init; } = new();
        public string? LastCompletedKey { get; init; }
        public bool Completed { get; init; }
    }

    /// <summary>Objects a worker has compared but not yet paid MaxObjectsPerSecond tokens for; carried across ranges.</summary>
    private sealed class ObjectTokenAccount
    {
        public int Owed { get; set; }
    }

    /// <summary>A worker's private state for one range.</summary>
    private sealed class RangeWork
    {
        private readonly ScanPartition _range;
        private readonly ScanCounters _batchCounters = new();
        private List<ObjectDifference> _batchDiffs = new();
        private int _keysSinceCommit;
        private long _lastCommitTick = Environment.TickCount64;

        /// <summary>Effects of the key being processed; moved into the batch only once the key is complete.</summary>
        public ScanCounters KeyCounters { get; } = new();
        public List<ObjectDifference> KeyDiffs { get; } = new();

        public string? LastCompletedKey { get; private set; }
        public string CurrentKeyA { get; set; } = "-";
        public string CurrentKeyB { get; set; } = "-";

        public RangeWork(ScanPartition range, string? lastCompletedKey)
        {
            _range = range;
            LastCompletedKey = lastCompletedKey;
        }

        public bool HasUncommitted => _keysSinceCommit > 0 || _batchDiffs.Count > 0;

        public void CompleteKey(string relativeKey)
        {
            KeyCounters.ObjectsCompared++;
            _batchCounters.Add(KeyCounters);
            KeyCounters.Reset();
            if (KeyDiffs.Count > 0)
            {
                _batchDiffs.AddRange(KeyDiffs);
                KeyDiffs.Clear();
            }
            LastCompletedKey = relativeKey;
            _keysSinceCommit++;
        }

        public bool ShouldCommit(int everyKeys, int intervalMs)
            => _keysSinceCommit >= everyKeys
               || _batchDiffs.Count >= everyKeys
               || (_keysSinceCommit > 0 && Environment.TickCount64 - _lastCommitTick >= intervalMs);

        public CommitBatch BuildBatch(bool completed) => new()
        {
            Range = _range,
            Differences = _batchDiffs.Count > 0 ? _batchDiffs : null,
            Delta = _batchCounters.Clone(),
            LastCompletedKey = LastCompletedKey,
            Completed = completed
        };

        public void MarkCommitted()
        {
            _batchCounters.Reset();
            if (_batchDiffs.Count > 0) _batchDiffs = new List<ObjectDifference>();
            _keysSinceCommit = 0;
            _lastCommitTick = Environment.TickCount64;
        }
    }
}
