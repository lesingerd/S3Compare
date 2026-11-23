using System.Collections.Concurrent;
using System.Diagnostics;
using S3Compare.App.Models;

namespace S3Compare.App.Services;

/// <summary>Tracks operations that have started but not finished (S3 requests, writer waits) so a stall can be shown and logged.</summary>
public sealed class InFlightOperations
{
    private static readonly AsyncLocal<Handle?> CurrentHandle = new();
    private readonly ConcurrentDictionary<long, Handle> _active = new();
    private long _nextId;

    /// <summary>The operation the current async flow is executing (lets a lister report its retry attempt).</summary>
    public static Handle? Current => CurrentHandle.Value;

    public Handle Begin(string description)
    {
        var handle = new Handle(this, Interlocked.Increment(ref _nextId), description);
        _active[handle.Id] = handle;
        return handle;
    }

    /// <summary>Oldest first.</summary>
    public List<InFlightOperationInfo> Snapshot()
        => _active.Values.OrderBy(h => h.StartedTick).Select(h => h.ToInfo()).ToList();

    public sealed class Handle : IDisposable
    {
        private readonly InFlightOperations _owner;
        private int _attempt = 1;

        internal Handle(InFlightOperations owner, long id, string description)
        {
            _owner = owner;
            Id = id;
            Description = description;
            StartedTick = Stopwatch.GetTimestamp();
        }

        public long Id { get; }
        public string Description { get; }
        public long StartedTick { get; }
        public int Attempt { get => Volatile.Read(ref _attempt); set => Volatile.Write(ref _attempt, value); }

        private string? _lastAttemptError;
        /// <summary>Why the previous attempt failed (set when a retry starts).</summary>
        public string? LastAttemptError => Volatile.Read(ref _lastAttemptError);

        /// <summary>Called by a lister when an attempt failed and it is about to try again.</summary>
        public void NoteFailedAttempt(string error)
        {
            Volatile.Write(ref _lastAttemptError, error);
            Attempt++;
        }

        /// <summary>Makes this the operation reported by <see cref="InFlightOperations.Current"/> in the calling async flow.</summary>
        public void MakeCurrent() => CurrentHandle.Value = this;

        public InFlightOperationInfo ToInfo() => new()
        {
            Description = Description,
            AgeSeconds = Stopwatch.GetElapsedTime(StartedTick).TotalSeconds,
            Attempt = Attempt
        };

        public void Dispose() => _owner._active.TryRemove(Id, out _);
    }
}

/// <summary>Per-bucket LIST request counters and latency.</summary>
public sealed class ListMetrics
{
    private readonly object _lock = new();
    private readonly Stopwatch _clock = Stopwatch.StartNew();
    private long _requests;
    private long _failures;
    private long _retried;
    private long _recovered;
    private double _totalLatencyMs;
    private double _lastLatencyMs;
    private string? _lastError;
    private DateTime? _lastErrorAt;
    private double _windowStart;
    private long _windowCount;
    private double _recentRate;

    public void RecordSuccess(double latencyMs, int attempts = 1)
    {
        lock (_lock)
        {
            if (attempts > 1) { _retried++; _recovered++; }
            _requests++;
            _windowCount++;
            _totalLatencyMs += latencyMs;
            _lastLatencyMs = latencyMs;
        }
    }

    public void RecordFailure(double latencyMs, string error, int attempts = 1)
    {
        lock (_lock)
        {
            if (attempts > 1) _retried++;
            _requests++;
            _windowCount++;
            _failures++;
            _lastLatencyMs = latencyMs;
            _lastError = error;
            _lastErrorAt = DateTime.Now;
        }
    }

    public ListRequestStats Snapshot(double rateLimiterWaitSeconds)
    {
        lock (_lock)
        {
            double now = _clock.Elapsed.TotalSeconds;
            if (now - _windowStart >= 5)
            {
                _recentRate = _windowCount / (now - _windowStart);
                _windowStart = now;
                _windowCount = 0;
            }
            long ok = _requests - _failures;
            return new ListRequestStats
            {
                Requests = _requests,
                Failures = _failures,
                Retried = _retried,
                Recovered = _recovered,
                AverageLatencyMs = ok > 0 ? _totalLatencyMs / ok : 0,
                LastLatencyMs = _lastLatencyMs,
                RequestsPerSecond = _recentRate > 0 ? _recentRate : (now > 0 ? _requests / now : 0),
                RateLimiterWaitSeconds = rateLimiterWaitSeconds,
                LastError = _lastError,
                LastErrorAt = _lastErrorAt
            };
        }
    }
}

/// <summary>Lets a caller stop waiting for a task that ignores its cancellation token.</summary>
public static class AbandonableTask
{
    /// <summary>
    /// Awaits <paramref name="task"/> but throws <see cref="OperationCanceledException"/> as soon as the token fires, even if the
    /// task itself never observes it. The abandoned task keeps running; its eventual failure is observed and dropped.
    /// </summary>
    public static async Task<T> WithCancellation<T>(Task<T> task, CancellationToken cancellationToken)
    {
        if (task.IsCompleted || !cancellationToken.CanBeCanceled) return await task.ConfigureAwait(false);

        var cancelled = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        using var registration = cancellationToken.Register(() => cancelled.TrySetResult());
        if (await Task.WhenAny(task, cancelled.Task).ConfigureAwait(false) == task)
        {
            return await task.ConfigureAwait(false);
        }

        _ = task.ContinueWith(t => _ = t.Exception, TaskContinuationOptions.OnlyOnFaulted | TaskContinuationOptions.ExecuteSynchronously);
        throw new OperationCanceledException("The operation was abandoned because it was cancelled.", cancellationToken);
    }
}

/// <summary>
/// Decorates a lister with request metrics, in-flight tracking and cancellation that holds even when the inner lister
/// ignores its token (a hung connection must not block Stop).
/// </summary>
public sealed class MeteredLister : IS3Lister
{
    private readonly IS3Lister _inner;
    private readonly string _label;
    private readonly ListMetrics _metrics;
    private readonly InFlightOperations _inFlight;
    private readonly IssueLog? _issues;

    public MeteredLister(IS3Lister inner, string label, ListMetrics metrics, InFlightOperations inFlight, IssueLog? issues = null)
    {
        _issues = issues;
        _inner = inner;
        _label = label;
        _metrics = metrics;
        _inFlight = inFlight;
    }

    public async Task<S3ListPage> ListPageAsync(string bucketName, string? prefix, string? startAfter, string? continuationToken,
        string? delimiter, int maxKeys, CancellationToken cancellationToken = default)
    {
        string continued = continuationToken != null ? "continuation " : string.Empty;
        string description = $"LIST {_label} bucket '{bucketName}' prefix='{Clip(prefix)}' startAfter='{Clip(startAfter)}' " +
                             $"{continued}delimiter='{delimiter ?? string.Empty}' maxKeys={maxKeys}";
        using var op = _inFlight.Begin(description);
        op.MakeCurrent();
        long start = Stopwatch.GetTimestamp();
        try
        {
            var page = await AbandonableTask.WithCancellation(
                _inner.ListPageAsync(bucketName, prefix, startAfter, continuationToken, delimiter, maxKeys, cancellationToken),
                cancellationToken).ConfigureAwait(false);
            int attempts = op.Attempt;
            _metrics.RecordSuccess(Stopwatch.GetElapsedTime(start).TotalMilliseconds, attempts);
            if (attempts > 1)
            {
                _issues?.Add(IssueOutcome.Recovered,
                    $"LIST {_label} bucket '{bucketName}' prefix='{Clip(prefix)}': recovered on attempt {attempts} — no impact (attempt {attempts - 1}: {op.LastAttemptError ?? "failed"})");
            }
            return page;
        }
        catch (OperationCanceledException) when (cancellationToken.IsCancellationRequested)
        {
            throw;
        }
        catch (Exception ex)
        {
            _metrics.RecordFailure(Stopwatch.GetElapsedTime(start).TotalMilliseconds, $"{ex.GetType().Name}: {ex.Message}", op.Attempt);
            throw;
        }
    }

    private static string Clip(string? value)
        => string.IsNullOrEmpty(value) ? string.Empty : value.Length <= 60 ? value : value[..57] + "...";
}
