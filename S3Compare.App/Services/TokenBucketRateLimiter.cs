using System.Diagnostics;

namespace S3Compare.App.Services;

/// <summary>
/// Thread-safe token bucket. Callers take tokens immediately and, if that leaves the bucket in debt, wait until the
/// debt would have been refilled. This keeps the long-run rate at <see cref="RatePerSecond"/> while allowing a
/// short burst of up to <see cref="Capacity"/> tokens, and lets callers take tokens in batches.
/// </summary>
public sealed class TokenBucketRateLimiter
{
    private readonly object _lock = new();
    private readonly Stopwatch _clock = Stopwatch.StartNew();
    private double _tokens;
    private double _lastRefillSeconds;

    public double RatePerSecond { get; }
    public double Capacity { get; }

    private long _waitedTicks;

    /// <summary>Total time callers have been delayed by this limiter (shown in diagnostics).</summary>
    public double TotalWaitSeconds => TimeSpan.FromTicks(Interlocked.Read(ref _waitedTicks)).TotalSeconds;

    public TokenBucketRateLimiter(double ratePerSecond, double capacity)
    {
        if (ratePerSecond <= 0) throw new ArgumentOutOfRangeException(nameof(ratePerSecond));
        RatePerSecond = ratePerSecond;
        Capacity = Math.Max(1, capacity);
        _tokens = Capacity;
    }

    /// <summary>Null (no limiting) when <paramref name="ratePerSecond"/> is 0 or less.</summary>
    public static TokenBucketRateLimiter? Create(double ratePerSecond, double burstSeconds = 1.0)
        => ratePerSecond > 0 ? new TokenBucketRateLimiter(ratePerSecond, ratePerSecond * burstSeconds) : null;

    public Task WaitAsync(int tokens = 1, CancellationToken cancellationToken = default)
    {
        cancellationToken.ThrowIfCancellationRequested();

        double debtSeconds;
        lock (_lock)
        {
            double now = _clock.Elapsed.TotalSeconds;
            _tokens = Math.Min(Capacity, _tokens + (now - _lastRefillSeconds) * RatePerSecond);
            _lastRefillSeconds = now;

            _tokens -= tokens;
            if (_tokens >= 0) return Task.CompletedTask;
            debtSeconds = -_tokens / RatePerSecond;
        }

        return DelayAsync(TimeSpan.FromSeconds(debtSeconds), cancellationToken);
    }

    private async Task DelayAsync(TimeSpan delay, CancellationToken cancellationToken)
    {
        long start = Stopwatch.GetTimestamp();
        try
        {
            await Task.Delay(delay, cancellationToken).ConfigureAwait(false);
        }
        finally
        {
            Interlocked.Add(ref _waitedTicks, Stopwatch.GetElapsedTime(start).Ticks);
        }
    }
}

public static class RateLimiterExtensions
{
    public static Task WaitIfLimitedAsync(this TokenBucketRateLimiter? limiter, int tokens = 1, CancellationToken cancellationToken = default)
        => limiter == null ? Task.CompletedTask : limiter.WaitAsync(tokens, cancellationToken);
}
