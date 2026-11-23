using System.ComponentModel;
using System.Text.Json.Serialization;

namespace S3Compare.App.Models;

public enum ComparisonMode
{
    ListingOnly,
    FullData
}

public enum DifferenceType
{
    MissingInBucketB,
    MissingInBucketA,
    SizeMismatch,
    ETagMismatch,
    ContentMismatch,
    PossibleMissingOnBothSides,
    ContentCheckError
}

public class S3EndpointPreset
{
    public string Name { get; set; } = string.Empty;
    public string Url { get; set; } = string.Empty;
    public string Region { get; set; } = string.Empty;

    public override string ToString() => Name;
}

public class S3BucketConfig
{
    public string EndpointUrl { get; set; } = "https://s3.wasabisys.com";
    public string Region { get; set; } = "us-east-1";
    public string BucketName { get; set; } = string.Empty;
    public string AccessKey { get; set; } = string.Empty;
    public string SecretKey { get; set; } = string.Empty;
    public string Prefix { get; set; } = string.Empty;
}

public class S3ObjectSummary
{
    public string Key { get; set; } = string.Empty;
    public long Size { get; set; }
    public string ETag { get; set; } = string.Empty;
    public DateTime LastModified { get; set; }
    public string StorageClass { get; set; } = string.Empty;

    public string FormattedSize => FormatSize(Size);

    public static string FormatSize(long bytes)
    {
        string[] suffixes = { "B", "KB", "MB", "GB", "TB", "PB" };
        int counter = 0;
        decimal number = bytes;
        while (number >= 1024 && counter < suffixes.Length - 1)
        {
            number /= 1024;
            counter++;
        }
        return $"{number:n2} {suffixes[counter]}";
    }
}

public class ObjectDifference
{
    public long Id { get; set; }
    public string Key { get; set; } = string.Empty;
    public DifferenceType Type { get; set; }
    public string Details { get; set; } = string.Empty;
    public long? BucketASize { get; set; }
    public long? BucketBSize { get; set; }
    public string? BucketAETag { get; set; }
    public string? BucketBETag { get; set; }
    public DateTime? BucketALastModified { get; set; }
    public DateTime? BucketBLastModified { get; set; }
    public string? BucketAStorageClass { get; set; }
    public string? BucketBStorageClass { get; set; }
    public DateTime DetectedAt { get; set; } = DateTime.Now;

    public string TypeDisplay => Type switch
    {
        DifferenceType.MissingInBucketB => "Missing in Bucket B",
        DifferenceType.MissingInBucketA => "Missing in Bucket A",
        DifferenceType.SizeMismatch => "Size Mismatch",
        DifferenceType.ETagMismatch => "ETag Mismatch",
        DifferenceType.ContentMismatch => "Content Mismatch",
        DifferenceType.PossibleMissingOnBothSides => "Possible Gap (Both Sides)",
        DifferenceType.ContentCheckError => "Content Check Error",
        _ => Type.ToString()
    };

    public string BucketASizeDisplay => BucketASize.HasValue ? S3ObjectSummary.FormatSize(BucketASize.Value) : "-";
    public string BucketBSizeDisplay => BucketBSize.HasValue ? S3ObjectSummary.FormatSize(BucketBSize.Value) : "-";

    public string MetadataSummary
    {
        get
        {
            if (Type == DifferenceType.MissingInBucketB && BucketASize.HasValue)
            {
                return $"A: {BucketASizeDisplay} | ETag: {BucketAETag ?? "-"} | Class: {BucketAStorageClass ?? "STANDARD"} | Mod: {BucketALastModified:yyyy-MM-dd HH:mm:ss}";
            }
            if (Type == DifferenceType.MissingInBucketA && BucketBSize.HasValue)
            {
                return $"B: {BucketBSizeDisplay} | ETag: {BucketBETag ?? "-"} | Class: {BucketBStorageClass ?? "STANDARD"} | Mod: {BucketBLastModified:yyyy-MM-dd HH:mm:ss}";
            }
            if (Type == DifferenceType.PossibleMissingOnBothSides)
            {
                return "Synthetic sequence gap (missing in both buckets)";
            }
            return $"A: {BucketASizeDisplay} vs B: {BucketBSizeDisplay}";
        }
    }
}

public class ComparisonStatistics
{
    public long ObjectsCompared { get; set; }
    public long MatchedCount { get; set; }
    public long TotalDifferences { get; set; }
    public long MissingInBucketBCount { get; set; }
    public long MissingInBucketACount { get; set; }
    public long SizeMismatchCount { get; set; }
    public long ETagMismatchCount { get; set; }
    public long ContentMismatchCount { get; set; }
    public long SequenceGapCount { get; set; }
    public long ContentCheckErrorCount { get; set; }

    public long BucketAObjectsRead { get; set; }
    public long BucketBObjectsRead { get; set; }
    public long BucketABytesRead { get; set; }
    public long BucketBBytesRead { get; set; }

    public TimeSpan ElapsedTime { get; set; }
    public double ItemsPerSecond { get; set; }
    public string CurrentKeyA { get; set; } = "-";
    public string CurrentKeyB { get; set; } = "-";

    // System diagnostics
    public double WorkingSetMB { get; set; }
    public double GCTotalMemoryMB { get; set; }
    public string LastCheckpointKey { get; set; } = "-";
    public DateTime? LastCheckpointTime { get; set; }

    // Parallel scan progress
    public string Phase { get; set; } = "Idle";
    public int PartitionsTotal { get; set; }
    public int PartitionsCompleted { get; set; }
    public int ActivePartitions { get; set; }
    public bool IsPaused { get; set; }

    // Diagnostics (screenshot-friendly panel and log)
    public string LatestStep { get; set; } = "-";
    public PlannerProgress? Planner { get; set; }
    public ListRequestStats BucketAList { get; set; } = new();
    public ListRequestStats BucketBList { get; set; } = new();
    public long TotalListRequests { get; set; }
    public double RateLimiterWaitSeconds { get; set; }
    public IssueSummary Issues { get; set; } = IssueSummary.Empty;
    public List<PartitionStatus> Partitions { get; set; } = new();
    public List<InFlightOperationInfo> InFlight { get; set; } = new();
}

/// <summary>An S3 request or writer wait that has not finished yet.</summary>
public sealed class InFlightOperationInfo
{
    public string Description { get; init; } = string.Empty;
    public double AgeSeconds { get; init; }
    public int Attempt { get; init; } = 1;

    public override string ToString() => Attempt > 1
        ? $"{Description} | age {AgeSeconds:N1}s | attempt {Attempt}"
        : $"{Description} | age {AgeSeconds:N1}s";
}

/// <summary>Live state of the partition planner (the work done before the scan starts).</summary>
public sealed class PlannerProgress
{
    /// <summary>What the planner is doing now, e.g. "A: delimiter depth 2/3 | B: probe round 4 (prefix depth 3)".</summary>
    public string Step { get; init; } = "Starting";
    public int RequestsUsed { get; init; }
    public int RequestBudget { get; init; }
    public int Candidates { get; init; }
    public TimeSpan Elapsed { get; init; }
    /// <summary>The time box (zero = unlimited).</summary>
    public TimeSpan TimeLimit { get; init; }
    public int FailedRequests { get; init; }
    public string? LastError { get; init; }
    public bool TimedOut { get; init; }
    public bool Finished { get; init; }
    /// <summary>Techniques switched off after failures, e.g. "A: delimiter listing".</summary>
    public string DisabledTechniques { get; init; } = string.Empty;

    public string Summary
    {
        get
        {
            string budget = RequestBudget > 0 ? $"{RequestsUsed:N0}/{RequestBudget:N0}" : $"{RequestsUsed:N0}";
            string limit = TimeLimit > TimeSpan.Zero ? $" of {TimeLimit.TotalSeconds:N0}s" : string.Empty;
            string text = $"{Step} | requests {budget} | candidates {Candidates:N0} | {Elapsed.TotalSeconds:N0}s{limit}";
            if (FailedRequests > 0) text += $" | {FailedRequests:N0} request(s) skipped";
            if (TimedOut) text += " | TIME LIMIT REACHED";
            return text;
        }
    }
}

/// <summary>LIST request statistics of one bucket (planning and scanning together).</summary>
public sealed class ListRequestStats
{
    public long Requests { get; init; }
    /// <summary>Calls that failed for good (after all attempts).</summary>
    public long Failures { get; init; }
    /// <summary>Calls that needed more than one attempt.</summary>
    public long Retried { get; init; }
    /// <summary>Retried calls that then succeeded.</summary>
    public long Recovered { get; init; }
    public double AverageLatencyMs { get; init; }
    public double LastLatencyMs { get; init; }
    public double RequestsPerSecond { get; init; }
    public double RateLimiterWaitSeconds { get; init; }
    public string? LastError { get; init; }
    public DateTime? LastErrorAt { get; init; }
}

/// <summary>One key range of the plan as shown in the diagnostics table.</summary>
public sealed class PartitionStatus : INotifyPropertyChanged
{
    private string _lower = string.Empty;
    private string _upper = string.Empty;
    private string _state = "Pending";
    private string _lastCompletedKey = "-";
    private long _objectsCompared;

    public int Index { get; set; }
    public string Lower { get => _lower; set => Set(ref _lower, value, nameof(Lower)); }
    public string Upper { get => _upper; set => Set(ref _upper, value, nameof(Upper)); }
    /// <summary>Pending, Running or Done.</summary>
    public string State { get => _state; set => Set(ref _state, value, nameof(State)); }
    public string LastCompletedKey { get => _lastCompletedKey; set => Set(ref _lastCompletedKey, value, nameof(LastCompletedKey)); }
    /// <summary>Keys compared in this range during this run (not counting a run it was resumed from).</summary>
    public long ObjectsCompared
    {
        get => _objectsCompared;
        set { if (_objectsCompared != value) { _objectsCompared = value; Raise(nameof(ObjectsCompared)); } }
    }

    public event PropertyChangedEventHandler? PropertyChanged;

    public void UpdateFrom(PartitionStatus other)
    {
        Lower = other.Lower;
        Upper = other.Upper;
        State = other.State;
        LastCompletedKey = other.LastCompletedKey;
        ObjectsCompared = other.ObjectsCompared;
    }

    private void Set(ref string field, string value, string name)
    {
        if (field == value) return;
        field = value;
        Raise(name);
    }

    private void Raise(string name) => PropertyChanged?.Invoke(this, new PropertyChangedEventArgs(name));
}

/// <summary>
/// One slice of the keyspace, over keys relative to each bucket's own prefix: Lower &lt; key &lt;= Upper in UTF-8 byte order.
/// A null Lower/Upper is unbounded. The partitions of a plan tile the keyspace exactly.
/// </summary>
public class ScanPartition
{
    public string? Lower { get; set; }
    public string? Upper { get; set; }

    /// <summary>Last relative key whose differences are all in the live CSV; the range resumes after it.</summary>
    public string? LastCompletedKey { get; set; }
    public bool Completed { get; set; }

    public ScanPartition Clone() => new() { Lower = Lower, Upper = Upper, LastCompletedKey = LastCompletedKey, Completed = Completed };

    public override string ToString() => $"({Lower ?? "-inf"}, {Upper ?? "+inf"}]";
}

/// <summary>Throughput and throttling settings. None of them affect which differences are found, only how fast.</summary>
public class ScanPerformanceOptions
{
    public const int DefaultMaxParallelPartitions = 8;
    public const int DefaultPrefetchPages = 2;
    public const int DefaultMaxListRequestsPerSecond = 20;
    public const int DefaultMaxObjectsPerSecond = 0;
    public const int DefaultMaxConcurrentContentChecks = 8;
    public const int DefaultMaxDiscoveryRequests = 500;
    public const int DefaultMaxPlanningSeconds = 60;

    /// <summary>
    /// Upper bound on MaxParallelPartitions x PrefetchPages. Each listing buffers up to PrefetchPages + 2 pages of 1,000
    /// object summaries, two listings per partition, so this keeps buffered summaries around two million at most.
    /// </summary>
    public const int MaxBufferedPagesPerBucket = 512;

    /// <summary>Key ranges compared concurrently. 1 = a single range (still prefetching).</summary>
    public int MaxParallelPartitions { get; set; } = DefaultMaxParallelPartitions;

    /// <summary>Pages buffered ahead of the comparison per bucket listing.</summary>
    public int PrefetchPages { get; set; } = DefaultPrefetchPages;

    /// <summary>Per-bucket cap on S3 requests per second, shared by all partitions. 0 = unlimited.</summary>
    public int MaxListRequestsPerSecond { get; set; } = DefaultMaxListRequestsPerSecond;

    /// <summary>Aggregate cap on objects compared per second. 0 = unlimited.</summary>
    public int MaxObjectsPerSecond { get; set; } = DefaultMaxObjectsPerSecond;

    /// <summary>Full Data mode: object pairs downloaded at the same time, across all partitions.</summary>
    public int MaxConcurrentContentChecks { get; set; } = DefaultMaxConcurrentContentChecks;

    /// <summary>Budget of LIST requests (both buckets together) the partition planner may spend.</summary>
    public int MaxDiscoveryRequests { get; set; } = DefaultMaxDiscoveryRequests;

    /// <summary>
    /// Time box for planning, in seconds (0 = unlimited). When exceeded the planner stops sending requests and builds ranges
    /// from the boundaries found so far (still tiling the keyspace), so a slow endpoint cannot stall the scan start.
    /// </summary>
    public int MaxPlanningSeconds { get; set; } = DefaultMaxPlanningSeconds;

    /// <summary>Number of key ranges the planner aims for; workers pull ranges from a queue.</summary>
    public int TargetPartitions => MaxParallelPartitions * 4;

    /// <summary>Connections each S3 client may open: every partition lists both buckets, plus content downloads.</summary>
    public int RequiredConnectionsPerServer => 2 * MaxParallelPartitions + MaxConcurrentContentChecks;

    /// <summary>Returns human-readable problems; empty when the options are valid.</summary>
    public List<string> Validate()
    {
        var errors = new List<string>();
        Check(errors, nameof(MaxParallelPartitions), MaxParallelPartitions, 1, 256);
        Check(errors, nameof(PrefetchPages), PrefetchPages, 1, 64);
        Check(errors, nameof(MaxListRequestsPerSecond), MaxListRequestsPerSecond, 0, 100_000);
        Check(errors, nameof(MaxObjectsPerSecond), MaxObjectsPerSecond, 0, 100_000_000);
        Check(errors, nameof(MaxConcurrentContentChecks), MaxConcurrentContentChecks, 1, 256);
        Check(errors, nameof(MaxDiscoveryRequests), MaxDiscoveryRequests, 0, 100_000);
        Check(errors, nameof(MaxPlanningSeconds), MaxPlanningSeconds, 0, 86_400);
        if (MaxParallelPartitions >= 1 && PrefetchPages >= 1 && (long)MaxParallelPartitions * PrefetchPages > MaxBufferedPagesPerBucket)
        {
            errors.Add($"{nameof(MaxParallelPartitions)} x {nameof(PrefetchPages)} must be at most {MaxBufferedPagesPerBucket:N0} to bound memory (was {MaxParallelPartitions:N0} x {PrefetchPages:N0} = {(long)MaxParallelPartitions * PrefetchPages:N0}).");
        }
        return errors;
    }

    private static void Check(List<string> errors, string name, int value, int min, int max)
    {
        if (value < min || value > max)
        {
            errors.Add($"{name} must be between {min:N0} and {max:N0} (was {value:N0}).");
        }
    }

    public ScanPerformanceOptions Clone() => (ScanPerformanceOptions)MemberwiseClone();
}

public class ScanCheckpoint
{
    /// <summary>1 = single key (LastEvaluatedKey); 2 = partition plan (Partitions).</summary>
    public int Version { get; set; } = 2;
    public DateTime CreatedAt { get; set; } = DateTime.UtcNow;
    public DateTime LastUpdatedAt { get; set; } = DateTime.UtcNow;

    // Identifying info for reconnect (Strictly NO credentials/keys saved)
    public string EndpointA { get; set; } = string.Empty;
    public string RegionA { get; set; } = string.Empty;
    public string BucketNameA { get; set; } = string.Empty;
    public string PrefixA { get; set; } = string.Empty;

    public string EndpointB { get; set; } = string.Empty;
    public string RegionB { get; set; } = string.Empty;
    public string BucketNameB { get; set; } = string.Empty;
    public string PrefixB { get; set; } = string.Empty;

    public ComparisonMode Mode { get; set; }

    /// <summary>
    /// Version 1: last fully processed key, relative to each bucket's own prefix (prefix stripped).
    /// Version 2 fills it only when the plan is a single unbounded range; otherwise progress is in <see cref="Partitions"/>.
    /// </summary>
    public string LastEvaluatedKey { get; set; } = string.Empty;

    /// <summary>The partition plan with per-range progress (version 2). Null for version 1 checkpoints.</summary>
    public List<ScanPartition>? Partitions { get; set; }

    /// <summary>Highest difference Id handed out, so a resumed scan never reuses an Id already in the live CSV.</summary>
    public long LastDifferenceId { get; set; }

    [JsonIgnore]
    public bool HasProgress =>
        !string.IsNullOrEmpty(LastEvaluatedKey)
        || (Partitions?.Any(p => p.Completed || !string.IsNullOrEmpty(p.LastCompletedKey)) ?? false);

    public string DescribeProgress()
    {
        if (Partitions is { Count: > 1 })
        {
            int done = Partitions.Count(p => p.Completed);
            return $"{done}/{Partitions.Count} partitions complete";
        }
        string key = Partitions is { Count: 1 } ? Partitions[0].LastCompletedKey ?? LastEvaluatedKey : LastEvaluatedKey;
        return $"at key '{key}'";
    }


    public string? LiveReportCsvPath { get; set; }

    /// <summary>Length in bytes of the live CSV when this checkpoint was taken; the file is truncated to this on resume.</summary>
    public long LiveReportCsvBytes { get; set; }

    public long ObjectsCompared { get; set; }
    public long MatchedCount { get; set; }
    public long TotalDifferences { get; set; }
    public long MissingInBucketBCount { get; set; }
    public long MissingInBucketACount { get; set; }
    public long SizeMismatchCount { get; set; }
    public long ETagMismatchCount { get; set; }
    public long ContentMismatchCount { get; set; }
    public long SequenceGapCount { get; set; }
    public long ContentCheckErrorCount { get; set; }
    public double ElapsedSeconds { get; set; }

    public bool DetectSequenceGaps { get; set; } = true;
    public int MaxSequenceGapThreshold { get; set; } = 25;
}
