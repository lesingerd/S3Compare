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
    PossibleMissingOnBothSides
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
        while (Math.Round(number / 1024) >= 1 && counter < suffixes.Length - 1)
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
}

public class ScanCheckpoint
{
    public int Version { get; set; } = 1;
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
    public string LastEvaluatedKey { get; set; } = string.Empty;
    public string? LiveReportCsvPath { get; set; }

    public long ObjectsCompared { get; set; }
    public long MatchedCount { get; set; }
    public long TotalDifferences { get; set; }
    public long MissingInBucketBCount { get; set; }
    public long MissingInBucketACount { get; set; }
    public long SizeMismatchCount { get; set; }
    public long ETagMismatchCount { get; set; }
    public long ContentMismatchCount { get; set; }
    public long SequenceGapCount { get; set; }
    public double ElapsedSeconds { get; set; }

    public bool DetectSequenceGaps { get; set; } = true;
    public int MaxSequenceGapThreshold { get; set; } = 25;
}
