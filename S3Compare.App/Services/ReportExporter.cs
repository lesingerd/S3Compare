using System.IO;
using System.Text.Json;
using S3Compare.App.Models;

namespace S3Compare.App.Services;

public static class ReportExporter
{
    public static async Task ExportToCsvAsync(string filePath, IEnumerable<ObjectDifference> differences, ComparisonStatistics stats)
    {
        await using var writer = new StreamWriter(filePath);
        
        await writer.WriteLineAsync("# S3 Bucket Comparison Difference Report");
        await writer.WriteLineAsync($"# Generated: {DateTime.Now:O}");
        await writer.WriteLineAsync($"# Objects Compared: {stats.ObjectsCompared:N0}, Matched: {stats.MatchedCount:N0}, Total Differences: {stats.TotalDifferences:N0}");
        await writer.WriteLineAsync($"# Missing in B: {stats.MissingInBucketBCount:N0}, Missing in A: {stats.MissingInBucketACount:N0}, Size Mismatch: {stats.SizeMismatchCount:N0}, ETag Mismatch: {stats.ETagMismatchCount:N0}, Content Mismatch: {stats.ContentMismatchCount:N0}, Sequence Gaps (Both Sides): {stats.SequenceGapCount:N0}");
        await writer.WriteLineAsync();
        await writer.WriteLineAsync("Id,Key,DifferenceType,Details,BucketASizeBytes,BucketBSizeBytes,BucketAETag,BucketBETag,BucketALastModified,BucketBLastModified,BucketAStorageClass,BucketBStorageClass,MetadataSummary,DetectedAt");

        string escapeCsv(string? val) => $"\"{val?.Replace("\"", "\"\"") ?? ""}\"";

        foreach (var diff in differences)
        {
            string line = $"{diff.Id},{escapeCsv(diff.Key)},{diff.Type},{escapeCsv(diff.Details)},{diff.BucketASize?.ToString() ?? ""},{diff.BucketBSize?.ToString() ?? ""},{escapeCsv(diff.BucketAETag)},{escapeCsv(diff.BucketBETag)},{diff.BucketALastModified:O},{diff.BucketBLastModified:O},{escapeCsv(diff.BucketAStorageClass)},{escapeCsv(diff.BucketBStorageClass)},{escapeCsv(diff.MetadataSummary)},{diff.DetectedAt:O}";
            await writer.WriteLineAsync(line);
        }
    }

    public static async Task ExportToJsonAsync(string filePath, IEnumerable<ObjectDifference> differences, ComparisonStatistics stats)
    {
        var report = new
        {
            GeneratedAt = DateTime.Now,
            Summary = stats,
            Differences = differences
        };

        var options = new JsonSerializerOptions { WriteIndented = true };
        await using var stream = File.Create(filePath);
        await JsonSerializer.SerializeAsync(stream, report, options);
    }
}
