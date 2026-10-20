using System.IO;
using System.Text;
using System.Text.Json;
using S3Compare.App.Models;

namespace S3Compare.App.Services;

public static class ReportExporter
{
    /// <summary>
    /// Writes a standard CSV (header + rows only). Run metadata goes to a sidecar "{file}.summary.txt"
    /// so that any CSV parser can read the report file itself.
    /// </summary>
    public static async Task ExportToCsvAsync(string filePath, IEnumerable<ObjectDifference> differences, ComparisonStatistics stats)
    {
        await using (var writer = new StreamWriter(filePath, false, CsvReport.Utf8WithBom))
        {
            await writer.WriteLineAsync(CsvReport.Header);
            foreach (var diff in differences)
            {
                await writer.WriteLineAsync(CsvReport.BuildLine(diff));
            }
        }

        await WriteSummaryAsync(filePath + ".summary.txt", stats);
    }

    /// <summary>Exports by copying the always-on session CSV (all differences, already in report format) and writing the summary sidecar.</summary>
    public static async Task ExportFromSessionCsvAsync(string filePath, string sessionCsvPath, ComparisonStatistics stats)
    {
        // Exporting onto the session file itself would truncate it before it is read
        if (!string.Equals(Path.GetFullPath(filePath), Path.GetFullPath(sessionCsvPath), StringComparison.OrdinalIgnoreCase))
        {
            await using var source = new FileStream(sessionCsvPath, FileMode.Open, FileAccess.Read, FileShare.ReadWrite);
            await using var target = new FileStream(filePath, FileMode.Create, FileAccess.Write, FileShare.Read);
            await source.CopyToAsync(target);
        }

        await WriteSummaryAsync(filePath + ".summary.txt", stats);
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

    private static async Task WriteSummaryAsync(string path, ComparisonStatistics stats)
    {
        var sb = new StringBuilder();
        sb.AppendLine("S3 Bucket Comparison Difference Report");
        sb.AppendLine($"Generated: {DateTime.Now:O}");
        sb.AppendLine($"Objects Compared: {stats.ObjectsCompared:N0}, Matched: {stats.MatchedCount:N0}, Total Differences: {stats.TotalDifferences:N0}");
        sb.AppendLine($"Missing in B: {stats.MissingInBucketBCount:N0}, Missing in A: {stats.MissingInBucketACount:N0}, Size Mismatch: {stats.SizeMismatchCount:N0}, ETag Mismatch: {stats.ETagMismatchCount:N0}, Content Mismatch: {stats.ContentMismatchCount:N0}, Content Check Errors: {stats.ContentCheckErrorCount:N0}, Sequence Gaps (Both Sides): {stats.SequenceGapCount:N0}");
        await File.WriteAllTextAsync(path, sb.ToString(), CsvReport.Utf8WithBom);
    }
}
