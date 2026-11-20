using System.IO;
using System.Text;
using System.Text.Json;
using S3Compare.App.Models;

namespace S3Compare.App.Services;

public static class ReportExporter
{
    /// <summary>
    /// Parallel scans find differences out of key order. Exports with at most this many rows are sorted by key in
    /// memory; larger ones keep discovery order (sorting them would mean loading everything or an external sort).
    /// </summary>
    public const int MaxSortedExportRows = 200_000;

    /// <summary>Report order: key (as S3 lists it), then type, then Id.</summary>
    public static int CompareForReport(ObjectDifference x, ObjectDifference y)
    {
        int c = Utf8KeyComparer.Instance.Compare(x.Key, y.Key);
        if (c != 0) return c;
        c = x.Type.CompareTo(y.Type);
        return c != 0 ? c : x.Id.CompareTo(y.Id);
    }

    /// <summary>Loads and sorts the differences if there are at most <paramref name="maxRows"/>; otherwise returns null.</summary>
    public static List<ObjectDifference>? TryLoadSorted(IEnumerable<ObjectDifference> differences, int maxRows = MaxSortedExportRows)
    {
        var rows = new List<ObjectDifference>();
        foreach (var diff in differences)
        {
            if (rows.Count >= maxRows) return null;
            rows.Add(diff);
        }
        rows.Sort(CompareForReport);
        return rows;
    }

    /// <summary>
    /// Writes a standard CSV (header + rows only). Run metadata goes to a sidecar "{file}.summary.txt"
    /// so that any CSV parser can read the report file itself.
    /// </summary>
    public static async Task ExportToCsvAsync(string filePath, IEnumerable<ObjectDifference> differences, ComparisonStatistics stats, string? orderNote = null)
    {
        await using (var writer = new StreamWriter(filePath, false, CsvReport.Utf8WithBom))
        {
            await writer.WriteLineAsync(CsvReport.Header);
            foreach (var diff in differences)
            {
                await writer.WriteLineAsync(CsvReport.BuildLine(diff));
            }
        }

        await WriteSummaryAsync(filePath + ".summary.txt", stats, orderNote);
    }

    /// <summary>
    /// Exports from the always-on session CSV (all differences, already in report format) and writes the summary sidecar.
    /// Up to <paramref name="maxSortedRows"/> rows are written sorted by key; a larger report is copied in discovery order.
    /// </summary>
    /// <returns>True when the export is sorted by key.</returns>
    public static async Task<bool> ExportFromSessionCsvAsync(string filePath, string sessionCsvPath, ComparisonStatistics stats, int maxSortedRows = MaxSortedExportRows)
    {
        // Exporting onto the session file itself would truncate it before it is read
        if (string.Equals(Path.GetFullPath(filePath), Path.GetFullPath(sessionCsvPath), StringComparison.OrdinalIgnoreCase))
        {
            await WriteSummaryAsync(filePath + ".summary.txt", stats, UnsortedNote);
            return false;
        }

        var sorted = TryLoadSorted(CsvReport.ReadDifferences(sessionCsvPath), maxSortedRows);
        if (sorted != null)
        {
            await ExportToCsvAsync(filePath, sorted, stats, SortedNote);
            return true;
        }

        await using (var source = new FileStream(sessionCsvPath, FileMode.Open, FileAccess.Read, FileShare.ReadWrite))
        await using (var target = new FileStream(filePath, FileMode.Create, FileAccess.Write, FileShare.Read))
        {
            await source.CopyToAsync(target);
        }

        await WriteSummaryAsync(filePath + ".summary.txt", stats, UnsortedNote);
        return false;
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

    private const string SortedNote = "Row order: sorted by key.";
    private static readonly string UnsortedNote =
        $"Row order: discovery order (parallel scans find differences out of key order; reports over {MaxSortedExportRows:N0} rows are not re-sorted).";

    private static async Task WriteSummaryAsync(string path, ComparisonStatistics stats, string? orderNote)
    {
        var sb = new StringBuilder();
        sb.AppendLine("S3 Bucket Comparison Difference Report");
        sb.AppendLine($"Generated: {DateTime.Now:O}");
        sb.AppendLine($"Objects Compared: {stats.ObjectsCompared:N0}, Matched: {stats.MatchedCount:N0}, Total Differences: {stats.TotalDifferences:N0}");
        sb.AppendLine($"Missing in B: {stats.MissingInBucketBCount:N0}, Missing in A: {stats.MissingInBucketACount:N0}, Size Mismatch: {stats.SizeMismatchCount:N0}, ETag Mismatch: {stats.ETagMismatchCount:N0}, Content Mismatch: {stats.ContentMismatchCount:N0}, Content Check Errors: {stats.ContentCheckErrorCount:N0}, Sequence Gaps (Both Sides): {stats.SequenceGapCount:N0}");
        if (!string.IsNullOrEmpty(orderNote))
        {
            sb.AppendLine(orderNote);
        }
        await File.WriteAllTextAsync(path, sb.ToString(), CsvReport.Utf8WithBom);
    }
}
