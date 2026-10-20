using S3Compare.App.Models;
using S3Compare.App.Services;

namespace S3Compare.Tests;

public class CsvReportTests
{
    [Fact]
    public void Escape_QuotesAndNeutralisesFormulas()
    {
        Assert.Equal("\"plain\"", CsvReport.Escape("plain"));
        Assert.Equal("\"say \"\"hi\"\", ok\"", CsvReport.Escape("say \"hi\", ok"));
        Assert.Equal("\"\"", CsvReport.Escape(null));

        foreach (var trigger in new[] { "=1+1", "+1", "-1", "@SUM(A1)", "\tx", "\rx" })
        {
            string escaped = CsvReport.Escape(trigger);
            Assert.StartsWith("\"'", escaped);
            Assert.Equal(trigger, CsvReport.Unguard(escaped.Trim('"')));
        }
    }

    [Fact]
    public async Task ExportToCsv_WritesBomHeaderFirst_AndSummaryInSidecar()
    {
        string path = Path.Combine(Path.GetTempPath(), "S3CompareTests_" + Guid.NewGuid().ToString("N") + ".csv");
        string summary = path + ".summary.txt";
        try
        {
            var diffs = new[]
            {
                new ObjectDifference
                {
                    Id = 1, Key = "=HYPERLINK(\"http://evil\")", Type = DifferenceType.MissingInBucketB,
                    Details = "line one\nline two, with comma", BucketASize = 5, BucketAETag = "abc"
                },
                new ObjectDifference { Id = 2, Key = "normal/key.txt", Type = DifferenceType.ContentCheckError, Details = "boom" }
            };

            await ReportExporter.ExportToCsvAsync(path, diffs, new ComparisonStatistics { ObjectsCompared = 9, TotalDifferences = 2 });

            var bytes = await File.ReadAllBytesAsync(path);
            Assert.Equal(new byte[] { 0xEF, 0xBB, 0xBF }, bytes.Take(3).ToArray());

            // Standard CSV: header first, no '#' metadata lines, quoted newlines survive a parse
            var records = CsvReport.ReadRecords(path).ToList();
            Assert.Equal(CsvReport.Header.Split(','), records[0]);
            Assert.Equal(3, records.Count);
            Assert.All(records, r => Assert.Equal(14, r.Count));
            Assert.Equal("'=HYPERLINK(\"http://evil\")", records[1][1]);
            Assert.Equal("line one\nline two, with comma", records[1][3]);

            Assert.True(File.Exists(summary));
            Assert.Contains("Objects Compared: 9", await File.ReadAllTextAsync(summary));

            // Round trip restores the original values
            var back = CsvReport.ReadDifferences(path).ToList();
            Assert.Equal(2, back.Count);
            Assert.Equal(diffs[0].Key, back[0].Key);
            Assert.Equal(diffs[0].Details, back[0].Details);
            Assert.Equal(DifferenceType.ContentCheckError, back[1].Type);
        }
        finally
        {
            if (File.Exists(path)) File.Delete(path);
            if (File.Exists(summary)) File.Delete(summary);
        }
    }

    [Fact]
    public async Task ExportFromSessionCsv_ContainsEveryDifference_NotJustAGridWorthOfRows()
    {
        string session = Path.Combine(Path.GetTempPath(), "S3CompareTests_" + Guid.NewGuid().ToString("N") + ".csv");
        string export = Path.Combine(Path.GetTempPath(), "S3CompareTests_" + Guid.NewGuid().ToString("N") + ".csv");
        try
        {
            var items = Enumerable.Range(0, 3000).Select(i => new S3ObjectSummary { Key = $"k{i:D5}", Size = 1, ETag = "e" }).ToList();
            var engine = new BucketComparisonEngine(
                new MockS3ObjectStream(items), new MockS3ObjectStream(Array.Empty<S3ObjectSummary>()),
                ComparisonMode.ListingOnly, session, detectSequenceGaps: false);
            var stats = await engine.RunAsync();

            await ReportExporter.ExportFromSessionCsvAsync(export, session, stats);

            Assert.Equal(3000, CsvReport.ReadDifferences(export).Count());
        }
        finally
        {
            foreach (var p in new[] { session, export, export + ".summary.txt" })
                if (File.Exists(p)) File.Delete(p);
        }
    }
}
