using Amazon.S3;
using S3Compare.App.Models;
using S3Compare.App.Services;

namespace S3Compare.Tests;

public class EngineBehaviorTests
{
    private static S3ObjectSummary Obj(string key, long size = 100, string etag = "e") =>
        new() { Key = key, Size = size, ETag = etag };

    [Fact]
    public async Task Compare_DifferentPrefixes_MatchesOnRelativeKeys()
    {
        var streamA = new MockS3ObjectStream(new[] { Obj("in/x1"), Obj("in/x2"), Obj("in/x4") });
        var streamB = new MockS3ObjectStream(new[] { Obj("out/x1"), Obj("out/x3"), Obj("out/x4") });

        var diffs = new List<ObjectDifference>();
        var engine = new BucketComparisonEngine(
            streamA, streamB, ComparisonMode.ListingOnly,
            detectSequenceGaps: false, prefixA: "in/", prefixB: "out/");
        engine.DifferenceFound += diffs.Add;

        var stats = await engine.RunAsync();

        Assert.Equal(2, stats.MatchedCount);
        Assert.Equal(4, stats.ObjectsCompared);
        Assert.Equal(2, diffs.Count);
        Assert.Contains(diffs, d => d.Key == "x2" && d.Type == DifferenceType.MissingInBucketB);
        Assert.Contains(diffs, d => d.Key == "x3" && d.Type == DifferenceType.MissingInBucketA);
    }

    [Fact]
    public async Task Compare_SharedPrefix_KeepsFullKeysInDifferences()
    {
        var streamA = new MockS3ObjectStream(new[] { Obj("data/a"), Obj("data/b") });
        var streamB = new MockS3ObjectStream(new[] { Obj("data/a") });

        var diffs = new List<ObjectDifference>();
        var engine = new BucketComparisonEngine(
            streamA, streamB, ComparisonMode.ListingOnly,
            detectSequenceGaps: false, prefixA: "data/", prefixB: "data/");
        engine.DifferenceFound += diffs.Add;

        await engine.RunAsync();

        var diff = Assert.Single(diffs);
        Assert.Equal("data/b", diff.Key);
    }

    [Fact]
    public void ResumeKeys_AreBuiltFromEachBucketsOwnPrefix()
    {
        Assert.Equal("x2", BucketComparisonEngine.ToRelativeKey("in/x2", "in/"));
        Assert.Equal("in/x2", BucketComparisonEngine.BuildStartAfter("in/", "x2"));
        Assert.Equal("out/x2", BucketComparisonEngine.BuildStartAfter("out/", "x2"));
        Assert.Null(BucketComparisonEngine.BuildStartAfter("in/", ""));
    }

    [Fact]
    public void Utf8KeyComparer_OrdersSupplementaryCharactersAfterBmpHighCodePoints()
    {
        string emoji = "\U0001F600";   // UTF-8: F0 9F 98 80
        string fullWidth = "！";    // UTF-8: EF BC 81

        // UTF-16 ordinal order is the opposite of S3's UTF-8 byte order
        Assert.True(string.CompareOrdinal(emoji, fullWidth) < 0);
        Assert.True(Utf8KeyComparer.Instance.Compare(fullWidth, emoji) < 0);
        Assert.True(Utf8KeyComparer.Instance.Compare(emoji, fullWidth) > 0);
        Assert.Equal(0, Utf8KeyComparer.Instance.Compare(emoji, "\U0001F600"));
        Assert.True(Utf8KeyComparer.Instance.Compare("ab", "abc") < 0);
    }

    [Fact]
    public async Task Compare_Utf8OrderedKeys_AlignCorrectly()
    {
        string emoji = "dir/\U0001F600";
        string fullWidth = "dir/！";

        var streamA = new MockS3ObjectStream(new[] { Obj(fullWidth), Obj(emoji) });
        var streamB = new MockS3ObjectStream(new[] { Obj(emoji) });

        var diffs = new List<ObjectDifference>();
        var engine = new BucketComparisonEngine(streamA, streamB, ComparisonMode.ListingOnly, detectSequenceGaps: false);
        engine.DifferenceFound += diffs.Add;

        var stats = await engine.RunAsync();

        Assert.Equal(1, stats.MatchedCount);
        var diff = Assert.Single(diffs);
        Assert.Equal(fullWidth, diff.Key);
        Assert.Equal(DifferenceType.MissingInBucketB, diff.Type);
    }

    [Fact]
    public async Task FullData_PerKeyErrors_AreRecordedAndScanContinues()
    {
        var itemsA = new[]
        {
            Obj("a-good", 10, "x-2"), Obj("b-bad", 10, "x-2"), Obj("c-diff", 10, "x-2"), Obj("d-single", 10, "aaa"), Obj("e-after", 10, "same")
        };
        var itemsB = new[]
        {
            Obj("a-good", 10, "y-2"), Obj("b-bad", 10, "y-2"), Obj("c-diff", 10, "y-2"), Obj("d-single", 10, "bbb"), Obj("e-after", 10, "same")
        };

        var compared = new List<string>();
        var diffs = new List<ObjectDifference>();
        var engine = new BucketComparisonEngine(
            new MockS3ObjectStream(itemsA), new MockS3ObjectStream(itemsB),
            ComparisonMode.FullData, detectSequenceGaps: false)
        {
            ContentComparer = (a, b, ct) =>
            {
                compared.Add(a.Key);
                return a.Key switch
                {
                    "b-bad" => throw new AmazonS3Exception("The operation is not valid for the object's storage class") { ErrorCode = "InvalidObjectState" },
                    "c-diff" => Task.FromResult(false),
                    _ => Task.FromResult(true)
                };
            }
        };
        engine.DifferenceFound += diffs.Add;

        var stats = await engine.RunAsync();

        Assert.Equal(5, stats.ObjectsCompared);
        Assert.Equal(3, stats.MatchedCount);           // a-good, d-single (bytes equal) and e-after (ETags equal)
        Assert.Equal(1, stats.ContentCheckErrorCount);
        Assert.Equal(1, stats.ContentMismatchCount);   // c-diff (bytes)
        Assert.Equal(2, stats.TotalDifferences);

        var error = diffs.Single(d => d.Type == DifferenceType.ContentCheckError);
        Assert.Equal("b-bad", error.Key);
        Assert.Contains("InvalidObjectState", error.Details);

        // Differing single-part ETags are still downloaded (SSE-KMS ETags are not MD5); equal ETags are not
        Assert.Contains("d-single", compared);
        Assert.DoesNotContain("e-after", compared);
    }

    [Fact]
    public async Task LiveCsv_FreshScanOverwrites_AndResumeTruncatesToCheckpointOffset()
    {
        string path = Path.Combine(Path.GetTempPath(), "S3CompareTests_" + Guid.NewGuid().ToString("N") + ".csv");
        try
        {
            var items = new[] { Obj("k1"), Obj("k2"), Obj("k3") };

            for (int run = 0; run < 2; run++)
            {
                // Two fresh scans in a row must not append to one another
                var engine = new BucketComparisonEngine(
                    new MockS3ObjectStream(items), new MockS3ObjectStream(Array.Empty<S3ObjectSummary>()),
                    ComparisonMode.ListingOnly, path, detectSequenceGaps: false);
                await engine.RunAsync();
            }

            var bytes = await File.ReadAllBytesAsync(path);
            Assert.Equal(new byte[] { 0xEF, 0xBB, 0xBF }, bytes.Take(3).ToArray());   // UTF-8 BOM
            var lines = (await File.ReadAllLinesAsync(path)).ToList();
            Assert.Equal(4, lines.Count);   // header + 3 rows

            // Offset just after the header and the first row
            int firstNewline = Array.IndexOf(bytes, (byte)'\n');
            int secondNewline = Array.IndexOf(bytes, (byte)'\n', firstNewline + 1);
            long keep = secondNewline + 1;

            var checkpoint = new ScanCheckpoint { LiveReportCsvBytes = keep, LastEvaluatedKey = "k1", ObjectsCompared = 1, TotalDifferences = 1 };
            var resumed = new BucketComparisonEngine(
                new MockS3ObjectStream(Array.Empty<S3ObjectSummary>()), new MockS3ObjectStream(Array.Empty<S3ObjectSummary>()),
                ComparisonMode.ListingOnly, path, detectSequenceGaps: false, resumeCheckpoint: checkpoint);
            await resumed.RunAsync();

            Assert.Equal(keep, new FileInfo(path).Length);
            Assert.Equal(2, (await File.ReadAllLinesAsync(path)).Length);
        }
        finally
        {
            if (File.Exists(path)) File.Delete(path);
        }
    }

    [Fact]
    public async Task Compare_SubscriberExceptions_DoNotAbortScan()
    {
        var engine = new BucketComparisonEngine(
            new MockS3ObjectStream(new[] { Obj("a"), Obj("b") }), new MockS3ObjectStream(Array.Empty<S3ObjectSummary>()),
            ComparisonMode.ListingOnly, detectSequenceGaps: false);
        engine.DifferenceFound += _ => throw new InvalidOperationException("UI went away");
        engine.ProgressUpdated += _ => throw new InvalidOperationException("UI went away");

        var stats = await engine.RunAsync();

        Assert.Equal(2, stats.MissingInBucketBCount);
    }

    [Fact]
    public void SequenceGapDetector_UnpaddedNumbers_DoNotProduceFalseGaps()
    {
        var detector = new SequenceGapDetector(maxGapThreshold: 25);
        long counter = 0;

        // Lexicographic listing of file1..file11 without padding
        foreach (var key in new[] { "file1", "file10", "file11", "file2", "file3", "file4", "file5", "file6", "file7", "file8", "file9" })
        {
            Assert.Empty(detector.CheckKey(key, ref counter));
        }
        Assert.Equal(0, counter);
    }

    [Fact]
    public void SequenceGapDetector_ZeroPaddedGap_StillDetectedAcrossWidthBoundary()
    {
        var detector = new SequenceGapDetector(maxGapThreshold: 5);
        long counter = 0;

        Assert.Empty(detector.CheckKey("img_008.png", ref counter));
        Assert.Empty(detector.CheckKey("img_009.png", ref counter));
        var gaps = detector.CheckKey("img_011.png", ref counter);

        Assert.Single(gaps);
        Assert.Equal("img_010.png", gaps[0].Key);
    }

    [Fact]
    public void FormatSize_UsesBinaryThresholds()
    {
        Assert.Equal("600.00 B", S3ObjectSummary.FormatSize(600));
        Assert.Equal("1.00 KB", S3ObjectSummary.FormatSize(1024));
        Assert.Equal("1.50 MB", S3ObjectSummary.FormatSize(1536 * 1024));
    }
}
