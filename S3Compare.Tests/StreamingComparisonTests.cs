using S3Compare.App.Models;
using S3Compare.App.Services;

namespace S3Compare.Tests;

public class MockS3ObjectStream : IS3ObjectStream
{
    private readonly IEnumerator<S3ObjectSummary> _enumerator;
    private long _totalObjectsRead;
    private long _totalBytesRead;
    private bool _hasMore = true;

    public MockS3ObjectStream(IEnumerable<S3ObjectSummary> items)
    {
        _enumerator = items.OrderBy(x => x.Key, StringComparer.Ordinal).GetEnumerator();
    }

    public S3ObjectSummary? Current => _enumerator.Current;
    public long TotalObjectsRead => _totalObjectsRead;
    public long TotalBytesRead => _totalBytesRead;
    public bool HasMore => _hasMore;

    public Task<bool> MoveNextAsync(CancellationToken cancellationToken = default)
    {
        if (_enumerator.MoveNext())
        {
            _totalObjectsRead++;
            _totalBytesRead += _enumerator.Current.Size;
            return Task.FromResult(true);
        }
        _hasMore = false;
        return Task.FromResult(false);
    }

    public ValueTask DisposeAsync()
    {
        _enumerator.Dispose();
        return ValueTask.CompletedTask;
    }
}

public class StreamingComparisonTests
{
    [Fact]
    public async Task Compare_IdenticalBuckets_ReportsZeroDifferences()
    {
        var itemsA = new List<S3ObjectSummary>
        {
            new() { Key = "file1.txt", Size = 100, ETag = "etag1" },
            new() { Key = "file2.jpg", Size = 200, ETag = "etag2" },
            new() { Key = "logs/app.log", Size = 300, ETag = "etag3" }
        };

        var itemsB = new List<S3ObjectSummary>
        {
            new() { Key = "file1.txt", Size = 100, ETag = "etag1" },
            new() { Key = "file2.jpg", Size = 200, ETag = "etag2" },
            new() { Key = "logs/app.log", Size = 300, ETag = "etag3" }
        };

        var streamA = new MockS3ObjectStream(itemsA);
        var streamB = new MockS3ObjectStream(itemsB);

        var differences = new List<ObjectDifference>();
        var engine = new BucketComparisonEngine(streamA, streamB, ComparisonMode.ListingOnly);
        engine.DifferenceFound += diff => differences.Add(diff);

        var stats = await engine.RunAsync();

        Assert.Equal(3, stats.ObjectsCompared);
        Assert.Equal(3, stats.MatchedCount);
        Assert.Equal(0, stats.TotalDifferences);
        Assert.Empty(differences);
    }

    [Fact]
    public async Task Compare_MissingInBucketB_DetectsCorrectly()
    {
        var itemsA = new List<S3ObjectSummary>
        {
            new() { Key = "file1.txt", Size = 100, ETag = "etag1" },
            new() { Key = "file2.jpg", Size = 200, ETag = "etag2" },
            new() { Key = "file3.png", Size = 300, ETag = "etag3" }
        };

        var itemsB = new List<S3ObjectSummary>
        {
            new() { Key = "file1.txt", Size = 100, ETag = "etag1" }
        };

        var streamA = new MockS3ObjectStream(itemsA);
        var streamB = new MockS3ObjectStream(itemsB);

        var differences = new List<ObjectDifference>();
        var engine = new BucketComparisonEngine(streamA, streamB, ComparisonMode.ListingOnly);
        engine.DifferenceFound += diff => differences.Add(diff);

        var stats = await engine.RunAsync();

        Assert.Equal(3, stats.ObjectsCompared);
        Assert.Equal(1, stats.MatchedCount);
        Assert.Equal(2, stats.TotalDifferences);
        Assert.Equal(2, stats.MissingInBucketBCount);
        Assert.Equal(0, stats.MissingInBucketACount);

        Assert.Contains(differences, d => d.Key == "file2.jpg" && d.Type == DifferenceType.MissingInBucketB);
        Assert.Contains(differences, d => d.Key == "file3.png" && d.Type == DifferenceType.MissingInBucketB);
    }

    [Fact]
    public async Task Compare_MissingInBucketA_DetectsCorrectly()
    {
        var itemsA = new List<S3ObjectSummary>
        {
            new() { Key = "common.txt", Size = 500, ETag = "etag-c" }
        };

        var itemsB = new List<S3ObjectSummary>
        {
            new() { Key = "alpha.txt", Size = 100, ETag = "etag-a" },
            new() { Key = "common.txt", Size = 500, ETag = "etag-c" },
            new() { Key = "omega.txt", Size = 200, ETag = "etag-z" }
        };

        var streamA = new MockS3ObjectStream(itemsA);
        var streamB = new MockS3ObjectStream(itemsB);

        var differences = new List<ObjectDifference>();
        var engine = new BucketComparisonEngine(streamA, streamB, ComparisonMode.ListingOnly);
        engine.DifferenceFound += diff => differences.Add(diff);

        var stats = await engine.RunAsync();

        Assert.Equal(3, stats.ObjectsCompared);
        Assert.Equal(1, stats.MatchedCount);
        Assert.Equal(2, stats.TotalDifferences);
        Assert.Equal(2, stats.MissingInBucketACount);

        Assert.Contains(differences, d => d.Key == "alpha.txt" && d.Type == DifferenceType.MissingInBucketA);
        Assert.Contains(differences, d => d.Key == "omega.txt" && d.Type == DifferenceType.MissingInBucketA);
    }

    [Fact]
    public async Task Compare_SizeAndETagMismatches_DetectedProperly()
    {
        var itemsA = new List<S3ObjectSummary>
        {
            new() { Key = "file_etag_diff.bin", Size = 1000, ETag = "etag-a" },
            new() { Key = "file_size_diff.bin", Size = 5000, ETag = "etag-same" }
        };

        var itemsB = new List<S3ObjectSummary>
        {
            new() { Key = "file_etag_diff.bin", Size = 1000, ETag = "etag-b" },
            new() { Key = "file_size_diff.bin", Size = 8000, ETag = "etag-same" }
        };

        var streamA = new MockS3ObjectStream(itemsA);
        var streamB = new MockS3ObjectStream(itemsB);

        var differences = new List<ObjectDifference>();
        var engine = new BucketComparisonEngine(streamA, streamB, ComparisonMode.ListingOnly);
        engine.DifferenceFound += diff => differences.Add(diff);

        var stats = await engine.RunAsync();

        Assert.Equal(2, stats.ObjectsCompared);
        Assert.Equal(0, stats.MatchedCount);
        Assert.Equal(2, stats.TotalDifferences);
        Assert.Equal(1, stats.SizeMismatchCount);
        Assert.Equal(1, stats.ETagMismatchCount);

        var sizeDiff = differences.First(d => d.Key == "file_size_diff.bin");
        Assert.Equal(DifferenceType.SizeMismatch, sizeDiff.Type);
        Assert.Equal(5000, sizeDiff.BucketASize);
        Assert.Equal(8000, sizeDiff.BucketBSize);

        var etagDiff = differences.First(d => d.Key == "file_etag_diff.bin");
        Assert.Equal(DifferenceType.ETagMismatch, etagDiff.Type);
        Assert.Equal("etag-a", etagDiff.BucketAETag);
        Assert.Equal("etag-b", etagDiff.BucketBETag);
    }

    [Fact]
    public async Task Compare_LargeStreamSimulation_PerformsQuicklyAndAccurately()
    {
        const int count = 20_000;
        var itemsA = new List<S3ObjectSummary>(count);
        var itemsB = new List<S3ObjectSummary>(count);

        for (int i = 0; i < count; i++)
        {
            string key = $"data/chunk_{i:D6}.dat";
            long size = 1024 + (i % 100);
            string etag = $"etag_{i}";

            itemsA.Add(new S3ObjectSummary { Key = key, Size = size, ETag = etag });

            // Bucket B has 100 missing items and 50 size differences
            if (i % 200 == 0)
            {
                // Missing in B
                continue;
            }
            if (i % 200 == 100)
            {
                // Size mismatch
                itemsB.Add(new S3ObjectSummary { Key = key, Size = size + 1, ETag = etag });
            }
            else
            {
                // Identical
                itemsB.Add(new S3ObjectSummary { Key = key, Size = size, ETag = etag });
            }
        }

        var streamA = new MockS3ObjectStream(itemsA);
        var streamB = new MockS3ObjectStream(itemsB);

        var engine = new BucketComparisonEngine(streamA, streamB, ComparisonMode.ListingOnly);
        var stats = await engine.RunAsync();

        Assert.Equal(count, stats.ObjectsCompared);
        Assert.Equal(100, stats.MissingInBucketBCount);
        Assert.Equal(100, stats.SizeMismatchCount);
        Assert.Equal(count - 200, stats.MatchedCount);
        Assert.Equal(200, stats.TotalDifferences);
    }

    [Fact]
    public async Task ReportExporter_ExportsValidCsvAndJson()
    {
        var tempCsv = Path.GetTempFileName() + ".csv";
        var tempJson = Path.GetTempFileName() + ".json";

        try
        {
            var diffs = new List<ObjectDifference>
            {
                new()
                {
                    Id = 1,
                    Key = "test/doc.pdf",
                    Type = DifferenceType.MissingInBucketB,
                    Details = "Missing in B",
                    BucketASize = 1048576,
                    BucketAETag = "abc123etag"
                }
            };
            var stats = new ComparisonStatistics
            {
                ObjectsCompared = 10,
                MatchedCount = 9,
                TotalDifferences = 1,
                MissingInBucketBCount = 1
            };

            await ReportExporter.ExportToCsvAsync(tempCsv, diffs, stats);
            await ReportExporter.ExportToJsonAsync(tempJson, diffs, stats);

            Assert.True(File.Exists(tempCsv));
            var csvLines = await File.ReadAllLinesAsync(tempCsv);
            Assert.Contains(csvLines, l => l.Contains("test/doc.pdf") && l.Contains("MissingInBucketB"));

            Assert.True(File.Exists(tempJson));
            var jsonContent = await File.ReadAllTextAsync(tempJson);
            Assert.Contains("test/doc.pdf", jsonContent);
            Assert.Contains("MissingInBucketB", jsonContent);
        }
        finally
        {
            if (File.Exists(tempCsv)) File.Delete(tempCsv);
            if (File.Exists(tempJson)) File.Delete(tempJson);
        }
    }

    [Fact]
    public async Task Compare_BothBucketsEmpty_ReturnsZeroStats()
    {
        var streamA = new MockS3ObjectStream(new List<S3ObjectSummary>());
        var streamB = new MockS3ObjectStream(new List<S3ObjectSummary>());

        var engine = new BucketComparisonEngine(streamA, streamB, ComparisonMode.ListingOnly);
        var stats = await engine.RunAsync();

        Assert.Equal(0, stats.ObjectsCompared);
        Assert.Equal(0, stats.MatchedCount);
        Assert.Equal(0, stats.TotalDifferences);
    }

    [Fact]
    public async Task Compare_EmptyBucketAVsPopulatedBucketB_AllMissingInA()
    {
        var itemsB = new List<S3ObjectSummary>
        {
            new() { Key = "file1.txt", Size = 10 },
            new() { Key = "file2.txt", Size = 20 }
        };

        var streamA = new MockS3ObjectStream(new List<S3ObjectSummary>());
        var streamB = new MockS3ObjectStream(itemsB);

        var differences = new List<ObjectDifference>();
        var engine = new BucketComparisonEngine(streamA, streamB, ComparisonMode.ListingOnly);
        engine.DifferenceFound += diff => differences.Add(diff);

        var stats = await engine.RunAsync();

        Assert.Equal(2, stats.ObjectsCompared);
        Assert.Equal(2, stats.MissingInBucketACount);
        Assert.Equal(2, stats.TotalDifferences);
        Assert.Equal(0, stats.MatchedCount);
    }

    [Fact]
    public async Task Compare_LexicographicalOrderWithSlashesAndSpecialCharacters()
    {
        // Tests strict S3 ordinal binary sorting with slashes and special characters
        var keys = new[] { "app-1.0/", "app/1.0/", "app/config.json", "app_data/", "data/file!1.bin", "data/file#2.bin" };
        var itemsA = keys.Select(k => new S3ObjectSummary { Key = k, Size = 100, ETag = "etag" }).ToList();
        var itemsB = keys.Select(k => new S3ObjectSummary { Key = k, Size = 100, ETag = "etag" }).ToList();

        var streamA = new MockS3ObjectStream(itemsA);
        var streamB = new MockS3ObjectStream(itemsB);

        var engine = new BucketComparisonEngine(streamA, streamB, ComparisonMode.ListingOnly);
        var stats = await engine.RunAsync();

        Assert.Equal(keys.Length, stats.ObjectsCompared);
        Assert.Equal(keys.Length, stats.MatchedCount);
        Assert.Equal(0, stats.TotalDifferences);
    }

    [Fact]
    public async Task Compare_CancellationMidStream_ThrowsOperationCanceledException()
    {
        var items = Enumerable.Range(0, 1000).Select(i => new S3ObjectSummary { Key = $"k_{i:D5}", Size = 10, ETag = "e" }).ToList();
        var streamA = new MockS3ObjectStream(items);
        var streamB = new MockS3ObjectStream(items);

        using var cts = new CancellationTokenSource();
        cts.Cancel(); // Pre-canceled or canceled token

        var engine = new BucketComparisonEngine(streamA, streamB, ComparisonMode.ListingOnly);

        await Assert.ThrowsAnyAsync<OperationCanceledException>(async () =>
        {
            await engine.RunAsync(cts.Token);
        });
    }

    [Fact]
    public async Task Compare_PauseAndResume_ContinuesToCompletion()
    {
        var itemsA = Enumerable.Range(0, 100).Select(i => new S3ObjectSummary { Key = $"file_{i:D3}.dat", Size = 50, ETag = "e" }).ToList();
        var itemsB = Enumerable.Range(0, 100).Select(i => new S3ObjectSummary { Key = $"file_{i:D3}.dat", Size = 50, ETag = "e" }).ToList();

        var streamA = new MockS3ObjectStream(itemsA);
        var streamB = new MockS3ObjectStream(itemsB);

        var engine = new BucketComparisonEngine(streamA, streamB, ComparisonMode.ListingOnly);
        
        engine.Pause();
        Assert.True(engine.IsPaused);
        engine.Resume();
        Assert.False(engine.IsPaused);

        var stats = await engine.RunAsync();
        Assert.Equal(100, stats.ObjectsCompared);
        Assert.Equal(100, stats.MatchedCount);
    }

    [Fact]
    public void SequenceGapDetector_DetectsMissingMiddleNumber()
    {
        var detector = new SequenceGapDetector(maxGapThreshold: 5);
        long counter = 0;

        // Observe 01, 02, then skip to 05, then 06
        var g1 = detector.CheckKey("images/pic_01.png", ref counter);
        var g2 = detector.CheckKey("images/pic_02.png", ref counter);
        var g3 = detector.CheckKey("images/pic_05.png", ref counter);
        var g4 = detector.CheckKey("images/pic_06.png", ref counter);

        Assert.Empty(g1);
        Assert.Empty(g2);
        Assert.Equal(2, g3.Count);
        Assert.Contains(g3, d => d.Key == "images/pic_03.png" && d.Type == DifferenceType.PossibleMissingOnBothSides);
        Assert.Contains(g3, d => d.Key == "images/pic_04.png" && d.Type == DifferenceType.PossibleMissingOnBothSides);
        Assert.Empty(g4);
    }

    [Fact]
    public void SequenceGapDetector_IgnoresGapsExceedingThreshold()
    {
        var detector = new SequenceGapDetector(maxGapThreshold: 5);
        long counter = 0;

        detector.CheckKey("data/chunk_001.bin", ref counter);
        // Jump from 1 to 100 (gap of 98, exceeds threshold 5)
        var gaps = detector.CheckKey("data/chunk_100.bin", ref counter);

        Assert.Empty(gaps);
    }

    [Fact]
    public async Task Compare_WithSyntheticSequenceGaps_EmitsPossibleMissingOnBothSides()
    {
        // Both buckets have file_01 and file_03, but neither has file_02
        var itemsA = new List<S3ObjectSummary>
        {
            new() { Key = "data/part_01.csv", Size = 100, ETag = "etag1" },
            new() { Key = "data/part_03.csv", Size = 100, ETag = "etag3" }
        };
        var itemsB = new List<S3ObjectSummary>
        {
            new() { Key = "data/part_01.csv", Size = 100, ETag = "etag1" },
            new() { Key = "data/part_03.csv", Size = 100, ETag = "etag3" }
        };

        var streamA = new MockS3ObjectStream(itemsA);
        var streamB = new MockS3ObjectStream(itemsB);

        var differences = new List<ObjectDifference>();
        var engine = new BucketComparisonEngine(
            streamA,
            streamB,
            ComparisonMode.ListingOnly,
            detectSequenceGaps: true,
            maxSequenceGapThreshold: 5);

        engine.DifferenceFound += diff => differences.Add(diff);

        var stats = await engine.RunAsync();

        Assert.Equal(2, stats.MatchedCount);
        Assert.Equal(1, stats.SequenceGapCount);
        Assert.Single(differences);
        Assert.Equal(DifferenceType.PossibleMissingOnBothSides, differences[0].Type);
        Assert.Equal("data/part_02.csv", differences[0].Key);
    }

    [Fact]
    public async Task CheckpointManager_SaveLoadDiscard_RoundTrip()
    {
        var checkpoint = new ScanCheckpoint
        {
            EndpointA = "https://s3.wasabisys.com",
            BucketNameA = "source-bucket",
            EndpointB = "https://s3.us-east-1.wasabisys.com",
            BucketNameB = "target-bucket",
            LastEvaluatedKey = "folder/file_00500.dat",
            Mode = ComparisonMode.ListingOnly,
            ObjectsCompared = 500,
            TotalDifferences = 3
        };

        await CheckpointManager.SaveCheckpointAsync(checkpoint);

        var loaded = await CheckpointManager.FindCheckpointAsync(
            "source-bucket",
            "https://s3.wasabisys.com",
            "",
            "target-bucket",
            "https://s3.us-east-1.wasabisys.com",
            "");

        Assert.NotNull(loaded);
        Assert.Equal("folder/file_00500.dat", loaded.LastEvaluatedKey);
        Assert.Equal(500, loaded.ObjectsCompared);

        CheckpointManager.DeleteCheckpoint(loaded);

        var afterDiscard = await CheckpointManager.FindCheckpointAsync(
            "source-bucket",
            "https://s3.wasabisys.com",
            "",
            "target-bucket",
            "https://s3.us-east-1.wasabisys.com",
            "");

        Assert.Null(afterDiscard);
    }

    [Fact]
    public async Task Compare_MissingInPartner_CapturesPartnerMetadata()
    {
        var modTime = new DateTime(2026, 10, 8, 12, 0, 0, DateTimeKind.Utc);
        var itemsA = new List<S3ObjectSummary>
        {
            new() { Key = "archive/backup.zip", Size = 1048576, ETag = "abc123etag", StorageClass = "GLACIER", LastModified = modTime }
        };
        var itemsB = new List<S3ObjectSummary>(); // empty in B

        var streamA = new MockS3ObjectStream(itemsA);
        var streamB = new MockS3ObjectStream(itemsB);

        var differences = new List<ObjectDifference>();
        var engine = new BucketComparisonEngine(streamA, streamB, ComparisonMode.ListingOnly);
        engine.DifferenceFound += diff => differences.Add(diff);

        await engine.RunAsync();

        Assert.Single(differences);
        var diff = differences[0];
        Assert.Equal(DifferenceType.MissingInBucketB, diff.Type);
        Assert.Equal(1048576, diff.BucketASize);
        Assert.Equal("abc123etag", diff.BucketAETag);
        Assert.Equal("GLACIER", diff.BucketAStorageClass);
        Assert.Contains("GLACIER", diff.MetadataSummary);
        Assert.Contains("abc123etag", diff.MetadataSummary);
    }
}
