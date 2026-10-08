using System.Diagnostics;
using System.IO;
using Amazon.S3;
using Amazon.S3.Model;
using S3Compare.App.Models;

namespace S3Compare.App.Services;

public class BucketComparisonEngine
{
    private readonly S3BucketConfig? _configA;
    private readonly S3BucketConfig? _configB;
    private readonly IS3ObjectStream? _providedStreamA;
    private readonly IS3ObjectStream? _providedStreamB;
    private readonly ComparisonMode _mode;
    private readonly string? _liveReportCsvPath;
    private readonly ScanCheckpoint? _resumeCheckpoint;
    private readonly bool _detectSequenceGaps;
    private readonly int _maxSequenceGapThreshold;
    private readonly SequenceGapDetector? _gapDetector;

    private readonly ComparisonStatistics _stats = new();
    private readonly Stopwatch _stopwatch = new();
    private long _differenceIdCounter = 0;
    private string _lastEvaluatedKey = string.Empty;

    private volatile bool _isPaused = false;
    private readonly SemaphoreSlim _pauseSemaphore = new(1, 1);

    public event Action<ComparisonStatistics>? ProgressUpdated;
    public event Action<ObjectDifference>? DifferenceFound;
    public event Action<List<S3ObjectSummary>>? PageLoadedA;
    public event Action<List<S3ObjectSummary>>? PageLoadedB;

    public BucketComparisonEngine(
        S3BucketConfig configA,
        S3BucketConfig configB,
        ComparisonMode mode,
        string? liveReportCsvPath = null,
        ScanCheckpoint? resumeCheckpoint = null,
        bool detectSequenceGaps = true,
        int maxSequenceGapThreshold = 25)
    {
        _configA = configA ?? throw new ArgumentNullException(nameof(configA));
        _configB = configB ?? throw new ArgumentNullException(nameof(configB));
        _mode = mode;
        _liveReportCsvPath = liveReportCsvPath;
        _resumeCheckpoint = resumeCheckpoint;
        _detectSequenceGaps = detectSequenceGaps;
        _maxSequenceGapThreshold = maxSequenceGapThreshold;

        if (_detectSequenceGaps)
        {
            _gapDetector = new SequenceGapDetector(_maxSequenceGapThreshold);
        }

        if (resumeCheckpoint != null)
        {
            InitializeStatsFromCheckpoint(resumeCheckpoint);
        }
    }

    public BucketComparisonEngine(
        IS3ObjectStream streamA,
        IS3ObjectStream streamB,
        ComparisonMode mode,
        string? liveReportCsvPath = null,
        bool detectSequenceGaps = true,
        int maxSequenceGapThreshold = 25)
    {
        _providedStreamA = streamA ?? throw new ArgumentNullException(nameof(streamA));
        _providedStreamB = streamB ?? throw new ArgumentNullException(nameof(streamB));
        _mode = mode;
        _liveReportCsvPath = liveReportCsvPath;
        _detectSequenceGaps = detectSequenceGaps;
        _maxSequenceGapThreshold = maxSequenceGapThreshold;

        if (_detectSequenceGaps)
        {
            _gapDetector = new SequenceGapDetector(_maxSequenceGapThreshold);
        }
    }

    private void InitializeStatsFromCheckpoint(ScanCheckpoint cp)
    {
        _stats.ObjectsCompared = cp.ObjectsCompared;
        _stats.MatchedCount = cp.MatchedCount;
        _stats.TotalDifferences = cp.TotalDifferences;
        _stats.MissingInBucketBCount = cp.MissingInBucketBCount;
        _stats.MissingInBucketACount = cp.MissingInBucketACount;
        _stats.SizeMismatchCount = cp.SizeMismatchCount;
        _stats.ETagMismatchCount = cp.ETagMismatchCount;
        _stats.ContentMismatchCount = cp.ContentMismatchCount;
        _stats.SequenceGapCount = cp.SequenceGapCount;
        _stats.ElapsedTime = TimeSpan.FromSeconds(cp.ElapsedSeconds);
        _lastEvaluatedKey = cp.LastEvaluatedKey;
        _differenceIdCounter = cp.TotalDifferences;
    }

    public void Pause()
    {
        if (!_isPaused)
        {
            _isPaused = true;
            _pauseSemaphore.WaitAsync();
        }
    }

    public void Resume()
    {
        if (_isPaused)
        {
            _isPaused = false;
            if (_pauseSemaphore.CurrentCount == 0)
            {
                _pauseSemaphore.Release();
            }
        }
    }

    public bool IsPaused => _isPaused;

    public async Task<ComparisonStatistics> RunAsync(CancellationToken cancellationToken = default)
    {
        _stopwatch.Restart();

        IAmazonS3? clientA = null;
        IAmazonS3? clientB = null;
        IS3ObjectStream? enumA = null;
        IS3ObjectStream? enumB = null;
        ScanDiagnosticsLogger? diagnosticsLogger = null;

        StreamWriter? csvWriter = null;
        if (!string.IsNullOrWhiteSpace(_liveReportCsvPath))
        {
            var dir = Path.GetDirectoryName(_liveReportCsvPath);
            if (!string.IsNullOrEmpty(dir) && !Directory.Exists(dir))
            {
                Directory.CreateDirectory(dir);
            }

            bool isNewFile = !File.Exists(_liveReportCsvPath) || new FileInfo(_liveReportCsvPath).Length == 0;
            var fileStream = new FileStream(_liveReportCsvPath, FileMode.Append, FileAccess.Write, FileShare.Read);
            csvWriter = new StreamWriter(fileStream);

            if (isNewFile)
            {
                await csvWriter.WriteLineAsync("Id,Key,Type,Details,BucketASize,BucketBSize,BucketAETag,BucketBETag,BucketALastModified,BucketBLastModified,BucketAStorageClass,BucketBStorageClass,MetadataSummary,Timestamp").ConfigureAwait(false);
                await csvWriter.FlushAsync().ConfigureAwait(false);
            }
        }

        try
        {
            if (_configA != null && _configB != null)
            {
                diagnosticsLogger = new ScanDiagnosticsLogger();
                diagnosticsLogger.LogSessionConfig(_configA, _configB, _mode, _resumeCheckpoint?.LastEvaluatedKey);
            }

            if (_providedStreamA != null && _providedStreamB != null)
            {
                enumA = _providedStreamA;
                enumB = _providedStreamB;
            }
            else if (_configA != null && _configB != null)
            {
                clientA = S3ClientFactory.CreateClient(_configA);
                clientB = S3ClientFactory.CreateClient(_configB);

                string? startAfter = _resumeCheckpoint?.LastEvaluatedKey;

                enumA = new S3PagingEnumerator(
                    clientA,
                    _configA.BucketName,
                    _configA.Prefix,
                    startAfter,
                    (page, _) => PageLoadedA?.Invoke(page));

                enumB = new S3PagingEnumerator(
                    clientB,
                    _configB.BucketName,
                    _configB.Prefix,
                    startAfter,
                    (page, _) => PageLoadedB?.Invoke(page));
            }
            else
            {
                throw new InvalidOperationException("No valid S3 configuration or object streams provided.");
            }

            bool hasA = await enumA.MoveNextAsync(cancellationToken).ConfigureAwait(false);
            bool hasB = await enumB.MoveNextAsync(cancellationToken).ConfigureAwait(false);

            long lastProgressReportTime = 0;
            long lastCheckpointTime = 0;
            long itemsSinceLastCheckpoint = 0;

            while (hasA && hasB)
            {
                await CheckPauseAsync(cancellationToken).ConfigureAwait(false);
                cancellationToken.ThrowIfCancellationRequested();

                var itemA = enumA.Current!;
                var itemB = enumB.Current!;

                _stats.CurrentKeyA = itemA.Key;
                _stats.CurrentKeyB = itemB.Key;

                int cmp = string.CompareOrdinal(itemA.Key, itemB.Key);

                if (cmp == 0)
                {
                    _stats.ObjectsCompared++;
                    _lastEvaluatedKey = itemA.Key;

                    // Synthetic sequence gap check
                    CheckSequenceGap(itemA.Key, csvWriter);

                    await CompareSameKeyAsync(clientA, clientB, itemA, itemB, csvWriter, cancellationToken).ConfigureAwait(false);

                    hasA = await enumA.MoveNextAsync(cancellationToken).ConfigureAwait(false);
                    hasB = await enumB.MoveNextAsync(cancellationToken).ConfigureAwait(false);
                }
                else if (cmp < 0)
                {
                    // itemA exists in Bucket A but missing in Bucket B
                    _stats.ObjectsCompared++;
                    _lastEvaluatedKey = itemA.Key;

                    CheckSequenceGap(itemA.Key, csvWriter);

                    var diff = new ObjectDifference
                    {
                        Id = ++_differenceIdCounter,
                        Key = itemA.Key,
                        Type = DifferenceType.MissingInBucketB,
                        Details = $"Object exists in Bucket A but is missing in Bucket B. Size: {itemA.FormattedSize}, ETag: {itemA.ETag}, StorageClass: {itemA.StorageClass}, LastModified: {itemA.LastModified:yyyy-MM-dd HH:mm:ss}",
                        BucketASize = itemA.Size,
                        BucketBSize = null,
                        BucketAETag = itemA.ETag,
                        BucketBETag = null,
                        BucketALastModified = itemA.LastModified,
                        BucketBLastModified = null,
                        BucketAStorageClass = itemA.StorageClass,
                        BucketBStorageClass = null
                    };
                    await RecordDifferenceAsync(diff, csvWriter).ConfigureAwait(false);
                    _stats.MissingInBucketBCount++;
                    _stats.TotalDifferences++;

                    hasA = await enumA.MoveNextAsync(cancellationToken).ConfigureAwait(false);
                }
                else
                {
                    // itemB exists in Bucket B but missing in Bucket A
                    _stats.ObjectsCompared++;
                    _lastEvaluatedKey = itemB.Key;

                    CheckSequenceGap(itemB.Key, csvWriter);

                    var diff = new ObjectDifference
                    {
                        Id = ++_differenceIdCounter,
                        Key = itemB.Key,
                        Type = DifferenceType.MissingInBucketA,
                        Details = $"Object exists in Bucket B but is missing in Bucket A. Size: {itemB.FormattedSize}, ETag: {itemB.ETag}, StorageClass: {itemB.StorageClass}, LastModified: {itemB.LastModified:yyyy-MM-dd HH:mm:ss}",
                        BucketASize = null,
                        BucketBSize = itemB.Size,
                        BucketAETag = null,
                        BucketBETag = itemB.ETag,
                        BucketALastModified = null,
                        BucketBLastModified = itemB.LastModified,
                        BucketAStorageClass = null,
                        BucketBStorageClass = itemB.StorageClass
                    };
                    await RecordDifferenceAsync(diff, csvWriter).ConfigureAwait(false);
                    _stats.MissingInBucketACount++;
                    _stats.TotalDifferences++;

                    hasB = await enumB.MoveNextAsync(cancellationToken).ConfigureAwait(false);
                }

                UpdateLiveStats(enumA, enumB);
                itemsSinceLastCheckpoint++;

                // Heartbeat & Checkpoint check
                long elapsedMs = _stopwatch.ElapsedMilliseconds;
                if (elapsedMs - lastCheckpointTime > 5000 || itemsSinceLastCheckpoint >= 10000)
                {
                    lastCheckpointTime = elapsedMs;
                    itemsSinceLastCheckpoint = 0;
                    await SaveCheckpointIfNeededAsync(diagnosticsLogger).ConfigureAwait(false);
                }

                if (elapsedMs - lastProgressReportTime > 200)
                {
                    lastProgressReportTime = elapsedMs;
                    if (diagnosticsLogger != null)
                    {
                        await diagnosticsLogger.LogHeartbeatAsync(
                            _stats.ObjectsCompared,
                            _stats.ItemsPerSecond,
                            _stats.CurrentKeyA,
                            _stats.CurrentKeyB,
                            _stats.TotalDifferences,
                            _stats.ElapsedTime).ConfigureAwait(false);
                    }
                    ProgressUpdated?.Invoke(CloneStats());
                }
            }

            // Drain remaining items in Bucket A
            while (hasA)
            {
                await CheckPauseAsync(cancellationToken).ConfigureAwait(false);
                cancellationToken.ThrowIfCancellationRequested();

                var itemA = enumA.Current!;
                _stats.CurrentKeyA = itemA.Key;
                _stats.ObjectsCompared++;
                _lastEvaluatedKey = itemA.Key;

                CheckSequenceGap(itemA.Key, csvWriter);

                var diff = new ObjectDifference
                {
                    Id = ++_differenceIdCounter,
                    Key = itemA.Key,
                    Type = DifferenceType.MissingInBucketB,
                    Details = $"Object exists in Bucket A only (Tail). Size: {itemA.FormattedSize}, ETag: {itemA.ETag}, StorageClass: {itemA.StorageClass}, LastModified: {itemA.LastModified:yyyy-MM-dd HH:mm:ss}",
                    BucketASize = itemA.Size,
                    BucketAETag = itemA.ETag,
                    BucketALastModified = itemA.LastModified,
                    BucketAStorageClass = itemA.StorageClass
                };
                await RecordDifferenceAsync(diff, csvWriter).ConfigureAwait(false);
                _stats.MissingInBucketBCount++;
                _stats.TotalDifferences++;

                hasA = await enumA.MoveNextAsync(cancellationToken).ConfigureAwait(false);
                UpdateLiveStats(enumA, enumB);

                if (_stopwatch.ElapsedMilliseconds - lastProgressReportTime > 200)
                {
                    lastProgressReportTime = _stopwatch.ElapsedMilliseconds;
                    ProgressUpdated?.Invoke(CloneStats());
                }
            }

            // Drain remaining items in Bucket B
            while (hasB)
            {
                await CheckPauseAsync(cancellationToken).ConfigureAwait(false);
                cancellationToken.ThrowIfCancellationRequested();

                var itemB = enumB.Current!;
                _stats.CurrentKeyB = itemB.Key;
                _stats.ObjectsCompared++;
                _lastEvaluatedKey = itemB.Key;

                CheckSequenceGap(itemB.Key, csvWriter);

                var diff = new ObjectDifference
                {
                    Id = ++_differenceIdCounter,
                    Key = itemB.Key,
                    Type = DifferenceType.MissingInBucketA,
                    Details = $"Object exists in Bucket B only (Tail). Size: {itemB.FormattedSize}, ETag: {itemB.ETag}, StorageClass: {itemB.StorageClass}, LastModified: {itemB.LastModified:yyyy-MM-dd HH:mm:ss}",
                    BucketBSize = itemB.Size,
                    BucketBETag = itemB.ETag,
                    BucketBLastModified = itemB.LastModified,
                    BucketBStorageClass = itemB.StorageClass
                };
                await RecordDifferenceAsync(diff, csvWriter).ConfigureAwait(false);
                _stats.MissingInBucketACount++;
                _stats.TotalDifferences++;

                hasB = await enumB.MoveNextAsync(cancellationToken).ConfigureAwait(false);
                UpdateLiveStats(enumA, enumB);

                if (_stopwatch.ElapsedMilliseconds - lastProgressReportTime > 200)
                {
                    lastProgressReportTime = _stopwatch.ElapsedMilliseconds;
                    ProgressUpdated?.Invoke(CloneStats());
                }
            }

            // If reached completion without cancellation, remove active checkpoint
            if (_configA != null && _configB != null)
            {
                var finalCheckpoint = CreateCheckpointSnapshot();
                CheckpointManager.DeleteCheckpoint(finalCheckpoint);
                if (diagnosticsLogger != null)
                {
                    await diagnosticsLogger.LogInfoAsync($"Scan completed successfully. Total objects: {_stats.ObjectsCompared:N0}, Matched: {_stats.MatchedCount:N0}, Differences: {_stats.TotalDifferences:N0}").ConfigureAwait(false);
                }
            }
        }
        catch (Exception ex)
        {
            if (diagnosticsLogger != null)
            {
                await diagnosticsLogger.LogErrorAsync("ScanExecution", ex).ConfigureAwait(false);
            }
            throw;
        }
        finally
        {
            _stopwatch.Stop();
            if (csvWriter != null)
            {
                await csvWriter.FlushAsync().ConfigureAwait(false);
                await csvWriter.DisposeAsync().ConfigureAwait(false);
            }
            if (enumA != null) await enumA.DisposeAsync().ConfigureAwait(false);
            if (enumB != null) await enumB.DisposeAsync().ConfigureAwait(false);
            clientA?.Dispose();
            clientB?.Dispose();

            if (diagnosticsLogger != null)
            {
                await diagnosticsLogger.DisposeAsync().ConfigureAwait(false);
            }
        }

        UpdateLiveStats(null, null);
        ProgressUpdated?.Invoke(CloneStats());
        return CloneStats();
    }

    private void CheckSequenceGap(string key, StreamWriter? csvWriter)
    {
        if (_gapDetector == null) return;

        var gaps = _gapDetector.CheckKey(key, ref _differenceIdCounter);
        foreach (var gap in gaps)
        {
            _ = RecordDifferenceAsync(gap, csvWriter);
            _stats.SequenceGapCount++;
            _stats.TotalDifferences++;
        }
    }

    private async Task SaveCheckpointIfNeededAsync(ScanDiagnosticsLogger? diagnosticsLogger)
    {
        if (_configA == null || _configB == null || string.IsNullOrWhiteSpace(_lastEvaluatedKey))
            return;

        try
        {
            var cp = CreateCheckpointSnapshot();
            string path = await CheckpointManager.SaveCheckpointAsync(cp).ConfigureAwait(false);
            _stats.LastCheckpointKey = _lastEvaluatedKey;
            _stats.LastCheckpointTime = DateTime.Now;

            if (diagnosticsLogger != null)
            {
                await diagnosticsLogger.LogCheckpointAsync(path, _lastEvaluatedKey, _stats.ObjectsCompared).ConfigureAwait(false);
            }
        }
        catch
        {
            // Never break comparison on checkpoint write failure
        }
    }

    private ScanCheckpoint CreateCheckpointSnapshot()
    {
        return new ScanCheckpoint
        {
            BucketNameA = _configA?.BucketName ?? "",
            EndpointA = _configA?.EndpointUrl ?? "",
            RegionA = _configA?.Region ?? "",
            PrefixA = _configA?.Prefix ?? "",
            BucketNameB = _configB?.BucketName ?? "",
            EndpointB = _configB?.EndpointUrl ?? "",
            RegionB = _configB?.Region ?? "",
            PrefixB = _configB?.Prefix ?? "",
            Mode = _mode,
            LastEvaluatedKey = _lastEvaluatedKey,
            LiveReportCsvPath = _liveReportCsvPath,
            ObjectsCompared = _stats.ObjectsCompared,
            MatchedCount = _stats.MatchedCount,
            TotalDifferences = _stats.TotalDifferences,
            MissingInBucketBCount = _stats.MissingInBucketBCount,
            MissingInBucketACount = _stats.MissingInBucketACount,
            SizeMismatchCount = _stats.SizeMismatchCount,
            ETagMismatchCount = _stats.ETagMismatchCount,
            ContentMismatchCount = _stats.ContentMismatchCount,
            SequenceGapCount = _stats.SequenceGapCount,
            ElapsedSeconds = _stats.ElapsedTime.TotalSeconds,
            DetectSequenceGaps = _detectSequenceGaps,
            MaxSequenceGapThreshold = _maxSequenceGapThreshold
        };
    }

    private async Task CheckPauseAsync(CancellationToken cancellationToken)
    {
        if (_isPaused)
        {
            await _pauseSemaphore.WaitAsync(cancellationToken).ConfigureAwait(false);
            _pauseSemaphore.Release();
        }
    }

    private void UpdateLiveStats(IS3ObjectStream? enumA, IS3ObjectStream? enumB)
    {
        double totalSeconds = _stopwatch.Elapsed.TotalSeconds;
        if (_resumeCheckpoint != null)
        {
            totalSeconds += _resumeCheckpoint.ElapsedSeconds;
        }
        _stats.ElapsedTime = TimeSpan.FromSeconds(totalSeconds);
        _stats.ItemsPerSecond = totalSeconds > 0 ? _stats.ObjectsCompared / totalSeconds : 0;

        var proc = Process.GetCurrentProcess();
        _stats.WorkingSetMB = proc.WorkingSet64 / (1024.0 * 1024.0);
        _stats.GCTotalMemoryMB = GC.GetTotalMemory(false) / (1024.0 * 1024.0);

        if (enumA != null)
        {
            _stats.BucketAObjectsRead = enumA.TotalObjectsRead;
            _stats.BucketABytesRead = enumA.TotalBytesRead;
        }
        if (enumB != null)
        {
            _stats.BucketBObjectsRead = enumB.TotalObjectsRead;
            _stats.BucketBBytesRead = enumB.TotalBytesRead;
        }
    }

    private async Task CompareSameKeyAsync(
        IAmazonS3? clientA,
        IAmazonS3? clientB,
        S3ObjectSummary itemA,
        S3ObjectSummary itemB,
        StreamWriter? csvWriter,
        CancellationToken cancellationToken)
    {
        // 1. Check size
        if (itemA.Size != itemB.Size)
        {
            var diff = new ObjectDifference
            {
                Id = ++_differenceIdCounter,
                Key = itemA.Key,
                Type = DifferenceType.SizeMismatch,
                Details = $"Size mismatch: A has {itemA.FormattedSize} ({itemA.Size} B), B has {itemB.FormattedSize} ({itemB.Size} B)",
                BucketASize = itemA.Size,
                BucketBSize = itemB.Size,
                BucketAETag = itemA.ETag,
                BucketBETag = itemB.ETag,
                BucketALastModified = itemA.LastModified,
                BucketBLastModified = itemB.LastModified,
                BucketAStorageClass = itemA.StorageClass,
                BucketBStorageClass = itemB.StorageClass
            };
            await RecordDifferenceAsync(diff, csvWriter).ConfigureAwait(false);
            _stats.SizeMismatchCount++;
            _stats.TotalDifferences++;
            return;
        }

        // 2. Check ETags
        bool etagsIdentical = string.Equals(itemA.ETag, itemB.ETag, StringComparison.OrdinalIgnoreCase);

        if (etagsIdentical)
        {
            _stats.MatchedCount++;
            return;
        }

        // ETags differ
        if (_mode == ComparisonMode.ListingOnly || clientA == null || clientB == null || _configA == null || _configB == null)
        {
            var diff = new ObjectDifference
            {
                Id = ++_differenceIdCounter,
                Key = itemA.Key,
                Type = DifferenceType.ETagMismatch,
                Details = $"ETags differ (A: {itemA.ETag}, B: {itemB.ETag}). Size is identical ({itemA.FormattedSize}). Note: multipart uploads have different ETags even if content matches.",
                BucketASize = itemA.Size,
                BucketBSize = itemB.Size,
                BucketAETag = itemA.ETag,
                BucketBETag = itemB.ETag,
                BucketALastModified = itemA.LastModified,
                BucketBLastModified = itemB.LastModified,
                BucketAStorageClass = itemA.StorageClass,
                BucketBStorageClass = itemB.StorageClass
            };
            await RecordDifferenceAsync(diff, csvWriter).ConfigureAwait(false);
            _stats.ETagMismatchCount++;
            _stats.TotalDifferences++;
        }
        else
        {
            // Full Data mode: Stream read both objects in 64KB chunks
            bool contentMatches = await StreamCompareObjectsAsync(clientA, clientB, itemA.Key, cancellationToken).ConfigureAwait(false);
            if (contentMatches)
            {
                _stats.MatchedCount++;
            }
            else
            {
                var diff = new ObjectDifference
                {
                    Id = ++_differenceIdCounter,
                    Key = itemA.Key,
                    Type = DifferenceType.ContentMismatch,
                    Details = $"Content byte mismatch! Size is identical ({itemA.FormattedSize}) but data stream differs.",
                    BucketASize = itemA.Size,
                    BucketBSize = itemB.Size,
                    BucketAETag = itemA.ETag,
                    BucketBETag = itemB.ETag,
                    BucketALastModified = itemA.LastModified,
                    BucketBLastModified = itemB.LastModified,
                    BucketAStorageClass = itemA.StorageClass,
                    BucketBStorageClass = itemB.StorageClass
                };
                await RecordDifferenceAsync(diff, csvWriter).ConfigureAwait(false);
                _stats.ContentMismatchCount++;
                _stats.TotalDifferences++;
            }
        }
    }

    private async Task<bool> StreamCompareObjectsAsync(
        IAmazonS3 clientA,
        IAmazonS3 clientB,
        string key,
        CancellationToken cancellationToken)
    {
        if (_configA == null || _configB == null) return false;

        var getA = new GetObjectRequest { BucketName = _configA.BucketName, Key = key };
        var getB = new GetObjectRequest { BucketName = _configB.BucketName, Key = key };

        using var respA = await clientA.GetObjectAsync(getA, cancellationToken).ConfigureAwait(false);
        using var respB = await clientB.GetObjectAsync(getB, cancellationToken).ConfigureAwait(false);

        await using var streamA = respA.ResponseStream;
        await using var streamB = respB.ResponseStream;

        byte[] bufA = new byte[65536];
        byte[] bufB = new byte[65536];

        while (true)
        {
            int bytesReadA = await ReadFullBufferAsync(streamA, bufA, cancellationToken).ConfigureAwait(false);
            int bytesReadB = await ReadFullBufferAsync(streamB, bufB, cancellationToken).ConfigureAwait(false);

            if (bytesReadA != bytesReadB)
                return false;

            if (bytesReadA == 0)
                return true;

            if (!bufA.AsSpan(0, bytesReadA).SequenceEqual(bufB.AsSpan(0, bytesReadB)))
                return false;
        }
    }

    private static async Task<int> ReadFullBufferAsync(Stream stream, byte[] buffer, CancellationToken cancellationToken)
    {
        int totalRead = 0;
        while (totalRead < buffer.Length)
        {
            int read = await stream.ReadAsync(buffer.AsMemory(totalRead, buffer.Length - totalRead), cancellationToken).ConfigureAwait(false);
            if (read == 0)
                break;
            totalRead += read;
        }
        return totalRead;
    }

    private async Task RecordDifferenceAsync(ObjectDifference diff, StreamWriter? csvWriter)
    {
        DifferenceFound?.Invoke(diff);

        if (csvWriter != null)
        {
            string escapeCsv(string? val) => $"\"{val?.Replace("\"", "\"\"") ?? ""}\"";
            string line = $"{diff.Id},{escapeCsv(diff.Key)},{diff.Type},{escapeCsv(diff.Details)},{diff.BucketASize?.ToString() ?? ""},{diff.BucketBSize?.ToString() ?? ""},{escapeCsv(diff.BucketAETag)},{escapeCsv(diff.BucketBETag)},{diff.BucketALastModified:O},{diff.BucketBLastModified:O},{escapeCsv(diff.BucketAStorageClass)},{escapeCsv(diff.BucketBStorageClass)},{escapeCsv(diff.MetadataSummary)},{diff.DetectedAt:O}";
            await csvWriter.WriteLineAsync(line).ConfigureAwait(false);
        }
    }

    private ComparisonStatistics CloneStats()
    {
        return new ComparisonStatistics
        {
            ObjectsCompared = _stats.ObjectsCompared,
            MatchedCount = _stats.MatchedCount,
            TotalDifferences = _stats.TotalDifferences,
            MissingInBucketBCount = _stats.MissingInBucketBCount,
            MissingInBucketACount = _stats.MissingInBucketACount,
            SizeMismatchCount = _stats.SizeMismatchCount,
            ETagMismatchCount = _stats.ETagMismatchCount,
            ContentMismatchCount = _stats.ContentMismatchCount,
            SequenceGapCount = _stats.SequenceGapCount,
            BucketAObjectsRead = _stats.BucketAObjectsRead,
            BucketBObjectsRead = _stats.BucketBObjectsRead,
            BucketABytesRead = _stats.BucketABytesRead,
            BucketBBytesRead = _stats.BucketBBytesRead,
            ElapsedTime = _stats.ElapsedTime,
            ItemsPerSecond = _stats.ItemsPerSecond,
            CurrentKeyA = _stats.CurrentKeyA,
            CurrentKeyB = _stats.CurrentKeyB,
            WorkingSetMB = _stats.WorkingSetMB,
            GCTotalMemoryMB = _stats.GCTotalMemoryMB,
            LastCheckpointKey = _stats.LastCheckpointKey,
            LastCheckpointTime = _stats.LastCheckpointTime
        };
    }
}
