using System.Diagnostics;
using System.IO;
using Amazon.Runtime;
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
    private readonly string _prefixA;
    private readonly string _prefixB;

    private readonly ComparisonStatistics _stats = new();
    private readonly Stopwatch _stopwatch = new();
    private readonly Stopwatch _pauseWatch = new();
    private long _differenceIdCounter = 0;

    /// <summary>Last fully processed key, relative to each bucket's own prefix.</summary>
    private string _lastEvaluatedKey = string.Empty;

    private FileStream? _csvStream;
    private long _lastProgressMs;
    private long _lastCheckpointMs;
    private long _itemsSinceCheckpoint;

    private volatile bool _isPaused = false;
    private readonly SemaphoreSlim _pauseSemaphore = new(1, 1);

    public event Action<ComparisonStatistics>? ProgressUpdated;
    public event Action<ObjectDifference>? DifferenceFound;
    public event Action<List<S3ObjectSummary>>? PageLoadedA;
    public event Action<List<S3ObjectSummary>>? PageLoadedB;

    /// <summary>
    /// Optional override for FullData byte comparison of an object pair (A item, B item). Intended for tests;
    /// when null the engine downloads both objects through its S3 clients.
    /// </summary>
    public Func<S3ObjectSummary, S3ObjectSummary, CancellationToken, Task<bool>>? ContentComparer { get; set; }

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
        _prefixA = configA.Prefix ?? string.Empty;
        _prefixB = configB.Prefix ?? string.Empty;
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
        int maxSequenceGapThreshold = 25,
        string prefixA = "",
        string prefixB = "",
        ScanCheckpoint? resumeCheckpoint = null)
    {
        _providedStreamA = streamA ?? throw new ArgumentNullException(nameof(streamA));
        _providedStreamB = streamB ?? throw new ArgumentNullException(nameof(streamB));
        _prefixA = prefixA ?? string.Empty;
        _prefixB = prefixB ?? string.Empty;
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

    /// <summary>Strips the bucket's prefix so keys from buckets with different prefixes can be compared.</summary>
    public static string ToRelativeKey(string key, string prefix)
    {
        return !string.IsNullOrEmpty(prefix) && key.StartsWith(prefix, StringComparison.Ordinal)
            ? key.Substring(prefix.Length)
            : key;
    }

    /// <summary>Builds the S3 StartAfter value for a bucket from the checkpoint's relative key.</summary>
    public static string? BuildStartAfter(string prefix, string relativeKey)
    {
        return string.IsNullOrEmpty(relativeKey) ? null : (prefix ?? string.Empty) + relativeKey;
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
        _stats.ContentCheckErrorCount = cp.ContentCheckErrorCount;
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
            _pauseWatch.Start();
            _pauseSemaphore.WaitAsync();
        }
    }

    public void Resume()
    {
        if (_isPaused)
        {
            _isPaused = false;
            _pauseWatch.Stop();
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
        _pauseWatch.Reset();
        if (_isPaused) _pauseWatch.Start();
        _lastProgressMs = 0;
        _lastCheckpointMs = 0;
        _itemsSinceCheckpoint = 0;

        IAmazonS3? clientA = null;
        IAmazonS3? clientB = null;
        IS3ObjectStream? enumA = null;
        IS3ObjectStream? enumB = null;
        ScanDiagnosticsLogger? diagnosticsLogger = null;
        bool completed = false;

        StreamWriter? csvWriter = null;
        if (!string.IsNullOrWhiteSpace(_liveReportCsvPath))
        {
            var dir = Path.GetDirectoryName(_liveReportCsvPath);
            if (!string.IsNullOrEmpty(dir) && !Directory.Exists(dir))
            {
                Directory.CreateDirectory(dir);
            }

            if (_resumeCheckpoint != null)
            {
                // Resume: keep rows written up to the checkpoint and drop anything after it (those items are re-processed).
                _csvStream = new FileStream(_liveReportCsvPath, FileMode.OpenOrCreate, FileAccess.Write, FileShare.Read);
                long keep = _resumeCheckpoint.LiveReportCsvBytes;
                if (keep > 0 && _csvStream.Length > keep)
                {
                    _csvStream.SetLength(keep);
                }
                _csvStream.Seek(0, SeekOrigin.End);
            }
            else
            {
                // Fresh scan: never append to a previous scan's report.
                _csvStream = new FileStream(_liveReportCsvPath, FileMode.Create, FileAccess.Write, FileShare.Read);
            }

            csvWriter = new StreamWriter(_csvStream, CsvReport.Utf8WithBom);

            if (_csvStream.Length == 0)
            {
                await csvWriter.WriteLineAsync(CsvReport.Header).ConfigureAwait(false);
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

                // Each bucket resumes from its own prefix + the shared relative key
                string? startAfterA = BuildStartAfter(_prefixA, _lastEvaluatedKey);
                string? startAfterB = BuildStartAfter(_prefixB, _lastEvaluatedKey);

                enumA = new S3PagingEnumerator(
                    clientA,
                    _configA.BucketName,
                    _configA.Prefix,
                    startAfterA,
                    (page, _) => SafeRaise(PageLoadedA, page));

                enumB = new S3PagingEnumerator(
                    clientB,
                    _configB.BucketName,
                    _configB.Prefix,
                    startAfterB,
                    (page, _) => SafeRaise(PageLoadedB, page));
            }
            else
            {
                throw new InvalidOperationException("No valid S3 configuration or object streams provided.");
            }

            bool hasA = await enumA.MoveNextAsync(cancellationToken).ConfigureAwait(false);
            bool hasB = await enumB.MoveNextAsync(cancellationToken).ConfigureAwait(false);

            while (hasA || hasB)
            {
                await CheckPauseAsync(cancellationToken).ConfigureAwait(false);
                cancellationToken.ThrowIfCancellationRequested();

                S3ObjectSummary? itemA = hasA ? enumA.Current : null;
                S3ObjectSummary? itemB = hasB ? enumB.Current : null;
                string? relA = null;
                string? relB = null;

                if (itemA != null)
                {
                    _stats.CurrentKeyA = itemA.Key;
                    relA = ToRelativeKey(itemA.Key, _prefixA);
                }
                if (itemB != null)
                {
                    _stats.CurrentKeyB = itemB.Key;
                    relB = ToRelativeKey(itemB.Key, _prefixB);
                }

                // cmp < 0: only in A (so far); cmp > 0: only in B; cmp == 0: in both
                int cmp = itemB == null ? -1
                    : itemA == null ? 1
                    : Utf8KeyComparer.Instance.Compare(relA, relB);

                string processedRelativeKey;
                if (cmp == 0)
                {
                    processedRelativeKey = relA!;
                    await CompareSameKeyAsync(clientA, clientB, itemA!, itemB!, relA!, csvWriter, cancellationToken).ConfigureAwait(false);
                }
                else if (cmp < 0)
                {
                    processedRelativeKey = relA!;
                    var diff = new ObjectDifference
                    {
                        Id = ++_differenceIdCounter,
                        Key = DisplayKey(itemA!, relA!),
                        Type = DifferenceType.MissingInBucketB,
                        Details = $"Object exists in Bucket A but is missing in Bucket B. Size: {itemA!.FormattedSize}, ETag: {itemA.ETag}, StorageClass: {itemA.StorageClass}, LastModified: {itemA.LastModified:yyyy-MM-dd HH:mm:ss}",
                        BucketASize = itemA.Size,
                        BucketAETag = itemA.ETag,
                        BucketALastModified = itemA.LastModified,
                        BucketAStorageClass = itemA.StorageClass
                    };
                    await RecordDifferenceAsync(diff, csvWriter).ConfigureAwait(false);
                    _stats.MissingInBucketBCount++;
                    _stats.TotalDifferences++;
                }
                else
                {
                    processedRelativeKey = relB!;
                    var diff = new ObjectDifference
                    {
                        Id = ++_differenceIdCounter,
                        Key = DisplayKey(itemB!, relB!),
                        Type = DifferenceType.MissingInBucketA,
                        Details = $"Object exists in Bucket B but is missing in Bucket A. Size: {itemB!.FormattedSize}, ETag: {itemB.ETag}, StorageClass: {itemB.StorageClass}, LastModified: {itemB.LastModified:yyyy-MM-dd HH:mm:ss}",
                        BucketBSize = itemB.Size,
                        BucketBETag = itemB.ETag,
                        BucketBLastModified = itemB.LastModified,
                        BucketBStorageClass = itemB.StorageClass
                    };
                    await RecordDifferenceAsync(diff, csvWriter).ConfigureAwait(false);
                    _stats.MissingInBucketACount++;
                    _stats.TotalDifferences++;
                }

                await CheckSequenceGapAsync(processedRelativeKey, csvWriter).ConfigureAwait(false);

                // Commit: the item is fully processed, so a checkpoint taken from here on may skip it.
                _stats.ObjectsCompared++;
                _lastEvaluatedKey = processedRelativeKey;

                if (cmp <= 0) hasA = await enumA.MoveNextAsync(cancellationToken).ConfigureAwait(false);
                if (cmp >= 0) hasB = await enumB.MoveNextAsync(cancellationToken).ConfigureAwait(false);

                await TickAsync(enumA, enumB, diagnosticsLogger, csvWriter).ConfigureAwait(false);
            }

            // Completed without cancellation: remove the active checkpoint
            if (_configA != null && _configB != null)
            {
                var finalCheckpoint = CreateCheckpointSnapshot();
                CheckpointManager.DeleteCheckpoint(finalCheckpoint);
                if (diagnosticsLogger != null)
                {
                    await diagnosticsLogger.LogInfoAsync($"Scan completed successfully. Total objects: {_stats.ObjectsCompared:N0}, Matched: {_stats.MatchedCount:N0}, Differences: {_stats.TotalDifferences:N0}").ConfigureAwait(false);
                }
            }
            completed = true;
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
            _pauseWatch.Stop();

            // Stopped or failed: persist where we got to (also flushes the live CSV to match the checkpoint)
            if (!completed)
            {
                await SaveCheckpointIfNeededAsync(diagnosticsLogger, csvWriter).ConfigureAwait(false);
            }

            UpdateLiveStats(enumA, enumB);
            SafeRaise(ProgressUpdated, CloneStats());

            if (csvWriter != null)
            {
                try
                {
                    await csvWriter.FlushAsync().ConfigureAwait(false);
                }
                finally
                {
                    await csvWriter.DisposeAsync().ConfigureAwait(false);
                    _csvStream = null;
                }
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

        return CloneStats();
    }

    /// <summary>
    /// Key shown to the user. With a shared prefix this is the full key; with different prefixes the
    /// keys are only comparable relative to their prefixes, so the relative key is shown.
    /// </summary>
    private string DisplayKey(S3ObjectSummary item, string relativeKey)
        => _prefixA == _prefixB ? item.Key : relativeKey;

    private static void SafeRaise<T>(Action<T>? handler, T arg)
    {
        if (handler == null) return;
        try
        {
            handler(arg);
        }
        catch
        {
            // A misbehaving subscriber (e.g. a closing window) must never abort the scan.
        }
    }

    private async Task CheckSequenceGapAsync(string relativeKey, StreamWriter? csvWriter)
    {
        if (_gapDetector == null) return;

        var gaps = _gapDetector.CheckKey(relativeKey, ref _differenceIdCounter);
        foreach (var gap in gaps)
        {
            if (_prefixA == _prefixB)
            {
                gap.Key = _prefixA + gap.Key;
            }
            await RecordDifferenceAsync(gap, csvWriter).ConfigureAwait(false);
            _stats.SequenceGapCount++;
            _stats.TotalDifferences++;
        }
    }

    /// <summary>Per-item bookkeeping; expensive work (stats, memory, UI notification) only runs on the ~200ms tick.</summary>
    private async Task TickAsync(IS3ObjectStream enumA, IS3ObjectStream enumB, ScanDiagnosticsLogger? diagnosticsLogger, StreamWriter? csvWriter)
    {
        _itemsSinceCheckpoint++;
        long elapsedMs = ActiveElapsedMs();

        if (elapsedMs - _lastCheckpointMs > 5000 || _itemsSinceCheckpoint >= 10000)
        {
            _lastCheckpointMs = elapsedMs;
            _itemsSinceCheckpoint = 0;
            await SaveCheckpointIfNeededAsync(diagnosticsLogger, csvWriter).ConfigureAwait(false);
        }

        if (elapsedMs - _lastProgressMs > 200)
        {
            _lastProgressMs = elapsedMs;
            UpdateLiveStats(enumA, enumB);
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
            SafeRaise(ProgressUpdated, CloneStats());
        }
    }

    private long ActiveElapsedMs() => (long)(_stopwatch.Elapsed - _pauseWatch.Elapsed).TotalMilliseconds;

    private async Task SaveCheckpointIfNeededAsync(ScanDiagnosticsLogger? diagnosticsLogger, StreamWriter? csvWriter)
    {
        if (_configA == null || _configB == null || string.IsNullOrEmpty(_lastEvaluatedKey))
            return;

        try
        {
            // The CSV must be on disk before the checkpoint records its length
            if (csvWriter != null)
            {
                await csvWriter.FlushAsync().ConfigureAwait(false);
            }

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
        UpdateElapsed();
        return new ScanCheckpoint
        {
            BucketNameA = _configA?.BucketName ?? "",
            EndpointA = _configA?.EndpointUrl ?? "",
            RegionA = _configA?.Region ?? "",
            PrefixA = _prefixA,
            BucketNameB = _configB?.BucketName ?? "",
            EndpointB = _configB?.EndpointUrl ?? "",
            RegionB = _configB?.Region ?? "",
            PrefixB = _prefixB,
            Mode = _mode,
            LastEvaluatedKey = _lastEvaluatedKey,
            LiveReportCsvPath = _liveReportCsvPath,
            LiveReportCsvBytes = _csvStream?.Length ?? 0,
            ObjectsCompared = _stats.ObjectsCompared,
            MatchedCount = _stats.MatchedCount,
            TotalDifferences = _stats.TotalDifferences,
            MissingInBucketBCount = _stats.MissingInBucketBCount,
            MissingInBucketACount = _stats.MissingInBucketACount,
            SizeMismatchCount = _stats.SizeMismatchCount,
            ETagMismatchCount = _stats.ETagMismatchCount,
            ContentMismatchCount = _stats.ContentMismatchCount,
            ContentCheckErrorCount = _stats.ContentCheckErrorCount,
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

    /// <summary>Elapsed scan time excluding time spent paused (plus time accumulated before a resume).</summary>
    private void UpdateElapsed()
    {
        double totalSeconds = (_stopwatch.Elapsed - _pauseWatch.Elapsed).TotalSeconds;
        if (_resumeCheckpoint != null)
        {
            totalSeconds += _resumeCheckpoint.ElapsedSeconds;
        }
        _stats.ElapsedTime = TimeSpan.FromSeconds(totalSeconds);
        _stats.ItemsPerSecond = totalSeconds > 0 ? _stats.ObjectsCompared / totalSeconds : 0;
    }

    private void UpdateLiveStats(IS3ObjectStream? enumA, IS3ObjectStream? enumB)
    {
        UpdateElapsed();

        // Cheap, allocation-free memory probes (Process.GetCurrentProcess() costs ~20ms and leaks a handle)
        _stats.WorkingSetMB = Environment.WorkingSet / (1024.0 * 1024.0);
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

    private ObjectDifference BuildPairDifference(S3ObjectSummary itemA, S3ObjectSummary itemB, string relativeKey, DifferenceType type, string details)
    {
        return new ObjectDifference
        {
            Id = ++_differenceIdCounter,
            Key = DisplayKey(itemA, relativeKey),
            Type = type,
            Details = details,
            BucketASize = itemA.Size,
            BucketBSize = itemB.Size,
            BucketAETag = itemA.ETag,
            BucketBETag = itemB.ETag,
            BucketALastModified = itemA.LastModified,
            BucketBLastModified = itemB.LastModified,
            BucketAStorageClass = itemA.StorageClass,
            BucketBStorageClass = itemB.StorageClass
        };
    }

    private async Task CompareSameKeyAsync(
        IAmazonS3? clientA,
        IAmazonS3? clientB,
        S3ObjectSummary itemA,
        S3ObjectSummary itemB,
        string relativeKey,
        StreamWriter? csvWriter,
        CancellationToken cancellationToken)
    {
        // 1. Check size
        if (itemA.Size != itemB.Size)
        {
            var diff = BuildPairDifference(itemA, itemB, relativeKey, DifferenceType.SizeMismatch,
                $"Size mismatch: A has {itemA.FormattedSize} ({itemA.Size} B), B has {itemB.FormattedSize} ({itemB.Size} B)");
            await RecordDifferenceAsync(diff, csvWriter).ConfigureAwait(false);
            _stats.SizeMismatchCount++;
            _stats.TotalDifferences++;
            return;
        }

        // 2. Check ETags
        if (string.Equals(itemA.ETag, itemB.ETag, StringComparison.OrdinalIgnoreCase))
        {
            _stats.MatchedCount++;
            return;
        }

        // ETags differ
        bool canDownload = ContentComparer != null
            || (clientA != null && clientB != null && _configA != null && _configB != null);

        if (_mode == ComparisonMode.ListingOnly || !canDownload)
        {
            var diff = BuildPairDifference(itemA, itemB, relativeKey, DifferenceType.ETagMismatch,
                $"ETags differ (A: {itemA.ETag}, B: {itemB.ETag}). Size is identical ({itemA.FormattedSize}). Note: multipart uploads have different ETags even if content matches.");
            await RecordDifferenceAsync(diff, csvWriter).ConfigureAwait(false);
            _stats.ETagMismatchCount++;
            _stats.TotalDifferences++;
            return;
        }

        // Full Data mode
        // Always download: single-part ETags are not MD5 content hashes for SSE-KMS/SSE-C objects,
        // so differing ETags alone do not prove the content differs.
        bool contentMatches;
        try
        {
            contentMatches = ContentComparer != null
                ? await ContentComparer(itemA, itemB, cancellationToken).ConfigureAwait(false)
                : await StreamCompareObjectsAsync(clientA!, clientB!, itemA.Key, itemB.Key, cancellationToken).ConfigureAwait(false);
        }
        catch (AmazonServiceException ex)
        {
            // Glacier InvalidObjectState, 404 races, AccessDenied, ...: record the object and keep scanning.
            var errDiff = BuildPairDifference(itemA, itemB, relativeKey, DifferenceType.ContentCheckError,
                $"Content could not be verified: {(string.IsNullOrEmpty(ex.ErrorCode) ? ex.GetType().Name : ex.ErrorCode)}: {ex.Message}");
            await RecordDifferenceAsync(errDiff, csvWriter).ConfigureAwait(false);
            _stats.ContentCheckErrorCount++;
            _stats.TotalDifferences++;
            return;
        }

        if (contentMatches)
        {
            _stats.MatchedCount++;
        }
        else
        {
            var diff = BuildPairDifference(itemA, itemB, relativeKey, DifferenceType.ContentMismatch,
                $"Content byte mismatch! Size is identical ({itemA.FormattedSize}) but data stream differs.");
            await RecordDifferenceAsync(diff, csvWriter).ConfigureAwait(false);
            _stats.ContentMismatchCount++;
            _stats.TotalDifferences++;
        }
    }

    private async Task<bool> StreamCompareObjectsAsync(
        IAmazonS3 clientA,
        IAmazonS3 clientB,
        string keyA,
        string keyB,
        CancellationToken cancellationToken)
    {
        if (_configA == null || _configB == null) return false;

        var getA = new GetObjectRequest { BucketName = _configA.BucketName, Key = keyA };
        var getB = new GetObjectRequest { BucketName = _configB.BucketName, Key = keyB };

        // Open both streams concurrently
        var taskA = clientA.GetObjectAsync(getA, cancellationToken);
        var taskB = clientB.GetObjectAsync(getB, cancellationToken);
        try
        {
            await Task.WhenAll(taskA, taskB).ConfigureAwait(false);
        }
        catch
        {
            // WhenAll waits for both; release whichever request did succeed
            if (taskA.Status == TaskStatus.RanToCompletion) taskA.Result.Dispose();
            if (taskB.Status == TaskStatus.RanToCompletion) taskB.Result.Dispose();
            throw;
        }

        using var respA = taskA.Result;
        using var respB = taskB.Result;

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
        SafeRaise(DifferenceFound, diff);

        if (csvWriter != null)
        {
            await csvWriter.WriteLineAsync(CsvReport.BuildLine(diff)).ConfigureAwait(false);
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
            ContentCheckErrorCount = _stats.ContentCheckErrorCount,
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
