using System.Diagnostics;
using System.IO;
using S3Compare.App.Models;

namespace S3Compare.App.Services;

public class ScanDiagnosticsLogger : IAsyncDisposable
{
    private readonly string _logFilePath;
    private readonly StreamWriter _writer;
    private readonly Stopwatch _stopwatch = new();
    private long _lastFlushTime = 0;

    public string LogFilePath => _logFilePath;

    public ScanDiagnosticsLogger(string? customLogDirectory = null)
    {
        string dir = customLogDirectory ?? AppPaths.LogsDirectory;
        if (!Directory.Exists(dir))
        {
            Directory.CreateDirectory(dir);
        }

        string fileName = $"scan_diagnostics_{DateTime.Now:yyyyMMdd_HHmmss}.log";
        _logFilePath = Path.Combine(dir, fileName);

        var fileStream = new FileStream(_logFilePath, FileMode.Create, FileAccess.Write, FileShare.ReadWrite);
        _writer = new StreamWriter(fileStream) { AutoFlush = false };
        _stopwatch.Start();

        WriteHeader();
    }

    private void WriteHeader()
    {
        var proc = Process.GetCurrentProcess();
        _writer.WriteLine("================================================================================");
        _writer.WriteLine("             S3COMPARE - STREAMING SCAN DIAGNOSTICS & SYSTEM AUDIT             ");
        _writer.WriteLine("================================================================================");
        _writer.WriteLine($"Start Time (Local)   : {DateTime.Now:O}");
        _writer.WriteLine($"Start Time (UTC)     : {DateTime.UtcNow:O}");
        _writer.WriteLine($"Process ID           : {proc.Id} ({proc.ProcessName})");
        _writer.WriteLine($"64-bit Process       : {Environment.Is64BitProcess}");
        _writer.WriteLine($"OS Version           : {Environment.OSVersion}");
        _writer.WriteLine($"Logical Processors   : {Environment.ProcessorCount}");
        _writer.WriteLine($".NET Runtime         : {Environment.Version}");
        _writer.WriteLine($"Initial Working Set  : {proc.WorkingSet64 / (1024.0 * 1024.0):F2} MB");
        _writer.WriteLine("================================================================================");
        _writer.WriteLine();
        _writer.Flush();
    }

    public void LogSessionConfig(S3BucketConfig configA, S3BucketConfig configB, ComparisonMode mode, string? startAfter)
    {
        _writer.WriteLine($"[CONFIG] Bucket A   : {configA.BucketName} @ {configA.EndpointUrl} (Prefix: '{configA.Prefix}')");
        _writer.WriteLine($"[CONFIG] Bucket B   : {configB.BucketName} @ {configB.EndpointUrl} (Prefix: '{configB.Prefix}')");
        _writer.WriteLine($"[CONFIG] Mode       : {mode}");
        _writer.WriteLine($"[CONFIG] StartAfter : {(string.IsNullOrWhiteSpace(startAfter) ? "[Beginning of Bucket]" : startAfter)}");
        _writer.WriteLine($"[SECURITY] Zero auth credentials or keys recorded in diagnostics.");
        _writer.WriteLine();
        _writer.Flush();
    }

    public async Task LogHeartbeatAsync(
        long objectsCompared,
        double itemsPerSecond,
        string keyA,
        string keyB,
        long totalDifferences,
        TimeSpan elapsed,
        bool forceFlush = false)
    {
        long now = _stopwatch.ElapsedMilliseconds;
        if (!forceFlush && now - _lastFlushTime < 5000)
        {
            return;
        }
        _lastFlushTime = now;

        var proc = Process.GetCurrentProcess();
        double workingSetMB = proc.WorkingSet64 / (1024.0 * 1024.0);
        double gcTotalMB = GC.GetTotalMemory(false) / (1024.0 * 1024.0);
        int gen0 = GC.CollectionCount(0);
        int gen1 = GC.CollectionCount(1);
        int gen2 = GC.CollectionCount(2);

        string line = $"[{DateTime.Now:yyyy-MM-dd HH:mm:ss.fff}] [HEARTBEAT] Evaluated: {objectsCompared:N0} ({itemsPerSecond:N0} obj/s) | Diffs: {totalDifferences:N0} | Elapsed: {elapsed:hh\\:mm\\:ss} | RAM WorkingSet: {workingSetMB:F1} MB, GC Total: {gcTotalMB:F1} MB (Gen0/1/2: {gen0}/{gen1}/{gen2}) | CurrentKeyA: '{Truncate(keyA, 60)}' | CurrentKeyB: '{Truncate(keyB, 60)}'";
        await _writer.WriteLineAsync(line).ConfigureAwait(false);
        await _writer.FlushAsync().ConfigureAwait(false);
    }

    public async Task LogCheckpointAsync(string checkpointFile, string lastKey, long objectsCompared)
    {
        string line = $"[{DateTime.Now:yyyy-MM-dd HH:mm:ss.fff}] [CHECKPOINT] Checkpoint saved at key '{lastKey}' (Total evaluated: {objectsCompared:N0}) -> {Path.GetFileName(checkpointFile)}";
        await _writer.WriteLineAsync(line).ConfigureAwait(false);
        await _writer.FlushAsync().ConfigureAwait(false);
    }

    public async Task LogErrorAsync(string context, Exception ex)
    {
        string line = $"[{DateTime.Now:yyyy-MM-dd HH:mm:ss.fff}] [ERROR] [{context}] {ex.GetType().Name}: {ex.Message}\n{ex.StackTrace}";
        await _writer.WriteLineAsync(line).ConfigureAwait(false);
        await _writer.FlushAsync().ConfigureAwait(false);
    }

    public async Task LogInfoAsync(string message)
    {
        string line = $"[{DateTime.Now:yyyy-MM-dd HH:mm:ss.fff}] [INFO] {message}";
        await _writer.WriteLineAsync(line).ConfigureAwait(false);
        await _writer.FlushAsync().ConfigureAwait(false);
    }

    private static string Truncate(string val, int maxLen)
    {
        if (string.IsNullOrEmpty(val)) return "-";
        return val.Length <= maxLen ? val : val.Substring(0, maxLen - 3) + "...";
    }

    public async ValueTask DisposeAsync()
    {
        _writer.WriteLine($"[{DateTime.Now:yyyy-MM-dd HH:mm:ss.fff}] [SHUTDOWN] Diagnostics logging finalized.");
        await _writer.FlushAsync().ConfigureAwait(false);
        await _writer.DisposeAsync().ConfigureAwait(false);
    }
}
