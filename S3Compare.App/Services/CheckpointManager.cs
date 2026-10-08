using System.IO;
using System.Security.Cryptography;
using System.Text;
using System.Text.Json;
using S3Compare.App.Models;

namespace S3Compare.App.Services;

public static class CheckpointManager
{
    private static readonly JsonSerializerOptions JsonOptions = new() { WriteIndented = true };

    public static string GetCheckpointDirectory(string? customBaseDir = null)
    {
        string dir = Path.Combine(customBaseDir ?? AppDomain.CurrentDomain.BaseDirectory, "logs", "checkpoints");
        if (!Directory.Exists(dir))
        {
            Directory.CreateDirectory(dir);
        }
        return dir;
    }

    public static string ComputeSessionHash(string bucketA, string endpointA, string prefixA, string bucketB, string endpointB, string prefixB)
    {
        string raw = $"{bucketA.Trim().ToLowerInvariant()}|{endpointA.Trim().ToLowerInvariant()}|{prefixA.Trim()}|{bucketB.Trim().ToLowerInvariant()}|{endpointB.Trim().ToLowerInvariant()}|{prefixB.Trim()}";
        byte[] hash = SHA256.HashData(Encoding.UTF8.GetBytes(raw));
        return Convert.ToHexString(hash)[..12].ToLowerInvariant();
    }

    public static string GetCheckpointFilePath(ScanCheckpoint cp, string? customBaseDir = null)
    {
        string dir = GetCheckpointDirectory(customBaseDir);
        string hash = ComputeSessionHash(cp.BucketNameA, cp.EndpointA, cp.PrefixA, cp.BucketNameB, cp.EndpointB, cp.PrefixB);
        string safeNameA = SanitizeFileName(cp.BucketNameA);
        string safeNameB = SanitizeFileName(cp.BucketNameB);
        return Path.Combine(dir, $"checkpoint_{safeNameA}_vs_{safeNameB}_{hash}.json");
    }

    public static async Task<string> SaveCheckpointAsync(ScanCheckpoint checkpoint, string? customBaseDir = null)
    {
        string targetPath = GetCheckpointFilePath(checkpoint, customBaseDir);
        string tempPath = targetPath + ".tmp";

        checkpoint.LastUpdatedAt = DateTime.UtcNow;

        string json = JsonSerializer.Serialize(checkpoint, JsonOptions);
        await File.WriteAllTextAsync(tempPath, json, Encoding.UTF8).ConfigureAwait(false);

        // Atomic replace
        if (File.Exists(targetPath))
        {
            File.Delete(targetPath);
        }
        File.Move(tempPath, targetPath);

        return targetPath;
    }

    public static async Task<ScanCheckpoint?> FindCheckpointAsync(
        string bucketA,
        string endpointA,
        string prefixA,
        string bucketB,
        string endpointB,
        string prefixB,
        string? customBaseDir = null)
    {
        if (string.IsNullOrWhiteSpace(bucketA) || string.IsNullOrWhiteSpace(bucketB)) return null;

        string dir = GetCheckpointDirectory(customBaseDir);
        string hash = ComputeSessionHash(bucketA, endpointA, prefixA, bucketB, endpointB, prefixB);

        var matchingFiles = Directory.GetFiles(dir, $"checkpoint_*_{hash}.json");
        if (matchingFiles.Length == 0) return null;

        try
        {
            string content = await File.ReadAllTextAsync(matchingFiles[0], Encoding.UTF8).ConfigureAwait(false);
            return JsonSerializer.Deserialize<ScanCheckpoint>(content, JsonOptions);
        }
        catch
        {
            return null;
        }
    }

    public static void DeleteCheckpoint(ScanCheckpoint checkpoint, string? customBaseDir = null)
    {
        try
        {
            string path = GetCheckpointFilePath(checkpoint, customBaseDir);
            if (File.Exists(path))
            {
                File.Delete(path);
            }
        }
        catch
        {
            // Ignore delete errors
        }
    }

    private static string SanitizeFileName(string name)
    {
        var invalid = Path.GetInvalidFileNameChars();
        var sb = new StringBuilder();
        foreach (char c in name)
        {
            sb.Append(invalid.Contains(c) ? '_' : c);
        }
        return sb.ToString();
    }
}
