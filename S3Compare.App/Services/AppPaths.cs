using System.IO;

namespace S3Compare.App.Services;

public static class AppPaths
{
    /// <summary>Per-user writable data root: %LocalAppData%\S3Compare (the install directory may be read-only).</summary>
    public static string DataDirectory { get; } = Path.Combine(
        Environment.GetFolderPath(Environment.SpecialFolder.LocalApplicationData),
        "S3Compare");

    public static string LogsDirectory => Path.Combine(DataDirectory, "logs");

    public static string SessionReportsDirectory => Path.Combine(DataDirectory, "reports");
}
