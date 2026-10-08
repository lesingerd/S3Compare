using System.Runtime.InteropServices;
using System.Windows;

namespace S3Compare.App;

public partial class App : Application
{
    [DllImport("kernel32.dll", SetLastError = true)]
    private static extern bool AttachConsole(int dwProcessId);

    private const int ATTACH_PARENT_PROCESS = -1;

    protected override void OnStartup(StartupEventArgs e)
    {
        base.OnStartup(e);

        try
        {
            if (AttachConsole(ATTACH_PARENT_PROCESS))
            {
                Console.WriteLine();
                Console.WriteLine("==================================================");
                Console.WriteLine("  S3Compare - Wasabi & S3 Bucket Comparison Tool  ");
                Console.WriteLine("==================================================");
                Console.WriteLine("[INFO] S3Compare application initialized.");
                Console.WriteLine("[INFO] Creating and displaying main window...");
            }
        }
        catch
        {
            // Ignore console attach failure in GUI mode
        }

        DispatcherUnhandledException += (sender, args) =>
        {
            try
            {
                Console.WriteLine($"[ERROR] Unhandled Exception: {args.Exception.Message}");
            }
            catch { }

            MessageBox.Show(
                $"An error occurred while running S3Compare:\n\n{args.Exception.Message}\n\nStack Trace:\n{args.Exception.StackTrace}",
                "S3Compare Error",
                MessageBoxButton.OK,
                MessageBoxImage.Error);
            args.Handled = true;
        };

        try
        {
            var window = new MainWindow();
            MainWindow = window;
            window.Show();
            window.Activate();

            try
            {
                Console.WriteLine("[INFO] Main window is now visible on screen.");
            }
            catch { }
        }
        catch (Exception ex)
        {
            try
            {
                Console.WriteLine($"[FATAL] Failed to create or show MainWindow: {ex.Message}");
            }
            catch { }

            MessageBox.Show($"Failed to launch MainWindow:\n\n{ex}", "S3Compare Launch Error", MessageBoxButton.OK, MessageBoxImage.Error);
        }
    }
}
