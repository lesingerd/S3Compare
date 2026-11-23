using System.Threading;
using System.Windows;
using S3Compare.App;
using S3Compare.App.ViewModels;

namespace S3Compare.Tests;

public class MainWindowTests
{
    [Fact]
    public void MainWindow_InstantiatesWithoutExceptions()
    {
        Exception? exception = null;
        var thread = new Thread(() =>
        {
            try
            {
                var window = new MainWindow();
                Assert.NotNull(window);
                Assert.IsType<MainViewModel>(window.DataContext);
            }
            catch (Exception ex)
            {
                exception = ex;
            }
        });

        thread.SetApartmentState(ApartmentState.STA);
        thread.Start();
        thread.Join();

        if (exception != null)
        {
            throw new Exception($"MainWindow initialization failed: {exception.Message}", exception);
        }
    }

    [Fact]
    public void ViewModel_PerformanceSettings_DefaultAndPassThrough()
    {
        Exception? exception = null;
        var thread = new Thread(() =>
        {
            try
            {
                var vm = new MainViewModel();
                var defaults = vm.BuildPerformanceOptions();
                Assert.Equal(8, defaults.MaxParallelPartitions);
                Assert.Equal(2, defaults.PrefetchPages);
                Assert.Equal(20, defaults.MaxListRequestsPerSecond);
                Assert.Equal(0, defaults.MaxObjectsPerSecond);
                Assert.Equal(8, defaults.MaxConcurrentContentChecks);
                Assert.Equal(500, defaults.MaxDiscoveryRequests);
                Assert.Equal(60, defaults.MaxPlanningSeconds);
                Assert.Empty(defaults.Validate());

                vm.MaxParallelPartitions = 0;
                vm.MaxObjectsPerSecond = 2500;
                var changed = vm.BuildPerformanceOptions();
                Assert.Equal(2500, changed.MaxObjectsPerSecond);
                Assert.Single(changed.Validate());
            }
            catch (Exception ex)
            {
                exception = ex;
            }
        });

        thread.SetApartmentState(ApartmentState.STA);
        thread.Start();
        thread.Join();

        if (exception != null) throw new Exception(exception.Message, exception);
    }
}
