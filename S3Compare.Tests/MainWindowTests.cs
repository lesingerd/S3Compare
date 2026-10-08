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
                var vm = new MainViewModel();
                Assert.NotNull(vm);

                var window = new MainWindow();
                Assert.NotNull(window);
                Assert.NotNull(window.DataContext);
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
}
