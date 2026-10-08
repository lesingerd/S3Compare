using System.Windows;
using S3Compare.App.ViewModels;

namespace S3Compare.App;

public partial class MainWindow : Window
{
    private bool _showSecretA = false;
    private bool _showSecretB = false;

    public MainWindow()
    {
        InitializeComponent();
        Loaded += async (s, e) =>
        {
            Activate();
            Focus();
            await Task.Delay(2000);
            Topmost = false;
        };
    }

    private void TxtSecretKeyA_PasswordChanged(object sender, RoutedEventArgs e)
    {
        if (DataContext is MainViewModel vm && !_showSecretA)
        {
            vm.SecretKeyA = TxtSecretKeyA.Password;
        }
    }

    private void TxtSecretKeyB_PasswordChanged(object sender, RoutedEventArgs e)
    {
        if (DataContext is MainViewModel vm && !_showSecretB)
        {
            vm.SecretKeyB = TxtSecretKeyB.Password;
        }
    }

    private void BtnToggleSecretA_Click(object sender, RoutedEventArgs e)
    {
        if (DataContext is not MainViewModel vm) return;

        _showSecretA = !_showSecretA;
        if (_showSecretA)
        {
            TxtSecretKeyAVisible.Text = TxtSecretKeyA.Password;
            TxtSecretKeyA.Visibility = Visibility.Collapsed;
            TxtSecretKeyAVisible.Visibility = Visibility.Visible;
            BtnToggleSecretA.Content = "🔒";
        }
        else
        {
            TxtSecretKeyA.Password = TxtSecretKeyAVisible.Text;
            vm.SecretKeyA = TxtSecretKeyAVisible.Text;
            TxtSecretKeyAVisible.Visibility = Visibility.Collapsed;
            TxtSecretKeyA.Visibility = Visibility.Visible;
            BtnToggleSecretA.Content = "👁";
        }
    }

    private void BtnToggleSecretB_Click(object sender, RoutedEventArgs e)
    {
        if (DataContext is not MainViewModel vm) return;

        _showSecretB = !_showSecretB;
        if (_showSecretB)
        {
            TxtSecretKeyBVisible.Text = TxtSecretKeyB.Password;
            TxtSecretKeyB.Visibility = Visibility.Collapsed;
            TxtSecretKeyBVisible.Visibility = Visibility.Visible;
            BtnToggleSecretB.Content = "🔒";
        }
        else
        {
            TxtSecretKeyB.Password = TxtSecretKeyBVisible.Text;
            vm.SecretKeyB = TxtSecretKeyBVisible.Text;
            TxtSecretKeyBVisible.Visibility = Visibility.Collapsed;
            TxtSecretKeyB.Visibility = Visibility.Visible;
            BtnToggleSecretB.Content = "👁";
        }
    }
}