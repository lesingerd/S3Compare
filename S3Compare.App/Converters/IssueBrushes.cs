using System.Globalization;
using System.Windows.Data;
using System.Windows.Media;
using S3Compare.App.Models;

namespace S3Compare.App.Converters;

/// <summary>Colour of an issue: only Failed is red; Degraded/Recorded amber; Skipped/Recovered muted.</summary>
public static class IssueBrushes
{
    public static readonly SolidColorBrush Failed = Make(248, 113, 113);     // red
    public static readonly SolidColorBrush Warning = Make(251, 191, 36);     // amber (Degraded, Recorded)
    public static readonly SolidColorBrush Skipped = Make(148, 163, 184);    // muted grey
    public static readonly SolidColorBrush Recovered = Make(134, 190, 160);  // muted green
    public static readonly SolidColorBrush None = Make(148, 163, 184);

    public static SolidColorBrush For(IssueOutcome outcome) => outcome switch
    {
        IssueOutcome.Failed => Failed,
        IssueOutcome.Degraded or IssueOutcome.Recorded => Warning,
        IssueOutcome.Skipped => Skipped,
        _ => Recovered
    };

    public static SolidColorBrush Frozen(byte r, byte g, byte b) => Make(r, g, b);

    private static SolidColorBrush Make(byte r, byte g, byte b)
    {
        var brush = new SolidColorBrush(Color.FromRgb(r, g, b));
        brush.Freeze();
        return brush;
    }
}

public class IssueOutcomeToBrushConverter : IValueConverter
{
    public object Convert(object? value, Type targetType, object? parameter, CultureInfo culture)
        => value is IssueOutcome outcome ? IssueBrushes.For(outcome) : IssueBrushes.None;

    public object ConvertBack(object? value, Type targetType, object? parameter, CultureInfo culture)
        => throw new NotSupportedException();
}
