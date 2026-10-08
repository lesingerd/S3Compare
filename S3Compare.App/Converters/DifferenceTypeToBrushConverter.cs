using System.Globalization;
using System.Windows.Data;
using System.Windows.Media;
using S3Compare.App.Models;

namespace S3Compare.App.Converters;

public class DifferenceTypeToBrushConverter : IValueConverter
{
    private static readonly SolidColorBrush AmberBrush = new(Color.FromRgb(217, 119, 6)); // Missing in B
    private static readonly SolidColorBrush BlueBrush = new(Color.FromRgb(37, 99, 235));   // Missing in A
    private static readonly SolidColorBrush RedBrush = new(Color.FromRgb(220, 38, 38));    // Size / Content mismatch
    private static readonly SolidColorBrush PurpleBrush = new(Color.FromRgb(147, 51, 234));// ETag mismatch
    private static readonly SolidColorBrush PinkBrush = new(Color.FromRgb(219, 39, 119));  // Possible Missing On Both Sides
    private static readonly SolidColorBrush GrayBrush = new(Color.FromRgb(107, 114, 128));

    static DifferenceTypeToBrushConverter()
    {
        AmberBrush.Freeze();
        BlueBrush.Freeze();
        RedBrush.Freeze();
        PurpleBrush.Freeze();
        PinkBrush.Freeze();
        GrayBrush.Freeze();
    }

    public object Convert(object? value, Type targetType, object? parameter, CultureInfo culture)
    {
        if (value is DifferenceType diffType)
        {
            return diffType switch
            {
                DifferenceType.MissingInBucketB => AmberBrush,
                DifferenceType.MissingInBucketA => BlueBrush,
                DifferenceType.SizeMismatch => RedBrush,
                DifferenceType.ContentMismatch => RedBrush,
                DifferenceType.ETagMismatch => PurpleBrush,
                DifferenceType.PossibleMissingOnBothSides => PinkBrush,
                _ => GrayBrush
            };
        }
        return GrayBrush;
    }

    public object ConvertBack(object? value, Type targetType, object? parameter, CultureInfo culture) => throw new NotImplementedException();
}
