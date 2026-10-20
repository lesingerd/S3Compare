namespace S3Compare.App.Services;

/// <summary>
/// Orders S3 keys the way S3 lists them: by UTF-8 byte order, which equals Unicode code point order.
/// Plain UTF-16 ordinal comparison differs for supplementary-plane characters (surrogates) versus U+E000..U+FFFF.
/// </summary>
public sealed class Utf8KeyComparer : IComparer<string>
{
    public static readonly Utf8KeyComparer Instance = new();

    private Utf8KeyComparer()
    {
    }

    public int Compare(string? x, string? y)
    {
        if (ReferenceEquals(x, y)) return 0;
        if (x is null) return -1;
        if (y is null) return 1;

        int len = Math.Min(x.Length, y.Length);
        for (int i = 0; i < len; i++)
        {
            char cx = x[i];
            char cy = y[i];
            if (cx == cy) continue;

            // Surrogates (D800-DFFF) represent code points above U+FFFF, so they must sort after U+E000-U+FFFF.
            if (cx >= 0xD800 && cy >= 0xD800)
            {
                cx = Fixup(cx);
                cy = Fixup(cy);
            }
            return cx < cy ? -1 : 1;
        }

        return x.Length.CompareTo(y.Length);
    }

    private static char Fixup(char c) => c >= 0xE000 ? (char)(c - 0x800) : (char)(c + 0x2000);
}
