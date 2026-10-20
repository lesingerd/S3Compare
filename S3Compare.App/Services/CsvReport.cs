using System.Globalization;
using System.IO;
using System.Text;
using S3Compare.App.Models;

namespace S3Compare.App.Services;

/// <summary>
/// Single source of truth for the difference CSV format shared by the live report and the exported report.
/// </summary>
public static class CsvReport
{
    public const string Header = "Id,Key,Type,Details,BucketASize,BucketBSize,BucketAETag,BucketBETag,BucketALastModified,BucketBLastModified,BucketAStorageClass,BucketBStorageClass,MetadataSummary,Timestamp";

    /// <summary>UTF-8 with BOM so Excel detects the encoding.</summary>
    public static readonly Encoding Utf8WithBom = new UTF8Encoding(encoderShouldEmitUTF8Identifier: true);

    private static bool IsFormulaTrigger(char c) => c is '=' or '+' or '-' or '@' or '\t' or '\r';

    /// <summary>Quotes a text cell, neutralising spreadsheet formula injection by prefixing a single quote.</summary>
    public static string Escape(string? value)
    {
        value ??= string.Empty;
        if (value.Length > 0 && IsFormulaTrigger(value[0]))
        {
            value = "'" + value;
        }
        return "\"" + value.Replace("\"", "\"\"") + "\"";
    }

    /// <summary>Reverses the formula guard applied by <see cref="Escape"/>.</summary>
    public static string Unguard(string value)
    {
        return value.Length > 1 && value[0] == '\'' && IsFormulaTrigger(value[1]) ? value.Substring(1) : value;
    }

    public static string BuildLine(ObjectDifference diff)
    {
        return $"{diff.Id},{Escape(diff.Key)},{diff.Type},{Escape(diff.Details)},{diff.BucketASize?.ToString() ?? ""},{diff.BucketBSize?.ToString() ?? ""},{Escape(diff.BucketAETag)},{Escape(diff.BucketBETag)},{diff.BucketALastModified:O},{diff.BucketBLastModified:O},{Escape(diff.BucketAStorageClass)},{Escape(diff.BucketBStorageClass)},{Escape(diff.MetadataSummary)},{diff.DetectedAt:O}";
    }

    /// <summary>Streams records (rows of raw fields) from a CSV file; handles quoted fields, escaped quotes and embedded newlines.</summary>
    public static IEnumerable<List<string>> ReadRecords(string path)
    {
        using var stream = new FileStream(path, FileMode.Open, FileAccess.Read, FileShare.ReadWrite);
        using var reader = new StreamReader(stream, new UTF8Encoding(false), detectEncodingFromByteOrderMarks: true);

        var fields = new List<string>();
        var sb = new StringBuilder();
        bool inQuotes = false;
        bool any = false;
        int ch;
        while ((ch = reader.Read()) != -1)
        {
            char c = (char)ch;
            if (inQuotes)
            {
                if (c == '"')
                {
                    if (reader.Peek() == '"')
                    {
                        reader.Read();
                        sb.Append('"');
                    }
                    else
                    {
                        inQuotes = false;
                    }
                }
                else
                {
                    sb.Append(c);
                }
                continue;
            }

            switch (c)
            {
                case '"':
                    inQuotes = true;
                    any = true;
                    break;
                case ',':
                    fields.Add(sb.ToString());
                    sb.Clear();
                    any = true;
                    break;
                case '\r':
                    break;
                case '\n':
                    if (any || sb.Length > 0)
                    {
                        fields.Add(sb.ToString());
                        yield return fields;
                        fields = new List<string>();
                        sb.Clear();
                        any = false;
                    }
                    break;
                default:
                    sb.Append(c);
                    any = true;
                    break;
            }
        }

        if (any || sb.Length > 0)
        {
            fields.Add(sb.ToString());
            yield return fields;
        }
    }

    /// <summary>Reads differences back from a CSV produced with <see cref="Header"/> and <see cref="BuildLine"/>.</summary>
    public static IEnumerable<ObjectDifference> ReadDifferences(string path)
    {
        bool first = true;
        foreach (var f in ReadRecords(path))
        {
            if (first)
            {
                first = false;
                if (f.Count > 0 && f[0] == "Id") continue;
            }
            if (f.Count < 14 || !long.TryParse(f[0], out long id) || !Enum.TryParse(f[2], out DifferenceType type))
                continue;

            yield return new ObjectDifference
            {
                Id = id,
                Key = Unguard(f[1]),
                Type = type,
                Details = Unguard(f[3]),
                BucketASize = ParseLong(f[4]),
                BucketBSize = ParseLong(f[5]),
                BucketAETag = NullIfEmpty(Unguard(f[6])),
                BucketBETag = NullIfEmpty(Unguard(f[7])),
                BucketALastModified = ParseDate(f[8]),
                BucketBLastModified = ParseDate(f[9]),
                BucketAStorageClass = NullIfEmpty(Unguard(f[10])),
                BucketBStorageClass = NullIfEmpty(Unguard(f[11])),
                DetectedAt = ParseDate(f[13]) ?? DateTime.Now
            };
        }
    }

    private static long? ParseLong(string s) => long.TryParse(s, out long v) ? v : null;

    private static DateTime? ParseDate(string s) =>
        DateTime.TryParse(s, CultureInfo.InvariantCulture, DateTimeStyles.RoundtripKind, out var d) ? d : null;

    private static string? NullIfEmpty(string s) => s.Length == 0 ? null : s;
}
