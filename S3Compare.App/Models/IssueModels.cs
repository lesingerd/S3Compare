namespace S3Compare.App.Models;

/// <summary>What an error actually meant for the scan. Declared in ascending severity: only <see cref="Failed"/> is a real failure.</summary>
public enum IssueOutcome
{
    /// <summary>Retried and then succeeded: no impact.</summary>
    Recovered,
    /// <summary>A planner request was skipped or a planner technique switched off: results unaffected, only split quality may be lower.</summary>
    Skipped,
    /// <summary>A per-object problem written to the report (Content Check Error); the scan continued.</summary>
    Recorded,
    /// <summary>Planning ended early or produced fewer ranges: speed, not correctness.</summary>
    Degraded,
    /// <summary>Stopped the scan or the writer.</summary>
    Failed
}

/// <summary>One classified issue as shown in the status bar, the diagnostics panel and the log.</summary>
public sealed class IssueEntry
{
    public long Id { get; init; }
    public DateTime Time { get; init; }
    public IssueOutcome Outcome { get; init; }
    /// <summary>The wording after the outcome word, e.g. "A LIST ...: recovered on attempt 2 - no impact".</summary>
    public string Detail { get; init; } = string.Empty;

    public string OutcomeWord => Outcome.ToString().ToUpperInvariant();
    public string Marker => MarkerFor(Outcome);
    public string TimeText => Time.ToString("HH:mm:ss");

    /// <summary>"[12:01:03] ✓ RECOVERED A LIST ...": the same text in the status bar, the panel and the log.</summary>
    public string Display => Format(Time, Outcome, Detail);

    public static string MarkerFor(IssueOutcome outcome) => outcome switch
    {
        IssueOutcome.Recovered => "\u2713",
        IssueOutcome.Skipped => "\u24D8",
        IssueOutcome.Recorded => "\u26A0",
        IssueOutcome.Degraded => "\u26A0",
        _ => "\u2716"
    };

    public static string Format(DateTime time, IssueOutcome outcome, string detail)
        => $"[{time:HH:mm:ss}] {MarkerFor(outcome)} {outcome.ToString().ToUpperInvariant()} {detail}";

    public override string ToString() => Display;
}

/// <summary>Counts per outcome plus the most severe recent issue and the newest few.</summary>
public sealed class IssueSummary
{
    public static readonly IssueSummary Empty = new();

    public int Failed { get; init; }
    public int Recovered { get; init; }
    public int Skipped { get; init; }
    public int Degraded { get; init; }
    public int Recorded { get; init; }
    /// <summary>Failed > Degraded > Recorded > Skipped > Recovered; the most recent among equals.</summary>
    public IssueEntry? MostSevere { get; init; }
    /// <summary>Newest first.</summary>
    public IReadOnlyList<IssueEntry> Recent { get; init; } = Array.Empty<IssueEntry>();

    public int Total => Failed + Recovered + Skipped + Degraded + Recorded;

    /// <summary>"Issues: 0 failed · 2 recovered · 1 skipped · 0 degraded · 0 recorded"</summary>
    public string Line => $"Issues: {Failed:N0} failed \u00B7 {Recovered:N0} recovered \u00B7 {Skipped:N0} skipped \u00B7 {Degraded:N0} degraded \u00B7 {Recorded:N0} recorded";
}
