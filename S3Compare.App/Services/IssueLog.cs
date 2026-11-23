using S3Compare.App.Models;

namespace S3Compare.App.Services;

/// <summary>Thread-safe record of the issues of one scan, each classified by what it meant for the scan.</summary>
public sealed class IssueLog
{
    public const int RecentShown = 10;

    private readonly object _lock = new();
    private readonly LinkedList<IssueEntry> _recent = new(); // newest first
    private readonly int[] _counts = new int[Enum.GetValues<IssueOutcome>().Length];
    private IssueEntry? _mostSevere;
    private long _nextId;

    /// <summary>Called (outside the lock) for every new entry; used to write the diagnostics log.</summary>
    public Action<IssueEntry, int>? Added { get; set; }

    public bool HasFailed { get { lock (_lock) return _counts[(int)IssueOutcome.Failed] > 0; } }

    public IssueEntry Add(IssueOutcome outcome, string detail)
    {
        IssueEntry entry;
        int count;
        lock (_lock)
        {
            entry = new IssueEntry { Id = ++_nextId, Time = DateTime.Now, Outcome = outcome, Detail = detail };
            count = ++_counts[(int)outcome];
            _recent.AddFirst(entry);
            while (_recent.Count > RecentShown) _recent.RemoveLast();
            // Most recent among equals wins
            if (_mostSevere == null || outcome >= _mostSevere.Outcome) _mostSevere = entry;
        }
        try { Added?.Invoke(entry, count); } catch { /* logging must not affect the scan */ }
        return entry;
    }

    public void Clear()
    {
        lock (_lock)
        {
            _recent.Clear();
            Array.Clear(_counts);
            _mostSevere = null;
        }
    }

    public IssueSummary Snapshot()
    {
        lock (_lock)
        {
            return new IssueSummary
            {
                Failed = _counts[(int)IssueOutcome.Failed],
                Degraded = _counts[(int)IssueOutcome.Degraded],
                Recorded = _counts[(int)IssueOutcome.Recorded],
                Skipped = _counts[(int)IssueOutcome.Skipped],
                Recovered = _counts[(int)IssueOutcome.Recovered],
                MostSevere = _mostSevere,
                Recent = _recent.ToList()
            };
        }
    }
}
