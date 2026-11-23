using System.Text;
using S3Compare.App.Models;

namespace S3Compare.App.Services;

/// <summary>Renders scan diagnostics as plain text, for the UI panel and the diagnostics log (same text, so a screenshot matches the log).</summary>
public static class DiagnosticsFormatter
{
    public static string DisplayPhase(ComparisonStatistics s)
        => s.IsPaused && (s.Phase == "Planning" || s.Phase == "Scanning") ? "Paused" : s.Phase;

    public static string Format(ComparisonStatistics s, bool includePartitions = false, bool includeIssues = true)
    {
        var sb = new StringBuilder();
        string pausedNote = s.IsPaused ? $" (paused during {s.Phase})" : string.Empty;
        sb.AppendLine($"Phase            : {DisplayPhase(s)}{(s.IsPaused ? pausedNote : string.Empty)}");
        sb.AppendLine($"Latest step      : {s.LatestStep}");
        sb.AppendLine($"Elapsed          : {s.ElapsedTime:hh\\:mm\\:ss}   Evaluated: {s.ObjectsCompared:N0} ({s.ItemsPerSecond:N0}/s)");
        if (s.Planner != null)
        {
            var p = s.Planner;
            string timedOut = p.TimedOut ? ", TIME LIMIT REACHED (ranges built from candidates found so far)" : string.Empty;
            sb.AppendLine($"Planner          : {(p.Finished ? "finished" : "running")}{timedOut}");
            sb.AppendLine($"  step           : {p.Step}");
            string budget = p.RequestBudget > 0 ? p.RequestBudget.ToString("N0") : "no budget";
            sb.AppendLine($"  requests       : {p.RequestsUsed:N0} / {budget}   skipped (no impact on results): {p.FailedRequests:N0}");
            string limit = p.TimeLimit > TimeSpan.Zero ? p.TimeLimit.TotalSeconds.ToString("N0") + "s" : "unlimited";
            sb.AppendLine($"  candidates     : {p.Candidates:N0}   elapsed: {p.Elapsed.TotalSeconds:N1}s / {limit}");
            if (!string.IsNullOrEmpty(p.DisabledTechniques)) sb.AppendLine($"  disabled       : {p.DisabledTechniques}");
            if (!string.IsNullOrEmpty(p.LastError)) sb.AppendLine($"  last issue     : {p.LastError}");
        }
        sb.AppendLine($"Partitions       : {s.PartitionsCompleted:N0}/{s.PartitionsTotal:N0} done, {s.ActivePartitions} running{(s.PartitionSplits > 0 ? $", {s.PartitionSplits} split by idle workers" : string.Empty)}");
        AppendBucket(sb, "LIST bucket A", s.BucketAList);
        AppendBucket(sb, "LIST bucket B", s.BucketBList);
        sb.AppendLine($"Total LIST reqs  : {s.TotalListRequests:N0}   rate-limiter wait: {s.RateLimiterWaitSeconds:N1}s total");
        if (s.InFlight.Count > 0)
        {
            sb.AppendLine("In flight        :");
            foreach (var op in s.InFlight.Take(8)) sb.AppendLine($"  {op}");
        }
        else
        {
            sb.AppendLine("In flight        : none");
        }
        if (includeIssues) AppendIssues(sb, s.Issues);
        if (includePartitions)
        {
            sb.AppendLine("Partitions (index | lower | upper | state | last completed key | objects compared):");
            foreach (var p in s.Partitions)
                sb.AppendLine($"  {p.Index,4} | {p.Lower} | {p.Upper} | {p.State} | {p.LastCompletedKey} | {p.ObjectsCompared:N0}");
        }
        return sb.ToString().TrimEnd();
    }

    private static void AppendBucket(StringBuilder sb, string name, ListRequestStats b)
        => sb.AppendLine($"{name,-16} : {b.Requests:N0} reqs {DescribeRetries(b)}, {b.RequestsPerSecond:N1}/s, latency avg {b.AverageLatencyMs:N0} ms / last {b.LastLatencyMs:N0} ms, limiter wait {b.RateLimiterWaitSeconds:N1}s");
    /// <summary>"retried 2 (recovered 2) · failed 0"</summary>
    public static string DescribeRetries(ListRequestStats b)
        => $"| retried {b.Retried:N0} (recovered {b.Recovered:N0}) \u00B7 failed {b.Failures:N0}";

    /// <summary>The issue summary, the most severe recent issue and the newest issues, with the same wording as the UI.</summary>
    public static void AppendIssues(StringBuilder sb, IssueSummary issues)
    {
        sb.AppendLine(issues.Line);
        sb.AppendLine($"Most severe      : {(issues.MostSevere != null ? issues.MostSevere.Display : "none")}");
        if (issues.Recent.Count > 0)
        {
            sb.AppendLine("Recent issues    :");
            foreach (var issue in issues.Recent) sb.AppendLine($"  {issue.Display}");
        }
    }
}
