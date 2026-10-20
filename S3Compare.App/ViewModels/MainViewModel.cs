using System.Collections.Concurrent;
using System.Collections.ObjectModel;
using System.ComponentModel;
using System.IO;
using System.Runtime.CompilerServices;
using System.Windows;
using System.Windows.Data;
using System.Windows.Input;
using System.Windows.Threading;
using Microsoft.Win32;
using S3Compare.App.Models;
using S3Compare.App.Services;

namespace S3Compare.App.ViewModels;

public class MainViewModel : INotifyPropertyChanged
{
    private BucketComparisonEngine? _engine;
    private CancellationTokenSource? _cts;
    private TaskCompletionSource? _runCompletion;
    private string? _sessionCsvPath;

    // Differences travel engine thread -> queue -> UI timer; the grid keeps only the newest rows (all rows are in the session CSV)
    private const int MaxUiDifferences = 2000;
    private const int MaxDifferencesPerTick = 500;
    private readonly ConcurrentQueue<ObjectDifference> _diffQueue = new();
    private DispatcherTimer? _diffTimer;

    private CancellationTokenSource? _checkpointDebounceCts;
    private int _checkpointCheckTicket;

    // Presets
    public ObservableCollection<S3EndpointPreset> Endpoints { get; }

    // Bucket A
    private S3EndpointPreset _selectedPresetA;
    private string _endpointUrlA = "https://s3.wasabisys.com";
    private string _regionA = "us-east-1";
    private string _bucketNameA = string.Empty;
    private string _accessKeyA = string.Empty;
    private string _secretKeyA = string.Empty;
    private string _prefixA = string.Empty;
    private string _testResultA = string.Empty;
    private bool _isTestingA = false;
    private bool _isFetchingBucketsA = false;
    public ObservableCollection<string> AvailableBucketsA { get; } = new();

    // Bucket B
    private S3EndpointPreset _selectedPresetB;
    private string _endpointUrlB = "https://s3.wasabisys.com";
    private string _regionB = "us-east-1";
    private string _bucketNameB = string.Empty;
    private string _accessKeyB = string.Empty;
    private string _secretKeyB = string.Empty;
    private string _prefixB = string.Empty;
    private string _testResultB = string.Empty;
    private bool _isTestingB = false;
    private bool _isFetchingBucketsB = false;
    public ObservableCollection<string> AvailableBucketsB { get; } = new();

    // Mode & Options
    private bool _isListingOnlyMode = true;
    private bool _autoSaveLiveReport = false;
    private string _liveReportPath = string.Empty;
    private bool _detectSequenceGaps = true;
    private int _maxSequenceGapThreshold = 25;

    // Checkpoint & Resume state (NO credentials/keys saved)
    private bool _hasResumableCheckpoint = false;
    private string _checkpointBannerText = string.Empty;
    private ScanCheckpoint? _activeCheckpoint;

    // Runtime state
    private bool _isRunning = false;
    private bool _isPaused = false;
    private string _statusMessage = "Ready to configure and start comparison.";

    // Panes
    private string _statusA = "Idle";
    private string _statusB = "Idle";
    private string _currentKeyA = "-";
    private string _currentKeyB = "-";
    private long _objectsReadA = 0;
    private long _objectsReadB = 0;
    private string _bytesReadA = "0 B";
    private string _bytesReadB = "0 B";

    public ObservableCollection<S3ObjectSummary> RecentItemsA { get; } = new();
    public ObservableCollection<S3ObjectSummary> RecentItemsB { get; } = new();

    // Differences & Stats
    private ComparisonStatistics _stats = new();
    public ObservableCollection<ObjectDifference> Differences { get; } = new();
    public ICollectionView FilteredDifferences { get; }

    private string _selectedFilter = "All";
    public List<string> FilterOptions { get; } = new()
    {
        "All",
        "Missing in Bucket B",
        "Missing in Bucket A",
        "Size Mismatch",
        "ETag Mismatch",
        "Content Mismatch",
        "Possible Gap (Both Sides)",
        "Content Check Error"
    };

    // Commands
    public ICommand StartCommand { get; }
    public ICommand ResumeScanCommand { get; }
    public ICommand DiscardCheckpointCommand { get; }
    public ICommand PauseResumeCommand { get; }
    public ICommand StopCommand { get; }
    public ICommand TestConnectionACommand { get; }
    public ICommand TestConnectionBCommand { get; }
    public ICommand FetchBucketsACommand { get; }
    public ICommand FetchBucketsBCommand { get; }
    public ICommand ExportReportCommand { get; }
    public ICommand ClearResultsCommand { get; }
    public ICommand BrowseLiveReportPathCommand { get; }

    public MainViewModel()
    {
        Endpoints = new ObservableCollection<S3EndpointPreset>(S3ClientFactory.GetWasabiEndpoints());
        _selectedPresetA = Endpoints[0];
        _selectedPresetB = Endpoints[0];

        FilteredDifferences = CollectionViewSource.GetDefaultView(Differences);
        FilteredDifferences.Filter = FilterDifferencesPredicate;

        StartCommand = new RelayCommand(async () => await StartComparisonAsync(isResuming: false), () => !IsRunning && !IsTestingA && !IsTestingB);
        ResumeScanCommand = new RelayCommand(async () => await StartComparisonAsync(isResuming: true), () => !IsRunning && HasResumableCheckpoint);
        DiscardCheckpointCommand = new RelayCommand(DiscardActiveCheckpoint, () => !IsRunning && HasResumableCheckpoint);
        PauseResumeCommand = new RelayCommand(TogglePause, () => IsRunning);
        StopCommand = new RelayCommand(StopComparison, () => IsRunning);
        TestConnectionACommand = new RelayCommand(async () => await TestConnectionAAsync(), () => !IsRunning && !IsTestingA);
        TestConnectionBCommand = new RelayCommand(async () => await TestConnectionBAsync(), () => !IsRunning && !IsTestingB);
        FetchBucketsACommand = new RelayCommand(async () => await FetchBucketsAAsync(), () => !IsRunning && !IsFetchingBucketsA);
        FetchBucketsBCommand = new RelayCommand(async () => await FetchBucketsBAsync(), () => !IsRunning && !IsFetchingBucketsB);
        ExportReportCommand = new RelayCommand(async () => await ExportReportAsync(), () => (Differences.Count > 0 || Statistics.TotalDifferences > 0) && !IsRunning);
        ClearResultsCommand = new RelayCommand(ClearResults, () => !IsRunning);
        BrowseLiveReportPathCommand = new RelayCommand(BrowseLiveReportPath);
    }

    #region Properties

    public S3EndpointPreset SelectedPresetA
    {
        get => _selectedPresetA;
        set
        {
            if (SetField(ref _selectedPresetA, value) && value != null)
            {
                if (!string.IsNullOrWhiteSpace(value.Url))
                {
                    EndpointUrlA = value.Url;
                    RegionA = value.Region;
                }
            }
        }
    }

    public string EndpointUrlA
    {
        get => _endpointUrlA;
        set
        {
            if (SetField(ref _endpointUrlA, value))
            {
                ScheduleCheckpointCheck();
            }
        }
    }

    public string RegionA
    {
        get => _regionA;
        set => SetField(ref _regionA, value);
    }

    public string BucketNameA
    {
        get => _bucketNameA;
        set
        {
            if (SetField(ref _bucketNameA, value))
            {
                ScheduleCheckpointCheck();
            }
        }
    }

    public string AccessKeyA
    {
        get => _accessKeyA;
        set => SetField(ref _accessKeyA, value);
    }

    public string SecretKeyA
    {
        get => _secretKeyA;
        set => SetField(ref _secretKeyA, value);
    }

    public string PrefixA
    {
        get => _prefixA;
        set
        {
            if (SetField(ref _prefixA, value))
            {
                ScheduleCheckpointCheck();
            }
        }
    }

    public string TestResultA
    {
        get => _testResultA;
        set => SetField(ref _testResultA, value);
    }

    public bool IsTestingA
    {
        get => _isTestingA;
        set
        {
            if (SetField(ref _isTestingA, value))
            {
                InvalidateCommands();
            }
        }
    }

    public bool IsFetchingBucketsA
    {
        get => _isFetchingBucketsA;
        set
        {
            if (SetField(ref _isFetchingBucketsA, value))
            {
                InvalidateCommands();
            }
        }
    }

    public S3EndpointPreset SelectedPresetB
    {
        get => _selectedPresetB;
        set
        {
            if (SetField(ref _selectedPresetB, value) && value != null)
            {
                if (!string.IsNullOrWhiteSpace(value.Url))
                {
                    EndpointUrlB = value.Url;
                    RegionB = value.Region;
                }
            }
        }
    }

    public string EndpointUrlB
    {
        get => _endpointUrlB;
        set
        {
            if (SetField(ref _endpointUrlB, value))
            {
                ScheduleCheckpointCheck();
            }
        }
    }

    public string RegionB
    {
        get => _regionB;
        set => SetField(ref _regionB, value);
    }

    public string BucketNameB
    {
        get => _bucketNameB;
        set
        {
            if (SetField(ref _bucketNameB, value))
            {
                ScheduleCheckpointCheck();
            }
        }
    }

    public string AccessKeyB
    {
        get => _accessKeyB;
        set => SetField(ref _accessKeyB, value);
    }

    public string SecretKeyB
    {
        get => _secretKeyB;
        set => SetField(ref _secretKeyB, value);
    }

    public string PrefixB
    {
        get => _prefixB;
        set
        {
            if (SetField(ref _prefixB, value))
            {
                ScheduleCheckpointCheck();
            }
        }
    }

    public string TestResultB
    {
        get => _testResultB;
        set => SetField(ref _testResultB, value);
    }

    public bool IsTestingB
    {
        get => _isTestingB;
        set
        {
            if (SetField(ref _isTestingB, value))
            {
                InvalidateCommands();
            }
        }
    }

    public bool IsFetchingBucketsB
    {
        get => _isFetchingBucketsB;
        set
        {
            if (SetField(ref _isFetchingBucketsB, value))
            {
                InvalidateCommands();
            }
        }
    }

    public bool IsListingOnlyMode
    {
        get => _isListingOnlyMode;
        set
        {
            if (SetField(ref _isListingOnlyMode, value))
            {
                OnPropertyChanged(nameof(IsFullDataMode));
                ScheduleCheckpointCheck();
            }
        }
    }

    public bool IsFullDataMode
    {
        get => !_isListingOnlyMode;
        set
        {
            if (value != !_isListingOnlyMode)
            {
                IsListingOnlyMode = !value;
            }
        }
    }

    public bool AutoSaveLiveReport
    {
        get => _autoSaveLiveReport;
        set => SetField(ref _autoSaveLiveReport, value);
    }

    public string LiveReportPath
    {
        get => _liveReportPath;
        set => SetField(ref _liveReportPath, value);
    }

    public bool DetectSequenceGaps
    {
        get => _detectSequenceGaps;
        set => SetField(ref _detectSequenceGaps, value);
    }

    public int MaxSequenceGapThreshold
    {
        get => _maxSequenceGapThreshold;
        set => SetField(ref _maxSequenceGapThreshold, value);
    }

    public bool HasResumableCheckpoint
    {
        get => _hasResumableCheckpoint;
        set
        {
            if (SetField(ref _hasResumableCheckpoint, value))
            {
                InvalidateCommands();
            }
        }
    }

    public string CheckpointBannerText
    {
        get => _checkpointBannerText;
        set => SetField(ref _checkpointBannerText, value);
    }

    public ScanCheckpoint? ActiveCheckpoint
    {
        get => _activeCheckpoint;
        set => SetField(ref _activeCheckpoint, value);
    }

    public bool IsRunning
    {
        get => _isRunning;
        set
        {
            if (SetField(ref _isRunning, value))
            {
                OnPropertyChanged(nameof(CanEditConfig));
                InvalidateCommands();
            }
        }
    }

    public bool CanEditConfig => !_isRunning;

    public bool IsPaused
    {
        get => _isPaused;
        set
        {
            if (SetField(ref _isPaused, value))
            {
                InvalidateCommands();
            }
        }
    }

    public string StatusMessage
    {
        get => _statusMessage;
        set => SetField(ref _statusMessage, value);
    }

    public string StatusA
    {
        get => _statusA;
        set => SetField(ref _statusA, value);
    }

    public string StatusB
    {
        get => _statusB;
        set => SetField(ref _statusB, value);
    }

    public string CurrentKeyA
    {
        get => _currentKeyA;
        set => SetField(ref _currentKeyA, value);
    }

    public string CurrentKeyB
    {
        get => _currentKeyB;
        set => SetField(ref _currentKeyB, value);
    }

    public long ObjectsReadA
    {
        get => _objectsReadA;
        set => SetField(ref _objectsReadA, value);
    }

    public long ObjectsReadB
    {
        get => _objectsReadB;
        set => SetField(ref _objectsReadB, value);
    }

    public string BytesReadA
    {
        get => _bytesReadA;
        set => SetField(ref _bytesReadA, value);
    }

    public string BytesReadB
    {
        get => _bytesReadB;
        set => SetField(ref _bytesReadB, value);
    }

    public ComparisonStatistics Statistics
    {
        get => _stats;
        set => SetField(ref _stats, value);
    }

    public string SelectedFilter
    {
        get => _selectedFilter;
        set
        {
            if (SetField(ref _selectedFilter, value))
            {
                FilteredDifferences.Refresh();
            }
        }
    }

    #endregion

    private bool FilterDifferencesPredicate(object item)
    {
        if (item is not ObjectDifference diff) return false;
        return _selectedFilter switch
        {
            "Missing in Bucket B" => diff.Type == DifferenceType.MissingInBucketB,
            "Missing in Bucket A" => diff.Type == DifferenceType.MissingInBucketA,
            "Size Mismatch" => diff.Type == DifferenceType.SizeMismatch,
            "ETag Mismatch" => diff.Type == DifferenceType.ETagMismatch,
            "Content Mismatch" => diff.Type == DifferenceType.ContentMismatch,
            "Possible Gap (Both Sides)" => diff.Type == DifferenceType.PossibleMissingOnBothSides,
            "Content Check Error" => diff.Type == DifferenceType.ContentCheckError,
            _ => true
        };
    }

    /// <summary>Config fields change per keystroke; wait for typing to settle before touching the disk.</summary>
    private void ScheduleCheckpointCheck()
    {
        _checkpointDebounceCts?.Cancel();
        var cts = new CancellationTokenSource();
        _checkpointDebounceCts = cts;
        _ = DebouncedCheckpointCheckAsync(cts.Token);
    }

    private async Task DebouncedCheckpointCheckAsync(CancellationToken token)
    {
        try
        {
            await Task.Delay(400, token);
            await CheckForCheckpointsAsync();
        }
        catch (OperationCanceledException)
        {
            // Superseded by a newer edit
        }
    }

    public async Task CheckForCheckpointsAsync()
    {
        int ticket = ++_checkpointCheckTicket;

        if (IsRunning || string.IsNullOrWhiteSpace(BucketNameA) || string.IsNullOrWhiteSpace(BucketNameB))
        {
            HasResumableCheckpoint = false;
            ActiveCheckpoint = null;
            return;
        }

        var mode = IsListingOnlyMode ? ComparisonMode.ListingOnly : ComparisonMode.FullData;
        var cp = await CheckpointManager.FindCheckpointAsync(BucketNameA, EndpointUrlA, PrefixA, BucketNameB, EndpointUrlB, PrefixB, mode);

        // A newer check started while we were reading; its result wins
        if (ticket != _checkpointCheckTicket) return;

        if (cp != null && !string.IsNullOrEmpty(cp.LastEvaluatedKey))
        {
            ActiveCheckpoint = cp;
            HasResumableCheckpoint = true;
            CheckpointBannerText = $"Found saved scan checkpoint at key '{cp.LastEvaluatedKey}' ({cp.ObjectsCompared:N0} objects evaluated, {cp.TotalDifferences:N0} diffs).";
        }
        else
        {
            HasResumableCheckpoint = false;
            ActiveCheckpoint = null;
        }
    }

    private void DiscardActiveCheckpoint()
    {
        if (ActiveCheckpoint != null)
        {
            CheckpointManager.DeleteCheckpoint(ActiveCheckpoint);
            ActiveCheckpoint = null;
            HasResumableCheckpoint = false;
            StatusMessage = "Previous scan checkpoint discarded.";
        }
    }

    private void BrowseLiveReportPath()
    {
        var sfd = new SaveFileDialog
        {
            Title = "Select Live Differences Stream Report Location",
            Filter = "CSV File (*.csv)|*.csv",
            FileName = $"S3Compare_LiveReport_{DateTime.Now:yyyyMMdd_HHmmss}.csv"
        };
        if (sfd.ShowDialog() == true)
        {
            LiveReportPath = sfd.FileName;
            AutoSaveLiveReport = true;
        }
    }

    private async Task TestConnectionAAsync()
    {
        IsTestingA = true;
        TestResultA = "Testing connection...";
        try
        {
            var config = new S3BucketConfig
            {
                EndpointUrl = EndpointUrlA,
                Region = RegionA,
                BucketName = BucketNameA,
                AccessKey = AccessKeyA,
                SecretKey = SecretKeyA,
                Prefix = PrefixA
            };
            var res = await S3ClientFactory.TestConnectionAsync(config);
            TestResultA = res.Success ? "✓ " + res.Message : "✗ " + res.Message;

            if (res.Success && AvailableBucketsA.Count == 0)
            {
                _ = FetchBucketsAAsync();
            }
            _ = CheckForCheckpointsAsync();
        }
        finally
        {
            IsTestingA = false;
        }
    }

    public async Task FetchBucketsAAsync()
    {
        if (string.IsNullOrWhiteSpace(AccessKeyA) || string.IsNullOrWhiteSpace(SecretKeyA))
        {
            TestResultA = "⚠️ Enter Access & Secret Key first";
            return;
        }

        IsFetchingBucketsA = true;
        TestResultA = "Discovering buckets...";
        try
        {
            var config = new S3BucketConfig
            {
                EndpointUrl = EndpointUrlA,
                Region = RegionA,
                AccessKey = AccessKeyA,
                SecretKey = SecretKeyA
            };
            var res = await S3ClientFactory.ListBucketsAsync(config);
            if (res.Success)
            {
                AvailableBucketsA.Clear();
                foreach (var b in res.Buckets)
                {
                    AvailableBucketsA.Add(b);
                }
                if (string.IsNullOrWhiteSpace(BucketNameA) && res.Buckets.Count > 0)
                {
                    BucketNameA = res.Buckets[0];
                }
                TestResultA = $"✓ Discovered {res.Buckets.Count} bucket(s)";
            }
            else
            {
                TestResultA = $"✗ {res.Message}";
            }
        }
        finally
        {
            IsFetchingBucketsA = false;
        }
    }

    private async Task TestConnectionBAsync()
    {
        IsTestingB = true;
        TestResultB = "Testing connection...";
        try
        {
            var config = new S3BucketConfig
            {
                EndpointUrl = EndpointUrlB,
                Region = RegionB,
                BucketName = BucketNameB,
                AccessKey = AccessKeyB,
                SecretKey = SecretKeyB,
                Prefix = PrefixB
            };
            var res = await S3ClientFactory.TestConnectionAsync(config);
            TestResultB = res.Success ? "✓ " + res.Message : "✗ " + res.Message;

            if (res.Success && AvailableBucketsB.Count == 0)
            {
                _ = FetchBucketsBAsync();
            }
            _ = CheckForCheckpointsAsync();
        }
        finally
        {
            IsTestingB = false;
        }
    }

    public async Task FetchBucketsBAsync()
    {
        if (string.IsNullOrWhiteSpace(AccessKeyB) || string.IsNullOrWhiteSpace(SecretKeyB))
        {
            TestResultB = "⚠️ Enter Access & Secret Key first";
            return;
        }

        IsFetchingBucketsB = true;
        TestResultB = "Discovering buckets...";
        try
        {
            var config = new S3BucketConfig
            {
                EndpointUrl = EndpointUrlB,
                Region = RegionB,
                AccessKey = AccessKeyB,
                SecretKey = SecretKeyB
            };
            var res = await S3ClientFactory.ListBucketsAsync(config);
            if (res.Success)
            {
                AvailableBucketsB.Clear();
                foreach (var b in res.Buckets)
                {
                    AvailableBucketsB.Add(b);
                }
                if (string.IsNullOrWhiteSpace(BucketNameB) && res.Buckets.Count > 0)
                {
                    BucketNameB = res.Buckets[0];
                }
                TestResultB = $"✓ Discovered {res.Buckets.Count} bucket(s)";
            }
            else
            {
                TestResultB = $"✗ {res.Message}";
            }
        }
        finally
        {
            IsFetchingBucketsB = false;
        }
    }

    private void TogglePause()
    {
        if (_engine == null) return;
        if (IsPaused)
        {
            _engine.Resume();
            IsPaused = false;
            StatusMessage = "Resumed comparison...";
        }
        else
        {
            _engine.Pause();
            IsPaused = true;
            StatusMessage = "Paused comparison.";
        }
    }

    private void StopComparison()
    {
        _cts?.Cancel();
        StatusMessage = "Stopping comparison...";
    }

    private void ClearResults()
    {
        _sessionCsvPath = null;
        Differences.Clear();
        RecentItemsA.Clear();
        RecentItemsB.Clear();
        Statistics = new ComparisonStatistics();
        StatusA = "Idle";
        StatusB = "Idle";
        CurrentKeyA = "-";
        CurrentKeyB = "-";
        ObjectsReadA = 0;
        ObjectsReadB = 0;
        BytesReadA = "0 B";
        BytesReadB = "0 B";
        StatusMessage = "Results cleared.";
    }

    private async Task StartComparisonAsync(bool isResuming = false)
    {
        if (string.IsNullOrWhiteSpace(BucketNameA) || string.IsNullOrWhiteSpace(BucketNameB))
        {
            MessageBox.Show("Please enter both Bucket A and Bucket B names.", "Validation Error", MessageBoxButton.OK, MessageBoxImage.Warning);
            return;
        }

        if (string.IsNullOrWhiteSpace(AccessKeyA) || string.IsNullOrWhiteSpace(SecretKeyA) ||
            string.IsNullOrWhiteSpace(AccessKeyB) || string.IsNullOrWhiteSpace(SecretKeyB))
        {
            MessageBox.Show("Please provide Access Key and Secret Key for both buckets.\n\nCredentials are kept strictly in memory for this session and are never saved to disk.", "Authentication Required", MessageBoxButton.OK, MessageBoxImage.Warning);
            return;
        }

        if (IsFullDataMode)
        {
            var prompt = MessageBox.Show(
                "You have selected 'Full Data Comparison'.\n\nThis mode downloads and verifies byte streams for matching objects, which will incur data egress charges from Wasabi / S3 for objects that have differing or multipart ETags.\n\nDo you wish to proceed?",
                "Egress Warning - Full Data Mode",
                MessageBoxButton.YesNo,
                MessageBoxImage.Question);
            if (prompt != MessageBoxResult.Yes)
                return;
        }

        ScanCheckpoint? checkpointToUse = isResuming ? ActiveCheckpoint : null;
        var cts = new CancellationTokenSource();
        _cts = cts;
        _runCompletion = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);

        try
        {
            IsRunning = true;
            IsPaused = false;

            if (!isResuming)
            {
                ClearResults();
            }

            StatusMessage = isResuming
                ? $"Resuming streaming scan from checkpoint key '{checkpointToUse?.LastEvaluatedKey}'..."
                : "Initializing streaming comparison...";
            StatusA = "Connecting...";
            StatusB = "Connecting...";

            var configA = new S3BucketConfig
            {
                EndpointUrl = EndpointUrlA,
                Region = RegionA,
                BucketName = BucketNameA,
                AccessKey = AccessKeyA,
                SecretKey = SecretKeyA,
                Prefix = PrefixA
            };

            var configB = new S3BucketConfig
            {
                EndpointUrl = EndpointUrlB,
                Region = RegionB,
                BucketName = BucketNameB,
                AccessKey = AccessKeyB,
                SecretKey = SecretKeyB,
                Prefix = PrefixB
            };

            var mode = IsListingOnlyMode ? ComparisonMode.ListingOnly : ComparisonMode.FullData;

            // Every difference is always written to a CSV on disk; it is the source for exports and for resuming.
            string csvPath;
            if (checkpointToUse != null && !string.IsNullOrWhiteSpace(checkpointToUse.LiveReportCsvPath))
            {
                csvPath = checkpointToUse.LiveReportCsvPath;
            }
            else if (AutoSaveLiveReport && !string.IsNullOrWhiteSpace(LiveReportPath))
            {
                csvPath = LiveReportPath;
            }
            else
            {
                PurgeOldSessionReports();
                csvPath = Path.Combine(AppPaths.SessionReportsDirectory, $"session_{DateTime.Now:yyyyMMdd_HHmmss}.csv");
            }
            _sessionCsvPath = csvPath;

            var engine = new BucketComparisonEngine(
                configA,
                configB,
                mode,
                csvPath,
                checkpointToUse,
                DetectSequenceGaps,
                MaxSequenceGapThreshold);
            _engine = engine;

            var dispatcher = Application.Current?.Dispatcher;
            _diffQueue.Clear();

            // Differences are queued by the engine thread and drained in batches on the UI thread by a timer,
            // so the engine never blocks on (or is killed by) the UI.
            _diffTimer = new DispatcherTimer(DispatcherPriority.Background) { Interval = TimeSpan.FromMilliseconds(100) };
            _diffTimer.Tick += (_, _) => DrainDifferences(MaxDifferencesPerTick);
            _diffTimer.Start();

            engine.ProgressUpdated += stats =>
            {
                dispatcher?.InvokeAsync(() =>
                {
                    Statistics = stats;
                    CurrentKeyA = stats.CurrentKeyA;
                    CurrentKeyB = stats.CurrentKeyB;
                    ObjectsReadA = stats.BucketAObjectsRead;
                    ObjectsReadB = stats.BucketBObjectsRead;
                    BytesReadA = S3ObjectSummary.FormatSize(stats.BucketABytesRead);
                    BytesReadB = S3ObjectSummary.FormatSize(stats.BucketBBytesRead);
                    StatusA = $"Paging ({stats.BucketAObjectsRead:N0} objects read)";
                    StatusB = $"Paging ({stats.BucketBObjectsRead:N0} objects read)";
                    StatusMessage = $"Streaming comparison in progress... {stats.ObjectsCompared:N0} items evaluated ({stats.ItemsPerSecond:N1} items/sec) | RAM: {stats.WorkingSetMB:F0} MB";
                });
            };

            engine.DifferenceFound += diff => _diffQueue.Enqueue(diff);

            engine.PageLoadedA += page => ShowRecentItems(dispatcher, RecentItemsA, page);
            engine.PageLoadedB += page => ShowRecentItems(dispatcher, RecentItemsB, page);

            var finalStats = await Task.Run(() => engine.RunAsync(cts.Token));

            StatusMessage = $"Comparison Complete! Evaluated {finalStats.ObjectsCompared:N0} items. Found {finalStats.TotalDifferences:N0} differences, {finalStats.MatchedCount:N0} matched.";
            StatusA = "Complete";
            StatusB = "Complete";
            HasResumableCheckpoint = false;
            ActiveCheckpoint = null;
        }
        catch (OperationCanceledException) when (cts.IsCancellationRequested)
        {
            // Only a user stop is a stop; a timeout (also an OperationCanceledException) falls through to the error handler.
            StatusMessage = "Comparison paused or stopped by user. Checkpoint saved for resuming.";
            StatusA = "Stopped";
            StatusB = "Stopped";
        }
        catch (Exception ex)
        {
            StatusMessage = $"Error during comparison: {ex.Message}";
            StatusA = "Error";
            StatusB = "Error";
            MessageBox.Show($"Comparison stopped due to error:\n\n{ex.Message}\n\nA checkpoint has been preserved if you wish to resume after addressing the issue.", "Comparison Error", MessageBoxButton.OK, MessageBoxImage.Error);
        }
        finally
        {
            // Always show whatever the engine queued, whether it finished, was stopped or failed
            _diffTimer?.Stop();
            _diffTimer = null;
            DrainDifferences(MaxUiDifferences);

            IsRunning = false;
            IsPaused = false;
            cts.Dispose();
            _cts = null;
            _engine = null;
            _runCompletion?.TrySetResult();
            _ = CheckForCheckpointsAsync();
        }
    }

    /// <summary>
    /// Cancels a running scan and waits (bounded) for the engine to write its final checkpoint and flush the live CSV.
    /// Call before closing the window.
    /// </summary>
    public async Task ShutdownAsync()
    {
        var completion = _runCompletion;
        if (!IsRunning || completion == null) return;

        try
        {
            _cts?.Cancel();
        }
        catch (ObjectDisposedException)
        {
        }
        _engine?.Resume();

        await Task.WhenAny(completion.Task, Task.Delay(TimeSpan.FromSeconds(30)));
    }

    private static void ShowRecentItems(Dispatcher? dispatcher, ObservableCollection<S3ObjectSummary> target, List<S3ObjectSummary> page)
    {
        if (dispatcher == null) return;
        var snapshot = page.Take(30).ToList();
        dispatcher.InvokeAsync(() =>
        {
            target.Clear();
            foreach (var item in snapshot)
            {
                target.Add(item);
            }
        });
    }

    /// <summary>
    /// Moves queued differences into the UI collection. The grid only shows the newest <see cref="MaxUiDifferences"/>
    /// rows (the complete set is in the session CSV), so anything beyond <paramref name="maxBatch"/> is skipped, oldest first.
    /// </summary>
    private void DrainDifferences(int maxBatch)
    {
        int pending = _diffQueue.Count;
        if (pending == 0) return;

        int skip = Math.Max(0, pending - maxBatch);
        var batch = new List<ObjectDifference>(Math.Min(pending, maxBatch));
        for (int taken = 0; taken < pending && _diffQueue.TryDequeue(out var diff); taken++)
        {
            if (taken >= skip)
            {
                batch.Add(diff);
            }
        }

        int overflow = Differences.Count + batch.Count - MaxUiDifferences;
        if (overflow >= Differences.Count)
        {
            Differences.Clear();
        }
        else
        {
            for (int i = 0; i < overflow; i++)
            {
                Differences.RemoveAt(0);
            }
        }

        foreach (var diff in batch)
        {
            Differences.Add(diff);
        }
    }

    private static void PurgeOldSessionReports()
    {
        try
        {
            var dir = AppPaths.SessionReportsDirectory;
            if (!Directory.Exists(dir)) return;
            foreach (var file in Directory.GetFiles(dir, "session_*.csv"))
            {
                if (File.GetLastWriteTimeUtc(file) < DateTime.UtcNow.AddDays(-14))
                {
                    File.Delete(file);
                }
            }
        }
        catch
        {
            // Best-effort housekeeping
        }
    }

    private static void InvalidateCommands()
    {
        var dispatcher = Application.Current?.Dispatcher;
        if (dispatcher == null || dispatcher.CheckAccess())
        {
            CommandManager.InvalidateRequerySuggested();
        }
        else
        {
            dispatcher.InvokeAsync(() => CommandManager.InvalidateRequerySuggested());
        }
    }

    private async Task ExportReportAsync()
    {
        string? sessionCsv = _sessionCsvPath;
        bool haveSessionCsv = !string.IsNullOrEmpty(sessionCsv) && File.Exists(sessionCsv);

        if (!haveSessionCsv && Statistics.TotalDifferences > Differences.Count)
        {
            var proceed = MessageBox.Show(
                $"The full difference report file is no longer available. Only the {Differences.Count:N0} most recent differences (of {Statistics.TotalDifferences:N0}) can be exported from the grid.\n\nExport this partial list anyway?",
                "Partial Export",
                MessageBoxButton.YesNo,
                MessageBoxImage.Warning);
            if (proceed != MessageBoxResult.Yes)
                return;
        }

        var sfd = new SaveFileDialog
        {
            Title = "Export Differences Report",
            Filter = "CSV File (*.csv)|*.csv|JSON File (*.json)|*.json",
            FileName = $"S3Compare_Report_{DateTime.Now:yyyyMMdd_HHmmss}.csv"
        };

        if (sfd.ShowDialog() == true)
        {
            try
            {
                string target = sfd.FileName;
                var stats = Statistics;
                bool json = target.EndsWith(".json", StringComparison.OrdinalIgnoreCase);
                var gridCopy = haveSessionCsv ? null : Differences.ToList();

                // Heavy file I/O (the report can hold millions of rows) stays off the UI thread
                await Task.Run(async () =>
                {
                    if (json)
                    {
                        IEnumerable<ObjectDifference> source = haveSessionCsv ? CsvReport.ReadDifferences(sessionCsv!) : gridCopy!;
                        await ReportExporter.ExportToJsonAsync(target, source, stats);
                    }
                    else if (haveSessionCsv)
                    {
                        await ReportExporter.ExportFromSessionCsvAsync(target, sessionCsv!, stats);
                    }
                    else
                    {
                        await ReportExporter.ExportToCsvAsync(target, gridCopy!, stats);
                    }
                });
                MessageBox.Show($"Report exported successfully to:\n{target}", "Export Succeeded", MessageBoxButton.OK, MessageBoxImage.Information);
            }
            catch (Exception ex)
            {
                MessageBox.Show($"Failed to export report:\n{ex.Message}", "Export Error", MessageBoxButton.OK, MessageBoxImage.Error);
            }
        }
    }

    public event PropertyChangedEventHandler? PropertyChanged;
    protected virtual void OnPropertyChanged([CallerMemberName] string? propertyName = null)
    {
        PropertyChanged?.Invoke(this, new PropertyChangedEventArgs(propertyName));
    }

    protected bool SetField<T>(ref T field, T value, [CallerMemberName] string? propertyName = null)
    {
        if (EqualityComparer<T>.Default.Equals(field, value)) return false;
        field = value;
        OnPropertyChanged(propertyName);
        return true;
    }
}
