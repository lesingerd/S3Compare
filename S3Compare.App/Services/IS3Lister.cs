using System.IO;
using Amazon.Runtime;
using Amazon.S3;
using Amazon.S3.Model;
using S3Compare.App.Models;

namespace S3Compare.App.Services;

/// <summary>One ListObjectsV2 response, reduced to what the engine needs.</summary>
public sealed class S3ListPage
{
    public List<S3ObjectSummary> Objects { get; init; } = new();
    public List<string> CommonPrefixes { get; init; } = new();
    public string? NextContinuationToken { get; init; }
    public bool IsTruncated { get; init; }
}

/// <summary>
/// A single ListObjectsV2 call. Abstracted so the listing, prefetching and partition planning can be tested
/// against an in-memory bucket.
/// </summary>
public interface IS3Lister
{
    Task<S3ListPage> ListPageAsync(
        string bucketName,
        string? prefix,
        string? startAfter,
        string? continuationToken,
        string? delimiter,
        int maxKeys,
        CancellationToken cancellationToken = default);
}

/// <summary>
/// <see cref="IS3Lister"/> over a real S3 client. Retries are done here, not by the SDK (create the client with
/// MaxErrorRetry 0): each attempt has its own timeout and its number is published through
/// <see cref="InFlightOperations.Current"/>, so a struggling request is visible as "attempt 2" in the diagnostics.
/// </summary>
public sealed class AmazonS3Lister : IS3Lister
{
    public const int DefaultMaxAttempts = 3;
    public static readonly TimeSpan DefaultAttemptTimeout = TimeSpan.FromSeconds(30);

    private readonly IAmazonS3 _client;
    private readonly int _maxAttempts;
    private readonly TimeSpan _attemptTimeout;
    private readonly TimeSpan _retryDelay;

    public AmazonS3Lister(IAmazonS3 client, int maxAttempts = DefaultMaxAttempts, TimeSpan? attemptTimeout = null, TimeSpan? retryDelay = null)
    {
        _client = client ?? throw new ArgumentNullException(nameof(client));
        _maxAttempts = Math.Max(1, maxAttempts);
        _attemptTimeout = attemptTimeout ?? DefaultAttemptTimeout;
        _retryDelay = retryDelay ?? TimeSpan.FromSeconds(1);
    }

    public async Task<S3ListPage> ListPageAsync(
        string bucketName,
        string? prefix,
        string? startAfter,
        string? continuationToken,
        string? delimiter,
        int maxKeys,
        CancellationToken cancellationToken = default)
    {
        var request = new ListObjectsV2Request
        {
            BucketName = bucketName,
            Prefix = string.IsNullOrEmpty(prefix) ? null : prefix,
            StartAfter = string.IsNullOrEmpty(startAfter) ? null : startAfter,
            ContinuationToken = string.IsNullOrEmpty(continuationToken) ? null : continuationToken,
            Delimiter = string.IsNullOrEmpty(delimiter) ? null : delimiter,
            MaxKeys = maxKeys
        };

        ListObjectsV2Response response;
        for (int attempt = 1; ; attempt++)
        {
            var tracked = InFlightOperations.Current;
            if (tracked != null) tracked.Attempt = attempt;

            using var attemptCts = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken);
            attemptCts.CancelAfter(_attemptTimeout);
            try
            {
                response = await _client.ListObjectsV2Async(request, attemptCts.Token).ConfigureAwait(false);
                break;
            }
            catch (OperationCanceledException) when (cancellationToken.IsCancellationRequested)
            {
                throw;
            }
            catch (Exception ex) when (IsRetryable(ex) && attempt < _maxAttempts)
            {
                await Task.Delay(_retryDelay * attempt, cancellationToken).ConfigureAwait(false);
            }
            catch (OperationCanceledException ex)
            {
                throw new TimeoutException($"LIST request timed out after {_attemptTimeout.TotalSeconds:N0} s (attempt {attempt} of {_maxAttempts}).", ex);
            }
        }

        var objects = new List<S3ObjectSummary>(response.S3Objects?.Count ?? 0);
        foreach (var obj in response.S3Objects ?? [])
        {
            objects.Add(new S3ObjectSummary
            {
                Key = obj.Key,
                Size = obj.Size ?? 0,
                ETag = (obj.ETag ?? string.Empty).Trim('\"'),
                LastModified = obj.LastModified ?? DateTime.MinValue,
                StorageClass = obj.StorageClass?.Value ?? "STANDARD"
            });
        }

        return new S3ListPage
        {
            Objects = objects,
            CommonPrefixes = response.CommonPrefixes ?? new List<string>(),
            NextContinuationToken = response.NextContinuationToken,
            IsTruncated = response.IsTruncated ?? false
        };
    }

    /// <summary>Timeouts, network errors, throttling and 5xx are retried; other 4xx answers (access denied, no such bucket) are not.</summary>
    private static bool IsRetryable(Exception ex)
    {
        switch (ex)
        {
            case OperationCanceledException:
                return true; // the attempt's own timeout (the caller's cancellation is handled before)
            case AmazonServiceException service:
                int code = (int)service.StatusCode;
                return code >= 500 || code == 408 || code == 429 || code == 0;
            case AmazonClientException or System.Net.Http.HttpRequestException or IOException:
                return true;
            default:
                return false;
        }
    }
}
