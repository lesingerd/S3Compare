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

/// <summary><see cref="IS3Lister"/> over a real S3 client.</summary>
public sealed class AmazonS3Lister : IS3Lister
{
    private readonly IAmazonS3 _client;

    public AmazonS3Lister(IAmazonS3 client)
    {
        _client = client ?? throw new ArgumentNullException(nameof(client));
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

        var response = await _client.ListObjectsV2Async(request, cancellationToken).ConfigureAwait(false);

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
}
