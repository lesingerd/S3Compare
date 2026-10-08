using S3Compare.App.Models;

namespace S3Compare.App.Services;

public interface IS3ObjectStream : IAsyncDisposable
{
    S3ObjectSummary? Current { get; }
    long TotalObjectsRead { get; }
    long TotalBytesRead { get; }
    bool HasMore { get; }
    Task<bool> MoveNextAsync(CancellationToken cancellationToken = default);
}
