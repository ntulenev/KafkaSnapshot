using KafkaSnapshot.Models.Names;

namespace KafkaSnapshot.Abstractions.Export;

/// <summary>Writes exports to their configured destination.</summary>
public interface IExportFileStorage
{
    /// <summary>Saves text to an export file.</summary>
    Task SaveAsync(FileName fileName, string content, CancellationToken ct);

    /// <summary>Writes one complete export. File storage publishes it after the writer succeeds.</summary>
    async Task WriteAsync(FileName fileName, Func<Stream, Task> write, CancellationToken ct)
    {
        ArgumentNullException.ThrowIfNull(write);
        ct.ThrowIfCancellationRequested();
        var stream = CreateFileStream(fileName);
        await using var scope = stream.ConfigureAwait(false);
        await write(stream).ConfigureAwait(false);
        ct.ThrowIfCancellationRequested();
    }

    /// <summary>Opens a writable export stream owned by the caller.</summary>
    Stream CreateFileStream(FileName fileName);
}
