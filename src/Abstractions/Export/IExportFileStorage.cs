using KafkaSnapshot.Models.Names;

namespace KafkaSnapshot.Abstractions.Export;

/// <summary>Writes exports to their configured destination.</summary>
public interface IExportFileStorage
{
    /// <summary>Saves text to an export file.</summary>
    Task SaveAsync(FileName fileName, string content, CancellationToken ct);

    /// <summary>Opens a writable export stream owned by the caller.</summary>
    Stream CreateFileStream(FileName fileName);
}
