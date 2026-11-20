using KafkaSnapshot.Abstractions.Export;
using KafkaSnapshot.Export.Configuration;
using KafkaSnapshot.Models.Names;

using Microsoft.Extensions.Options;

namespace KafkaSnapshot.Export.File.Common;

/// <summary>Resolves and writes export files in the configured output directory.</summary>
public sealed class ExportFileStorage : IExportFileStorage
{
    /// <summary>Creates storage using the configured output directory.</summary>
    public ExportFileStorage(IOptions<JsonFileDataExporterConfiguration> options)
    {
        ArgumentNullException.ThrowIfNull(options);
        ArgumentNullException.ThrowIfNull(options.Value, nameof(options));
        _outputDirectory = options.Value.OutputDirectory;
    }

    /// <inheritdoc/>
    public async Task SaveAsync(FileName fileName, string content, CancellationToken ct)
    {
        ArgumentNullException.ThrowIfNull(fileName);
        ArgumentNullException.ThrowIfNull(content);
        ct.ThrowIfCancellationRequested();
        await System.IO.File.WriteAllTextAsync(ResolvePath(fileName), content, ct)
            .ConfigureAwait(false);
    }

    /// <inheritdoc/>
    public Stream CreateFileStream(FileName fileName)
    {
        ArgumentNullException.ThrowIfNull(fileName);
        return System.IO.File.Create(ResolvePath(fileName));
    }

    private string ResolvePath(FileName fileName)
    {
        if (!string.IsNullOrWhiteSpace(_outputDirectory))
        {
            _ = Directory.CreateDirectory(_outputDirectory);
        }

        return fileName.ResolvePath(_outputDirectory);
    }

    private readonly string? _outputDirectory;
}
