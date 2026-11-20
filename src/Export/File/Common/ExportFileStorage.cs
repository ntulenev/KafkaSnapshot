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
        await WriteAsync(fileName, async stream =>
        {
            using var writer = new StreamWriter(stream, leaveOpen: true);
            await writer.WriteAsync(content.AsMemory(), ct).ConfigureAwait(false);
            await writer.FlushAsync(ct).ConfigureAwait(false);
        }, ct).ConfigureAwait(false);
    }

    /// <inheritdoc/>
    public async Task WriteAsync(FileName fileName, Func<Stream, Task> write, CancellationToken ct)
    {
        ArgumentNullException.ThrowIfNull(fileName);
        ArgumentNullException.ThrowIfNull(write);
        ct.ThrowIfCancellationRequested();

        var destination = ResolvePath(fileName);
        var temporary = Path.Combine(Path.GetDirectoryName(destination)!, Path.GetRandomFileName());
        var stream = new FileStream(temporary, FileMode.CreateNew, FileAccess.Write,
            FileShare.None, 4096, FileOptions.Asynchronous);
        try
        {
            await using (stream.ConfigureAwait(false))
            {
                await write(stream).ConfigureAwait(false);
            }

            ct.ThrowIfCancellationRequested();
            System.IO.File.Move(temporary, destination, overwrite: true);
        }
        catch
        {
            System.IO.File.Delete(temporary);
            throw;
        }
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
