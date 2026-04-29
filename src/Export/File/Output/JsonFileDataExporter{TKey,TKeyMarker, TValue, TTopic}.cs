using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;

using KafkaSnapshot.Abstractions.Export;
using KafkaSnapshot.Models.Export;
using KafkaSnapshot.Models.Message;
using KafkaSnapshot.Export.Configuration;

namespace KafkaSnapshot.Export.File.Output;

/// <summary>
/// Exports topic data to a JSON file.
/// </summary>
/// <typeparam name="TKey">Message key.</typeparam>
/// <typeparam name="TKeyMarker">Key marker.</typeparam>
/// <typeparam name="TValue">Message value.</typeparam>
/// <typeparam name="TTopic">Export topic type.</typeparam>
public sealed class JsonFileDataExporter<TKey, TKeyMarker, TValue, TTopic> :
                    IDataExporter<TKey, TKeyMarker, TValue, TTopic>
                    where TTopic : ExportedTopic
                    where TKeyMarker : IKeyRepresentationMarker
                    where TValue : notnull
{
    /// <summary>
    /// Creates <see cref="JsonFileDataExporter{TKey, TKeyMarker, TValue, TTopic}"/>.
    /// </summary>
    /// <param name="config">Exporter configuration.</param>
    /// <param name="logger">Logger for 
    /// <see cref="JsonFileDataExporter{TKey, TKeyMarker, TValue, TTopic}"/>.</param>
    /// <param name="fileSaver">Utility that saves content to file.</param>
    /// <param name="streamProvider">Utility that provides output streams.</param>
    /// <param name="serializer">Serializer for export payload.</param>
    /// <exception cref="ArgumentNullException">Thrown if any of the 
    /// constructor arguments are null.</exception>
    public JsonFileDataExporter(
                                IOptions<JsonFileDataExporterConfiguration> config,
                                ILogger<JsonFileDataExporter<TKey, TKeyMarker,
                                TValue, TTopic>> logger,
                                IFileSaver fileSaver,
                                IFileStreamProvider streamProvider,
                                ISerializer<TKey, TValue, TKeyMarker> serializer)
    {
        ArgumentNullException.ThrowIfNull(config);

        ArgumentNullException.ThrowIfNull(config.Value, nameof(config));
        ArgumentNullException.ThrowIfNull(logger);
        ArgumentNullException.ThrowIfNull(fileSaver);
        ArgumentNullException.ThrowIfNull(streamProvider);
        ArgumentNullException.ThrowIfNull(serializer);

        _logger = logger;
        _fileSaver = fileSaver;
        _streamProvider = streamProvider;
        _serializer = serializer;
        _isStreamingMode = config.Value.UseFileStreaming;
        _outputDirectory = config.Value.OutputDirectory;

        _logger.LogDebug("Instance created.");
    }

    /// <inheritdoc/>
    /// <exception cref="ArgumentNullException">Thrown if either <paramref name="data"/> 
    /// or <paramref name="topic"/> is null.</exception>
    public async Task ExportAsync(
        IEnumerable<KeyValuePair<TKey, KafkaMessage<TValue>>> data,
        TTopic topic,
        CancellationToken ct)
    {
        ArgumentNullException.ThrowIfNull(data);
        ArgumentNullException.ThrowIfNull(topic);

        ct.ThrowIfCancellationRequested();

        if (_isStreamingMode)
        {
            await InnerStreamExportAsync(data, topic, ct).ConfigureAwait(false);
        }
        else
        {
            await InnerFileExportAsync(data, topic, ct).ConfigureAwait(false);
        }
    }

    private async Task InnerFileExportAsync(
        IEnumerable<KeyValuePair<TKey, KafkaMessage<TValue>>> data,
        TTopic topic,
        CancellationToken ct)
    {
        using var _ = _logger.BeginScope("Data from topic {Topic} to File {File}",
                            topic.TopicName.Name,
                            ResolveExportName(topic.ExportName));

        _logger.LogDebug("Starting saving data.");

        var cancellableData = EnumerateWithCancellation(data, ct);

        await _fileSaver.SaveAsync(
                    ResolveExportName(topic.ExportName),
                    _serializer.Serialize(cancellableData, topic.ExportRawMessage),
                    ct)
                    .ConfigureAwait(false);

        _logger.LogDebug("Data saved successfully.");
    }

    private async Task InnerStreamExportAsync(
            IEnumerable<KeyValuePair<TKey, KafkaMessage<TValue>>> data,
            TTopic topic,
            CancellationToken ct)
    {
        using var _ = _logger.BeginScope("Data from topic {Topic} to File {File} with stream",
                        topic.TopicName.Name,
                        ResolveExportName(topic.ExportName));

        ct.ThrowIfCancellationRequested();

        var stream = _streamProvider.CreateFileStream(ResolveExportName(topic.ExportName));
        await using var streamScope = stream.ConfigureAwait(false);

        await _serializer.SerializeAsync(
            EnumerateWithCancellation(data, ct),
            topic.ExportRawMessage,
            stream,
            ct).ConfigureAwait(false);

        ct.ThrowIfCancellationRequested();
    }

    private Models.Names.FileName ResolveExportName(Models.Names.FileName fileName)
    {
        if (string.IsNullOrWhiteSpace(_outputDirectory))
        {
            return fileName;
        }

        _ = Directory.CreateDirectory(_outputDirectory);
        return new Models.Names.FileName(Path.Combine(_outputDirectory, fileName.FullName));
    }

    private static IEnumerable<T> EnumerateWithCancellation<T>(
        IEnumerable<T> source,
        CancellationToken ct)
    {
        foreach (var item in source)
        {
            ct.ThrowIfCancellationRequested();
            yield return item;
        }
    }

    private readonly ILogger _logger;
    private readonly IFileSaver _fileSaver;
    private readonly IFileStreamProvider _streamProvider;
    private readonly ISerializer<TKey, TValue, TKeyMarker> _serializer;
    private readonly bool _isStreamingMode;
    private readonly string? _outputDirectory;
}
