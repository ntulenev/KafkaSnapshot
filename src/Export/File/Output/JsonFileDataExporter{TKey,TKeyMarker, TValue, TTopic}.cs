using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;

using KafkaSnapshot.Abstractions.Export;
using KafkaSnapshot.Models.Export;
using KafkaSnapshot.Models.Message;
using KafkaSnapshot.Export.Configuration;

namespace KafkaSnapshot.Export.File.Output;

/// <summary>
/// Default Json File exporter.
/// </summary>
/// <typeparam name="TKey">Message Key.</typeparam>
/// <typeparam name="TKeyMarker">Key marker.</typeparam>
/// <typeparam name="TValue">Message Value.</typeparam>
/// <typeparam name="TTopic">Topic object.</typeparam>
public class JsonFileDataExporter<TKey, TKeyMarker, TValue, TTopic> :
                    IDataExporter<TKey, TKeyMarker, TValue, TTopic>
                    where TTopic : ExportedTopic
                    where TKeyMarker : IKeyRepresentationMarker
                    where TValue : notnull
{
    /// <summary>
    /// Creates <see cref="JsonFileDataExporter{TKey, TKeyMarker, TValue, TTopic}"/>.
    /// </summary>
    /// <param name="logger">Logger for 
    /// <see cref="JsonFileDataExporter{TKey, TKeyMarker, TValue, TTopic}"/>.</param>
    /// <param name="fileSaver">Utility that saves content to file.</param>
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

        if (config.Value is null)
        {
            throw new ArgumentException("Config is not set", nameof(config));
        }

        _logger = logger ?? throw new ArgumentNullException(nameof(logger));
        _fileSaver = fileSaver ?? throw new ArgumentNullException(nameof(fileSaver));
        _streamProvider = streamProvider ?? throw new ArgumentNullException(nameof(streamProvider));
        _serializer = serializer ?? throw new ArgumentNullException(nameof(serializer));
        _isStreamingMode = config.Value.UseFileStreaming;

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

        if (_isStreamingMode)
        {
            await InnerStreamExportAsync(data, topic, ct);
        }
        else
        {
            await InnerFileExportAsync(data, topic, ct);
        }
    }

    private async Task InnerFileExportAsync(
        IEnumerable<KeyValuePair<TKey, KafkaMessage<TValue>>> data, 
        TTopic topic, 
        CancellationToken ct)
    {
        using var _ = _logger.BeginScope("Data from topic {topic} to File {file}", 
                            topic.TopicName.Name, 
                            topic.ExportName);

        _logger.LogDebug("Starting saving data.");

        await _fileSaver.SaveAsync(
                    topic.ExportName, 
                    _serializer.Serialize(data, topic.ExportRawMessage), 
                    ct)
                    .ConfigureAwait(false);

        _logger.LogDebug("Data saved successfully.");
    }

    private async Task InnerStreamExportAsync(
            IEnumerable<KeyValuePair<TKey, KafkaMessage<TValue>>> data, 
            TTopic topic, 
            CancellationToken ct)
    {
        using var _ = _logger.BeginScope("Data from topic {topic} to File {file} with stream",
                        topic.TopicName.Name, 
                        topic.ExportName);

        using var stream = _streamProvider.CreateFileStream(topic.ExportName);

        _serializer.Serialize(data, topic.ExportRawMessage, stream);

        await Task.CompletedTask;
    }

    private readonly ILogger<JsonFileDataExporter<TKey, TKeyMarker, TValue, TTopic>> _logger;
    private readonly IFileSaver _fileSaver;
    private readonly IFileStreamProvider _streamProvider;
    private readonly ISerializer<TKey, TValue, TKeyMarker> _serializer;
    private readonly bool _isStreamingMode;
}
