using System.Runtime.CompilerServices;

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
public sealed class JsonFileDataExporter<TKey, TKeyMarker, TValue> :
                    IDataExporter<TKey, TKeyMarker, TValue>
                    where TKeyMarker : IKeyRepresentationMarker
                    where TValue : notnull
{
    /// <summary>
    /// Creates <see cref="JsonFileDataExporter{TKey, TKeyMarker, TValue}"/>.
    /// </summary>
    /// <param name="config">Exporter configuration.</param>
    /// <param name="logger">Logger for 
    /// <see cref="JsonFileDataExporter{TKey, TKeyMarker, TValue}"/>.</param>
    /// <param name="storage">Export file storage.</param>
    /// <param name="serializer">Serializer for export payload.</param>
    /// <exception cref="ArgumentNullException">Thrown if any of the 
    /// constructor arguments are null.</exception>
    public JsonFileDataExporter(
        IOptions<JsonFileDataExporterConfiguration> config,
        ILogger<JsonFileDataExporter<TKey, TKeyMarker, TValue>> logger,
        IExportFileStorage storage,
        ISerializer<TKey, TValue, TKeyMarker> serializer)
    {
        ArgumentNullException.ThrowIfNull(config);

        ArgumentNullException.ThrowIfNull(config.Value, nameof(config));
        ArgumentNullException.ThrowIfNull(logger);
        ArgumentNullException.ThrowIfNull(storage);
        ArgumentNullException.ThrowIfNull(serializer);

        _logger = logger;
        _storage = storage;
        _serializer = serializer;
        _isStreamingMode = config.Value.UseFileStreaming;

        _logger.LogDebug("Instance created.");
    }

    /// <inheritdoc/>
    /// <exception cref="ArgumentNullException">Thrown if either <paramref name="data"/> 
    /// or <paramref name="topic"/> is null.</exception>
    public async Task ExportAsync(
        IEnumerable<KeyValuePair<TKey, KafkaMessage<TValue>>> data,
        ExportedTopic topic,
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

    /// <inheritdoc/>
    public bool UseStreaming => _isStreamingMode;

    /// <inheritdoc/>
    public async Task ExportAsync(
        IAsyncEnumerable<KeyValuePair<TKey, KafkaMessage<TValue>>> data,
        ExportedTopic topic, CancellationToken ct)
    {
        ArgumentNullException.ThrowIfNull(data);
        ArgumentNullException.ThrowIfNull(topic);
        ct.ThrowIfCancellationRequested();

        if (!_isStreamingMode)
        {
            var buffered = new List<KeyValuePair<TKey, KafkaMessage<TValue>>>();
            await foreach (var item in data.WithCancellation(ct).ConfigureAwait(false))
            {
                buffered.Add(item);
            }

            await ExportAsync(buffered, topic, ct).ConfigureAwait(false);
            return;
        }

        // Resolve metadata and the first read before truncating an existing export.
        await using var enumerator = data.GetAsyncEnumerator(ct);
        var hasItem = await enumerator.MoveNextAsync().ConfigureAwait(false);
        ct.ThrowIfCancellationRequested();
        var stream = _storage.CreateFileStream(topic.ExportName);
        await using var streamScope = stream.ConfigureAwait(false);
        await _serializer.SerializeAsync(
            EnumerateAsync(enumerator, hasItem, ct), topic.ExportRawMessage, stream, ct)
            .ConfigureAwait(false);
        ct.ThrowIfCancellationRequested();
    }

    private async Task InnerFileExportAsync(
        IEnumerable<KeyValuePair<TKey, KafkaMessage<TValue>>> data,
        ExportedTopic topic,
        CancellationToken ct)
    {
        using var _ = _logger.BeginScope("Data from topic {Topic} to File {File}",
                            topic.TopicName.Name,
                            topic.ExportName.FullName);

        _logger.LogDebug("Starting saving data.");

        var cancellableData = EnumerateWithCancellation(data, ct);

        await _storage.SaveAsync(
                    topic.ExportName,
                    _serializer.Serialize(cancellableData, topic.ExportRawMessage),
                    ct)
                    .ConfigureAwait(false);

        _logger.LogDebug("Data saved successfully.");
    }

    private async Task InnerStreamExportAsync(
            IEnumerable<KeyValuePair<TKey, KafkaMessage<TValue>>> data,
            ExportedTopic topic,
            CancellationToken ct)
    {
        using var _ = _logger.BeginScope("Data from topic {Topic} to File {File} with stream",
                        topic.TopicName.Name,
                        topic.ExportName.FullName);

        ct.ThrowIfCancellationRequested();

        var stream = _storage.CreateFileStream(topic.ExportName);
        await using var streamScope = stream.ConfigureAwait(false);

        await _serializer.SerializeAsync(
            EnumerateWithCancellation(data, ct),
            topic.ExportRawMessage,
            stream,
            ct).ConfigureAwait(false);

        ct.ThrowIfCancellationRequested();
    }

    private static async IAsyncEnumerable<T> EnumerateAsync<T>(IAsyncEnumerator<T> enumerator,
        bool hasItem, [EnumeratorCancellation] CancellationToken ct)
    {
        while (hasItem)
        {
            ct.ThrowIfCancellationRequested();
            yield return enumerator.Current;
            hasItem = await enumerator.MoveNextAsync().ConfigureAwait(false);
        }
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
    private readonly IExportFileStorage _storage;
    private readonly ISerializer<TKey, TValue, TKeyMarker> _serializer;
    private readonly bool _isStreamingMode;
}
