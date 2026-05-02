using System.Text.Json.Nodes;

using Microsoft.Extensions.Logging;

using KafkaSnapshot.Abstractions.Export;
using KafkaSnapshot.Export.Markers;
using KafkaSnapshot.Models.Message;

namespace KafkaSnapshot.Export.Serialization;

/// <summary>
/// Serializer for data with no keys.
/// </summary>
/// <remarks>
/// Creates <see cref="IgnoreKeySerializer"/>.
/// </remarks>
/// <param name="logger">Logger.</param>
/// <exception cref="ArgumentNullException">Thrown when logger is null.</exception>
public sealed class IgnoreKeySerializer(ILogger<IgnoreKeySerializer> logger) :
    JsonSerializerBase(logger),
    ISerializer<string, string, IgnoreKeyMarker>
{
    private object ProjectData(
                    IEnumerable<KeyValuePair<string, KafkaMessage<string>>> data,
                    bool exportRawMessage)
    {
        if (data.Any(x => x.Key is not null))
        {
            Logger.LogWarning("Serialization data contains non-null keys. " +
                "These keys will be ignored.");
        }

        var items = data.Select(x => new
        {
            Value = exportRawMessage ? x.Value.Message : ParseJson(x.Value.Message),
            x.Value.Meta
        });

        return items;
    }

    /// <inheritdoc/>
    /// <exception cref="ArgumentNullException">Thrown when data is null.</exception>
    public string Serialize(
            IEnumerable<KeyValuePair<string, KafkaMessage<string>>> data,
            bool exportRawMessage)
    {
        ArgumentNullException.ThrowIfNull(data);

        return SerializeData(ProjectData(data, exportRawMessage));
    }

    /// <inheritdoc/>
    /// <exception cref="ArgumentNullException">Thrown when data or stream is null.</exception>
    public Task SerializeAsync(
            IEnumerable<KeyValuePair<string, KafkaMessage<string>>> data,
            bool exportRawMessage,
            Stream stream,
            CancellationToken ct)
    {
        ArgumentNullException.ThrowIfNull(data);
        ArgumentNullException.ThrowIfNull(stream);

        return SerializeDataToStreamAsync(ProjectData(data, exportRawMessage), stream, ct);
    }

    private static JsonNode ParseJson(string value)
        => JsonNode.Parse(value) ?? throw new InvalidOperationException("JSON value cannot be null.");
}
