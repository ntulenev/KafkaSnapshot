using System.Text.Json.Nodes;

using Microsoft.Extensions.Logging;

using KafkaSnapshot.Abstractions.Export;
using KafkaSnapshot.Models.Message;
using KafkaSnapshot.Export.Markers;

namespace KafkaSnapshot.Export.Serialization;

/// <summary>
/// Serializer for data with keys of <typeparamref name="TKey"/> type.
/// </summary>
/// <typeparam name="TKey">Data key type.</typeparam>
/// <remarks>
/// Creates <see cref="OriginalKeySerializer{TKey}"/>.
/// </remarks>
/// <param name="logger">Logger.</param>
/// <exception cref="ArgumentNullException">Thrown when logger is null.</exception>
public sealed class OriginalKeySerializer<TKey>(ILogger<OriginalKeySerializer<TKey>> logger) :
             JsonSerializerBase(logger),
             ISerializer<TKey, string, OriginalKeyMarker>
{
    private static object ProjectData(
                IEnumerable<KeyValuePair<TKey, KafkaMessage<string>>> data,
                bool exportRawMessage)
        => data.Select(x => new
        {
            x.Key,
            Value = exportRawMessage ? x.Value.Message : ParseJson(x.Value.Message),
            x.Value.Meta
        });

    /// <inheritdoc/>
    /// <exception cref="ArgumentNullException">Thrown when data is null.</exception>
    public string Serialize(
                IEnumerable<KeyValuePair<TKey, KafkaMessage<string>>> data,
                bool exportRawMessage)
    {
        ArgumentNullException.ThrowIfNull(data);

        return SerializeData(ProjectData(data, exportRawMessage));
    }

    /// <inheritdoc/>
    /// <exception cref="ArgumentNullException">Thrown when data or stream is null.</exception>
    public Task SerializeAsync(
                IEnumerable<KeyValuePair<TKey, KafkaMessage<string>>> data,
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
