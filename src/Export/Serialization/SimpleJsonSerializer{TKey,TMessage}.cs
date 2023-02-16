using Microsoft.Extensions.Logging;

using KafkaSnapshot.Abstractions.Export;
using KafkaSnapshot.Models.Message;
using KafkaSnapshot.Export.Markers;

namespace KafkaSnapshot.Export.Serialization;

/// <summary>
/// Basic serializer.
/// </summary>
/// <typeparam name="TKey">Data key type.</typeparam>
public class SimpleJsonSerializer<TKey, TMessage> : JsonSerializerBase,
                                                    ISerializer<TKey, TMessage, OriginalKeyMarker>
                                                    where TMessage : notnull
{
    /// <summary>
    /// Creates <see cref="SimpleJsonSerializer{TKey, TMessage}"/>.
    /// </summary>
    /// <param name="logger">Logger.</param>
    /// <exception cref="ArgumentNullException">Thrown when logger is null.</exception>
    public SimpleJsonSerializer(ILogger<SimpleJsonSerializer<TKey, TMessage>> logger) : base(logger) { }

    /// <inheritdoc/>
    /// <exception cref="ArgumentNullException">Thrown when data is null.</exception>
    public string Serialize(IEnumerable<KeyValuePair<TKey, KafkaMessage<TMessage>>> data, bool exportRawMessage)
    {
        ArgumentNullException.ThrowIfNull(data);

        _ = exportRawMessage; // not needed for this implementation.

        return SerializeData(data);
    }

    /// <inheritdoc/>
    /// <exception cref="ArgumentNullException">Thrown when data or stream is null.</exception>
    public void Serialize(IEnumerable<KeyValuePair<TKey, KafkaMessage<TMessage>>> data, bool exportRawMessage, Stream stream)
    {
        ArgumentNullException.ThrowIfNull(data);
        ArgumentNullException.ThrowIfNull(stream);

        _ = exportRawMessage; // not needed for this implementation.

        SerializeDataToStream(data, stream);
    }
}
