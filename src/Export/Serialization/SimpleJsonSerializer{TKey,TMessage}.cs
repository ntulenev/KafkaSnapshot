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
    public SimpleJsonSerializer(ILogger<SimpleJsonSerializer<TKey, TMessage>> logger) : base(logger) { }

    /// <inheritdoc/>
    public string Serialize(IEnumerable<KeyValuePair<TKey, KafkaMessage<TMessage>>> data, bool exportRawMessage)
    {
        ArgumentNullException.ThrowIfNull(data);

        _ = exportRawMessage; // not needed for this implementation.
        return SerializeData(data);
    }

    /// <inheritdoc/>
    public void Serialize(IEnumerable<KeyValuePair<TKey, KafkaMessage<TMessage>>> data, bool exportRawMessage, Stream stream)
    {
        throw new NotImplementedException();
    }
}
