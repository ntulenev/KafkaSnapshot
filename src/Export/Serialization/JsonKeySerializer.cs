using Microsoft.Extensions.Logging;

using Newtonsoft.Json.Linq;

using KafkaSnapshot.Abstractions.Export;
using KafkaSnapshot.Export.Markers;
using KafkaSnapshot.Models.Message;

namespace KafkaSnapshot.Export.Serialization;

/// <summary>
/// Serializer for data with json keys.
/// </summary>
public class JsonKeySerializer :
    JsonSerializerBase,
    ISerializer<string, string, JsonKeyMarker>
{

    /// <summary>
    /// Creates <see cref="JsonKeySerializer"/>.
    /// </summary>
    /// <param name="logger">Logger.</param>
    /// <exception cref="ArgumentNullException">Thrown when logger is null.</exception>
    public JsonKeySerializer(ILogger<JsonKeySerializer> logger) : base(logger) { }

    private static object ProjectData(
                IEnumerable<KeyValuePair<string, KafkaMessage<string>>> data,
                bool exportRawMessage)
        => data.Select(x => new
        {
            Key = JToken.Parse(x.Key),
            Value = exportRawMessage ? x.Value.Message : JToken.Parse(x.Value.Message),
            x.Value.Meta
        });

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
    public void Serialize(
            IEnumerable<KeyValuePair<string, KafkaMessage<string>>> data,
            bool exportRawMessage,
            Stream stream)
    {
        ArgumentNullException.ThrowIfNull(data);
        ArgumentNullException.ThrowIfNull(stream);

        SerializeDataToStream(ProjectData(data, exportRawMessage), stream);
    }
}
