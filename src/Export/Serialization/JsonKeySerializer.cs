using KafkaSnapshot.Abstractions.Export;
using KafkaSnapshot.Export.Markers;
using KafkaSnapshot.Models.Message;

using Newtonsoft.Json.Linq;

namespace KafkaSnapshot.Export.Serialization
{
    /// <summary>
    /// Serializer for data with json keys.
    /// </summary>
    public class JsonKeySerializer : JsonSerializerBase, ISerializer<string, string, JsonKeyMarker>
    {
        /// <inheritdoc/>
        public string Serialize(IEnumerable<KeyValuePair<string, DatedMessage<string>>> data, bool exportRawMessage)
        {
            if (data is null)
            {
                throw new ArgumentNullException(nameof(data));
            }

            var items = data.Select(x => new
            {
                Key = JToken.Parse(x.Key),
                Value = exportRawMessage ? x.Value.Message : JToken.Parse(x.Value.Message),
                x.Value.Timestamp
            });

            return SerializeData(items);
        }
    }
}
