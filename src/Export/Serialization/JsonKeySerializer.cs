using Microsoft.Extensions.Logging;

using Newtonsoft.Json.Linq;

using KafkaSnapshot.Abstractions.Export;
using KafkaSnapshot.Export.Markers;
using KafkaSnapshot.Models.Message;

namespace KafkaSnapshot.Export.Serialization
{
    /// <summary>
    /// Serializer for data with json keys.
    /// </summary>
    public class JsonKeySerializer : JsonSerializerBase, ISerializer<string, string, JsonKeyMarker>
    {

        /// <summary>
        /// Creates <see cref="JsonKeySerializer"/>.
        /// </summary>
        /// <param name="logger">Logger.</param>
        public JsonKeySerializer(ILogger<JsonKeySerializer> logger) : base(logger) { }

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
