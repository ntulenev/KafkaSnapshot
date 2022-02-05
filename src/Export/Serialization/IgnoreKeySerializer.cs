using Newtonsoft.Json.Linq;

using Microsoft.Extensions.Logging;

using KafkaSnapshot.Abstractions.Export;
using KafkaSnapshot.Export.Markers;
using KafkaSnapshot.Models.Message;

namespace KafkaSnapshot.Export.Serialization
{

    public class IgnoreKeySerializer : JsonSerializerBase, ISerializer<string, string, IgnoreKeyMarker>
    {
        /// <summary>
        /// Creates <see cref="IgnoreKeySerializer"/>.
        /// </summary>
        /// <param name="logger">Logger.</param>
        public IgnoreKeySerializer(ILogger<IgnoreKeySerializer> logger) : base(logger) { }

        /// <inheritdoc/>
        public string Serialize(IEnumerable<KeyValuePair<string, DatedMessage<string>>> data, bool exportRawMessage)
        {
            if (data is null)
            {
                throw new ArgumentNullException(nameof(data));
            }

            if (data.Any(x => x.Key is not null))
            {
                
            }

            var items = data.Select(x => new
            {
                Value = exportRawMessage ? x.Value.Message : JToken.Parse(x.Value.Message),
                x.Value.Timestamp
            });

            return SerializeData(items);
        }
    }
}
