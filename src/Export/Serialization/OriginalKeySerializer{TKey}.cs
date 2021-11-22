using Newtonsoft.Json.Linq;

using KafkaSnapshot.Abstractions.Export;
using KafkaSnapshot.Models.Message;
using KafkaSnapshot.Export.Markers;

namespace KafkaSnapshot.Export.Serialization
{
    /// <summary>
    /// Serializer for data with keys of <typeparamref name="TKey"/> type.
    /// </summary>
    /// <typeparam name="TKey">Data key type.</typeparam>
    public class OriginalKeySerializer<TKey> : JsonSerializerBase, ISerializer<TKey, string, OriginalKeyMarker>
    {
        /// <inheritdoc/>
        public string Serialize(IEnumerable<KeyValuePair<TKey, DatedMessage<string>>> data, bool exportRawMessage)
        {
            if (data is null)
            {
                throw new ArgumentNullException(nameof(data));
            }

            var items = data.Select(x => new
            {
                x.Key,
                Value = exportRawMessage ? x.Value.Message : JToken.Parse(x.Value.Message),
                x.Value.Timestamp
            });

            return SerializeData(items);
        }
    }
}
