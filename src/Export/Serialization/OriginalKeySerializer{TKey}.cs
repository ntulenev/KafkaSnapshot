using Newtonsoft.Json.Linq;

using Microsoft.Extensions.Logging;

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
        /// <summary>
        /// Creates <see cref="OriginalKeySerializer{TKey}"/>.
        /// </summary>
        /// <param name="logger">Logger.</param>
        public OriginalKeySerializer(ILogger<OriginalKeySerializer<TKey>> logger) : base(logger) { }

        /// <inheritdoc/>
        public string Serialize(IEnumerable<KeyValuePair<TKey, DatedMessage<string>>> data, bool exportRawMessage)
        {
            ArgumentNullException.ThrowIfNull(data);

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
