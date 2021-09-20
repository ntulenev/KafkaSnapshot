using System;
using System.Collections.Generic;
using System.Linq;

using Newtonsoft.Json;
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
    public class OriginalKeySerializer<TKey> : ISerializer<TKey, string, OriginalKeyMarker>
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

            return JsonConvert.SerializeObject(items, Formatting.Indented);
        }
    }
}
