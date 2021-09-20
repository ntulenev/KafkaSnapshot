using System;

using System.Collections.Generic;

using Newtonsoft.Json;

using KafkaSnapshot.Abstractions.Export;
using KafkaSnapshot.Models.Message;

namespace KafkaSnapshot.Export.Serialization
{
    /// <summary>
    /// Basic serializer.
    /// </summary>
    /// <typeparam name="TKey">Data key type.</typeparam>
    public class SimpleJsonSerializer<TKey> : ISerializer<TKey>
    {
        /// <inheritdoc/>
        public string Serialize(IEnumerable<KeyValuePair<TKey, DatedMessage<string>>> data, bool exportRawMessage)
        {
            if (data is null)
            {
                throw new ArgumentNullException(nameof(data));
            }

            _ = exportRawMessage; // not needed for this implementation.
            return JsonConvert.SerializeObject(data, Formatting.Indented);
        }
    }
}
