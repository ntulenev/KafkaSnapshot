using System;

using System.Collections.Generic;

using Newtonsoft.Json;

using KafkaSnapshot.Abstractions.Export;
using KafkaSnapshot.Models.Message;
using KafkaSnapshot.Export.Markers;

namespace KafkaSnapshot.Export.Serialization
{
    /// <summary>
    /// Basic serializer.
    /// </summary>
    /// <typeparam name="TKey">Data key type.</typeparam>
    public class SimpleJsonSerializer<TKey, TMessage> : ISerializer<TKey, TMessage, OriginalKeyMarker> where TMessage : notnull
    {
        /// <inheritdoc/>
        public string Serialize(IEnumerable<KeyValuePair<TKey, DatedMessage<TMessage>>> data, bool exportRawMessage)
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
