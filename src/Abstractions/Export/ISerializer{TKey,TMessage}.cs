using KafkaSnapshot.Models.Message;

namespace KafkaSnapshot.Abstractions.Export
{
    /// <summary>
    /// Export data serializer.
    /// </summary>
    public interface ISerializer<TKey, TMessage, TKeyMarker> where TMessage : notnull
                                                             where TKeyMarker : IKeyRepresentationMarker
    {
        /// <summary>
        /// Serializes data as string.
        /// </summary>
        /// <param name="data">Data for serialization.</param>
        /// <param name="exportRawMessage">Rule for message serialization.</param>
        /// <returns>String data representation.</returns>
        public string Serialize(IEnumerable<KeyValuePair<TKey, MetaMessage<TMessage>>> data, bool exportRawMessage);
    }
}
