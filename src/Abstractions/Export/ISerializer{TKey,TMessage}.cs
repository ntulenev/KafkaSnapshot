using KafkaSnapshot.Models.Message;

namespace KafkaSnapshot.Abstractions.Export;

/// <summary>
/// Interface for serializing data.
/// </summary>
/// <typeparam name="TKey">Type of the message key.</typeparam>
/// <typeparam name="TMessage">Type of the message value.</typeparam>
/// <typeparam name="TKeyMarker">Type for interpreting the message key.</typeparam>
public interface ISerializer<TKey, TMessage, TKeyMarker> 
        where TMessage : notnull
        where TKeyMarker : IKeyRepresentationMarker
{
    /// <summary>
    /// Serializes data to a string.
    /// </summary>
    /// <param name="data">Data to be serialized.</param>
    /// <param name="exportRawMessage">Specifies the message serialization rule.</param>
    /// <returns>The serialized data as a string.</returns>
    public string Serialize(
        IEnumerable<KeyValuePair<TKey, KafkaMessage<TMessage>>> data, 
        bool exportRawMessage);

    /// <summary>
    /// Serializes data to a stream.
    /// </summary>
    /// <param name="data">Data to be serialized.</param>
    /// <param name="exportRawMessage">Specifies the message serialization rule.</param>
    /// <param name="stream">The stream to which the data is written.</param>
    public void Serialize(
        IEnumerable<KeyValuePair<TKey, KafkaMessage<TMessage>>> data, 
        bool exportRawMessage, 
        Stream stream);
}
