using KafkaSnapshot.Models.Export;
using KafkaSnapshot.Models.Message;

namespace KafkaSnapshot.Abstractions.Export;

/// <summary>
/// Abstraction for exporting Kafka topic data as a file.
/// </summary>
/// <typeparam name="TKey">The type of the message key.</typeparam>
/// <typeparam name="TKeyMarker">A marker interface for TKey.</typeparam>
/// <typeparam name="TMessage">The type of the message value.</typeparam>
/// <typeparam name="TTopic">The type of the Kafka topic object to be exported.</typeparam>
public interface IDataExporter<TKey, TKeyMarker, TMessage, TTopic>
    where TTopic : ExportedTopic
    where TKeyMarker : IKeyRepresentationMarker
    where TMessage : notnull
{
    /// <summary>
    /// Exports the data to a file.
    /// </summary>
    /// <param name="data">The data to be exported as an enumerable collection of key-value pairs.</param>
    /// <param name="topic">The Kafka topic description.</param>
    /// <param name="ct">The token for cancelling the operation.</param>
    /// <returns>A task representing the asynchronous export operation.</returns>
    public Task ExportAsync(
        IEnumerable<KeyValuePair<TKey, KafkaMessage<TMessage>>> data,
        TTopic topic,
        CancellationToken ct);
}
