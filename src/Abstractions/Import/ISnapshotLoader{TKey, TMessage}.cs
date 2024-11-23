using KafkaSnapshot.Abstractions.Filters;
using KafkaSnapshot.Models.Import;
using KafkaSnapshot.Models.Message;

namespace KafkaSnapshot.Abstractions.Import;

/// <summary>
/// Interface for loading a snapshot of Kafka messages.
/// </summary>
/// <typeparam name="TKey">The type of the message key.</typeparam>
/// <typeparam name="TMessage">The type of the message value.</typeparam>
public interface ISnapshotLoader<TKey, TMessage> where TKey : notnull
                                                 where TMessage : notnull
{
    /// <summary>
    /// Loads a snapshot of Kafka messages for the specified topic.
    /// </summary>
    /// <param name="loadingTopic">The loading topic.</param>
    /// <param name="keyFilter">The key filter used to filter the snapshot by key.</param>
    /// <param name="valueFilter">The value filter used to filter the snapshot by value.</param>
    /// <param name="ct">The cancellation token.</param>
    /// <returns>An enumerable collection of key-value pairs 
    /// representing the loaded Kafka messages.</returns>
    public Task<IEnumerable<KeyValuePair<TKey, KafkaMessage<TMessage>>>> LoadSnapshotAsync(
        LoadingTopic loadingTopic,
        IDataFilter<TKey> keyFilter,
        IDataFilter<TMessage> valueFilter,
        CancellationToken ct);
}
