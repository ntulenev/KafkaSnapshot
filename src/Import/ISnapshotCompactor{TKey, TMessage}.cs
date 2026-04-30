using KafkaSnapshot.Models.Message;

namespace KafkaSnapshot.Import;

/// <summary>
/// Creates final snapshots from consumed Kafka messages.
/// </summary>
/// <typeparam name="TKey">Kafka message key type.</typeparam>
/// <typeparam name="TMessage">Decoded Kafka message value type.</typeparam>
public interface ISnapshotCompactor<TKey, TMessage>
    where TKey : notnull
    where TMessage : notnull
{
    /// <summary>
    /// Creates a snapshot from consumed messages.
    /// </summary>
    /// <param name="items">Consumed messages.</param>
    /// <param name="withCompacting">Whether duplicated keys should be compacted.</param>
    IEnumerable<KeyValuePair<TKey, KafkaMessage<TMessage>>> CreateSnapshot(
        IEnumerable<KeyValuePair<TKey, KafkaMessage<TMessage>>> items,
        bool withCompacting);
}
