using KafkaSnapshot.Models.Message;

namespace KafkaSnapshot.Abstractions.Sorting;

/// <summary>
/// Sort contract for Kafka messages.
/// </summary>
/// <typeparam name="TKey">Kafka key type.</typeparam>
/// <typeparam name="TValue">Kafka message type.</typeparam>
public interface IMessageSorter<TKey, TValue> where TKey : notnull
                                              where TValue : notnull
{
    /// <summary>
    /// Sort Kafka messages.
    /// </summary>
    /// <param name="source">Kafka messages.</param>
    /// <returns>Sorted kafka messages.</returns>
    public IEnumerable<KeyValuePair<TKey, KafkaMessage<TValue>>> Sort(IEnumerable<KeyValuePair<TKey, KafkaMessage<TValue>>> source);
}
