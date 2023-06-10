using KafkaSnapshot.Models.Message;

namespace KafkaSnapshot.Abstractions.Sorting;

/// <summary>
/// Defines a contract for sorting Kafka messages by key.
/// </summary>
/// <typeparam name="TKey">The type of the message key.</typeparam>
/// <typeparam name="TValue">The type of the message value.</typeparam>
public interface IMessageSorter<TKey, TValue> where TKey : notnull
                                              where TValue : notnull
{
    /// <summary>
    /// Sorts the provided collection of Kafka messages by key in ascending order.
    /// </summary>
    /// <param name="source">The collection of Kafka messages to sort.</param>
    /// <returns>An enumerable of sorted Kafka messages.</returns>
    public IEnumerable<KeyValuePair<TKey, KafkaMessage<TValue>>> Sort(
                IEnumerable<KeyValuePair<TKey, KafkaMessage<TValue>>> source);
}
