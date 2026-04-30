using KafkaSnapshot.Abstractions.Filters;
using KafkaSnapshot.Import.Watermarks;
using KafkaSnapshot.Models.Import;
using KafkaSnapshot.Models.Message;

namespace KafkaSnapshot.Import;

/// <summary>
/// Reads Kafka messages from multiple topic partitions.
/// </summary>
/// <typeparam name="TKey">Kafka message key type.</typeparam>
/// <typeparam name="TMessage">Decoded Kafka message value type.</typeparam>
public interface IPartitionSnapshotBatchReader<TKey, TMessage>
    where TKey : notnull
    where TMessage : notnull
{
    /// <summary>
    /// Reads partitions one by one until the first non-empty partition snapshot is found.
    /// </summary>
    /// <param name="watermarks">Partition watermarks.</param>
    /// <param name="topicParams">Topic loading parameters.</param>
    /// <param name="keyFilter">Message key filter.</param>
    /// <param name="valueFilter">Message value filter.</param>
    /// <param name="ct">Cancellation token.</param>
    Task<IReadOnlyCollection<KeyValuePair<TKey, KafkaMessage<TMessage>>>> ReadFirstNonEmptyAsync(
        IEnumerable<PartitionWatermark> watermarks,
        LoadingTopic topicParams,
        IDataFilter<TKey> keyFilter,
        IDataFilter<TMessage> valueFilter,
        CancellationToken ct);

    /// <summary>
    /// Reads all partition snapshots with configured concurrency.
    /// </summary>
    /// <param name="watermarks">Partition watermarks.</param>
    /// <param name="topicParams">Topic loading parameters.</param>
    /// <param name="keyFilter">Message key filter.</param>
    /// <param name="valueFilter">Message value filter.</param>
    /// <param name="ct">Cancellation token.</param>
    Task<IReadOnlyCollection<KeyValuePair<TKey, KafkaMessage<TMessage>>>> ReadAllAsync(
        IEnumerable<PartitionWatermark> watermarks,
        LoadingTopic topicParams,
        IDataFilter<TKey> keyFilter,
        IDataFilter<TMessage> valueFilter,
        CancellationToken ct);
}
