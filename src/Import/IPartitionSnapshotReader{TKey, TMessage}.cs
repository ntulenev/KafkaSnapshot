using KafkaSnapshot.Abstractions.Filters;
using KafkaSnapshot.Import.Watermarks;
using KafkaSnapshot.Models.Import;
using KafkaSnapshot.Models.Message;

namespace KafkaSnapshot.Import;

/// <summary>
/// Reads Kafka messages from a single topic partition.
/// </summary>
/// <typeparam name="TKey">Kafka message key type.</typeparam>
/// <typeparam name="TMessage">Decoded Kafka message value type.</typeparam>
public interface IPartitionSnapshotReader<TKey, TMessage>
    where TKey : notnull
    where TMessage : notnull
{
    /// <summary>
    /// Reads messages from a single partition watermark.
    /// </summary>
    /// <param name="watermark">Partition watermark.</param>
    /// <param name="topicParams">Topic loading parameters.</param>
    /// <param name="keyFilter">Message key filter.</param>
    /// <param name="valueFilter">Message value filter.</param>
    /// <param name="ct">Cancellation token.</param>
    IEnumerable<KeyValuePair<TKey, KafkaMessage<TMessage>>> Read(
        PartitionWatermark watermark,
        LoadingTopic topicParams,
        IDataFilter<TKey> keyFilter,
        IDataFilter<TMessage> valueFilter,
        CancellationToken ct);
}
