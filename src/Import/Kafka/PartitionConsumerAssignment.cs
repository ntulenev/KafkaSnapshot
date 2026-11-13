using Confluent.Kafka;

using KafkaSnapshot.Models.Import;

namespace KafkaSnapshot.Import.Kafka;

/// <summary>Assigns Kafka consumers to snapshot start offsets.</summary>
public sealed class PartitionConsumerAssignment : IPartitionConsumerAssignment
{
    /// <summary>
    /// Assigns the consumer to the partition topic.
    /// </summary>
    /// <typeparam name="TKey">Message key.</typeparam>
    /// <typeparam name="TValue">Message value.</typeparam>
    /// <param name="watermark">Partition watermark.</param>
    /// <param name="consumer">Consumer.</param>
    public void Assign<TKey, TValue>(PartitionWatermark watermark, IConsumer<TKey, TValue> consumer)
    {
        ArgumentNullException.ThrowIfNull(watermark);
        ArgumentNullException.ThrowIfNull(consumer);

        consumer.Assign(new TopicPartitionOffset(
            watermark.Topic.Value.Name, watermark.Partition, new Offset(watermark.Offset.Low)));

    }

    /// <summary>
    /// Assigns the consumer to the partition topic with an offset resolved from <paramref name="startDate"/>.
    /// </summary>
    /// <typeparam name="TKey">Message key.</typeparam>
    /// <typeparam name="TValue">Message value.</typeparam>
    /// <param name="watermark">Partition watermark.</param>
    /// <param name="consumer">Consumer.</param>
    /// <param name="startDate">Start date for offset.</param>
    /// <param name="timeout">Timeout for offset lookup.</param>
    public bool Assign<TKey, TValue>(PartitionWatermark watermark, IConsumer<TKey, TValue> consumer, DateTimeOffset startDate, TimeSpan timeout)
        => Assign(watermark, consumer, startDate, timeout, out _);

    /// <summary>
    /// Assigns the consumer to the partition topic with an offset resolved from <paramref name="startDate"/>.
    /// </summary>
    /// <typeparam name="TKey">Message key.</typeparam>
    /// <typeparam name="TValue">Message value.</typeparam>
    /// <param name="watermark">Partition watermark.</param>
    /// <param name="consumer">Consumer.</param>
    /// <param name="startDate">Start date for offset.</param>
    /// <param name="timeout">Timeout for offset lookup.</param>
    /// <param name="assignedOffset">Resolved Kafka offset assigned to the consumer.</param>
    public bool Assign<TKey, TValue>(
        PartitionWatermark watermark,
        IConsumer<TKey, TValue> consumer,
        DateTimeOffset startDate,
        TimeSpan timeout,
        out TopicPartitionOffset assignedOffset)
    {
        ArgumentNullException.ThrowIfNull(watermark);
        ArgumentNullException.ThrowIfNull(consumer);

        var topicPartition = new TopicPartition(watermark.Topic.Value.Name, watermark.Partition);

        var partitionTimestamp = new TopicPartitionTimestamp(topicPartition, new Timestamp(startDate.UtcDateTime));

        var offsets = consumer.OffsetsForTimes([partitionTimestamp], timeout);

        var singleOffset = offsets.Single();

        if (singleOffset.Offset.IsSpecial || singleOffset.Offset.Value >= watermark.Offset.High)
        {
            assignedOffset = singleOffset;
            return false;
        }

        consumer.Assign(singleOffset);

        assignedOffset = singleOffset;
        return true;
    }

}
