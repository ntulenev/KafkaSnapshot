using Confluent.Kafka;

using KafkaSnapshot.Models.Import;

namespace KafkaSnapshot.Import.Watermarks;

/// <summary>
/// Offset watermark for single partition in topic.
/// </summary>
public class PartitionWatermark
{
    /// <summary>
    /// Topic partition.
    /// </summary>
    public Partition Partition { get; }

    /// <summary>
    /// Partition watermark offsets.
    /// </summary>
    public WatermarkOffsets Offset { get; }

    /// <summary>
    /// Loading topic configuration.
    /// </summary>
    public LoadingTopic Topic { get; }

    /// <summary>
    /// Creates partition offset watermark.
    /// </summary>
    /// <param name="topicName">Name of the topic.</param>
    /// <param name="offset">Raw Kafka offset representation.</param>
    /// <param name="partition">Raw Kafka partition representation.</param>
    public PartitionWatermark(LoadingTopic topicName,
                              WatermarkOffsets offset,
                              Partition partition)
    {
        ArgumentNullException.ThrowIfNull(topicName);
        ArgumentNullException.ThrowIfNull(offset);

        Topic = topicName;

        Offset = offset;

        Partition = partition;
    }

    /// <summary>
    /// Checks that partition if valid for reading.
    /// </summary>
    public bool IsReadyToRead() => Offset.High > Offset.Low;

    /// <summary>
    /// Checks that end of the partition is achieved by consumer.
    /// </summary>
    /// <typeparam name="TKey">Message key.</typeparam>
    /// <typeparam name="TValue">Message value.</typeparam>
    /// <param name="consumeResult">Consumer result.</param>
    public bool IsWatermarkAchievedBy<TKey, TValue>(ConsumeResult<TKey, TValue> consumeResult)
    {
        ArgumentNullException.ThrowIfNull(consumeResult);

        return consumeResult.Offset != Offset.High - 1;
    }

    /// <summary>
    /// Assign consumer to a partition as topic.
    /// </summary>
    /// <typeparam name="TKey">Message key.</typeparam>
    /// <typeparam name="TValue">Message value.</typeparam>
    /// <param name="consumer">Consumer.</param>
    public void AssignWithConsumer<TKey, TValue>(IConsumer<TKey, TValue> consumer)
    {
        ArgumentNullException.ThrowIfNull(consumer);

        consumer.Assign(new TopicPartition(Topic.Value.Name, Partition));

    }

    /// <summary>
    ///  Assign consumer to a partition as topic with offset started from <paramref name="startDate"/>.
    /// </summary>
    /// <typeparam name="TKey">Message key.</typeparam>
    /// <typeparam name="TValue">Message value.</typeparam>
    /// <param name="consumer">Consumer.</param>
    /// <param name="startDate">Start date for offset.</param>
    /// <param name="timeout">Timeout for offset searching</param>
    public bool AssignWithConsumer<TKey, TValue>(IConsumer<TKey, TValue> consumer, DateTimeOffset startDate, TimeSpan timeout)
        => AssignWithConsumer(consumer, startDate, timeout, out _);

    /// <summary>
    ///  Assign consumer to a partition as topic with offset started from <paramref name="startDate"/>.
    /// </summary>
    /// <typeparam name="TKey">Message key.</typeparam>
    /// <typeparam name="TValue">Message value.</typeparam>
    /// <param name="consumer">Consumer.</param>
    /// <param name="startDate">Start date for offset.</param>
    /// <param name="timeout">Timeout for offset searching.</param>
    /// <param name="assignedOffset">Resolved Kafka offset assigned to the consumer.</param>
    public bool AssignWithConsumer<TKey, TValue>(
        IConsumer<TKey, TValue> consumer,
        DateTimeOffset startDate,
        TimeSpan timeout,
        out TopicPartitionOffset assignedOffset)
    {
        ArgumentNullException.ThrowIfNull(consumer);

        var topicPartition = new TopicPartition(Topic.Value.Name, Partition);

        var partitionTimestamp = new TopicPartitionTimestamp(topicPartition, new Timestamp(startDate.UtcDateTime));

        var offsets = consumer.OffsetsForTimes([partitionTimestamp], timeout);

        var singleOffset = offsets.Single();

        if (singleOffset.Offset.IsSpecial)
        {
            assignedOffset = singleOffset;
            return false;
        }

        consumer.Assign(singleOffset);

        assignedOffset = singleOffset;
        return true;
    }

}
