using Confluent.Kafka;

using KafkaSnapshot.Import.Watermarks;

namespace KafkaSnapshot.Import.Kafka;

/// <summary>Assigns a consumer to a snapshot start position.</summary>
public interface IPartitionConsumerAssignment
{
    /// <summary>Assigns a partition without a timestamp lookup.</summary>
    void Assign<TKey, TValue>(PartitionWatermark watermark, IConsumer<TKey, TValue> consumer);

    /// <summary>Resolves and assigns the start offset for a timestamp.</summary>
    bool Assign<TKey, TValue>(PartitionWatermark watermark, IConsumer<TKey, TValue> consumer,
        DateTimeOffset startDate, TimeSpan timeout, out TopicPartitionOffset assignedOffset);
}
