using Confluent.Kafka;

using KafkaSnapshot.Models.Import;

namespace KafkaSnapshot.Import.Kafka;

/// <summary>Converts Kafka watermark data to a validated domain snapshot.</summary>
public static class KafkaWatermarkMapper
{
    /// <summary>Copies broker offsets and partition identifiers into domain values.</summary>
    public static PartitionWatermark ToDomain(LoadingTopic topic, WatermarkOffsets offsets,
        Partition partition)
    {
        ArgumentNullException.ThrowIfNull(offsets);
        return new PartitionWatermark(topic,
            new OffsetRange(offsets.Low.Value, offsets.High.Value), partition.Value);
    }
}
