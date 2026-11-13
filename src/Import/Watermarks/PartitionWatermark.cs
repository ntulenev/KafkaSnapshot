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
    /// Checks whether the partition is valid for reading.
    /// </summary>
    public bool IsReadyToRead() => Offset.High > Offset.Low;

    /// <summary>Checks whether reading should continue after an offset.</summary>
    public bool ShouldContinueReadingAfter(long offset) => offset < Offset.High.Value - 1;

    /// <summary>Checks whether an offset belongs to this snapshot.</summary>
    public bool Contains(long offset) => offset >= Offset.Low.Value && offset < Offset.High.Value;
}
