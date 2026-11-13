namespace KafkaSnapshot.Models.Import;

/// <summary>A captured snapshot interval for a topic partition.</summary>
public sealed class PartitionWatermark
{
    /// <summary>Creates a watermark with a non-negative partition identifier.</summary>
    public PartitionWatermark(LoadingTopic topic, OffsetRange offset, int partition)
    {
        ArgumentNullException.ThrowIfNull(topic);
        ArgumentNullException.ThrowIfNull(offset);
        ArgumentOutOfRangeException.ThrowIfNegative(partition);
        Topic = topic;
        Offset = offset;
        Partition = partition;
    }

    /// <summary>Topic loading parameters.</summary>
    public LoadingTopic Topic { get; }

    /// <summary>Partition identifier.</summary>
    public int Partition { get; }

    /// <summary>Captured offset interval.</summary>
    public OffsetRange Offset { get; }

    /// <summary>Checks whether this partition has a non-empty snapshot.</summary>
    public bool IsReadyToRead() => !Offset.IsEmpty;

    /// <summary>Checks whether an offset belongs to this snapshot.</summary>
    public bool Contains(long offset) => Offset.Contains(offset);

    /// <summary>Checks whether more offsets remain in this partition snapshot.</summary>
    public bool ShouldContinueReadingAfter(long offset) => Offset.ShouldContinueReadingAfter(offset);
}
