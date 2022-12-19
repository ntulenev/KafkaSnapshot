namespace KafkaSnapshot.Import.Watermarks;

/// <summary>
/// Offset watermark for a topic.
/// </summary>
public class TopicWatermark
{
    /// <summary>
    /// Creates watermark for a topic.
    /// </summary>
    /// <param name="partitionWatermarks">Raw topic partition watermarks.</param>
    public TopicWatermark(IEnumerable<PartitionWatermark> partitionWatermarks)
    {
        ArgumentNullException.ThrowIfNull(partitionWatermarks);

        Watermarks = partitionWatermarks;
    }

    /// <summary>
    /// Topic partition watermarks.
    /// </summary>
    public IEnumerable<PartitionWatermark> Watermarks { get; }
}
