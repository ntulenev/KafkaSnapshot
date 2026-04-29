namespace KafkaSnapshot.Models.Message;

/// <summary>
/// Message metadata.
/// </summary>
public sealed record KafkaMetadata
{
    /// <summary>
    /// Creates <see cref="KafkaMetadata"/>.
    /// </summary>
    /// <param name="timestamp">Message creation timestamp.</param>
    /// <param name="partition">Message partition number.</param>
    /// <param name="offset">Message partition offset.</param>
    public KafkaMetadata(DateTime timestamp, int partition, long offset)
    {
        ArgumentOutOfRangeException.ThrowIfNegative(partition);
        ArgumentOutOfRangeException.ThrowIfNegative(offset);

        Timestamp = timestamp;
        Partition = partition;
        Offset = offset;
    }

    /// <summary>
    /// Message creation timestamp.
    /// </summary>
    public DateTime Timestamp { get; init; }

    /// <summary>
    /// Message partition number.
    /// </summary>
    public int Partition { get; init; }

    /// <summary>
    /// Message partition offset.
    /// </summary>
    public long Offset { get; init; }
}
