namespace KafkaSnapshot.Models.Message
{
    /// <summary>
    /// Message metadata.
    /// </summary>
    /// <param name="Timestamp">Message creation timestamp.</param>
    /// <param name="Partition">Message partition number.</param>
    public record MessageMeta(DateTime Timestamp, int Partition);
}
