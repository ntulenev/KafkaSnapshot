namespace KafkaSnapshot.Models.Sorting;

/// <summary>
/// Kafka Meta sorting field.
/// </summary>
public enum SortingType
{
    /// <summary>
    /// Sort by message timestamp.
    /// </summary>
    Time,

    /// <summary>
    /// Sort by partition id.
    /// </summary>
    Partition
}
