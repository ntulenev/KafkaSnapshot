namespace KafkaSnapshot.Import.Configuration;

/// <summary>
/// Snapshot loader settings.
/// </summary>
public class SnapshotLoaderConfiguration
{
    /// <summary>
    /// Timeout for searching date offset.
    /// </summary>
    public TimeSpan DateOffsetTimeout { get; init; }

    /// <summary>
    /// If true stops search after finds any data in any partition.
    /// </summary>
    public bool SearchSinglePartition { get; init; }

    /// <summary>
    /// Maximum number of partitions to consume concurrently. Null or less than 1 means no limit.
    /// </summary>
    public int? MaxConcurrentPartitions { get; init; }
}
