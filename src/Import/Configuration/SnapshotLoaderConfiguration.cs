namespace KafkaSnapshot.Import.Configuration;

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
}
