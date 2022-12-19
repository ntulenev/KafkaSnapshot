namespace KafkaSnapshot.Import.Configuration;

public class SnapshotLoaderConfiguration
{
    /// <summary>
    /// Timout for searching date offset.
    /// </summary>
    public TimeSpan DateOffsetTimeout { get; set; }

    /// <summary>
    /// If true stops search after finds any data in any partition.
    /// </summary>
    public bool SearchSinglePartition { get; set; }
}
