using KafkaSnapshot.Models.Filters;

namespace KafkaSnapshot.Models.Processing
{
    /// <summary>
    /// Topic that could be processed.
    /// </summary>
    /// <param name="Name">Topic name.</param>
    /// <param name="ExportName">Export name.</param>
    /// <param name="LoadWithCompacting">Compacting rule for duplicated keys.</param>
    /// <param name="FilterKeyType">Filtering key type.</param>
    /// <param name="KeyType">Message key type.</param>
    /// <param name="FilterKeyValue">Filter key value.</param>
    /// <param name="StartingDate">Optional date for initial osset.</param>
    /// <param name="ExportRawMessage">Use raw string for message or json.</param>
    /// <param name="IgnoreNextParitionsAfterDataFound">Skips next partitions if data was found.</param>
    public record ProcessingTopic<TKey>(string Name,
                                        string ExportName,
                                        bool LoadWithCompacting,
                                        FilterType FilterKeyType,
                                        KeyType KeyType,
                                        TKey FilterKeyValue,
                                        DateTime? StartingDate,
                                        DateTime? EndingDate,
                                        bool ExportRawMessage,
                                        HashSet<int>? PartitionIdsFilter = null,
                                        bool IgnoreNextParitionsAfterDataFound = false);
}
