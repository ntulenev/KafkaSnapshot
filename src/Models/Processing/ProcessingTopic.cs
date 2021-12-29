using KafkaSnapshot.Models.Filters;

namespace KafkaSnapshot.Models.Processing
{
    /// <summary>
    /// Topic that could be processed.
    /// </summary>
    /// <param name="Name">Topic name.</param>
    /// <param name="ExportName">Export name.</param>
    /// <param name="LoadWithCompacting">Compacting rule for duplicated keys.</param>
    /// <param name="FilterType">Filtering type.</param>
    /// <param name="KeyType">Message key type.</param>
    /// <param name="FilterValue">Filter value.</param>
    /// <param name="StartingDate">Optional date for initial osset.</param>
    /// <param name="ExportRawMessage">Use raw string for message or json.</param>
    public record ProcessingTopic<TKey>(string Name,
                                        string ExportName,
                                        bool LoadWithCompacting,
                                        FilterType FilterType,
                                        KeyType KeyType,
                                        TKey FilterValue,
                                        DateTime? StartingDate,
                                        DateTime? EndingDate,
                                        bool ExportRawMessage);
}
