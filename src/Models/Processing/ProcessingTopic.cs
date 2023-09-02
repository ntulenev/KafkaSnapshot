using KafkaSnapshot.Models.Export;
using KafkaSnapshot.Models.Filters;
using KafkaSnapshot.Models.Import;
using KafkaSnapshot.Models.Names;

namespace KafkaSnapshot.Models.Processing;

/// <summary>
/// Topic that could be processed.
/// </summary>
/// <param name="Name">Topic name.</param>
/// <param name="ExportName">Export name.</param>
/// <param name="LoadWithCompacting">Compacting rule for duplicated keys.</param>
/// <param name="FilterKeyType">Filtering key type.</param>
/// <param name="KeyType">Message key type.</param>
/// <param name="FilterKeyValue">Filter key value.</param>
/// <param name="DateRange">Date interval for offsets.</param>
/// <param name="ExportRawMessage">Use raw string for message or json.</param>
public record ProcessingTopic<TKey>(TopicName TopicName,
                                    FileName ExportName,
                                    bool LoadWithCompacting,
                                    FilterType FilterKeyType,
                                    KeyType KeyType,
                                    TKey FilterKeyValue,
                                    DateFilterRange DateRange,
                                    bool ExportRawMessage,
                                    EncoderRules ValueEncoderRule,
                                    HashSet<int>? PartitionIdsFilter = null
                                    )
{
    /// <summary>
    /// Creates <see cref="LoadingTopic"/>.
    /// </summary>
    public LoadingTopic CreateLoadingParams()
    {
        return new LoadingTopic(TopicName, LoadWithCompacting, DateRange, ValueEncoderRule,PartitionIdsFilter);
    }

    /// <summary>
    /// Creates <see cref="ExportedTopic"/>.
    /// </summary>
    public ExportedTopic CreateExportParams()
    {
        return new ExportedTopic(TopicName, ExportName, ExportRawMessage);
    }
}
