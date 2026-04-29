using KafkaSnapshot.Models.Export;
using KafkaSnapshot.Models.Filters;
using KafkaSnapshot.Models.Import;
using KafkaSnapshot.Models.Names;

namespace KafkaSnapshot.Models.Processing;

/// <summary>
/// Topic that can be processed.
/// </summary>
/// <param name="TopicName">Topic name.</param>
/// <param name="ExportName">Export name.</param>
/// <param name="LoadWithCompacting">Whether duplicated keys should be compacted.</param>
/// <param name="FilterKeyType">Filtering key type.</param>
/// <param name="KeyType">Message key type.</param>
/// <param name="FilterKeyValue">Filter key value.</param>
/// <param name="DateRange">Date interval for offsets.</param>
/// <param name="ExportRawMessage">Whether to export message values as raw strings instead of JSON.</param>
/// <param name="ValueEncoderRule">Encoding rule for topic values.</param>
/// <param name="PartitionIdsFilter">Optional partition filter.</param>
public sealed record ProcessingTopic<TKey>(TopicName TopicName,
                                    FileName ExportName,
                                    bool LoadWithCompacting,
                                    FilterType FilterKeyType,
                                    KeyType KeyType,
                                    TKey FilterKeyValue,
                                    DateFilterRange DateRange,
                                    bool ExportRawMessage,
                                    EncoderRules ValueEncoderRule,
                                    IReadOnlySet<int>? PartitionIdsFilter = null
                                    )
{
    /// <summary>
    /// Creates <see cref="LoadingTopic"/>.
    /// </summary>
    public LoadingTopic CreateLoadingParams() =>
        new(TopicName, LoadWithCompacting, DateRange, ValueEncoderRule, PartitionIdsFilter);

    /// <summary>
    /// Creates <see cref="ExportedTopic"/>.
    /// </summary>
    public ExportedTopic CreateExportParams() => new(TopicName, ExportName, ExportRawMessage);
}
