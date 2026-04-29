using KafkaSnapshot.Models.Export;
using KafkaSnapshot.Models.Filters;
using KafkaSnapshot.Models.Import;
using KafkaSnapshot.Models.Names;

namespace KafkaSnapshot.Models.Processing;

/// <summary>
/// Topic that can be processed.
/// </summary>
public sealed record ProcessingTopic<TKey>
{
    /// <summary>
    /// Creates <see cref="ProcessingTopic{TKey}"/>.
    /// </summary>
    /// <param name="topicName">Topic name.</param>
    /// <param name="exportName">Export name.</param>
    /// <param name="loadWithCompacting">Whether duplicated keys should be compacted.</param>
    /// <param name="filterKeyType">Filtering key type.</param>
    /// <param name="keyType">Message key type.</param>
    /// <param name="filterKeyValue">Filter key value.</param>
    /// <param name="dateRange">Date interval for offsets.</param>
    /// <param name="exportRawMessage">Whether to export message values as raw strings instead of JSON.</param>
    /// <param name="valueEncoderRule">Encoding rule for topic values.</param>
    /// <param name="partitionIdsFilter">Optional partition filter.</param>
    public ProcessingTopic(
        TopicName topicName,
        FileName exportName,
        bool loadWithCompacting,
        FilterType filterKeyType,
        KeyType keyType,
        TKey filterKeyValue,
        DateFilterRange dateRange,
        bool exportRawMessage,
        EncoderRules valueEncoderRule,
        IReadOnlySet<int>? partitionIdsFilter = null)
    {
        ArgumentNullException.ThrowIfNull(topicName);
        ArgumentNullException.ThrowIfNull(exportName);
        ArgumentNullException.ThrowIfNull(dateRange);

        if (!Enum.IsDefined(filterKeyType))
        {
            throw new ArgumentOutOfRangeException(
                nameof(filterKeyType),
                filterKeyType,
                $"Invalid FilterType value {filterKeyType}.");
        }

        if (!Enum.IsDefined(keyType))
        {
            throw new ArgumentOutOfRangeException(
                nameof(keyType),
                keyType,
                $"Invalid KeyType value {keyType}.");
        }

        if (!Enum.IsDefined(valueEncoderRule))
        {
            throw new ArgumentOutOfRangeException(
                nameof(valueEncoderRule),
                valueEncoderRule,
                $"Invalid EncoderRules value {valueEncoderRule}.");
        }

        if (partitionIdsFilter is not null)
        {
            ArgumentOutOfRangeException.ThrowIfZero(partitionIdsFilter.Count, nameof(partitionIdsFilter));
        }

        TopicName = topicName;
        ExportName = exportName;
        LoadWithCompacting = loadWithCompacting;
        FilterKeyType = filterKeyType;
        KeyType = keyType;
        FilterKeyValue = filterKeyValue;
        DateRange = dateRange;
        ExportRawMessage = exportRawMessage;
        ValueEncoderRule = valueEncoderRule;
        PartitionIdsFilter = partitionIdsFilter;
    }

    /// <summary>
    /// Topic name.
    /// </summary>
    public TopicName TopicName { get; init; }

    /// <summary>
    /// Export name.
    /// </summary>
    public FileName ExportName { get; init; }

    /// <summary>
    /// Whether duplicated keys should be compacted.
    /// </summary>
    public bool LoadWithCompacting { get; init; }

    /// <summary>
    /// Filtering key type.
    /// </summary>
    public FilterType FilterKeyType { get; init; }

    /// <summary>
    /// Message key type.
    /// </summary>
    public KeyType KeyType { get; init; }

    /// <summary>
    /// Filter key value.
    /// </summary>
    public TKey FilterKeyValue { get; init; }

    /// <summary>
    /// Date interval for offsets.
    /// </summary>
    public DateFilterRange DateRange { get; init; }

    /// <summary>
    /// Whether to export message values as raw strings instead of JSON.
    /// </summary>
    public bool ExportRawMessage { get; init; }

    /// <summary>
    /// Encoding rule for topic values.
    /// </summary>
    public EncoderRules ValueEncoderRule { get; init; }

    /// <summary>
    /// Optional partition filter.
    /// </summary>
    public IReadOnlySet<int>? PartitionIdsFilter { get; init; }

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
