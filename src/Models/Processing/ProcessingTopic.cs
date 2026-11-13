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

        KeyFilter = new KeyFilterDefinition<TKey>(filterKeyType, keyType, filterKeyValue);
        if (!ProcessingTopicRules.SupportsCompacting(keyType, loadWithCompacting))
        {
            throw new ArgumentException("Compacting is not supported for ignored keys.", nameof(loadWithCompacting));
        }

        _loadingTopic = new LoadingTopic(topicName, loadWithCompacting, dateRange,
            valueEncoderRule, partitionIdsFilter);
        _exportedTopic = new ExportedTopic(topicName, exportName, exportRawMessage);

    }

    /// <summary>
    /// Topic name.
    /// </summary>
    public TopicName TopicName => _exportedTopic.TopicName;

    /// <summary>
    /// Export name.
    /// </summary>
    public FileName ExportName => _exportedTopic.ExportName;

    /// <summary>
    /// Whether duplicated keys should be compacted.
    /// </summary>
    public bool LoadWithCompacting => _loadingTopic.LoadWithCompacting;

    /// <summary>
    /// Filtering key type.
    /// </summary>
    public FilterType FilterKeyType => KeyFilter.Type;

    /// <summary>
    /// Message key type.
    /// </summary>
    public KeyType KeyType => KeyFilter.KeyType;

    /// <summary>
    /// Filter key value.
    /// </summary>
    public TKey FilterKeyValue => KeyFilter.Value;

    /// <summary>
    /// Date interval for offsets.
    /// </summary>
    public DateFilterRange DateRange => _loadingTopic.DateRange;

    /// <summary>
    /// Whether to export message values as raw strings instead of JSON.
    /// </summary>
    public bool ExportRawMessage => _exportedTopic.ExportRawMessage;

    /// <summary>
    /// Encoding rule for topic values.
    /// </summary>
    public EncoderRules ValueEncoderRule => _loadingTopic.TopicValueEncoderRule;

    /// <summary>
    /// Optional partition filter.
    /// </summary>
    public IReadOnlySet<int>? PartitionIdsFilter => _loadingTopic.HasPartitionFilter
        ? _loadingTopic.PartitionFilter
        : null;

    /// <summary>Validated key filter.</summary>
    public KeyFilterDefinition<TKey> KeyFilter { get; }

    /// <summary>
    /// Creates <see cref="LoadingTopic"/>.
    /// </summary>
    public LoadingTopic CreateLoadingParams() => _loadingTopic;

    /// <summary>
    /// Creates <see cref="ExportedTopic"/>.
    /// </summary>
    public ExportedTopic CreateExportParams() => _exportedTopic;

    private readonly LoadingTopic _loadingTopic;
    private readonly ExportedTopic _exportedTopic;
}
