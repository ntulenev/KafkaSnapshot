using KafkaSnapshot.Models.Filters;
using KafkaSnapshot.Models.Import;

namespace KafkaSnapshot.Processing.Configuration;

/// <summary>
/// Topic representation in configuration <see cref="LoaderToolConfiguration"/>.
/// </summary>
public sealed class TopicConfiguration
{
    /// <summary>
    /// Topic name in Apache Kafka.
    /// </summary>
    public required string Name { get; init; }

    /// <summary>
    /// Topic key type.
    /// </summary>
    public KeyType KeyType { get; init; }

    /// <summary>
    /// Compaction mode for duplicated keys.
    /// </summary>
    public CompactingMode Compacting { get; init; } = CompactingMode.On;

    /// <summary>
    /// Exported file name for topic data.
    /// </summary>
    public required string ExportFileName { get; init; }
    /// <summary>
    /// Topic filter key type.
    /// </summary>
    public FilterType FilterKeyType { get; init; } = FilterType.None;

    /// <summary>
    /// Date and time for starting offset.
    /// </summary>
    public DateTime? OffsetStartDate { get; init; }

    /// <summary>
    /// Date and time for final offset.
    /// </summary>
    public DateTime? OffsetEndDate { get; init; }

    /// <summary>
    /// Whether to export message values as raw strings.
    /// </summary>
    public bool ExportRawMessage { get; init; }

    /// <summary>
    /// Optional filter key value.
    /// </summary>
    public string? FilterKeyValue { get; init; }

    /// <summary>
    /// Partition id filter.
    /// </summary>
    public HashSet<int>? PartitionsIds { get; init; }

    /// <summary>
    /// Message value encoder rule.
    /// </summary>
    public EncoderRules MessageEncoderRule { get; init; } = EncoderRules.String;

}
