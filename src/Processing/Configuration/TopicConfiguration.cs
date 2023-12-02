using KafkaSnapshot.Models.Filters;
using KafkaSnapshot.Models.Import;
using KafkaSnapshot.Models.Names;
using KafkaSnapshot.Models.Processing;

namespace KafkaSnapshot.Processing.Configuration;

/// <summary>
/// Topic representatin in configuration <see cref="LoaderToolConfiguration"/>.
/// </summary>
public class TopicConfiguration
{
    /// <summary>
    /// Topic name in Apache Kafka.
    /// </summary>
    public required string Name { get; init; }

    /// <summary>
    /// Topic Key Type.
    /// </summary>
    public KeyType KeyType { get; init; }

    /// <summary>
    /// Setting to On or Off compacting.
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
    /// Message format in export JSON.
    /// </summary>
    public bool ExportRawMessage { get; init; }

    /// <summary>
    /// Optional filter key value.
    /// </summary>
    public object? FilterKeyValue { get; init; }

    /// <summary>
    /// Partition ids filter.
    /// </summary>
    public HashSet<int>? PartitionsIds { get; init; }


    public EncoderRules MessageEncoderRule { get; init; } = EncoderRules.String;

    /// <summary>
    /// Converts configuration to <see cref="ProcessingTopic{TKey}"/>.
    /// </summary>
    /// <typeparam name="TKey">Message key.</typeparam>
    public ProcessingTopic<TKey> ConvertToProcess<TKey>()
    {
        var typedFilterKeyValue = FilterKeyValue is not null ?
                              (TKey)Convert.ChangeType(FilterKeyValue, typeof(TKey))
                              :
                              default;

        var dateRange = new DateFilterRange(OffsetStartDate, OffsetEndDate);

        return new ProcessingTopic<TKey>(new TopicName(Name),
                                         new FileName(ExportFileName),
                                         Compacting == CompactingMode.On,
                                         FilterKeyType,
                                         KeyType,
                                         typedFilterKeyValue!,
                                         dateRange,
                                         ExportRawMessage,
                                         MessageEncoderRule,
                                         PartitionsIds);
    }
}
