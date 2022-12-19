using KafkaSnapshot.Models.Filters;
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
    public string Name { get; set; } = default!;

    /// <summary>
    /// Topic Key Type.
    /// </summary>
    public KeyType KeyType { get; set; }

    /// <summary>
    /// Setting to On or Off compacting.
    /// </summary>
    public CompactingMode Compacting { get; set; } = CompactingMode.On;

    /// <summary>
    /// Exported file name for topic data.
    /// </summary>
    public string ExportFileName { get; set; } = default!;

    /// <summary>
    /// Topic filter key type.
    /// </summary>
    public FilterType FilterKeyType { get; set; } = FilterType.None;

    /// <summary>
    /// Date and time for starting offset.
    /// </summary>
    public DateTime? OffsetStartDate { get; set; }

    /// <summary>
    /// Date and time for final offset.
    /// </summary>
    public DateTime? OffsetEndDate { get; set; }

    /// <summary>
    /// Message format in export JSON.
    /// </summary>
    public bool ExportRawMessage { get; set; }

    /// <summary>
    /// Optional filter key value.
    /// </summary>
    public object? FilterKeyValue { get; set; }

    /// <summary>
    /// Partition ids filter.
    /// </summary>
    public HashSet<int>? PartitionsIds { get; set; }

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

        return new ProcessingTopic<TKey>(Name,
                                         ExportFileName,
                                         Compacting == CompactingMode.On,
                                         FilterKeyType,
                                         KeyType,
                                         typedFilterKeyValue!,
                                         dateRange,
                                         ExportRawMessage,
                                         PartitionsIds);
    }
}
