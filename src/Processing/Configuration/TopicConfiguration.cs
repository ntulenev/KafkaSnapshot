using KafkaSnapshot.Models.Filters;
using KafkaSnapshot.Models.Processing;

namespace KafkaSnapshot.Processing.Configuration
{
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
        /// Setting to On or Off compacting
        /// </summary>
        public CompactingMode Compacting { get; set; } = CompactingMode.On;

        /// <summary>
        /// Exported file name for topic data.
        /// </summary>
        public string ExportFileName { get; set; } = default!;

        /// <summary>
        /// Topic filter type
        /// </summary>
        public FilterType FilterType { get; set; } = FilterType.None;

        /// <summary>
        /// Date and time for starting offset
        /// </summary>
        public DateTime? OffsetStartDate { get; set; }

        /// <summary>
        /// Date and time for final offset
        /// </summary>
        public DateTime? OffsetEndDate { get; set; }

        /// <summary>
        /// Message format in export JSON 
        /// </summary>
        public bool ExportRawMessage { get; set; }

        /// <summary>
        /// Optional filter value
        /// </summary>
        public object? FilterValue { get; set; }

        public ProcessingTopic<TKey> ConvertToProcess<TKey>()
        {
            var typedFilterValue = FilterValue is not null ?
                                  (TKey)Convert.ChangeType(FilterValue, typeof(TKey))
                                  :
                                  default;

            return new ProcessingTopic<TKey>(Name,
                                             ExportFileName,
                                             Compacting == CompactingMode.On,
                                             FilterType,
                                             KeyType,
                                             typedFilterValue!, OffsetStartDate, OffsetEndDate, ExportRawMessage);
        }
    }
}
