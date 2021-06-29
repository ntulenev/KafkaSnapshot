using KafkaSnapshot.Abstractions.Filters;

namespace KafkaSnapshot.Processing.Configuration
{
    /// <summary>
    /// Topic representatin in configuration <see cref="LoaderToolConfiguration"/>.
    /// </summary>
    public class LoadedTopic
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
        /// Optional filter value
        /// </summary>
        public string? FilterValue { get; set; }
    }
}
