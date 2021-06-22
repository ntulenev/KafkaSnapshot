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
        /// Exported file name for topic data.
        /// </summary>
        public string ExportFileName { get; set; } = default!;
    }
}
