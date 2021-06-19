using System.Collections.Generic;

namespace KafkaSnapshot.Processing.Configuration
{
    public class LoaderToolConfiguration
    {
        /// <summary>
        /// List of bootstrap servers
        /// </summary>
        public List<string> BootstrapServers { get; set; } = default!;

        /// <summary>
        /// List of  topics with string Key.
        /// </summary>
        public List<LoadedTopic> Topics { get; set; } = default!;

        /// <summary>
        /// Timout for metadata request.
        /// </summary>
        public int MetadataTimeout { get; set; }
    }
}
