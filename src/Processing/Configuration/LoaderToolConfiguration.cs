using System.Collections.Generic;

namespace KafkaSnapshot.Processing.Configuration
{
    /// <summary>
    /// Application configuration.
    /// </summary>
    public class LoaderToolConfiguration
    {
        /// <summary>
        /// List of  topics with string Key.
        /// </summary>
        public List<LoadedTopic> Topics { get; set; } = default!;

        /// <summary>
        /// User <see cref="LoaderConcurrentTool"/> to process topics in concurrent mode.
        /// </summary>
        public bool UseConcurrentLoad { get; set; }
    }
}
