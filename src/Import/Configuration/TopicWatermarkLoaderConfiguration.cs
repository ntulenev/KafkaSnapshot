using System;

namespace KafkaSnapshot.Import.Configuration
{
    // TODO: Add validation

    public class TopicWatermarkLoaderConfiguration
    {
        /// <summary>
        /// Timout for metadata request.
        /// </summary>
        public TimeSpan AdminClientTimeout { get; set; }
    }
}
