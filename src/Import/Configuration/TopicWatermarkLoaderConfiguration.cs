using System;

namespace KafkaSnapshot.Import.Configuration
{
    public class TopicWatermarkLoaderConfiguration
    {
        /// <summary>
        /// Timout for metadata request.
        /// </summary>
        public TimeSpan AdminClientTimeout { get; set; }
    }
}
