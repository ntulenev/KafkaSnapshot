using System;
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
    }
}
