using System.Collections.Generic;

namespace ConsoleLoaderUtility.Tool.Configuration
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
        public List<string> StringKeyTopics { get; set; } = default!;

        /// <summary>
        /// List of  topics with long Key.
        /// </summary>
        public List<string> LongKeyTopics { get; set; } = default!;

        /// <summary>
        /// Timout for metadata request.
        /// </summary>
        public int MetadataTimeout { get; set; }
    }
}
