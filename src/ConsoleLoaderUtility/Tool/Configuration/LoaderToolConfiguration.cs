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
        /// List of reserved topics (Hided and unable to be deleted).
        /// </summary>
        public List<string> Topics { get; set; } = default!;

        /// <summary>
        /// Timout for metadata request.
        /// </summary>
        public int MetadataTimeout { get; set; }
    }
}
