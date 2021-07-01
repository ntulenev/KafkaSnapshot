using System.Collections.Generic;

namespace KafkaSnapshot.Import.Configuration
{
    public class BootstrapServersConfiguration
    {
        /// <summary>
        /// List of bootstrap servers.
        /// </summary>
        public List<string> BootstrapServers { get; set; } = default!;
    }
}
