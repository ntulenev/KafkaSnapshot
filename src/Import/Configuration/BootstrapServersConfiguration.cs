namespace KafkaSnapshot.Import.Configuration
{
    /// <summary>
    /// Bootstrap servers configuration.
    /// </summary>
    public class BootstrapServersConfiguration
    {
        /// <summary>
        /// List of bootstrap servers.
        /// </summary>
        public List<string> BootstrapServers { get; set; } = default!;
    }
}
