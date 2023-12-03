using Confluent.Kafka;

namespace KafkaSnapshot.Import.Configuration;

/// <summary>
/// Bootstrap servers configuration.
/// </summary>
public class BootstrapServersConfiguration
{
    /// <summary>
    /// List of bootstrap servers.
    /// </summary>
    public required List<string> BootstrapServers { get; init; }

    /// <summary>
    /// Kafka user name, is any.
    /// </summary>
    public string? Username { get; set; }

    /// <summary>
    /// Kafka password name, is any.
    /// </summary>
    public string? Password { get; init; }

    /// <summary>
    /// Kafka security protocol.
    /// </summary>
    public SecurityProtocol SecurityProtocol { get; init; } = SecurityProtocol.Plaintext;

    /// <summary>
    /// Kafka security protocol mechanism.
    /// </summary>
    public SaslMechanism? SASLMechanism { get; init; }
}
