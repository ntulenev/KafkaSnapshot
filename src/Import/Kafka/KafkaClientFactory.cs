using Confluent.Kafka;

using KafkaSnapshot.Import.Configuration;

using Microsoft.Extensions.Options;

namespace KafkaSnapshot.Import.Kafka;

/// <inheritdoc />
public sealed class KafkaClientFactory : IKafkaClientFactory
{
    /// <summary>
    /// Creates <see cref="KafkaClientFactory"/>.
    /// </summary>
    public KafkaClientFactory(IOptions<BootstrapServersConfiguration> options)
    {
        ArgumentNullException.ThrowIfNull(options);

        _config = options.Value ?? throw new ArgumentException("Config is not set", nameof(options));
    }

    /// <inheritdoc />
    public IAdminClient CreateAdminClient()
    {
        var adminConfig = new AdminClientConfig
        {
            BootstrapServers = BootstrapServers,
            SecurityProtocol = _config.SecurityProtocol,
            SaslMechanism = _config.SASLMechanism,
            SaslUsername = _config.Username,
            SaslPassword = _config.Password
        };

        return new AdminClientBuilder(adminConfig).Build();
    }

    /// <inheritdoc />
    public IConsumer<TKey, byte[]> CreateConsumer<TKey>()
    {
        var consumerConfig = new ConsumerConfig
        {
            BootstrapServers = BootstrapServers,
            AutoOffsetReset = AutoOffsetReset.Earliest,
            GroupId = Guid.NewGuid().ToString(),
            EnableAutoCommit = false,
            SecurityProtocol = _config.SecurityProtocol,
            SaslMechanism = _config.SASLMechanism,
            SaslUsername = _config.Username,
            SaslPassword = _config.Password
        };

        return new ConsumerBuilder<TKey, byte[]>(consumerConfig).Build();
    }

    private string BootstrapServers => string.Join(",", _config.BootstrapServers);

    private readonly BootstrapServersConfiguration _config;
}
