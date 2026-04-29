using Confluent.Kafka;

using KafkaSnapshot.Import.Configuration;

using Microsoft.Extensions.Options;

namespace KafkaSnapshot.Import.Kafka;

/// <inheritdoc />
public class KafkaClientFactory : IKafkaClientFactory
{
    /// <summary>
    /// Creates <see cref="KafkaClientFactory"/>.
    /// </summary>
    public KafkaClientFactory(IOptions<BootstrapServersConfiguration> options)
    {
        ArgumentNullException.ThrowIfNull(options);

        ArgumentNullException.ThrowIfNull(options.Value, nameof(options));

        _config = options.Value;
    }

    /// <inheritdoc />
    public IAdminClient CreateAdminClient() => BuildAdminClient(CreateAdminConfig());

    /// <inheritdoc />
    public IConsumer<TKey, byte[]> CreateConsumer<TKey>() => BuildConsumer<TKey>(CreateConsumerConfig<TKey>());

    /// <summary>
    /// Creates Apache Kafka admin client configuration.
    /// </summary>
    public AdminClientConfig CreateAdminConfig()
        =>
        new()
        {
            BootstrapServers = BootstrapServers,
            SecurityProtocol = _config.SecurityProtocol,
            SaslMechanism = _config.SASLMechanism,
            SaslUsername = _config.Username,
            SaslPassword = _config.Password
        };

    /// <summary>
    /// Creates Apache Kafka consumer configuration.
    /// </summary>
    public ConsumerConfig CreateConsumerConfig<TKey>()
        =>
        new()
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

    /// <summary>
    /// Builds an Apache Kafka admin client.
    /// </summary>
    /// <param name="config">Admin client configuration.</param>
    protected virtual IAdminClient BuildAdminClient(AdminClientConfig config)
    {
        ArgumentNullException.ThrowIfNull(config);

        return new AdminClientBuilder(config).Build();
    }

    /// <summary>
    /// Builds an Apache Kafka consumer.
    /// </summary>
    /// <typeparam name="TKey">Kafka message key type.</typeparam>
    /// <param name="config">Consumer configuration.</param>
    protected virtual IConsumer<TKey, byte[]> BuildConsumer<TKey>(ConsumerConfig config)
    {
        ArgumentNullException.ThrowIfNull(config);

        return new ConsumerBuilder<TKey, byte[]>(config).Build();
    }

    private string BootstrapServers => string.Join(",", _config.BootstrapServers);

    private readonly BootstrapServersConfiguration _config;
}
