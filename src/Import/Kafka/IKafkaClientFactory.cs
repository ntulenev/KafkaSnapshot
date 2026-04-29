using Confluent.Kafka;

namespace KafkaSnapshot.Import.Kafka;

/// <summary>
/// Creates Apache Kafka clients from application configuration.
/// </summary>
public interface IKafkaClientFactory
{
    /// <summary>
    /// Creates an admin client.
    /// </summary>
    IAdminClient CreateAdminClient();

    /// <summary>
    /// Creates a consumer for the specified key type.
    /// </summary>
    IConsumer<TKey, byte[]> CreateConsumer<TKey>();
}
