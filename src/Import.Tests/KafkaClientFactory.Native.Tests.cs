using Confluent.Kafka;

using FluentAssertions;

using KafkaSnapshot.Import.Configuration;
using KafkaSnapshot.Import.Kafka;

using Microsoft.Extensions.Options;

using Xunit;

namespace KafkaSnapshot.Import.Tests;

[Trait("Category", "Integration")]
public class KafkaClientFactoryNativeTests
{
    [Fact(DisplayName = "KafkaClientFactory builds and disposes native admin and consumer clients without a broker.")]
    public void BuildsNativeClientsWithoutBroker()
    {
        var factory = new ExposedFactory();

        using var admin = factory.CreateAdminClient();
        using var consumer = factory.CreateConsumer<string>();

        admin.Name.Should().NotBeNullOrWhiteSpace();
        consumer.Name.Should().NotBeNullOrWhiteSpace();
        consumer.Subscription.Should().BeEmpty();
        consumer.Close();
    }

    [Fact(DisplayName = "KafkaClientFactory rejects null native client configurations.")]
    public void NativeBuildersRejectNullConfiguration()
    {
        var factory = new ExposedFactory();

        Action admin = () => factory.BuildAdmin(null!);
        Action consumer = () => factory.BuildStringConsumer(null!);

        admin.Should().Throw<ArgumentNullException>().WithParameterName("config");
        consumer.Should().Throw<ArgumentNullException>().WithParameterName("config");
    }

    private sealed class ExposedFactory() : KafkaClientFactory(Options.Create(new BootstrapServersConfiguration
    {
        BootstrapServers = []
    }))
    {
        public IAdminClient BuildAdmin(AdminClientConfig config) => BuildAdminClient(config);
        public IConsumer<string, byte[]> BuildStringConsumer(ConsumerConfig config) => BuildConsumer<string>(config);
    }
}
