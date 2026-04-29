using Confluent.Kafka;

using FluentAssertions;

using KafkaSnapshot.Import.Configuration;
using KafkaSnapshot.Import.Kafka;

using Microsoft.Extensions.Options;

using Moq;

using Xunit;

namespace KafkaSnapshot.Import.Tests;

public class KafkaClientFactoryTests
{
    [Fact(DisplayName = "KafkaClientFactory can't be created with null options.")]
    [Trait("Category", "Unit")]
    public void KafkaClientFactoryCantBeCreatedWithNullOptions()
    {
        // Act
        var exception = Record.Exception(() => new KafkaClientFactory(null!));

        // Assert
        exception.Should().NotBeNull().And.BeOfType<ArgumentNullException>();
    }

    [Fact(DisplayName = "KafkaClientFactory can't be created with null options value.")]
    [Trait("Category", "Unit")]
    public void KafkaClientFactoryCantBeCreatedWithNullOptionsValue()
    {
        // Arrange
        var optionsMock = new Mock<IOptions<BootstrapServersConfiguration>>(MockBehavior.Strict);
        optionsMock.Setup(x => x.Value).Returns((BootstrapServersConfiguration)null!);

        // Act
        var exception = Record.Exception(() => new KafkaClientFactory(optionsMock.Object));

        // Assert
        exception.Should().NotBeNull().And.BeOfType<ArgumentNullException>();
    }

    [Fact(DisplayName = "KafkaClientFactory can be created with valid options.")]
    [Trait("Category", "Unit")]
    public void KafkaClientFactoryCanBeCreatedWithValidOptions()
    {
        // Arrange
        var optionsMock = new Mock<IOptions<BootstrapServersConfiguration>>(MockBehavior.Strict);
        optionsMock.Setup(x => x.Value).Returns(new BootstrapServersConfiguration
        {
            BootstrapServers = ["localhost:9092"]
        });

        // Act
        var exception = Record.Exception(() => new KafkaClientFactory(optionsMock.Object));

        // Assert
        exception.Should().BeNull();
    }

    [Fact(DisplayName = "KafkaClientFactory can create admin client config.")]
    [Trait("Category", "Unit")]
    public void KafkaClientFactoryCanCreateAdminClientConfig()
    {
        // Arrange
        var factory = CreateFactory(new BootstrapServersConfiguration
        {
            BootstrapServers = ["host1:9092", "host2:9092"],
            SecurityProtocol = SecurityProtocol.SaslSsl,
            SASLMechanism = SaslMechanism.Plain,
            Username = "user",
            Password = "password"
        });

        // Act
        var config = factory.CreateAdminConfig();

        // Assert
        config.BootstrapServers.Should().Be("host1:9092,host2:9092");
        config.SecurityProtocol.Should().Be(SecurityProtocol.SaslSsl);
        config.SaslMechanism.Should().Be(SaslMechanism.Plain);
        config.SaslUsername.Should().Be("user");
        config.SaslPassword.Should().Be("password");
    }

    [Fact(DisplayName = "KafkaClientFactory can create consumer config.")]
    [Trait("Category", "Unit")]
    public void KafkaClientFactoryCanCreateConsumerConfig()
    {
        // Arrange
        var factory = CreateFactory(new BootstrapServersConfiguration
        {
            BootstrapServers = ["host1:9092", "host2:9092"],
            SecurityProtocol = SecurityProtocol.SaslPlaintext,
            SASLMechanism = SaslMechanism.ScramSha512,
            Username = "user",
            Password = "password"
        });

        // Act
        var config = factory.CreateConsumerConfig<string>();

        // Assert
        config.BootstrapServers.Should().Be("host1:9092,host2:9092");
        config.AutoOffsetReset.Should().Be(AutoOffsetReset.Earliest);
        config.GroupId.Should().NotBeNullOrWhiteSpace();
        config.EnableAutoCommit.Should().BeFalse();
        config.SecurityProtocol.Should().Be(SecurityProtocol.SaslPlaintext);
        config.SaslMechanism.Should().Be(SaslMechanism.ScramSha512);
        config.SaslUsername.Should().Be("user");
        config.SaslPassword.Should().Be("password");
    }

    [Fact(DisplayName = "KafkaClientFactory creates admin client from mapped config.")]
    [Trait("Category", "Unit")]
    public void KafkaClientFactoryCreatesAdminClientFromMappedConfig()
    {
        // Arrange
        var factory = CreateTestFactory();

        // Act
        var client = factory.CreateAdminClient();

        // Assert
        client.Should().BeSameAs(factory.AdminClient);
        factory.AdminConfig.Should().NotBeNull();
        factory.AdminConfig!.BootstrapServers.Should().Be("localhost:9092");
    }

    [Fact(DisplayName = "KafkaClientFactory creates consumer from mapped config.")]
    [Trait("Category", "Unit")]
    public void KafkaClientFactoryCreatesConsumerFromMappedConfig()
    {
        // Arrange
        var factory = CreateTestFactory();

        // Act
        var consumer = factory.CreateConsumer<string>();

        // Assert
        consumer.Should().BeSameAs(factory.Consumer);
        factory.ConsumerConfig.Should().NotBeNull();
        factory.ConsumerConfig!.BootstrapServers.Should().Be("localhost:9092");
    }

    private static KafkaClientFactory CreateFactory(BootstrapServersConfiguration? configuration = null)
    {
        var optionsMock = new Mock<IOptions<BootstrapServersConfiguration>>(MockBehavior.Strict);
        optionsMock.Setup(x => x.Value).Returns(configuration ?? new BootstrapServersConfiguration
        {
            BootstrapServers = ["localhost:9092"]
        });

        return new KafkaClientFactory(optionsMock.Object);
    }

    private static TestKafkaClientFactory CreateTestFactory()
    {
        var optionsMock = new Mock<IOptions<BootstrapServersConfiguration>>(MockBehavior.Strict);
        optionsMock.Setup(x => x.Value).Returns(new BootstrapServersConfiguration
        {
            BootstrapServers = ["localhost:9092"]
        });

        return new TestKafkaClientFactory(optionsMock.Object);
    }

    private sealed class TestKafkaClientFactory(IOptions<BootstrapServersConfiguration> options)
        : KafkaClientFactory(options)
    {
        public IAdminClient AdminClient { get; } = Mock.Of<IAdminClient>();

        public IConsumer<string, byte[]> Consumer { get; } = Mock.Of<IConsumer<string, byte[]>>();

        public AdminClientConfig? AdminConfig { get; private set; }

        public ConsumerConfig? ConsumerConfig { get; private set; }

        protected override IAdminClient BuildAdminClient(AdminClientConfig config)
        {
            AdminConfig = config;

            return AdminClient;
        }

        protected override IConsumer<TKey, byte[]> BuildConsumer<TKey>(ConsumerConfig config)
        {
            ConsumerConfig = config;

            return (IConsumer<TKey, byte[]>)Consumer;
        }
    }
}
