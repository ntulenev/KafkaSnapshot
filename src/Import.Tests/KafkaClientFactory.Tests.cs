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
}
