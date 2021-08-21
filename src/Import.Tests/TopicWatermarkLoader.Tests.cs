using System;

using Microsoft.Extensions.Options;

using Confluent.Kafka;

using FluentAssertions;

using Moq;

using Xunit;

using KafkaSnapshot.Import.Configuration;
using KafkaSnapshot.Import.Metadata;

namespace KafkaAsTable.Tests
{
    public class TopicWatermarkLoaderTests
    {
        [Fact(DisplayName = "TopicWatermarkLoader can be created with valid params.")]
        [Trait("Category", "Unit")]
        public void TopicWatermarkLoaderCanBeCreated()
        {

            // Arrange
            var client = (new Mock<IAdminClient>()).Object;
            var options = (new Mock<IOptions<TopicWatermarkLoaderConfiguration>>());
            options.Setup(x => x.Value).Returns(new TopicWatermarkLoaderConfiguration
            {

            });

            // Act
            var exception = Record.Exception(() => new TopicWatermarkLoader(client, options.Object));

            // Assert
            exception.Should().BeNull();
        }

        [Fact(DisplayName = "TopicWatermarkLoader can't be created with null client.")]
        [Trait("Category", "Unit")]
        public void TopicWatermarkLoaderCantBeCreatedWithNullClient()
        {

            // Arrange
            var client = (IAdminClient)null!;
            var options = (new Mock<IOptions<TopicWatermarkLoaderConfiguration>>());
            options.Setup(x => x.Value).Returns(new TopicWatermarkLoaderConfiguration
            {

            });

            // Act
            var exception = Record.Exception(() => new TopicWatermarkLoader(client, options.Object));

            // Assert
            exception.Should().NotBeNull().And.BeOfType<ArgumentNullException>();
        }

        [Fact(DisplayName = "TopicWatermarkLoader can't be created with null options.")]
        [Trait("Category", "Unit")]
        public void TopicWatermarkLoaderCantBeCreatedWithNullOptions()
        {

            // Arrange
            var client = (new Mock<IAdminClient>()).Object;
            var options = (IOptions<TopicWatermarkLoaderConfiguration>)null!;

            // Act
            var exception = Record.Exception(() => new TopicWatermarkLoader(client, options));

            // Assert
            exception.Should().NotBeNull().And.BeOfType<ArgumentNullException>();
        }

        [Fact(DisplayName = "TopicWatermarkLoader can't be created with null options value.")]
        [Trait("Category", "Unit")]
        public void TopicWatermarkLoaderCantBeCreatedWithNullOptionsValue()
        {

            // Arrange
            var client = (new Mock<IAdminClient>()).Object;
            var options = (new Mock<IOptions<TopicWatermarkLoaderConfiguration>>());

            // Act
            var exception = Record.Exception(() => new TopicWatermarkLoader(client, options.Object));

            // Assert
            exception.Should().NotBeNull().And.BeOfType<ArgumentException>();
        }
    }
}
