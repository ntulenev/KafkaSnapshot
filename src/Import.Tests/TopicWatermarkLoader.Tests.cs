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
    }
}
