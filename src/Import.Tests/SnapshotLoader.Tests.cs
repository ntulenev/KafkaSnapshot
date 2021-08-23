using System;
using System.Threading;
using System.Threading.Tasks;

using Confluent.Kafka;

using FluentAssertions;

using Microsoft.Extensions.Logging;

using Moq;

using Xunit;

using KafkaSnapshot.Import.Metadata;
using KafkaSnapshot.Models.Import;
using KafkaSnapshot.Abstractions.Filters;


namespace KafkaSnapshot.Import.Tests
{
    public class SnapshotLoaderTests
    {
        [Fact(DisplayName = "SnapshotLoader can't be created with null logger.")]
        [Trait("Category", "Unit")]
        public void CantCreateSnapshotLoaderWithNulLogger()
        {

            // Arrange
            var logger = (ILogger<SnapshotLoader<object, object>>)null!;
            IConsumer<object, object> consumerFactory() => null!;
            var topicLoaderMock = new Mock<ITopicWatermarkLoader>();
            var topicLoader = topicLoaderMock.Object;

            // Act
            var exception = Record.Exception(() => new SnapshotLoader<object, object>(logger, consumerFactory, topicLoader));

            // Assert
            exception.Should().NotBeNull().And.BeOfType<ArgumentNullException>();
        }

        [Fact(DisplayName = "SnapshotLoader can't be created with null factory.")]
        [Trait("Category", "Unit")]
        public void CantCreateSnapshotLoaderWithNullFactory()
        {

            // Arrange
            var loggerMock = new Mock<ILogger<SnapshotLoader<object, object>>>();
            var logger = loggerMock.Object;
            var topicLoaderMock = new Mock<ITopicWatermarkLoader>();
            var topicLoader = topicLoaderMock.Object;

            // Act
            var exception = Record.Exception(() => new SnapshotLoader<object, object>(logger, null!, topicLoader));

            // Assert
            exception.Should().NotBeNull().And.BeOfType<ArgumentNullException>();
        }

        [Fact(DisplayName = "SnapshotLoader can't be created with null topic loader.")]
        [Trait("Category", "Unit")]
        public void CantCreateSnapshotLoaderWithNullTopicLoader()
        {

            // Arrange
            var loggerMock = new Mock<ILogger<SnapshotLoader<object, object>>>();
            var logger = loggerMock.Object;
            IConsumer<object, object> consumerFactory() => null!;
            ITopicWatermarkLoader topicLoader = null!;

            // Act
            var exception = Record.Exception(() => new SnapshotLoader<object, object>(logger, consumerFactory, topicLoader));

            // Assert
            exception.Should().NotBeNull().And.BeOfType<ArgumentNullException>();
        }

        [Fact(DisplayName = "SnapshotLoader can be created with valid params.")]
        [Trait("Category", "Unit")]
        public void SnapshotLoaderCanBeCreated()
        {

            // Arrange
            var loggerMock = new Mock<ILogger<SnapshotLoader<object, object>>>();
            var logger = loggerMock.Object;
            IConsumer<object, object> consumerFactory() => null!;
            var topicLoaderMock = new Mock<ITopicWatermarkLoader>();
            var topicLoader = topicLoaderMock.Object;

            // Act
            var exception = Record.Exception(() => new SnapshotLoader<object, object>(logger, consumerFactory, topicLoader));

            // Assert
            exception.Should().BeNull();
        }

        [Fact(DisplayName = "SnapshotLoader can't load for null topic.")]
        [Trait("Category", "Unit")]
        public async Task SnapshotLoaderCantLoadForNullTopicAsync()
        {

            // Arrange
            var loggerMock = new Mock<ILogger<SnapshotLoader<object, object>>>();
            var logger = loggerMock.Object;
            IConsumer<object, object> consumerFactory() => null!;
            var topicLoaderMock = new Mock<ITopicWatermarkLoader>();
            var topicLoader = topicLoaderMock.Object;
            var loader = new SnapshotLoader<object, object>(logger, consumerFactory, topicLoader);
            bool withCompacting = true;
            var topicName = (TopicName)null!;
            var filterMock = new Mock<IKeyFilter<object>>();
            var filter = filterMock.Object;
            // Act
            var exception = await Record.ExceptionAsync(
                async () => _ = await loader.LoadCompactSnapshotAsync(withCompacting, topicName, filter, CancellationToken.None));


            // Assert
            exception.Should().NotBeNull().And.BeOfType<ArgumentNullException>();
        }

        [Fact(DisplayName = "SnapshotLoader can't load for null filter.")]
        [Trait("Category", "Unit")]
        public async Task SnapshotLoaderCantLoadForNullFilterAsync()
        {

            // Arrange
            var loggerMock = new Mock<ILogger<SnapshotLoader<object, object>>>();
            var logger = loggerMock.Object;
            IConsumer<object, object> consumerFactory() => null!;
            var topicLoaderMock = new Mock<ITopicWatermarkLoader>();
            var topicLoader = topicLoaderMock.Object;
            var loader = new SnapshotLoader<object, object>(logger, consumerFactory, topicLoader);
            bool withCompacting = true;
            var topicName = new TopicName("test");
            var filter = (IKeyFilter<object>)null!;
            // Act
            var exception = await Record.ExceptionAsync(
                async () => _ = await loader.LoadCompactSnapshotAsync(withCompacting, topicName, filter, CancellationToken.None));


            // Assert
            exception.Should().NotBeNull().And.BeOfType<ArgumentNullException>();
        }
    }
}
