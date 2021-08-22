using System;

using Confluent.Kafka;

using FluentAssertions;

using Microsoft.Extensions.Logging;

using Moq;

using Xunit;

using KafkaSnapshot.Import.Metadata;

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
    }
}
