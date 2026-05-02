using Confluent.Kafka;

using FluentAssertions;

using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;

using Moq;

using Xunit;

using KafkaSnapshot.Import.Metadata;
using KafkaSnapshot.Models.Import;
using KafkaSnapshot.Abstractions.Filters;
using KafkaSnapshot.Models.Message;
using KafkaSnapshot.Import.Watermarks;
using KafkaSnapshot.Import.Configuration;
using KafkaSnapshot.Models.Filters;
using KafkaSnapshot.Abstractions.Sorting;
using KafkaSnapshot.Models.Names;
using KafkaSnapshot.Abstractions.Import;

using System.Text;

namespace KafkaSnapshot.Import.Tests;

public partial class SnapshotLoaderTests
{
    [Fact(DisplayName = "SnapshotLoader can't be created with null logger.")]
    [Trait("Category", "Unit")]
    public void CannotCreateSnapshotLoaderWithNullLogger()
    {

        // Arrange
        var logger = (ILogger<SnapshotLoader<object, object>>)null!;
        IConsumer<object, byte[]> consumerFactory() => throw new NotImplementedException();
        var topicLoaderMock = new Mock<ITopicWatermarkLoader>(MockBehavior.Strict);
        var topicLoader = topicLoaderMock.Object;
        var optionsMock = new Mock<IOptions<SnapshotLoaderConfiguration>>(MockBehavior.Strict);
        optionsMock.Setup(x => x.Value).Returns(new SnapshotLoaderConfiguration() { });
        var options = optionsMock.Object;
        var sorterMock = new Mock<IMessageSorter<object, object>>(MockBehavior.Strict);
        var sorter = sorterMock.Object;
        var encoderMock = new Mock<IMessageEncoder<byte[], object>>(MockBehavior.Strict);
        var encoder = encoderMock.Object;

        // Act
        var exception = Record.Exception(
            () => CreateLoader(logger, options, consumerFactory, topicLoader, sorter, encoder));

        // Assert
        exception.Should().NotBeNull().And.BeOfType<ArgumentNullException>();
    }

    [Fact(DisplayName = "SnapshotLoader can't be created with null factory.")]
    [Trait("Category", "Unit")]
    public void CannotCreateSnapshotLoaderWithNullFactory()
    {

        // Arrange
        var loggerMock = CreateLoggerMock();
        var logger = loggerMock.Object;
        var topicLoaderMock = new Mock<ITopicWatermarkLoader>(MockBehavior.Strict);
        var topicLoader = topicLoaderMock.Object;
        var optionsMock = new Mock<IOptions<SnapshotLoaderConfiguration>>(MockBehavior.Strict);
        optionsMock.Setup(x => x.Value).Returns(new SnapshotLoaderConfiguration() { });
        var options = optionsMock.Object;
        var sorterMock = new Mock<IMessageSorter<object, object>>(MockBehavior.Strict);
        var sorter = sorterMock.Object;
        var encoderMock = new Mock<IMessageEncoder<byte[], object>>(MockBehavior.Strict);
        var encoder = encoderMock.Object;

        // Act
        var exception = Record.Exception(
            () => CreateLoader(logger, options, null!, topicLoader, sorter, encoder));

        // Assert
        exception.Should().NotBeNull().And.BeOfType<ArgumentNullException>();
    }

    [Fact(DisplayName = "SnapshotLoader can't be created with null options.")]
    [Trait("Category", "Unit")]
    public void CannotCreateSnapshotLoaderWithNullOptions()
    {

        // Arrange
        var loggerMock = CreateLoggerMock();
        var logger = loggerMock.Object;
        IConsumer<object, byte[]> consumerFactory() => throw new NotImplementedException();
        var topicLoaderMock = new Mock<ITopicWatermarkLoader>(MockBehavior.Strict);
        var topicLoader = topicLoaderMock.Object;
        var optionsMock = new Mock<IOptions<SnapshotLoaderConfiguration>>(MockBehavior.Strict);
        optionsMock.Setup(x => x.Value).Returns(new SnapshotLoaderConfiguration() { });
        var options = optionsMock.Object;
        var sorterMock = new Mock<IMessageSorter<object, object>>(MockBehavior.Strict);
        var sorter = sorterMock.Object;
        var encoderMock = new Mock<IMessageEncoder<byte[], object>>(MockBehavior.Strict);
        var encoder = encoderMock.Object;

        // Act
        var exception = Record.Exception(
            () => CreateLoader(logger, null!, consumerFactory, topicLoader, sorter, encoder));

        // Assert
        exception.Should().NotBeNull().And.BeOfType<ArgumentNullException>();
    }

    [Fact(DisplayName = "SnapshotLoader can't be created with null options value.")]
    [Trait("Category", "Unit")]
    public void CannotCreateSnapshotLoaderWithNullOptionsValue()
    {

        // Arrange
        var loggerMock = CreateLoggerMock();
        var logger = loggerMock.Object;
        IConsumer<object, byte[]> consumerFactory() => throw new NotImplementedException();
        var topicLoaderMock = new Mock<ITopicWatermarkLoader>(MockBehavior.Strict);
        var topicLoader = topicLoaderMock.Object;
        var optionsMock = new Mock<IOptions<SnapshotLoaderConfiguration>>(MockBehavior.Strict);
        optionsMock.Setup(x => x.Value).Returns((SnapshotLoaderConfiguration)null!);
        var options = optionsMock.Object;
        var sorterMock = new Mock<IMessageSorter<object, object>>(MockBehavior.Strict);
        var sorter = sorterMock.Object;
        var encoderMock = new Mock<IMessageEncoder<byte[], object>>(MockBehavior.Strict);
        var encoder = encoderMock.Object;

        // Act
        var exception = Record.Exception(
            () => CreateLoader(logger, options, consumerFactory, topicLoader, sorter, encoder));

        // Assert
        exception.Should().NotBeNull().And.BeOfType<ArgumentNullException>();
    }

    [Fact(DisplayName = "SnapshotLoader can't be created with null topic loader.")]
    [Trait("Category", "Unit")]
    public void CannotCreateSnapshotLoaderWithNullTopicLoader()
    {

        // Arrange
        var loggerMock = CreateLoggerMock();
        var logger = loggerMock.Object;
        IConsumer<object, byte[]> consumerFactory() => throw new NotImplementedException();
        var optionsMock = new Mock<IOptions<SnapshotLoaderConfiguration>>(MockBehavior.Strict);
        optionsMock.Setup(x => x.Value).Returns(new SnapshotLoaderConfiguration() { });
        var options = optionsMock.Object;
        var sorterMock = new Mock<IMessageSorter<object, object>>(MockBehavior.Strict);
        var sorter = sorterMock.Object;
        var encoderMock = new Mock<IMessageEncoder<byte[], object>>(MockBehavior.Strict);
        var encoder = encoderMock.Object;

        // Act
        var exception = Record.Exception(
            () => CreateLoader(logger, options, consumerFactory, null!, sorter, encoder));

        // Assert
        exception.Should().NotBeNull().And.BeOfType<ArgumentNullException>();
    }

    [Fact(DisplayName = "SnapshotLoader can't be created with null sorter.")]
    [Trait("Category", "Unit")]
    public void SnapshotLoaderCannotBeCreatedWithNullSorter()
    {

        // Arrange
        var loggerMock = new Mock<ILogger<SnapshotLoader<object, object>>>(MockBehavior.Strict);
        var logger = loggerMock.Object;
        IConsumer<object, byte[]> consumerFactory() => throw new NotImplementedException();
        var topicLoaderMock = new Mock<ITopicWatermarkLoader>(MockBehavior.Strict);
        var topicLoader = topicLoaderMock.Object;
        var optionsMock = new Mock<IOptions<SnapshotLoaderConfiguration>>(MockBehavior.Strict);
        optionsMock.Setup(x => x.Value).Returns(new SnapshotLoaderConfiguration() { });
        var options = optionsMock.Object;
        var encoderMock = new Mock<IMessageEncoder<byte[], object>>(MockBehavior.Strict);
        var encoder = encoderMock.Object;

        // Act
        var exception = Record.Exception(
            () => CreateLoader(logger, options, consumerFactory, topicLoader, null!, encoder));

        // Assert
        exception.Should().NotBeNull().And.BeOfType<ArgumentNullException>();
    }

    [Fact(DisplayName = "SnapshotLoader can't be created with null encoder.")]
    [Trait("Category", "Unit")]
    public void SnapshotLoaderCannotBeCreatedWithNullEncoder()
    {

        // Arrange
        var loggerMock = new Mock<ILogger<SnapshotLoader<object, object>>>(MockBehavior.Strict);
        var logger = loggerMock.Object;
        IConsumer<object, byte[]> consumerFactory() => throw new NotImplementedException();
        var topicLoaderMock = new Mock<ITopicWatermarkLoader>(MockBehavior.Strict);
        var topicLoader = topicLoaderMock.Object;
        var optionsMock = new Mock<IOptions<SnapshotLoaderConfiguration>>(MockBehavior.Strict);
        optionsMock.Setup(x => x.Value).Returns(new SnapshotLoaderConfiguration() { });
        var options = optionsMock.Object;
        var sorterMock = new Mock<IMessageSorter<object, object>>(MockBehavior.Strict);
        var sorter = sorterMock.Object;

        // Act
        var exception = Record.Exception(
            () => CreateLoader(logger, options, consumerFactory, topicLoader, sorter, null!));

        // Assert
        exception.Should().NotBeNull().And.BeOfType<ArgumentNullException>();
    }

    [Fact(DisplayName = "SnapshotLoader can be created with valid parameters.")]
    [Trait("Category", "Unit")]
    public void SnapshotLoaderCanBeCreated()
    {

        // Arrange
        var loggerMock = CreateLoggerMock();
        var logger = loggerMock.Object;
        IConsumer<object, byte[]> consumerFactory() => throw new NotImplementedException();
        var topicLoaderMock = new Mock<ITopicWatermarkLoader>(MockBehavior.Strict);
        var topicLoader = topicLoaderMock.Object;
        var optionsMock = new Mock<IOptions<SnapshotLoaderConfiguration>>(MockBehavior.Strict);
        optionsMock.Setup(x => x.Value).Returns(new SnapshotLoaderConfiguration() { });
        var options = optionsMock.Object;
        var sorterMock = new Mock<IMessageSorter<object, object>>(MockBehavior.Strict);
        var sorter = sorterMock.Object;
        var encoderMock = new Mock<IMessageEncoder<byte[], object>>(MockBehavior.Strict);
        encoderMock.Setup(x => x.Encode(It.IsAny<byte[]>(), It.IsAny<EncoderRules>()));
        var encoder = encoderMock.Object;

        // Act
        var exception = Record.Exception(
            () => CreateLoader(logger, options, consumerFactory, topicLoader, sorter, encoder));

        // Assert
        exception.Should().BeNull();
    }
}
