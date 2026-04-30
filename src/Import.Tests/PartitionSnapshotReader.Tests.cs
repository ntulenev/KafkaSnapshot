using System.Text;

using Confluent.Kafka;

using FluentAssertions;

using KafkaSnapshot.Abstractions.Filters;
using KafkaSnapshot.Abstractions.Import;
using KafkaSnapshot.Import.Configuration;
using KafkaSnapshot.Import.Watermarks;
using KafkaSnapshot.Models.Filters;
using KafkaSnapshot.Models.Import;
using KafkaSnapshot.Models.Names;

using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;

using Moq;

using Xunit;

namespace KafkaSnapshot.Import.Tests;

public class PartitionSnapshotReaderTests
{
    [Fact(DisplayName = "PartitionSnapshotReader can't be created with null logger.")]
    [Trait("Category", "Unit")]
    public void PartitionSnapshotReaderCantBeCreatedWithNullLogger()
    {
        // Arrange
        var encoderMock = new Mock<IMessageEncoder<byte[], object>>(MockBehavior.Strict);
        IConsumer<object, byte[]> consumerFactory() => Mock.Of<IConsumer<object, byte[]>>();

        // Act
        var exception = Record.Exception(() =>
            new PartitionSnapshotReader<object, object>(
                null!,
                Options.Create(new SnapshotLoaderConfiguration()),
                consumerFactory,
                encoderMock.Object));

        // Assert
        exception.Should().NotBeNull().And.BeOfType<ArgumentNullException>();
    }

    [Fact(DisplayName = "PartitionSnapshotReader reads matching partition messages.")]
    [Trait("Category", "Unit")]
    public void PartitionSnapshotReaderReadsMatchingPartitionMessages()
    {
        // Arrange
        using var tokenSource = new CancellationTokenSource();
        var token = tokenSource.Token;
        var loggerMock = CreateLoggerMock();
        var topic = new LoadingTopic(
            new TopicName("test"),
            false,
            new DateFilterRange(null!, null!),
            EncoderRules.String,
            null);
        var partition = new Partition(1);
        var watermark = new PartitionWatermark(
            topic,
            new WatermarkOffsets(new Offset(0), new Offset(1)),
            partition);
        var consumerMock = new Mock<IConsumer<object, byte[]>>(MockBehavior.Strict);
        consumerMock.Setup(x => x.Assign(It.Is<TopicPartition>(
            item => item.Topic == topic.Value.Name && item.Partition == partition)));
        consumerMock.Setup(x => x.Consume(token)).Returns(new ConsumeResult<object, byte[]>
        {
            Message = new Message<object, byte[]>
            {
                Key = "key",
                Value = Encoding.UTF8.GetBytes("value"),
                Timestamp = Timestamp.Default
            },
            Offset = new Offset(0)
        });
        consumerMock.Setup(x => x.Close());
        consumerMock.Setup(x => x.Dispose());
        var encoderMock = new Mock<IMessageEncoder<byte[], object>>(MockBehavior.Strict);
        encoderMock.Setup(x => x.Encode(It.IsAny<byte[]>(), EncoderRules.String)).Returns("value");
        var keyFilterMock = new Mock<IDataFilter<object>>(MockBehavior.Strict);
        keyFilterMock.Setup(x => x.IsMatch("key")).Returns(true);
        var valueFilterMock = new Mock<IDataFilter<object>>(MockBehavior.Strict);
        valueFilterMock.Setup(x => x.IsMatch("value")).Returns(true);
        var reader = new PartitionSnapshotReader<object, object>(
            loggerMock.Object,
            Options.Create(new SnapshotLoaderConfiguration()),
            () => consumerMock.Object,
            encoderMock.Object);

        // Act
        var result = reader.Read(
            watermark,
            topic,
            keyFilterMock.Object,
            valueFilterMock.Object,
            token).ToList();

        // Assert
        result.Should().ContainSingle();
        result[0].Key.Should().Be("key");
        result[0].Value.Message.Should().Be("value");
        result[0].Value.Meta.Partition.Should().Be(partition.Value);
    }

    [Fact(DisplayName = "PartitionSnapshotReader skips partition without date offset.")]
    [Trait("Category", "Unit")]
    public void PartitionSnapshotReaderSkipsPartitionWithoutDateOffset()
    {
        // Arrange
        using var tokenSource = new CancellationTokenSource();
        var token = tokenSource.Token;
        var loggerMock = CreateLoggerMock();
        var startDate = DateTime.UtcNow;
        var topic = new LoadingTopic(
            new TopicName("test"),
            false,
            new DateFilterRange(startDate, null!),
            EncoderRules.String,
            null);
        var partition = new Partition(1);
        var watermark = new PartitionWatermark(
            topic,
            new WatermarkOffsets(new Offset(0), new Offset(1)),
            partition);
        var consumerMock = new Mock<IConsumer<object, byte[]>>(MockBehavior.Strict);
        var topicPartition = new TopicPartition(topic.Value.Name, partition);
        consumerMock.Setup(x => x.OffsetsForTimes(
            It.Is<IEnumerable<TopicPartitionTimestamp>>(items =>
                items.Single().TopicPartition == topicPartition),
            It.IsAny<TimeSpan>())).Returns(
            [
                new TopicPartitionOffset(topicPartition, Offset.End)
            ]);
        consumerMock.Setup(x => x.Close());
        consumerMock.Setup(x => x.Dispose());
        var encoderMock = new Mock<IMessageEncoder<byte[], object>>(MockBehavior.Strict);
        var keyFilterMock = new Mock<IDataFilter<object>>(MockBehavior.Strict);
        var valueFilterMock = new Mock<IDataFilter<object>>(MockBehavior.Strict);
        var reader = new PartitionSnapshotReader<object, object>(
            loggerMock.Object,
            Options.Create(new SnapshotLoaderConfiguration()),
            () => consumerMock.Object,
            encoderMock.Object);

        // Act
        var result = reader.Read(
            watermark,
            topic,
            keyFilterMock.Object,
            valueFilterMock.Object,
            token).ToList();

        // Assert
        result.Should().BeEmpty();
    }

    private static Mock<ILogger<PartitionSnapshotReader<object, object>>> CreateLoggerMock()
    {
        var loggerMock = new Mock<ILogger<PartitionSnapshotReader<object, object>>>(MockBehavior.Strict);
        loggerMock.Setup(x => x.IsEnabled(It.IsAny<LogLevel>())).Returns(false);
        loggerMock.Setup(
            x => x.Log(
                It.IsAny<LogLevel>(),
                It.IsAny<EventId>(),
                It.IsAny<It.IsAnyType>(),
                It.IsAny<Exception?>(),
                It.IsAny<Func<It.IsAnyType, Exception?, string>>()));

        return loggerMock;
    }
}
