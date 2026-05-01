using System.Text;

using Confluent.Kafka;

using FluentAssertions;

using KafkaSnapshot.Abstractions.Filters;
using KafkaSnapshot.Abstractions.Import;
using KafkaSnapshot.Import.Watermarks;
using KafkaSnapshot.Models.Filters;

using Microsoft.Extensions.Logging;

using Moq;

using Xunit;

namespace KafkaSnapshot.Import.Tests;

public partial class PartitionSnapshotReaderTests
{
    [Fact(DisplayName = "PartitionSnapshotReader skips partition without date offset.")]
    [Trait("Category", "Unit")]
    public void PartitionSnapshotReaderSkipsPartitionWithoutDateOffset()
    {
        // Arrange
        using var tokenSource = new CancellationTokenSource();
        var token = tokenSource.Token;
        var logger = new RecordingLogger<PartitionSnapshotReader<object, object>>(
            LogLevel.Information,
            LogLevel.Warning);
        var startDate = DateTime.UtcNow;
        var topic = CreateTopic(new DateFilterRange(startDate, null!));
        var partition = CreatePartition();
        var watermark = CreateWatermark(topic, partition);
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
        var (keyFilterMock, valueFilterMock) = CreateStrictFilters();
        var reader = CreateReader(logger, consumerMock, encoderMock);

        // Act
        var result = reader.Read(
            watermark,
            topic,
            keyFilterMock.Object,
            valueFilterMock.Object,
            token).ToList();

        // Assert
        result.Should().BeEmpty();
        logger.Contains(LogLevel.Information, "Searching offset for topic test, partition 1").Should().BeTrue();
        logger.Contains(LogLevel.Warning, "No actual offset for topic test, partition 1").Should().BeTrue();
    }

    [Fact(DisplayName = "PartitionSnapshotReader logs resolved date offset.")]
    [Trait("Category", "Unit")]
    public void PartitionSnapshotReaderLogsResolvedDateOffset()
    {
        // Arrange
        using var tokenSource = new CancellationTokenSource();
        var token = tokenSource.Token;
        var logger = new RecordingLogger<PartitionSnapshotReader<object, object>>(LogLevel.Information);
        var startDate = DateTimeOffset.UtcNow;
        var topic = CreateTopic(new DateFilterRange(startDate, null!));
        var partition = CreatePartition();
        var watermark = CreateWatermark(topic, partition);
        var consumerMock = new Mock<IConsumer<object, byte[]>>(MockBehavior.Strict);
        var topicPartition = new TopicPartition(topic.Value.Name, partition);
        consumerMock.Setup(x => x.OffsetsForTimes(
            It.Is<IEnumerable<TopicPartitionTimestamp>>(items =>
                items.Single().TopicPartition == topicPartition),
            It.IsAny<TimeSpan>())).Returns(
            [
                new TopicPartitionOffset(topicPartition, new Offset(0))
            ]);
        consumerMock.Setup(x => x.Assign(It.Is<TopicPartitionOffset>(
            item => item.TopicPartition == topicPartition && item.Offset == new Offset(0))));
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
        var encoderMock = CreateEncoderMock();
        var (keyFilterMock, valueFilterMock) = CreateMatchingFilters();
        var reader = CreateReader(logger, consumerMock, encoderMock);

        // Act
        var result = reader.Read(
            watermark,
            topic,
            keyFilterMock.Object,
            valueFilterMock.Object,
            token).ToList();

        // Assert
        result.Should().ContainSingle();
        logger.Contains(LogLevel.Information, "Searching offset for topic test, partition 1").Should().BeTrue();
        logger.Contains(LogLevel.Information, "Resolved offset 0 for topic test, partition 1").Should().BeTrue();
    }

    [Fact(DisplayName = "PartitionSnapshotReader logs final date offset reached.")]
    [Trait("Category", "Unit")]
    public void PartitionSnapshotReaderLogsFinalDateOffsetReached()
    {
        // Arrange
        using var tokenSource = new CancellationTokenSource();
        var token = tokenSource.Token;
        var logger = new RecordingLogger<PartitionSnapshotReader<object, object>>(LogLevel.Information);
        var endDate = new DateTimeOffset(2024, 1, 1, 10, 0, 0, TimeSpan.Zero);
        var topic = CreateTopic(new DateFilterRange(null!, endDate));
        var partition = CreatePartition();
        var watermark = CreateWatermark(topic, partition);
        var consumerMock = new Mock<IConsumer<object, byte[]>>(MockBehavior.Strict);
        consumerMock.Setup(x => x.Assign(It.Is<TopicPartition>(
            item => item.Topic == topic.Value.Name && item.Partition == partition)));
        consumerMock.Setup(x => x.Consume(token)).Returns(new ConsumeResult<object, byte[]>
        {
            Message = new Message<object, byte[]>
            {
                Key = "key",
                Value = Encoding.UTF8.GetBytes("value"),
                Timestamp = new Timestamp(endDate.AddSeconds(1).UtcDateTime)
            },
            Offset = new Offset(0)
        });
        consumerMock.Setup(x => x.Close());
        consumerMock.Setup(x => x.Dispose());
        var encoderMock = new Mock<IMessageEncoder<byte[], object>>(MockBehavior.Strict);
        var (keyFilterMock, valueFilterMock) = CreateStrictFilters();
        var reader = CreateReader(logger, consumerMock, encoderMock);

        // Act
        var result = reader.Read(
            watermark,
            topic,
            keyFilterMock.Object,
            valueFilterMock.Object,
            token).ToList();

        // Assert
        result.Should().BeEmpty();
        logger.Contains(LogLevel.Information, "Final date offset").Should().BeTrue();
    }

    [Fact(DisplayName = "PartitionSnapshotReader logs date offset lookup failures.")]
    [Trait("Category", "Unit")]
    public void PartitionSnapshotReaderLogsDateOffsetLookupFailures()
    {
        // Arrange
        using var tokenSource = new CancellationTokenSource();
        var token = tokenSource.Token;
        var logger = new RecordingLogger<PartitionSnapshotReader<object, object>>(LogLevel.Error);
        var startDate = DateTimeOffset.UtcNow;
        var topic = CreateTopic(new DateFilterRange(startDate, null!));
        var partition = CreatePartition();
        var watermark = CreateWatermark(topic, partition);
        var consumerMock = new Mock<IConsumer<object, byte[]>>(MockBehavior.Strict);
        var kafkaException = new KafkaException(new Error(ErrorCode.Local_Transport, "lookup failed"));
        consumerMock.Setup(x => x.OffsetsForTimes(
            It.IsAny<IEnumerable<TopicPartitionTimestamp>>(),
            It.IsAny<TimeSpan>())).Throws(kafkaException);
        consumerMock.Setup(x => x.Close());
        consumerMock.Setup(x => x.Dispose());
        var encoderMock = new Mock<IMessageEncoder<byte[], object>>(MockBehavior.Strict);
        var (keyFilterMock, valueFilterMock) = CreateStrictFilters();
        var reader = CreateReader(logger, consumerMock, encoderMock);

        // Act
        var exception = Record.Exception(() => reader.Read(
            watermark,
            topic,
            keyFilterMock.Object,
            valueFilterMock.Object,
            token).ToList());

        // Assert
        exception.Should().BeSameAs(kafkaException);
        logger.Contains(LogLevel.Error, "Failed to resolve offset for topic test, partition 1").Should().BeTrue();
        consumerMock.Verify(x => x.Close(), Times.Once);
        consumerMock.Verify(x => x.Dispose(), Times.Once);
    }

    [Fact(DisplayName = "PartitionSnapshotReader logs consume failures.")]
    [Trait("Category", "Unit")]
    public void PartitionSnapshotReaderLogsConsumeFailures()
    {
        // Arrange
        using var tokenSource = new CancellationTokenSource();
        var token = tokenSource.Token;
        var logger = new RecordingLogger<PartitionSnapshotReader<object, object>>(LogLevel.Error);
        var topic = CreateTopic();
        var partition = CreatePartition();
        var watermark = CreateWatermark(topic, partition);
        var consumerMock = new Mock<IConsumer<object, byte[]>>(MockBehavior.Strict);
        var kafkaException = new KafkaException(new Error(ErrorCode.Local_Transport, "consume failed"));
        consumerMock.Setup(x => x.Assign(It.Is<TopicPartition>(
            item => item.Topic == topic.Value.Name && item.Partition == partition)));
        consumerMock.Setup(x => x.Consume(token)).Throws(kafkaException);
        consumerMock.Setup(x => x.Close());
        consumerMock.Setup(x => x.Dispose());
        var encoderMock = new Mock<IMessageEncoder<byte[], object>>(MockBehavior.Strict);
        var (keyFilterMock, valueFilterMock) = CreateStrictFilters();
        var reader = CreateReader(logger, consumerMock, encoderMock);

        // Act
        var exception = Record.Exception(() => reader.Read(
            watermark,
            topic,
            keyFilterMock.Object,
            valueFilterMock.Object,
            token).ToList());

        // Assert
        exception.Should().BeSameAs(kafkaException);
        logger.Contains(LogLevel.Error, "Failed to consume message from topic test, partition 1").Should().BeTrue();
        consumerMock.Verify(x => x.Close(), Times.Once);
        consumerMock.Verify(x => x.Dispose(), Times.Once);
    }

    [Fact(DisplayName = "PartitionSnapshotReader does not log cancellation as consume failure.")]
    [Trait("Category", "Unit")]
    public void PartitionSnapshotReaderDoesNotLogCancellationAsConsumeFailure()
    {
        // Arrange
        using var tokenSource = new CancellationTokenSource();
        var token = tokenSource.Token;
        var logger = new RecordingLogger<PartitionSnapshotReader<object, object>>(LogLevel.Error);
        var topic = CreateTopic();
        var partition = CreatePartition();
        var watermark = CreateWatermark(topic, partition);
        var consumerMock = new Mock<IConsumer<object, byte[]>>(MockBehavior.Strict);
        consumerMock.Setup(x => x.Assign(It.Is<TopicPartition>(
            item => item.Topic == topic.Value.Name && item.Partition == partition)));
        consumerMock.Setup(x => x.Consume(token)).Throws(new OperationCanceledException(token));
        consumerMock.Setup(x => x.Close());
        consumerMock.Setup(x => x.Dispose());
        var encoderMock = new Mock<IMessageEncoder<byte[], object>>(MockBehavior.Strict);
        var (keyFilterMock, valueFilterMock) = CreateStrictFilters();
        var reader = CreateReader(logger, consumerMock, encoderMock);

        // Act
        var exception = Record.Exception(() => reader.Read(
            watermark,
            topic,
            keyFilterMock.Object,
            valueFilterMock.Object,
            token).ToList());

        // Assert
        exception.Should().BeOfType<OperationCanceledException>();
        logger.Contains(LogLevel.Error, "Failed to consume message").Should().BeFalse();
        consumerMock.Verify(x => x.Close(), Times.Once);
        consumerMock.Verify(x => x.Dispose(), Times.Once);
    }
}
