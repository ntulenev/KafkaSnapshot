using System.Text;

using Confluent.Kafka;

using FluentAssertions;

using KafkaSnapshot.Abstractions.Filters;
using KafkaSnapshot.Abstractions.Import;
using KafkaSnapshot.Models.Filters;

using Moq;

using Xunit;

namespace KafkaSnapshot.Import.Tests;

public partial class PartitionSnapshotReaderTests
{
    [Fact(DisplayName = "PartitionSnapshotReader closes consumer when enumeration completes.")]
    [Trait("Category", "Unit")]
    public void PartitionSnapshotReaderClosesConsumerWhenEnumerationCompletes()
    {
        // Arrange
        using var tokenSource = new CancellationTokenSource();
        var token = tokenSource.Token;
        var loggerMock = CreateLoggerMock();
        var topic = CreateTopic();
        var partition = CreatePartition();
        var watermark = CreateWatermark(topic, partition);
        var consumerMock = CreateReadableConsumerMock(topic, partition, new Offset(0), token);
        var encoderMock = CreateEncoderMock();
        var (keyFilterMock, valueFilterMock) = CreateMatchingFilters();
        var reader = CreateReader(loggerMock.Object, consumerMock, encoderMock);

        // Act
        var result = reader.Read(
            watermark,
            topic,
            keyFilterMock.Object,
            valueFilterMock.Object,
            token).ToList();

        // Assert
        result.Should().ContainSingle();
        consumerMock.Verify(x => x.Close(), Times.Once);
        consumerMock.Verify(x => x.Dispose(), Times.Once);
    }

    [Fact(DisplayName = "PartitionSnapshotReader closes consumer when final date stops enumeration.")]
    [Trait("Category", "Unit")]
    public void PartitionSnapshotReaderClosesConsumerWhenFinalDateStopsEnumeration()
    {
        // Arrange
        using var tokenSource = new CancellationTokenSource();
        var token = tokenSource.Token;
        var loggerMock = CreateLoggerMock();
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
        var reader = CreateReader(loggerMock.Object, consumerMock, encoderMock);

        // Act
        var result = reader.Read(
            watermark,
            topic,
            keyFilterMock.Object,
            valueFilterMock.Object,
            token).ToList();

        // Assert
        result.Should().BeEmpty();
        consumerMock.Verify(x => x.Close(), Times.Once);
        consumerMock.Verify(x => x.Dispose(), Times.Once);
    }

    [Fact(DisplayName = "PartitionSnapshotReader closes consumer when consume throws.")]
    [Trait("Category", "Unit")]
    public void PartitionSnapshotReaderClosesConsumerWhenConsumeThrows()
    {
        // Arrange
        using var tokenSource = new CancellationTokenSource();
        var token = tokenSource.Token;
        var loggerMock = CreateLoggerMock();
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
        var reader = CreateReader(loggerMock.Object, consumerMock, encoderMock);

        // Act
        var exception = Record.Exception(() => reader.Read(
            watermark,
            topic,
            keyFilterMock.Object,
            valueFilterMock.Object,
            token).ToList());

        // Assert
        exception.Should().BeOfType<OperationCanceledException>();
        consumerMock.Verify(x => x.Close(), Times.Once);
        consumerMock.Verify(x => x.Dispose(), Times.Once);
    }

    [Fact(DisplayName = "PartitionSnapshotReader closes consumer when caller stops enumeration early.")]
    [Trait("Category", "Unit")]
    public void PartitionSnapshotReaderClosesConsumerWhenCallerStopsEnumerationEarly()
    {
        // Arrange
        using var tokenSource = new CancellationTokenSource();
        var token = tokenSource.Token;
        var loggerMock = CreateLoggerMock();
        var topic = CreateTopic();
        var partition = CreatePartition();
        var watermark = CreateWatermark(topic, partition, highOffset: 2);
        var consumerMock = CreateReadableConsumerMock(topic, partition, new Offset(0), token);
        var encoderMock = CreateEncoderMock();
        var (keyFilterMock, valueFilterMock) = CreateMatchingFilters();
        var reader = CreateReader(loggerMock.Object, consumerMock, encoderMock);

        // Act
        using (var enumerator = reader.Read(
            watermark,
            topic,
            keyFilterMock.Object,
            valueFilterMock.Object,
            token).GetEnumerator())
        {
            enumerator.MoveNext().Should().BeTrue();
        }

        // Assert
        consumerMock.Verify(x => x.Close(), Times.Once);
        consumerMock.Verify(x => x.Dispose(), Times.Once);
    }
}
