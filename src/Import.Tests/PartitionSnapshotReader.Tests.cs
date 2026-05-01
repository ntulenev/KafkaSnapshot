using Confluent.Kafka;

using FluentAssertions;

using KafkaSnapshot.Abstractions.Import;
using KafkaSnapshot.Import.Configuration;

using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;

using Moq;

using Xunit;

namespace KafkaSnapshot.Import.Tests;

public partial class PartitionSnapshotReaderTests
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
        var logger = new RecordingLogger<PartitionSnapshotReader<object, object>>(
            LogLevel.Information,
            LogLevel.Warning);
        var topic = CreateTopic();
        var partition = CreatePartition();
        var watermark = CreateWatermark(topic, partition);
        var consumerMock = CreateReadableConsumerMock(topic, partition, new Offset(0), token);
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
        result[0].Key.Should().Be("key");
        result[0].Value.Message.Should().Be("value");
        result[0].Value.Meta.Partition.Should().Be(partition.Value);
    }
}
