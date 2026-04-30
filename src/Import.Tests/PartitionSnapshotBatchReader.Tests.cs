using Confluent.Kafka;

using FluentAssertions;

using KafkaSnapshot.Abstractions.Filters;
using KafkaSnapshot.Import.Configuration;
using KafkaSnapshot.Import.Watermarks;
using KafkaSnapshot.Models.Filters;
using KafkaSnapshot.Models.Import;
using KafkaSnapshot.Models.Message;
using KafkaSnapshot.Models.Names;

using Moq;
using Microsoft.Extensions.Options;

using Xunit;

namespace KafkaSnapshot.Import.Tests;

public class PartitionSnapshotBatchReaderTests
{
    [Fact(DisplayName = "PartitionSnapshotBatchReader can't be created with null config.")]
    [Trait("Category", "Unit")]
    public void PartitionSnapshotBatchReaderCantBeCreatedWithNullConfig()
    {
        // Arrange
        var partitionReaderMock = new Mock<IPartitionSnapshotReader<object, object>>(MockBehavior.Strict);

        // Act
        var exception = Record.Exception(() =>
            new PartitionSnapshotBatchReader<object, object>(null!, partitionReaderMock.Object));

        // Assert
        exception.Should().NotBeNull().And.BeOfType<ArgumentNullException>();
    }

    [Fact(DisplayName = "PartitionSnapshotBatchReader returns empty snapshot without watermarks.")]
    [Trait("Category", "Unit")]
    public async Task PartitionSnapshotBatchReaderReturnsEmptySnapshotWithoutWatermarks()
    {
        // Arrange
        using var tokenSource = new CancellationTokenSource();
        var token = tokenSource.Token;
        var partitionReaderMock = new Mock<IPartitionSnapshotReader<object, object>>(MockBehavior.Strict);
        var reader = new PartitionSnapshotBatchReader<object, object>(
            Options.Create(new SnapshotLoaderConfiguration()),
            partitionReaderMock.Object);
        var topic = CreateTopic();
        var keyFilterMock = new Mock<IDataFilter<object>>(MockBehavior.Strict);
        var valueFilterMock = new Mock<IDataFilter<object>>(MockBehavior.Strict);

        // Act
        var result = await reader.ReadAllAsync(
            [],
            topic,
            keyFilterMock.Object,
            valueFilterMock.Object,
            token);

        // Assert
        result.Should().BeEmpty();
    }

    [Fact(DisplayName = "PartitionSnapshotBatchReader stops on first non-empty partition.")]
    [Trait("Category", "Unit")]
    public async Task PartitionSnapshotBatchReaderStopsOnFirstNonEmptyPartition()
    {
        // Arrange
        using var tokenSource = new CancellationTokenSource();
        var token = tokenSource.Token;
        var topic = CreateTopic();
        var watermarks = new[]
        {
            CreateWatermark(topic, 1),
            CreateWatermark(topic, 2),
            CreateWatermark(topic, 3)
        };
        var message = CreateMessage("key", "value", 2);
        var keyFilterMock = new Mock<IDataFilter<object>>(MockBehavior.Strict);
        var valueFilterMock = new Mock<IDataFilter<object>>(MockBehavior.Strict);
        var partitionReaderMock = new Mock<IPartitionSnapshotReader<object, object>>(MockBehavior.Strict);
        partitionReaderMock.Setup(x => x.Read(
            watermarks[0],
            topic,
            keyFilterMock.Object,
            valueFilterMock.Object,
            token)).Returns([]);
        partitionReaderMock.Setup(x => x.Read(
            watermarks[1],
            topic,
            keyFilterMock.Object,
            valueFilterMock.Object,
            token)).Returns([message]);
        var reader = new PartitionSnapshotBatchReader<object, object>(
            Options.Create(new SnapshotLoaderConfiguration()),
            partitionReaderMock.Object);

        // Act
        var result = await reader.ReadFirstNonEmptyAsync(
            watermarks,
            topic,
            keyFilterMock.Object,
            valueFilterMock.Object,
            token);

        // Assert
        result.Should().ContainSingle().Which.Should().Be(message);
        partitionReaderMock.Verify(x => x.Read(
            watermarks[2],
            It.IsAny<LoadingTopic>(),
            It.IsAny<IDataFilter<object>>(),
            It.IsAny<IDataFilter<object>>(),
            It.IsAny<CancellationToken>()), Times.Never);
    }

    [Fact(DisplayName = "PartitionSnapshotBatchReader reads all partitions.")]
    [Trait("Category", "Unit")]
    public async Task PartitionSnapshotBatchReaderReadsAllPartitions()
    {
        // Arrange
        using var tokenSource = new CancellationTokenSource();
        var token = tokenSource.Token;
        var topic = CreateTopic();
        var watermarks = new[]
        {
            CreateWatermark(topic, 1),
            CreateWatermark(topic, 2)
        };
        var firstMessage = CreateMessage("key1", "value1", 1);
        var secondMessage = CreateMessage("key2", "value2", 2);
        var keyFilterMock = new Mock<IDataFilter<object>>(MockBehavior.Strict);
        var valueFilterMock = new Mock<IDataFilter<object>>(MockBehavior.Strict);
        var partitionReaderMock = new Mock<IPartitionSnapshotReader<object, object>>(MockBehavior.Strict);
        partitionReaderMock.Setup(x => x.Read(
            watermarks[0],
            topic,
            keyFilterMock.Object,
            valueFilterMock.Object,
            token)).Returns([firstMessage]);
        partitionReaderMock.Setup(x => x.Read(
            watermarks[1],
            topic,
            keyFilterMock.Object,
            valueFilterMock.Object,
            token)).Returns([secondMessage]);
        var reader = new PartitionSnapshotBatchReader<object, object>(
            Options.Create(new SnapshotLoaderConfiguration { MaxConcurrentPartitions = 1 }),
            partitionReaderMock.Object);

        // Act
        var result = await reader.ReadAllAsync(
            watermarks,
            topic,
            keyFilterMock.Object,
            valueFilterMock.Object,
            token);

        // Assert
        result.Should().BeEquivalentTo([firstMessage, secondMessage]);
    }

    private static LoadingTopic CreateTopic()
        =>
        new(
            new TopicName("test"),
            false,
            new DateFilterRange(null!, null!),
            EncoderRules.String,
            null);

    private static PartitionWatermark CreateWatermark(LoadingTopic topic, int partition)
        =>
        new(
            topic,
            new WatermarkOffsets(new Offset(0), new Offset(1)),
            new Partition(partition));

    private static KeyValuePair<object, KafkaMessage<object>> CreateMessage(
        object key,
        object message,
        int partition)
        =>
        new(
            key,
            new KafkaMessage<object>(
                message,
                new KafkaMetadata(DateTime.UtcNow, partition, 0)));
}
