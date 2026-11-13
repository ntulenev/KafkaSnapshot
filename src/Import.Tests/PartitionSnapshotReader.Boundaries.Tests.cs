using Confluent.Kafka;

using FluentAssertions;

using KafkaSnapshot.Abstractions.Import;

using Moq;

using Xunit;

namespace KafkaSnapshot.Import.Tests;

public partial class PartitionSnapshotReaderTests
{
    [Fact(DisplayName = "Date-limited reading stops at the first later timestamp even if a subsequent message is earlier.")]
    [Trait("Category", "Unit")]
    public void DateBoundaryPreservesStopOnFirstLaterTimestamp()
    {
        var end = DateTimeOffset.UnixEpoch.AddDays(1);
        var topic = CreateTopic(new KafkaSnapshot.Models.Filters.DateFilterRange(null, end));
        var partition = CreatePartition();
        var consumer = CreateReadableConsumerMock(topic, partition, new Offset(0), CancellationToken.None);
        consumer.SetupSequence(x => x.Consume(CancellationToken.None))
            .Returns(new ConsumeResult<object, byte[]>
            {
                Offset = new Offset(0),
                Message = new Message<object, byte[]> { Timestamp = new Timestamp(end.AddSeconds(1)) }
            })
            .Returns(new ConsumeResult<object, byte[]>
            {
                Offset = new Offset(1),
                Message = new Message<object, byte[]> { Timestamp = new Timestamp(end.AddSeconds(-1)) }
            });
        var encoder = new Mock<IMessageEncoder<byte[], object>>(MockBehavior.Strict);
        var (keyFilter, valueFilter) = CreateStrictFilters();
        var reader = CreateReader(CreateLoggerMock().Object, consumer, encoder);

        reader.Read(CreateWatermark(topic, partition, 2), topic, keyFilter.Object,
            valueFilter.Object, CancellationToken.None).Should().BeEmpty();

        consumer.Verify(x => x.Consume(CancellationToken.None), Times.Once);
        encoder.VerifyNoOtherCalls();
        consumer.Verify(x => x.Close(), Times.Once);
        consumer.Verify(x => x.Dispose(), Times.Once);
    }

    [Theory(DisplayName = "Partition reader excludes offsets at or beyond the captured upper watermark.")]
    [Trait("Category", "Unit")]
    [InlineData(-1)]
    [InlineData(1)]
    [InlineData(5)]
    public void UpperWatermarkStopsBeforeDecoding(long offset)
    {
        var topic = CreateTopic();
        var partition = CreatePartition();
        var consumer = CreateReadableConsumerMock(topic, partition, new Offset(offset), CancellationToken.None);
        var encoder = new Mock<IMessageEncoder<byte[], object>>(MockBehavior.Strict);
        var (keyFilter, valueFilter) = CreateStrictFilters();
        var reader = CreateReader(CreateLoggerMock().Object, consumer, encoder);
        reader.Read(CreateWatermark(topic, partition), topic, keyFilter.Object, valueFilter.Object,
            CancellationToken.None).Should().BeEmpty();
        encoder.VerifyNoOtherCalls();
        consumer.Verify(x => x.Close(), Times.Once);
        consumer.Verify(x => x.Dispose(), Times.Once);
    }
}
