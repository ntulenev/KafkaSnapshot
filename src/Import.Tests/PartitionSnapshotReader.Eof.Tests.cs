using Confluent.Kafka;

using FluentAssertions;

using KafkaSnapshot.Abstractions.Import;
using KafkaSnapshot.Models.Import;

using Moq;

using Xunit;

namespace KafkaSnapshot.Import.Tests;

public partial class PartitionSnapshotReaderTests
{
    [Theory(DisplayName = "Partition EOF completes sparse snapshots and rejects a truncated upper boundary.")]
    [Trait("Category", "Unit")]
    [InlineData(2, true)]
    [InlineData(3, true)]
    [InlineData(1, false)]
    public void EofCompletesSparseSnapshots(long eof, bool valid)
    {
        var topic = CreateTopic();
        var partition = CreatePartition();
        var consumer = CreateReadableConsumerMock(topic, partition, new Offset(0), CancellationToken.None);
        consumer.SetupSequence(x => x.Consume(CancellationToken.None))
            .Returns(new ConsumeResult<object, byte[]> { IsPartitionEOF = true, Offset = new Offset(eof) });
        var encoder = new Mock<IMessageEncoder<byte[], object>>(MockBehavior.Strict);
        var (key, value) = CreateStrictFilters();
        var reader = CreateReader(CreateLoggerMock().Object, consumer, encoder);
        Action act = () => _ = reader.Read(CreateWatermark(topic, partition, 2), topic, key.Object, value.Object, CancellationToken.None).ToList();
        if (valid)
        {
            act.Should().NotThrow();
        }
        else
        {
            act.Should().Throw<InvalidOperationException>().WithMessage("*ended before captured offset*");
        }

        consumer.Verify(x => x.Close(), Times.Once);
        consumer.Verify(x => x.Dispose(), Times.Once);
        encoder.VerifyNoOtherCalls();
    }

    [Fact(DisplayName = "An empty captured interval does not create a consumer.")]
    [Trait("Category", "Unit")]
    public void EmptyIntervalDoesNotConsume()
    {
        var topic = CreateTopic();
        var consumer = new Mock<IConsumer<object, byte[]>>(MockBehavior.Strict);
        var encoder = new Mock<IMessageEncoder<byte[], object>>(MockBehavior.Strict);
        var (key, value) = CreateStrictFilters();
        var reader = CreateReader(CreateLoggerMock().Object, consumer, encoder);
        reader.Read(new PartitionWatermark(topic, new OffsetRange(0, 0), 0), topic,
            key.Object, value.Object, CancellationToken.None).Should().BeEmpty();
        consumer.VerifyNoOtherCalls();
    }
}
