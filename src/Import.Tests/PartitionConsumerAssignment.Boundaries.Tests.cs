using Confluent.Kafka;

using FluentAssertions;

using KafkaSnapshot.Import.Kafka;
using KafkaSnapshot.Models.Filters;
using KafkaSnapshot.Models.Import;
using KafkaSnapshot.Models.Names;

using Moq;

using Xunit;

namespace KafkaSnapshot.Import.Tests;

[Trait("Category", "Unit")]
public class PartitionConsumerAssignmentBoundariesTests
{
    [Fact(DisplayName = "Consumer assignment uses the captured start offset instead of group commits.")]
    public void CapturedLowIsAssignedExplicitly()
    {
        var topic = new LoadingTopic(new TopicName("orders"), false, new DateFilterRange(null, null), EncoderRules.String, null);
        var consumer = new Mock<IConsumer<string, byte[]>>(MockBehavior.Strict);
        consumer.Setup(x => x.Assign(It.Is<TopicPartitionOffset>(p => p.Topic == "orders" && p.Partition.Value == 2 && p.Offset.Value == 7)));
        new PartitionConsumerAssignment().Assign(new PartitionWatermark(topic, new OffsetRange(7, 10), 2), consumer.Object);
        consumer.VerifyAll();
    }

    [Fact(DisplayName = "A date offset beyond the captured end does not extend the snapshot.")]
    public void DateAfterSnapshotDoesNotAssignConsumer()
    {
        var topic = new LoadingTopic(new TopicName("orders"), false, new DateFilterRange(null, null), EncoderRules.String, null);
        var consumer = new Mock<IConsumer<string, byte[]>>(MockBehavior.Strict);
        consumer.Setup(x => x.OffsetsForTimes(It.IsAny<IEnumerable<TopicPartitionTimestamp>>(), It.IsAny<TimeSpan>()))
            .Returns([new TopicPartitionOffset("orders", 0, 12)]);
        var result = new PartitionConsumerAssignment().Assign(new PartitionWatermark(topic, new OffsetRange(0, 10), 0),
            consumer.Object, DateTimeOffset.UnixEpoch, TimeSpan.FromSeconds(1), out var assigned);
        result.Should().BeFalse();
        assigned.Offset.Value.Should().Be(12);
        consumer.VerifyAll();
        consumer.VerifyNoOtherCalls();
    }
    [Fact(DisplayName = "A date offset below the captured low is clamped to the snapshot start.")]
    public void DateBeforeSnapshotStartsAtCapturedLow()
    {
        var topic = new LoadingTopic(new TopicName("orders"), false, new DateFilterRange(null, null), EncoderRules.String, null);
        var consumer = new Mock<IConsumer<string, byte[]>>(MockBehavior.Strict);
        consumer.Setup(x => x.OffsetsForTimes(It.IsAny<IEnumerable<TopicPartitionTimestamp>>(), It.IsAny<TimeSpan>()))
            .Returns([new TopicPartitionOffset("orders", 0, 0)]);
        consumer.Setup(x => x.Assign(It.Is<TopicPartitionOffset>(p => p.Offset.Value == 7)));
        var result = new PartitionConsumerAssignment().Assign(new PartitionWatermark(topic, new OffsetRange(7, 10), 0),
            consumer.Object, DateTimeOffset.UnixEpoch, TimeSpan.FromSeconds(1), out var assigned);
        result.Should().BeTrue();
        assigned.Offset.Value.Should().Be(7);
        consumer.VerifyAll();
    }
}
