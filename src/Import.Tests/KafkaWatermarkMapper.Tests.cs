using Confluent.Kafka;

using FluentAssertions;

using KafkaSnapshot.Import.Kafka;
using KafkaSnapshot.Models.Filters;
using KafkaSnapshot.Models.Import;
using KafkaSnapshot.Models.Names;

using Xunit;

namespace KafkaSnapshot.Import.Tests;

[Trait("Category", "Unit")]
public class KafkaWatermarkMapperTests
{
    [Theory(DisplayName = "Kafka watermark mapping rejects special or inconsistent broker offsets.")]
    [InlineData(-1, 10)]
    [InlineData(-2, 10)]
    [InlineData(-1001, 10)]
    [InlineData(10, 5)]
    public void InvalidBrokerOffsetsAreRejected(long low, long high)
    {
        var topic = new LoadingTopic(new TopicName("orders"), false,
            new DateFilterRange(null, null), EncoderRules.String, null);
        Action act = () => _ = KafkaWatermarkMapper.ToDomain(topic,
            new WatermarkOffsets(new Offset(low), new Offset(high)), new Partition(0));
        act.Should().Throw<ArgumentOutOfRangeException>();
    }
}
