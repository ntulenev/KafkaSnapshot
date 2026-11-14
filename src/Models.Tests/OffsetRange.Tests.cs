using FluentAssertions;

using KafkaSnapshot.Models.Filters;
using KafkaSnapshot.Models.Import;
using KafkaSnapshot.Models.Names;

using Xunit;

namespace KafkaSnapshot.Models.Tests;

[Trait("Category", "Unit")]
public class OffsetRangeTests
{
    [Theory(DisplayName = "Offset ranges are inclusive at low and exclusive at high.")]
    [InlineData(-1, false, true)]
    [InlineData(4, false, true)]
    [InlineData(5, true, true)]
    [InlineData(8, true, true)]
    [InlineData(9, true, false)]
    [InlineData(10, false, false)]
    [InlineData(100, false, false)]
    public void CapturedBoundariesAreRespected(long offset, bool contains, bool continues)
    {
        var range = new OffsetRange(5, 10);
        range.Low.Should().Be(5);
        range.High.Should().Be(10);
        range.IsEmpty.Should().BeFalse();
        range.Contains(offset).Should().Be(contains);
        range.ShouldContinueReadingAfter(offset).Should().Be(continues);
    }

    [Theory(DisplayName = "Empty offset intervals never contain data or require reading.")]
    [InlineData(0)]
    [InlineData(5)]
    [InlineData(long.MaxValue)]
    public void EmptyIntervalsDoNotRead(long boundary)
    {
        var range = new OffsetRange(boundary, boundary);
        range.IsEmpty.Should().BeTrue();
        range.Contains(boundary).Should().BeFalse();
        range.ShouldContinueReadingAfter(-1).Should().BeFalse();
        range.ShouldContinueReadingAfter(boundary).Should().BeFalse();
    }

    [Theory(DisplayName = "Offset ranges reject negative and reversed boundaries.")]
    [InlineData(-1, 10)]
    [InlineData(0, -1)]
    [InlineData(10, 5)]
    public void InvalidIntervalsAreRejected(long low, long high)
    {
        Action act = () => _ = new OffsetRange(low, high);
        act.Should().Throw<ArgumentOutOfRangeException>();
    }

    [Fact(DisplayName = "Offset boundaries near Int64 maximum do not overflow.")]
    public void MaximumOffsetsAreSupported()
    {
        var range = new OffsetRange(long.MaxValue - 2, long.MaxValue);
        range.ShouldContinueReadingAfter(long.MaxValue - 2).Should().BeTrue();
        range.ShouldContinueReadingAfter(long.MaxValue - 1).Should().BeFalse();
        range.Contains(long.MaxValue).Should().BeFalse();
    }

    [Theory(DisplayName = "Requested starts are clamped to low and cannot extend the captured upper boundary.")]
    [InlineData(0, 7, true)]
    [InlineData(7, 7, true)]
    [InlineData(8, 8, true)]
    [InlineData(10, 10, false)]
    [InlineData(12, 12, false)]
    public void StartsRespectCapturedRange(long requested, long expected, bool hasData)
    {
        var range = new OffsetRange(7, 10);
        range.TryResolveStart(requested, out var resolved).Should().Be(hasData);
        resolved.Should().Be(expected);
    }

    [Fact(DisplayName = "Domain watermarks require a topic, interval, and non-negative partition.")]
    public void WatermarkInvariantsAreProtected()
    {
        var topic = new LoadingTopic(new TopicName("orders"), false,
            new DateFilterRange(null, null), EncoderRules.String, null);
        Action missingTopic = () => _ = new PartitionWatermark(null!, new OffsetRange(0, 1), 0);
        Action missingRange = () => _ = new PartitionWatermark(topic, null!, 0);
        Action negativePartition = () => _ = new PartitionWatermark(topic, new OffsetRange(0, 1), -1);
        missingTopic.Should().Throw<ArgumentNullException>();
        missingRange.Should().Throw<ArgumentNullException>();
        negativePartition.Should().Throw<ArgumentOutOfRangeException>();
    }
}
