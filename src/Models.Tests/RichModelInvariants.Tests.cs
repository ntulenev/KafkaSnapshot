using FluentAssertions;

using KafkaSnapshot.Models.Filters;
using KafkaSnapshot.Models.Import;
using KafkaSnapshot.Models.Names;
using KafkaSnapshot.Models.Processing;

using Xunit;

namespace KafkaSnapshot.Models.Tests;

[Trait("Category", "Unit")]
public class RichModelInvariantsTests
{
    [Theory(DisplayName = "Key filters accept compatible operations and typed values.")]
    [InlineData(FilterType.Equals, KeyType.Long)]
    [InlineData(FilterType.Equals, KeyType.String)]
    [InlineData(FilterType.Equals, KeyType.Json)]
    [InlineData(FilterType.Contains, KeyType.String)]
    [InlineData(FilterType.GreaterOrEquals, KeyType.Long)]
    [InlineData(FilterType.LessOrEquals, KeyType.Long)]
    [InlineData(FilterType.None, KeyType.Ignored)]
    public void CompatibleFiltersAreValid(FilterType type, KeyType keyType)
    {
        object value = keyType == KeyType.Long ? 42L : "value";
        var filter = new KeyFilterDefinition<object>(type, keyType, value);
        filter.Type.Should().Be(type);
        filter.KeyType.Should().Be(keyType);
        filter.Value.Should().Be(value);
    }

    [Theory(DisplayName = "Key filters reject incompatible operations or incorrectly typed values.")]
    [InlineData(FilterType.Contains, KeyType.Long, "value")]
    [InlineData(FilterType.Equals, KeyType.Ignored, "value")]
    [InlineData(FilterType.Equals, KeyType.Long, "value")]
    [InlineData(FilterType.Equals, KeyType.String, 42L)]
    [InlineData(FilterType.Equals, KeyType.String, null)]
    [InlineData((FilterType)999, KeyType.String, "value")]
    [InlineData(FilterType.None, (KeyType)999, null)]
    public void InvalidFiltersAreRejected(FilterType type, KeyType keyType, object? value)
    {
        Action act = () => _ = new KeyFilterDefinition<object?>(type, keyType, value);
        act.Should().Throw<ArgumentException>();
    }

    [Fact(DisplayName = "Processing topics retain their partition selection when the input collection changes.")]
    public void PartitionSelectionIsImmutable()
    {
        var partitions = new HashSet<int> { 1, 2 };
        var topic = Topic(KeyType.String, false, partitions);
        partitions.Clear();
        topic.PartitionIdsFilter.Should().BeEquivalentTo([1, 2]);
        topic.CreateLoadingParams().IncludesPartition(1).Should().BeTrue();
        topic.CreateLoadingParams().IncludesPartition(3).Should().BeFalse();
        Topic(KeyType.String, false, null).CreateLoadingParams().IncludesPartition(3).Should().BeTrue();
    }

    [Fact(DisplayName = "Processing topics reject compaction for ignored keys.")]
    public void IgnoredKeysCannotBeCompacted()
    {
        Action act = () => Topic(KeyType.Ignored, true, null);
        act.Should().Throw<ArgumentException>();
        Topic(KeyType.Ignored, false, null).LoadWithCompacting.Should().BeFalse();
    }

    [Fact(DisplayName = "Partition selection rejects negative partition ids.")]
    public void NegativePartitionsAreRejected()
    {
        Action act = () => Topic(KeyType.String, false, new HashSet<int> { -1 });
        act.Should().Throw<ArgumentOutOfRangeException>();
    }

    [Theory(DisplayName = "Date range upper bounds are inclusive and optional.")]
    [InlineData(-1, false)]
    [InlineData(0, false)]
    [InlineData(1, true)]
    public void DateUpperBoundIsInclusive(int seconds, bool afterEnd)
    {
        var end = DateTimeOffset.UnixEpoch;
        new DateFilterRange(null, end).IsAfterEnd(end.AddSeconds(seconds)).Should().Be(afterEnd);
        new DateFilterRange(null, null).IsAfterEnd(end.AddSeconds(seconds)).Should().BeFalse();
    }

    [Theory(DisplayName = "Compatibility rules reject unsupported filter operations.")]
    [InlineData((FilterType)999, KeyType.String)]
    [InlineData(FilterType.Contains, KeyType.Json)]
    [InlineData(FilterType.GreaterOrEquals, KeyType.String)]
    [InlineData(FilterType.LessOrEquals, KeyType.Ignored)]
    public void UnsupportedOperationsAreRejected(FilterType type, KeyType keyType)
        => KeyFilterRules.IsSupported(type, keyType).Should().BeFalse();

    private static ProcessingTopic<string> Topic(KeyType keyType, bool compact, IReadOnlySet<int>? partitions)
        => new(new TopicName("orders"), new FileName("orders.json"), compact,
            FilterType.None, keyType, null!, new DateFilterRange(null, null), false,
            EncoderRules.String, partitions);
}
