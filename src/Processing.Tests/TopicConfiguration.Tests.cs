using FluentAssertions;

using KafkaSnapshot.Models.Filters;
using KafkaSnapshot.Models.Import;
using KafkaSnapshot.Processing.Configuration;

using Xunit;

namespace KafkaSnapshot.Processing.Tests;

public class TopicConfigurationTests
{
    [Fact(DisplayName = "TopicConfiguration can convert to processing topic with all fields.")]
    [Trait("Category", "Unit")]
    public void TopicConfigurationCanConvertToProcessingTopic()
    {
        // Arrange
        var startDate = new DateTime(2024, 1, 2, 3, 4, 5, DateTimeKind.Utc);
        var endDate = startDate.AddHours(1);
        var partitions = new HashSet<int> { 1, 2, 3 };
        var config = new TopicConfiguration
        {
            Name = "topic-1",
            ExportFileName = "topic-1.json",
            Compacting = CompactingMode.Off,
            FilterKeyType = FilterType.Equals,
            KeyType = KeyType.Long,
            FilterKeyValue = "42",
            OffsetStartDate = startDate,
            OffsetEndDate = endDate,
            ExportRawMessage = true,
            MessageEncoderRule = EncoderRules.Base64,
            PartitionsIds = partitions
        };

        // Act
        var result = config.ConvertToProcess<long>();

        // Assert
        config.PartitionsIds.Should().BeSameAs(partitions);
        result.TopicName.Name.Should().Be("topic-1");
        result.ExportName.FullName.Should().EndWith("topic-1.json");
        result.LoadWithCompacting.Should().BeFalse();
        result.FilterKeyType.Should().Be(FilterType.Equals);
        result.KeyType.Should().Be(KeyType.Long);
        result.FilterKeyValue.Should().Be(42L);
        result.DateRange.StartDate.Should().Be(startDate);
        result.DateRange.EndDate.Should().Be(endDate);
        result.ExportRawMessage.Should().BeTrue();
        result.ValueEncoderRule.Should().Be(EncoderRules.Base64);
        result.PartitionIdsFilter.Should().BeEquivalentTo(partitions);
    }

    [Fact(DisplayName = "TopicConfiguration can convert to processing topic without filter value.")]
    [Trait("Category", "Unit")]
    public void TopicConfigurationCanConvertWithoutFilterValue()
    {
        // Arrange
        var config = new TopicConfiguration
        {
            Name = "topic-2",
            ExportFileName = "topic-2.json",
            Compacting = CompactingMode.On,
            FilterKeyType = FilterType.None,
            KeyType = KeyType.String,
            FilterKeyValue = null,
            ExportRawMessage = false,
            MessageEncoderRule = EncoderRules.String,
            PartitionsIds = null
        };

        // Act
        var result = config.ConvertToProcess<string>();

        // Assert
        result.FilterKeyValue.Should().BeNull();
        result.PartitionIdsFilter.Should().BeNull();
        result.LoadWithCompacting.Should().BeTrue();
    }
}
