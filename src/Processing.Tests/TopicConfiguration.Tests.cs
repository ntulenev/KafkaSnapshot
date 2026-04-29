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
        var result = TopicConfigurationMapper.ToProcessingTopic<long>(config);

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
        var result = TopicConfigurationMapper.ToProcessingTopic<string>(config);

        // Assert
        result.FilterKeyValue.Should().BeNull();
        result.PartitionIdsFilter.Should().BeNull();
        result.LoadWithCompacting.Should().BeTrue();
    }

    [Fact(DisplayName = "TopicConfiguration can't convert null configuration.")]
    [Trait("Category", "Unit")]
    public void TopicConfigurationCantConvertNullConfiguration()
    {
        // Act
        var exception = Record.Exception(() =>
            TopicConfigurationMapper.ToProcessingTopic<string>(null!));

        // Assert
        exception.Should().NotBeNull().And.BeOfType<ArgumentNullException>();
    }

    [Theory(DisplayName = "TopicConfiguration can convert string filter values.")]
    [Trait("Category", "Unit")]
    [InlineData(KeyType.String)]
    [InlineData(KeyType.Json)]
    public void TopicConfigurationCanConvertStringFilterValues(KeyType keyType)
    {
        // Arrange
        var config = new TopicConfiguration
        {
            Name = "topic-3",
            ExportFileName = "topic-3.json",
            FilterKeyType = FilterType.Equals,
            KeyType = keyType,
            FilterKeyValue = "value",
            MessageEncoderRule = EncoderRules.String
        };

        // Act
        var result = TopicConfigurationMapper.ToProcessingTopic<string>(config);

        // Assert
        result.FilterKeyValue.Should().Be("value");
    }

    [Fact(DisplayName = "TopicConfiguration ignores filter value for ignored key.")]
    [Trait("Category", "Unit")]
    public void TopicConfigurationIgnoresFilterValueForIgnoredKey()
    {
        // Arrange
        var config = new TopicConfiguration
        {
            Name = "topic-4",
            ExportFileName = "topic-4.json",
            FilterKeyType = FilterType.Equals,
            KeyType = KeyType.Ignored,
            FilterKeyValue = "value",
            MessageEncoderRule = EncoderRules.String
        };

        // Act
        var result = TopicConfigurationMapper.ToProcessingTopic<string>(config);

        // Assert
        result.FilterKeyValue.Should().BeNull();
    }

    [Fact(DisplayName = "TopicConfiguration can't convert invalid key type.")]
    [Trait("Category", "Unit")]
    public void TopicConfigurationCantConvertInvalidKeyType()
    {
        // Arrange
        var config = new TopicConfiguration
        {
            Name = "topic-5",
            ExportFileName = "topic-5.json",
            FilterKeyType = FilterType.Equals,
            KeyType = (KeyType)999,
            FilterKeyValue = "value",
            MessageEncoderRule = EncoderRules.String
        };

        // Act
        var exception = Record.Exception(() =>
            TopicConfigurationMapper.ToProcessingTopic<string>(config));

        // Assert
        exception.Should().NotBeNull().And.BeOfType<InvalidOperationException>();
    }
}
