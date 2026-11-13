using FluentAssertions;

using KafkaSnapshot.Models.Filters;
using KafkaSnapshot.Processing.Configuration;
using KafkaSnapshot.Processing.Configuration.Validation;

using Xunit;

namespace KafkaSnapshot.Processing.Tests;

[Trait("Category", "Unit")]
public class ConfigurationDomainParityTests
{
    [Theory(DisplayName = "Startup validation and domain mapping agree on key filter invariants.")]
    [InlineData(FilterType.Equals, KeyType.Long, "42", true)]
    [InlineData(FilterType.Equals, KeyType.Long, "bad", false)]
    [InlineData(FilterType.Equals, KeyType.Long, null, false)]
    [InlineData(FilterType.Contains, KeyType.Long, "bad", false)]
    [InlineData(FilterType.None, KeyType.Long, "unused", true)]
    [InlineData(FilterType.Equals, KeyType.String, null, false)]
    [InlineData(FilterType.Equals, KeyType.String, "", true)]
    [InlineData(FilterType.Contains, KeyType.String, "text", true)]
    [InlineData(FilterType.GreaterOrEquals, KeyType.String, "text", false)]
    [InlineData(FilterType.Equals, KeyType.Ignored, "text", false)]
    [InlineData(FilterType.None, KeyType.Ignored, null, true)]
    public void FilterValidationMatchesDomain(FilterType operation, KeyType keyType,
        string? value, bool valid)
    {
        var topic = new TopicConfiguration
        {
            Name = "orders",
            ExportFileName = "orders.json",
            Compacting = CompactingMode.Off,
            KeyType = keyType,
            FilterKeyType = operation,
            FilterKeyValue = value
        };
        var result = new LoaderToolConfigurationValidator().Validate(null,
            new LoaderToolConfiguration { Topics = [topic] });
        var exception = Record.Exception(() =>
        {
            if (keyType == KeyType.Long)
            {
                _ = TopicConfigurationMapper.ToProcessingTopic<long>(topic);
            }
            else
            {
                _ = TopicConfigurationMapper.ToProcessingTopic<string>(topic);
            }
        });
        result.Succeeded.Should().Be(valid);
        (exception is null).Should().Be(valid);
    }
}
