using FluentAssertions;

using KafkaSnapshot.Models.Filters;
using KafkaSnapshot.Processing.Configuration;
using KafkaSnapshot.Processing.Configuration.Validation;

using Xunit;

namespace KafkaSnapshot.Processing.Tests;

[Trait("Category", "Unit")]
public class DomainConfigurationValidationTests
{
    [Theory(DisplayName = "Configuration rejects invalid domain selections before services are created.")]
    [InlineData("operation", "KS_CFG_034")]
    [InlineData("number", "KS_CFG_035")]
    [InlineData("overflow", "KS_CFG_035")]
    [InlineData("empty", "KS_CFG_036")]
    [InlineData("negative", "KS_CFG_036")]
    public void InvalidDomainSelectionsFailAtStartup(string scenario, string code)
    {
        var topic = new TopicConfiguration
        {
            Name = "orders",
            ExportFileName = "orders.json",
            KeyType = KeyType.Long,
            FilterKeyType = scenario == "operation" ? FilterType.Contains : FilterType.Equals,
            FilterKeyValue = scenario switch
            {
                "number" => "abc",
                "overflow" => "9223372036854775808",
                _ => "42"
            },
            PartitionsIds = scenario switch
            {
                "empty" => [],
                "negative" => [-1],
                _ => null
            }
        };
        var result = new LoaderToolConfigurationValidator().Validate(null,
            new LoaderToolConfiguration { Topics = [topic] });
        result.Failed.Should().BeTrue();
        result.Failures.Should().ContainSingle().Which.Should().StartWith(code);
    }
}
