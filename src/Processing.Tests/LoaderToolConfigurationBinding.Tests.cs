using System.Text;

using FluentAssertions;

using KafkaSnapshot.Processing.Configuration;

using Microsoft.Extensions.Configuration;

using Xunit;

namespace KafkaSnapshot.Processing.Tests;

public class LoaderToolConfigurationBindingTests
{
    [Fact(DisplayName = "LoaderToolConfiguration binds offset dates with UTC and explicit offsets.")]
    [Trait("Category", "Unit")]
    public void LoaderToolConfigurationBindsOffsetDates()
    {
        // Arrange
        const string json = """
        {
          "LoaderToolConfiguration": {
            "Topics": [
              {
                "Name": "utc-topic",
                "ExportFileName": "utc-topic.json",
                "OffsetStartDate": "2024-01-01T10:00:00Z",
                "OffsetEndDate": "2024-01-01T11:00:00Z"
              },
              {
                "Name": "offset-topic",
                "ExportFileName": "offset-topic.json",
                "OffsetStartDate": "2024-01-01T13:00:00+03:00",
                "OffsetEndDate": "2024-01-01T14:00:00+03:00"
              }
            ]
          }
        }
        """;

        using var stream = new MemoryStream(Encoding.UTF8.GetBytes(json));
        var configuration = new ConfigurationBuilder()
            .AddJsonStream(stream)
            .Build();

        // Act
        var result = configuration
            .GetSection(nameof(LoaderToolConfiguration))
            .Get<LoaderToolConfiguration>();

        // Assert
        result.Should().NotBeNull();
        result!.Topics.Should().HaveCount(2);

        result.Topics[0].OffsetStartDate.Should().Be(
            new DateTimeOffset(2024, 1, 1, 10, 0, 0, TimeSpan.Zero));
        result.Topics[0].OffsetEndDate.Should().Be(
            new DateTimeOffset(2024, 1, 1, 11, 0, 0, TimeSpan.Zero));

        result.Topics[1].OffsetStartDate.Should().Be(
            new DateTimeOffset(2024, 1, 1, 13, 0, 0, TimeSpan.FromHours(3)));
        result.Topics[1].OffsetEndDate.Should().Be(
            new DateTimeOffset(2024, 1, 1, 14, 0, 0, TimeSpan.FromHours(3)));
    }
}
