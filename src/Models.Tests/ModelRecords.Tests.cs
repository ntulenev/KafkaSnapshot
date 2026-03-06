using FluentAssertions;

using KafkaSnapshot.Models.Message;
using KafkaSnapshot.Models.Sorting;

using Xunit;

namespace KafkaSnapshot.Models.Tests;

public class ModelRecordsTests
{
    [Fact(DisplayName = "Kafka metadata properties can be read.")]
    [Trait("Category", "Unit")]
    public void KafkaMetadataPropertiesCanBeRead()
    {
        // Arrange
        var timestamp = new DateTime(2025, 1, 2, 3, 4, 5, DateTimeKind.Utc);
        var metadata = new KafkaMetadata(timestamp, 7, 11);

        // Assert
        metadata.Timestamp.Should().Be(timestamp);
        metadata.Partition.Should().Be(7);
        metadata.Offset.Should().Be(11);
    }

    [Fact(DisplayName = "Kafka message properties can be read.")]
    [Trait("Category", "Unit")]
    public void KafkaMessagePropertiesCanBeRead()
    {
        // Arrange
        var metadata = new KafkaMetadata(DateTime.UtcNow, 1, 2);
        var message = new KafkaMessage<string>("payload", metadata);

        // Assert
        message.Message.Should().Be("payload");
        message.Meta.Should().Be(metadata);
    }

    [Fact(DisplayName = "Sorting params properties can be read.")]
    [Trait("Category", "Unit")]
    public void SortingParamsPropertiesCanBeRead()
    {
        // Arrange
        var sortingParams = new SortingParams(SortingType.Time, SortingOrder.Ask);

        // Assert
        sortingParams.Type.Should().Be(SortingType.Time);
        sortingParams.Order.Should().Be(SortingOrder.Ask);
    }
}
