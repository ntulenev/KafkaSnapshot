using FluentAssertions;

using KafkaSnapshot.Models.Export;
using KafkaSnapshot.Models.Message;
using KafkaSnapshot.Models.Names;
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
        var timestamp = new DateTimeOffset(2025, 1, 2, 3, 4, 5, TimeSpan.Zero);
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

    [Fact(DisplayName = "KafkaMessage can't be created with null message.")]
    [Trait("Category", "Unit")]
    public void KafkaMessageCantBeCreatedWithNullMessage()
    {
        // Arrange
        var metadata = new KafkaMetadata(DateTime.UtcNow, 1, 2);

        // Act
        var exception = Record.Exception(() => new KafkaMessage<string>(null!, metadata));

        // Assert
        exception.Should().NotBeNull().And.BeOfType<ArgumentNullException>();
    }

    [Fact(DisplayName = "KafkaMessage can't be created with null metadata.")]
    [Trait("Category", "Unit")]
    public void KafkaMessageCantBeCreatedWithNullMetadata()
    {
        // Act
        var exception = Record.Exception(() => new KafkaMessage<string>("payload", null!));

        // Assert
        exception.Should().NotBeNull().And.BeOfType<ArgumentNullException>();
    }

    [Theory(DisplayName = "KafkaMetadata can't be created with negative values.")]
    [Trait("Category", "Unit")]
    [InlineData(-1, 0)]
    [InlineData(0, -1)]
    public void KafkaMetadataCantBeCreatedWithNegativeValues(int partition, long offset)
    {
        // Act
        var exception = Record.Exception(() => new KafkaMetadata(DateTime.UtcNow, partition, offset));

        // Assert
        exception.Should().NotBeNull().And.BeOfType<ArgumentOutOfRangeException>();
    }

    [Fact(DisplayName = "ExportedTopic properties can be read.")]
    [Trait("Category", "Unit")]
    public void ExportedTopicPropertiesCanBeRead()
    {
        // Arrange
        var topicName = new TopicName("topic");
        var exportName = new FileName("topic.json");
        var exportedTopic = new ExportedTopic(topicName, exportName, true);

        // Assert
        exportedTopic.TopicName.Should().Be(topicName);
        exportedTopic.ExportName.Should().Be(exportName);
        exportedTopic.ExportRawMessage.Should().BeTrue();
    }

    [Fact(DisplayName = "ExportedTopic can't be created with null topic name.")]
    [Trait("Category", "Unit")]
    public void ExportedTopicCantBeCreatedWithNullTopicName()
    {
        // Arrange
        var exportName = new FileName("topic.json");

        // Act
        var exception = Record.Exception(() => new ExportedTopic(null!, exportName, true));

        // Assert
        exception.Should().NotBeNull().And.BeOfType<ArgumentNullException>();
    }

    [Fact(DisplayName = "ExportedTopic can't be created with null export name.")]
    [Trait("Category", "Unit")]
    public void ExportedTopicCantBeCreatedWithNullExportName()
    {
        // Arrange
        var topicName = new TopicName("topic");

        // Act
        var exception = Record.Exception(() => new ExportedTopic(topicName, null!, true));

        // Assert
        exception.Should().NotBeNull().And.BeOfType<ArgumentNullException>();
    }

    [Fact(DisplayName = "Sorting params properties can be read.")]
    [Trait("Category", "Unit")]
    public void SortingParamsPropertiesCanBeRead()
    {
        // Arrange
        var sortingParams = new SortingParams(SortingType.Time, SortingOrder.Ascending);

        // Assert
        sortingParams.Type.Should().Be(SortingType.Time);
        sortingParams.Order.Should().Be(SortingOrder.Ascending);
    }

    [Theory(DisplayName = "SortingParams can't be created with invalid enum values.")]
    [Trait("Category", "Unit")]
    [InlineData(999, 0)]
    [InlineData(0, 999)]
    public void SortingParamsCantBeCreatedWithInvalidEnumValues(int type, int order)
    {
        // Act
        var exception = Record.Exception(() =>
            new SortingParams((SortingType)type, (SortingOrder)order));

        // Assert
        exception.Should().NotBeNull().And.BeOfType<ArgumentOutOfRangeException>();
    }
}
