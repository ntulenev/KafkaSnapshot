using FluentAssertions;
using KafkaSnapshot.Models.Import;
using KafkaSnapshot.Models.Names;
using KafkaSnapshot.Models.Processing;

using Xunit;

namespace KafkaSnapshot.Models.Tests;

public class ProcessingTopicTests
{
    [Fact(DisplayName = "ProcessingTopic can be created.")]
    [Trait("Category", "Unit")]
    public void CanCreateProcessingTopic()
    {
        // Arrange
        var name = new TopicName("Test1");
        var exName = new FileName("Test2");
        var loadCompact = true;
        var filterType = Filters.FilterType.Equals;
        var keyType = Filters.KeyType.Long;
        var dateRange = new Filters.DateFilterRange(null!, null!);
        var filterKeyValue = 1;
        var isRaw = true;
        var partitions = new HashSet<int> { 1, 2, 3 };

        // Act
        var exception = Record.Exception(() => _ =
        new ProcessingTopic<int>(name, exName, loadCompact, filterType, keyType, filterKeyValue, dateRange, isRaw, EncoderRules.String, partitions));

        // Assert
        exception.Should().BeNull();
    }

    [Fact(DisplayName = "ProcessingTopic can be created with valid params.")]
    [Trait("Category", "Unit")]
    public void CanCreateProcessingTopicWithValidParams()
    {
        // Arrange
        var name = new TopicName("Test1");
        var exName = new FileName("Test2");
        var loadCompact = true;
        var filterType = Filters.FilterType.Equals;
        var keyType = Filters.KeyType.Long;
        var dateRange = new Filters.DateFilterRange(null!, null!);
        var filterKeyValue = 1;
        var isRaw = true;
        var partitions = new HashSet<int> { 1, 2, 3 };

        // Act
        var result =
        new ProcessingTopic<int>(name, exName, loadCompact, filterType, keyType, filterKeyValue, dateRange, isRaw, EncoderRules.String, partitions);

        // Assert
        result.TopicName.Should().Be(name);
        result.ExportName.Should().Be(exName);
        result.LoadWithCompacting.Should().Be(loadCompact);
        result.FilterKeyType.Should().Be(filterType);
        result.KeyType.Should().Be(keyType);
        result.DateRange.Should().Be(dateRange);
        result.FilterKeyValue.Should().Be(filterKeyValue);
        result.KeyType.Should().Be(keyType);
        result.ExportRawMessage.Should().Be(isRaw);
        result.PartitionIdsFilter.Should().BeEquivalentTo(partitions);
    }

    [Fact(DisplayName = "Can Create LoadingTopic from ProcessingTopic.")]
    [Trait("Category", "Unit")]
    public void CanCreateLoadingTopicFromProcessingTopic()
    {
        // Arrange
        var name = new TopicName("Test1");
        var exName = new FileName("Test2");
        var loadCompact = true;
        var filterType = Filters.FilterType.Equals;
        var keyType = Filters.KeyType.Long;
        var dateRange = new Filters.DateFilterRange(null!, null!);
        var filterKeyValue = 1;
        var isRaw = true;
        var partitions = new HashSet<int> { 1, 2, 3 };
        var processingTopic = new ProcessingTopic<int>(name, exName, loadCompact, filterType, keyType, filterKeyValue, dateRange, isRaw, EncoderRules.String, partitions);

        // Act
        var exception = Record.Exception(() => _ = processingTopic.CreateLoadingParams());

        // Assert
        exception.Should().BeNull();
    }

    [Fact(DisplayName = "Can Create LoadingTopic from ProcessingTopic with correct params.")]
    [Trait("Category", "Unit")]
    public void CanCreateLoadingTopicFromProcessingTopicWithCorrectParams()
    {
        // Arrange
        var name = new TopicName("Test1");
        var exName = new FileName("Test2");
        var loadCompact = true;
        var filterType = Filters.FilterType.Equals;
        var keyType = Filters.KeyType.Long;
        var dateRange = new Filters.DateFilterRange(null!, null!);
        var filterKeyValue = 1;
        var isRaw = true;
        var partitions = new HashSet<int> { 1, 2, 3 };
        var processingTopic = new ProcessingTopic<int>(name, exName, loadCompact, filterType, keyType, filterKeyValue, dateRange, isRaw, EncoderRules.String, partitions);

        // Act
        var result = processingTopic.CreateLoadingParams();

        // Assert
        result.Value.Should().Be(name);
        result.LoadWithCompacting.Should().Be(loadCompact);
        result.HasOffsetDate.Should().BeFalse();
        result.HasEndOffsetDate.Should().BeFalse();
        result.HasPartitionFilter.Should().BeTrue();
        result.PartitionFilter.Should().BeEquivalentTo(partitions);
    }

    [Fact(DisplayName = "Can Create LoadingTopic from ProcessingTopic with correct params with dates.")]
    [Trait("Category", "Unit")]
    public void CanCreateLoadingTopicFromProcessingTopicWithCorrectParamsWithDates()
    {
        // Arrange
        var name = new TopicName("Test1");
        var exName = new FileName("Test2");
        var loadCompact = true;
        var filterType = Filters.FilterType.Equals;
        var keyType = Filters.KeyType.Long;
        var dtStart = new DateTime(2022, 1, 1);
        var dtEnd = new DateTime(2023, 1, 1);
        var dateRange = new Filters.DateFilterRange(dtStart, dtEnd);
        var filterKeyValue = 1;
        var isRaw = true;
        var partitions = new HashSet<int> { 1, 2, 3 };
        var processingTopic = new ProcessingTopic<int>(name, exName, loadCompact, filterType, keyType, filterKeyValue, dateRange, isRaw, EncoderRules.String, partitions);

        // Act
        var result = processingTopic.CreateLoadingParams();

        // Assert
        result.Value.Should().Be(name);
        result.LoadWithCompacting.Should().Be(loadCompact);
        result.HasOffsetDate.Should().BeTrue();
        result.OffsetDate.Should().Be(dtStart);
        result.EndOffsetDate.Should().Be(dtEnd);
        result.HasEndOffsetDate.Should().BeTrue();
        result.HasPartitionFilter.Should().BeTrue();
        result.PartitionFilter.Should().BeEquivalentTo(partitions);
    }

    [Fact(DisplayName = "Can Create ExportTopic from ProcessingTopic.")]
    [Trait("Category", "Unit")]
    public void CanCreateExportTopicFromProcessingTopic()
    {
        // Arrange
        var name = new TopicName("Test1");
        var exName = new FileName("Test2");
        var loadCompact = true;
        var filterType = Filters.FilterType.Equals;
        var keyType = Filters.KeyType.Long;
        var dateRange = new Filters.DateFilterRange(null!, null!);
        var filterKeyValue = 1;
        var isRaw = true;
        var partitions = new HashSet<int> { 1, 2, 3 };
        var processingTopic = new ProcessingTopic<int>(name, exName, loadCompact, filterType, keyType, filterKeyValue, dateRange, isRaw, EncoderRules.String, partitions);

        // Act
        var exception = Record.Exception(() => _ = processingTopic.CreateExportParams());

        // Assert
        exception.Should().BeNull();
    }

    [Fact(DisplayName = "Can Create ExportTopic from ProcessingTopic with correct params.")]
    [Trait("Category", "Unit")]
    public void CanCreateExportTopicFromProcessingTopicWithCorrectParams()
    {
        // Arrange
        var name = new TopicName("Test1");
        var exName = new FileName("Test2");
        var loadCompact = true;
        var filterType = Filters.FilterType.Equals;
        var keyType = Filters.KeyType.Long;
        var dateRange = new Filters.DateFilterRange(null!, null!);
        var filterKeyValue = 1;
        var isRaw = true;
        var partitions = new HashSet<int> { 1, 2, 3 };
        var processingTopic = new ProcessingTopic<int>(name, exName, loadCompact, filterType, keyType, filterKeyValue, dateRange, isRaw, EncoderRules.String, partitions);

        // Act
        var result = processingTopic.CreateExportParams();

        // Assert
        result.TopicName.Should().Be(name);
        result.ExportName.Should().Be(exName);
        result.ExportRawMessage.Should().Be(isRaw);
    }
}
