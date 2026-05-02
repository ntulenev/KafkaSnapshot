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
        new ProcessingTopic<int>(
                name,
                exName,
                loadCompact,
                filterType,
                keyType,
                filterKeyValue,
                dateRange,
                isRaw,
                EncoderRules.String,
                partitions));

        // Assert
        exception.Should().BeNull();
    }

    [Fact(DisplayName = "ProcessingTopic can be created with valid parameters.")]
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
        new ProcessingTopic<int>(
                name,
                exName,
                loadCompact,
                filterType,
                keyType,
                filterKeyValue,
                dateRange,
                isRaw,
                EncoderRules.String,
                partitions);

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

    [Fact(DisplayName = "ProcessingTopic can be created without partition filter.")]
    [Trait("Category", "Unit")]
    public void CanCreateProcessingTopicWithoutPartitionFilter()
    {
        // Arrange
        var name = new TopicName("Test1");
        var exName = new FileName("Test2");
        var dateRange = new Filters.DateFilterRange(null!, null!);

        // Act
        var result = new ProcessingTopic<int>(
            name,
            exName,
            true,
            Filters.FilterType.Equals,
            Filters.KeyType.Long,
            1,
            dateRange,
            true,
            EncoderRules.String);

        // Assert
        result.PartitionIdsFilter.Should().BeNull();
    }

    [Fact(DisplayName = "ProcessingTopic can't be created with null topic name.")]
    [Trait("Category", "Unit")]
    public void ProcessingTopicCannotBeCreatedWithNullTopicName()
    {
        // Arrange
        var exportName = new FileName("Test2");
        var dateRange = new Filters.DateFilterRange(null!, null!);

        // Act
        var exception = Record.Exception(() =>
            new ProcessingTopic<int>(
                null!,
                exportName,
                true,
                Filters.FilterType.Equals,
                Filters.KeyType.Long,
                1,
                dateRange,
                true,
                EncoderRules.String));

        // Assert
        exception.Should().NotBeNull().And.BeOfType<ArgumentNullException>();
    }

    [Fact(DisplayName = "ProcessingTopic can't be created with null export name.")]
    [Trait("Category", "Unit")]
    public void ProcessingTopicCannotBeCreatedWithNullExportName()
    {
        // Arrange
        var topicName = new TopicName("Test1");
        var dateRange = new Filters.DateFilterRange(null!, null!);

        // Act
        var exception = Record.Exception(() =>
            new ProcessingTopic<int>(
                topicName,
                null!,
                true,
                Filters.FilterType.Equals,
                Filters.KeyType.Long,
                1,
                dateRange,
                true,
                EncoderRules.String));

        // Assert
        exception.Should().NotBeNull().And.BeOfType<ArgumentNullException>();
    }

    [Fact(DisplayName = "ProcessingTopic can't be created with null date range.")]
    [Trait("Category", "Unit")]
    public void ProcessingTopicCannotBeCreatedWithNullDateRange()
    {
        // Arrange
        var topicName = new TopicName("Test1");
        var exportName = new FileName("Test2");

        // Act
        var exception = Record.Exception(() =>
            new ProcessingTopic<int>(
                topicName,
                exportName,
                true,
                Filters.FilterType.Equals,
                Filters.KeyType.Long,
                1,
                null!,
                true,
                EncoderRules.String));

        // Assert
        exception.Should().NotBeNull().And.BeOfType<ArgumentNullException>();
    }

    [Theory(DisplayName = "ProcessingTopic can't be created with invalid enum values.")]
    [Trait("Category", "Unit")]
    [InlineData(999, 0, 0)]
    [InlineData(0, 999, 0)]
    [InlineData(0, 0, 999)]
    public void ProcessingTopicCannotBeCreatedWithInvalidEnumValues(
        int filterType,
        int keyType,
        int encoderRule)
    {
        // Arrange
        var topicName = new TopicName("Test1");
        var exportName = new FileName("Test2");
        var dateRange = new Filters.DateFilterRange(null!, null!);

        // Act
        var exception = Record.Exception(() =>
            new ProcessingTopic<int>(
                topicName,
                exportName,
                true,
                (Filters.FilterType)filterType,
                (Filters.KeyType)keyType,
                1,
                dateRange,
                true,
                (EncoderRules)encoderRule));

        // Assert
        exception.Should().NotBeNull().And.BeOfType<ArgumentOutOfRangeException>();
    }

    [Fact(DisplayName = "ProcessingTopic can't be created with empty partition filter.")]
    [Trait("Category", "Unit")]
    public void ProcessingTopicCannotBeCreatedWithEmptyPartitionFilter()
    {
        // Arrange
        var topicName = new TopicName("Test1");
        var exportName = new FileName("Test2");
        var dateRange = new Filters.DateFilterRange(null!, null!);

        // Act
        var exception = Record.Exception(() =>
            new ProcessingTopic<int>(
                topicName,
                exportName,
                true,
                Filters.FilterType.Equals,
                Filters.KeyType.Long,
                1,
                dateRange,
                true,
                EncoderRules.String,
                new HashSet<int>()));

        // Assert
        exception.Should().NotBeNull().And.BeOfType<ArgumentOutOfRangeException>();
    }

    [Fact(DisplayName = "Can create LoadingTopic from ProcessingTopic.")]
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
        var processingTopic = new ProcessingTopic<int>(
                name,
                exName,
                loadCompact,
                filterType,
                keyType,
                filterKeyValue,
                dateRange,
                isRaw,
                EncoderRules.String,
                partitions);

        // Act
        var exception = Record.Exception(() => _ = processingTopic.CreateLoadingParams());

        // Assert
        exception.Should().BeNull();
    }

    [Fact(DisplayName = "Can create LoadingTopic from ProcessingTopic with correct parameters.")]
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
        var processingTopic = new ProcessingTopic<int>(
                name,
                exName,
                loadCompact,
                filterType,
                keyType,
                filterKeyValue,
                dateRange,
                isRaw,
                EncoderRules.String,
                partitions);

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

    [Fact(DisplayName = "Can create LoadingTopic from ProcessingTopic with correct parameters and dates.")]
    [Trait("Category", "Unit")]
    public void CanCreateLoadingTopicFromProcessingTopicWithCorrectParamsWithDates()
    {
        // Arrange
        var name = new TopicName("Test1");
        var exName = new FileName("Test2");
        var loadCompact = true;
        var filterType = Filters.FilterType.Equals;
        var keyType = Filters.KeyType.Long;
        var dtStart = new DateTimeOffset(2022, 1, 1, 0, 0, 0, TimeSpan.Zero);
        var dtEnd = new DateTimeOffset(2023, 1, 1, 0, 0, 0, TimeSpan.Zero);
        var dateRange = new Filters.DateFilterRange(dtStart, dtEnd);
        var filterKeyValue = 1;
        var isRaw = true;
        var partitions = new HashSet<int> { 1, 2, 3 };
        var processingTopic = new ProcessingTopic<int>(
                name,
                exName,
                loadCompact,
                filterType,
                keyType,
                filterKeyValue,
                dateRange,
                isRaw,
                EncoderRules.String,
                partitions);

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

    [Fact(DisplayName = "Can create ExportTopic from ProcessingTopic.")]
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
        var processingTopic = new ProcessingTopic<int>(
                name,
                exName,
                loadCompact,
                filterType,
                keyType,
                filterKeyValue,
                dateRange,
                isRaw,
                EncoderRules.String,
                partitions);

        // Act
        var exception = Record.Exception(() => _ = processingTopic.CreateExportParams());

        // Assert
        exception.Should().BeNull();
    }

    [Fact(DisplayName = "Can create ExportTopic from ProcessingTopic with correct parameters.")]
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
        var processingTopic = new ProcessingTopic<int>(
                name,
                exName,
                loadCompact,
                filterType,
                keyType,
                filterKeyValue,
                dateRange,
                isRaw,
                EncoderRules.String,
                partitions);

        // Act
        var result = processingTopic.CreateExportParams();

        // Assert
        result.TopicName.Should().Be(name);
        result.ExportName.Should().Be(exName);
        result.ExportRawMessage.Should().Be(isRaw);
    }
}
