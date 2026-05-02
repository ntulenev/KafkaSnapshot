using Xunit;

using FluentAssertions;

using KafkaSnapshot.Models.Import;
using KafkaSnapshot.Models.Filters;
using KafkaSnapshot.Models.Names;

namespace KafkaSnapshot.Models.Tests;

public class LoadingTopicTests
{
    [Theory(DisplayName = "Topic name can't be null.")]
    [Trait("Category", "Unit")]
    [InlineData(true)]
    [InlineData(false)]
    public void CannotCreateNullTopicName(bool compactingRule)
    {

        // Arrange
        TopicName name = null!;
        HashSet<int> partitionFilter = null!;

        // Act
        var exception = Record.Exception(() =>
                new LoadingTopic(
                        name,
                        compactingRule,
                        new DateFilterRange(null!, null!),
                        EncoderRules.String,
                        partitionFilter));

        // Assert
        exception.Should().NotBeNull().And.BeOfType<ArgumentNullException>();
    }

    [Theory(DisplayName = "Topic with valid name can be created.")]
    [Trait("Category", "Unit")]
    [InlineData(true)]
    [InlineData(false)]
    public void CanCreateValidTopic(bool compactingRule)
    {

        // Arrange
        var name = new TopicName("test");
        LoadingTopic item = null!;
        HashSet<int> partitionFilter = null!;

        // Act
        var exception = Record.Exception(() =>
                            item = new LoadingTopic(
                                name,
                                compactingRule,
                                new DateFilterRange(null!, null!),
                                EncoderRules.String,
                                partitionFilter));

        // Assert
        exception.Should().BeNull();
        item.Should().NotBeNull();
        item.LoadWithCompacting.Should().Be(compactingRule);
        item.HasOffsetDate.Should().BeFalse();
        item.HasEndOffsetDate.Should().BeFalse();
        item.HasPartitionFilter.Should().BeFalse();
    }

    [Fact(DisplayName = "Topic can't be created with null date range.")]
    [Trait("Category", "Unit")]
    public void CannotCreateTopicWithNullDateRange()
    {
        // Arrange
        var name = new TopicName("test");

        // Act
        var exception = Record.Exception(() =>
                new LoadingTopic(
                        name,
                        true,
                        null!,
                        EncoderRules.String,
                        null));

        // Assert
        exception.Should().NotBeNull().And.BeOfType<ArgumentNullException>();
    }

    [Theory(DisplayName = "Topic with valid name can be created 2.")]
    [Trait("Category", "Unit")]
    [InlineData(true)]
    [InlineData(false)]
    public void CanCreateValidTopic2(bool compactingRule)
    {

        // Arrange
        var name = new TopicName("test");
        LoadingTopic item = null!;
        HashSet<int> partitionFilter = null!;

        // Act
        var exception = Record.Exception(() =>
                            item = new LoadingTopic(
                                name,
                                compactingRule,
                                new DateFilterRange(DateTime.UtcNow, DateTime.UtcNow),
                                EncoderRules.String,
                                partitionFilter));

        // Assert
        exception.Should().BeNull();
        item.Should().NotBeNull();
        item.LoadWithCompacting.Should().Be(compactingRule);
        item.HasOffsetDate.Should().BeTrue();
        item.HasEndOffsetDate.Should().BeTrue();
    }

    [Fact(DisplayName = "Can't get topic offset date if not set.")]
    [Trait("Category", "Unit")]
    public void CanGetTopicOffsetDate()
    {

        // Arrange
        var name = new TopicName("test");
        HashSet<int> partitionFilter = null!;
        var topic = new LoadingTopic(
                name,
                true,
                new DateFilterRange(null!, DateTime.UtcNow),
                EncoderRules.String,
                partitionFilter);


        // Act
        var exception = Record.Exception(() => topic.OffsetDate);

        // Assert
        exception.Should().NotBeNull().And.BeOfType<InvalidOperationException>();
    }

    [Fact(DisplayName = "Can get topic offset date if date is set.")]
    [Trait("Category", "Unit")]
    public void CannotGetTopicOffsetDate()
    {

        // Arrange
        var date = DateTime.UtcNow;
        var name = new TopicName("test");
        HashSet<int> partitionFilter = null!;
        var topic = new LoadingTopic(
                name,
                true,
                new DateFilterRange(date, null!),
                EncoderRules.String,
                partitionFilter);
        DateTimeOffset resultedDate = default;

        // Act
        var exception = Record.Exception(() => resultedDate = topic.OffsetDate);

        // Assert
        exception.Should().BeNull();
        resultedDate.Should().Be(date);
    }

    [Fact(DisplayName = "Can't get topic end offset date if not set.")]
    [Trait("Category", "Unit")]
    public void CannotGetTopicEndOffsetDate()
    {

        // Arrange
        var name = new TopicName("test");
        HashSet<int> partitionFilter = null!;
        var topic = new LoadingTopic(
                name,
                true,
                new DateFilterRange(DateTime.UtcNow, null!),
                EncoderRules.String,
                partitionFilter);

        // Act
        var exception = Record.Exception(() => topic.EndOffsetDate);

        // Assert
        exception.Should().NotBeNull().And.BeOfType<InvalidOperationException>();
    }

    [Fact(DisplayName = "Can get topic end offset date if date is set.")]
    [Trait("Category", "Unit")]
    public void CanGetTopicEndOffsetDate()
    {

        // Arrange
        var date = DateTime.UtcNow;
        var name = new TopicName("test");
        HashSet<int> partitionFilter = null!;
        var topic = new LoadingTopic(
                name,
                true,
                new DateFilterRange(null!, date),
                EncoderRules.String,
                partitionFilter);
        DateTimeOffset resultedDate = default;

        // Act
        var exception = Record.Exception(() => resultedDate = topic.EndOffsetDate);

        // Assert
        exception.Should().BeNull();
        resultedDate.Should().Be(date);
    }

    [Fact(DisplayName = "Can't setup empty partition filter.")]
    [Trait("Category", "Unit")]
    public void CannotCreateTopicWithEmptyPartitionFilter()
    {

        // Arrange
        var name = new TopicName("test");

        // Act
        var exception = Record.Exception(() =>
                new LoadingTopic(
                        name,
                        true,
                        new DateFilterRange(DateTime.UtcNow, DateTime.UtcNow.AddDays(1)),
                        EncoderRules.String,
                        new HashSet<int>()));

        // Assert
        exception.Should().NotBeNull().And.BeOfType<ArgumentOutOfRangeException>();
    }

    [Fact(DisplayName = "Can setup partition filter.")]
    [Trait("Category", "Unit")]
    public void CanCreateTopicWithValidPartitionFilter()
    {

        // Arrange
        var name = new TopicName("test");

        // Act
        var exception = Record.Exception(() =>
                    new LoadingTopic(
                            name,
                            true,
                            new DateFilterRange(DateTime.UtcNow, DateTime.UtcNow.AddDays(1)),
                            EncoderRules.String,
                            new HashSet<int> { 1, 2, 3 }));

        // Assert
        exception.Should().BeNull();
    }

    [Fact(DisplayName = "Can get topic value encoder rule.")]
    [Trait("Category", "Unit")]
    public void CanGetTopicValueEncoderRule()
    {
        // Arrange
        var topic = new LoadingTopic(
            new TopicName("test"),
            true,
            new DateFilterRange(null!, null!),
            EncoderRules.MessagePack,
            null);

        // Assert
        topic.TopicValueEncoderRule.Should().Be(EncoderRules.MessagePack);
    }

    [Fact(DisplayName = "Can't create topic with invalid encoder rule.")]
    [Trait("Category", "Unit")]
    public void CannotCreateTopicWithInvalidEncoderRule()
    {
        // Arrange
        var name = new TopicName("test");
        var invalidRule = (EncoderRules)999;

        // Act
        var exception = Record.Exception(() =>
            new LoadingTopic(
                name,
                true,
                new DateFilterRange(null!, null!),
                invalidRule,
                null));

        // Assert
        exception.Should().NotBeNull().And.BeOfType<ArgumentOutOfRangeException>();
    }

    [Fact(DisplayName = "Can get topic partition filter.")]
    [Trait("Category", "Unit")]
    public void CanGetTopicPartitionFilter()
    {

        // Arrange
        var items = new[] { 1, 2, 3 };
        var date = DateTime.UtcNow;
        var name = new TopicName("test");

        // Act
        var topic = new LoadingTopic(
                            name,
                            true,
                            new DateFilterRange(date, date),
                            EncoderRules.String,
                            new HashSet<int>(items));

        // Assert
        topic.HasPartitionFilter.Should().BeTrue();
        topic.PartitionFilter.Should().NotBeNull();
        topic.PartitionFilter.Should().Contain(items);
    }
}
