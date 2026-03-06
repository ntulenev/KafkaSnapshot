using Xunit;

using FluentAssertions;
using KafkaSnapshot.Models.Names;

namespace KafkaSnapshot.Models.Tests;

public class TopicNameTests
{
    [Fact(DisplayName = "TopicName constructor throws exception with null input")]
    [Trait("Category", "Unit")]
    public void ConstructorThrowsExceptionWithNullInput()
    {
        // Act
        var exception = Record.Exception(() => new TopicName(null!));

        // Assert
        exception.Should().NotBeNull().And.BeOfType<ArgumentNullException>();
    }

    [Fact(DisplayName = "TopicName constructor throws exception with long input")]
    [Trait("Category", "Unit")]
    public void ConstructorThrowsExceptionWithLongInput()
    {
        // Arrange
        var name = new string('a', 256);

        // Act
        var exception = Record.Exception(() => new TopicName(name));

        // Assert
        exception.Should().NotBeNull().And.BeOfType<ArgumentException>();
    }

    [Theory(DisplayName = "TopicName constructor throws exception with invalid input")]
    [InlineData("")]
    [InlineData("    ")]
    [InlineData("a/topic/name/with/invalid/characters")]
    [Trait("Category", "Unit")]
    public void ConstructorThrowsExceptionWithInvalidInput(string input)
    {
        // Act
        var exception = Record.Exception(() => new TopicName(input));

        // Assert
        exception.Should().NotBeNull().And.BeOfType<ArgumentException>();
    }

    [Fact(DisplayName = "TopicName constructor returns expected topic name")]
    [Trait("Category", "Unit")]
    public void ConstructorReturnsExpectedTopicName()
    {
        // Arrange
        var topicName = "test-topic";

        // Act
        var result = new TopicName(topicName);

        // Assert
        result.Name.Should().Be(topicName);
    }
}
