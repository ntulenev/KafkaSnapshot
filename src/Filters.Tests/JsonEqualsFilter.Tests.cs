using FluentAssertions;

using Newtonsoft.Json;

using Xunit;

namespace KafkaSnapshot.Filters.Tests;

public class JsonEqualsFilterTests
{
    [Fact(DisplayName = "Unable to create JsonEquals filter with null value.")]
    [Trait("Category", "Unit")]
    public void UnableToCreateJsonEqualsFilter()
    {

        // Arrange
        string value = null!;
        // Act
        var exception = Record.Exception(() => new JsonEqualsFilter(value));

        // Assert
        exception.Should().NotBeNull().And.BeOfType<ArgumentNullException>();
    }

    [Fact(DisplayName = "Unable to create JsonEquals filter with not json value.")]
    [Trait("Category", "Unit")]
    public void UnableToCreateJsonEqualsFilterWithBadString()
    {

        // Arrange
        string value = "aaaaaa";
        // Act
        var exception = Record.Exception(() => new JsonEqualsFilter(value));

        // Assert
        exception.Should().NotBeNull().And.BeOfType<JsonReaderException>();
    }

    [Fact(DisplayName = "JsonEquals filter could be created.")]
    [Trait("Category", "Unit")]
    public void JsonEqualsFilterCouldBeCreated()
    {

        // Arrange
        string value = "{\"value\": 1 }";
        // Act
        var exception = Record.Exception(() => new JsonEqualsFilter(value));

        // Assert
        exception.Should().BeNull();
    }

    [Fact(DisplayName = "JsonEquals filter can't match with null.")]
    [Trait("Category", "Unit")]
    public void JsonEqualsFilterCantMatchWithNull()
    {

        // Arrange
        string value = "{\"value\": 1 }";
        var filter = new JsonEqualsFilter(value);
        string comparand = null!;

        // Act
        var exception = Record.Exception(() => _ = filter.IsMatch(comparand));

        // Assert
        exception.Should().NotBeNull().And.BeOfType<ArgumentNullException>();
    }

    [Fact(DisplayName = "JsonEquals filter can't match with not json value.")]
    [Trait("Category", "Unit")]
    public void JsonEqualsFilterCantMatchWithNotJsonValue()
    {

        // Arrange
        string value = "{\"value\": 1 }";
        var filter = new JsonEqualsFilter(value);
        string comparand = "aaaa";

        // Act
        var exception = Record.Exception(() => _ = filter.IsMatch(comparand));

        // Assert
        exception.Should().NotBeNull().And.BeOfType<JsonReaderException>();
    }


    [Fact(DisplayName = "JsonEquals filter returns true for same objects.")]
    [Trait("Category", "Unit")]
    public void JsonEqualsFilterTrueForSameObjects()
    {

        // Arrange
        string value = "{\"value\": 1 }";
        var filter = new JsonEqualsFilter(value);
        string comparand = "{\"value\": 1 }";
        bool result = false;

        // Act
        var exception = Record.Exception(() => result = filter.IsMatch(comparand));

        // Assert
        exception.Should().BeNull();
        result.Should().BeTrue();
    }


    [Fact(DisplayName = "JsonEquals filter returns false for different objects.")]
    [Trait("Category", "Unit")]
    public void JsonEqualsFilterFalseForDifferentObjects()
    {

        // Arrange
        string value = "{\"value\": 1 }";
        var filter = new JsonEqualsFilter(value);
        string comparand = "{\"value\": 2 }";
        bool result = true;

        // Act
        var exception = Record.Exception(() => result = filter.IsMatch(comparand));

        // Assert
        exception.Should().BeNull();
        result.Should().BeFalse();
    }

}
