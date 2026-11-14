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

    [Fact(DisplayName = "Unable to create JsonEquals filter with non-JSON value.")]
    [Trait("Category", "Unit")]
    public void UnableToCreateJsonEqualsFilterWithBadString()
    {

        // Arrange
        var value = "aaaaaa";
        // Act
        var exception = Record.Exception(() => new JsonEqualsFilter(value));

        // Assert
        exception.Should().NotBeNull().And.BeOfType<JsonReaderException>();
    }

    [Fact(DisplayName = "JsonEquals filter can be created.")]
    [Trait("Category", "Unit")]
    public void JsonEqualsFilterCanBeCreated()
    {

        // Arrange
        var value = /*lang=json,strict*/ "{\"value\": 1 }";
        // Act
        var exception = Record.Exception(() => new JsonEqualsFilter(value));

        // Assert
        exception.Should().BeNull();
    }

    [Fact(DisplayName = "JsonEquals filter can't match with null.")]
    [Trait("Category", "Unit")]
    public void JsonEqualsFilterCannotMatchWithNull()
    {

        // Arrange
        var value = /*lang=json,strict*/ "{\"value\": 1 }";
        var filter = new JsonEqualsFilter(value);
        string comparand = null!;

        // Act
        var exception = Record.Exception(() => _ = filter.IsMatch(comparand));

        // Assert
        exception.Should().NotBeNull().And.BeOfType<ArgumentNullException>();
    }

    [Fact(DisplayName = "JsonEquals filter can't match non-JSON value.")]
    [Trait("Category", "Unit")]
    public void JsonEqualsFilterCannotMatchWithNotJsonValue()
    {

        // Arrange
        var value = /*lang=json,strict*/ "{\"value\": 1 }";
        var filter = new JsonEqualsFilter(value);
        var comparand = "aaaa";

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
        var value = /*lang=json,strict*/ "{\"value\": 1 }";
        var filter = new JsonEqualsFilter(value);
        var comparand = /*lang=json,strict*/ "{\"value\": 1 }";

        // Act
        var result = filter.IsMatch(comparand);

        // Assert
        result.Should().BeTrue();
    }


    [Fact(DisplayName = "JsonEquals filter returns false for different objects.")]
    [Trait("Category", "Unit")]
    public void JsonEqualsFilterFalseForDifferentObjects()
    {

        // Arrange
        var value = /*lang=json,strict*/ "{\"value\": 1 }";
        var filter = new JsonEqualsFilter(value);
        var comparand = /*lang=json,strict*/ "{\"value\": 2 }";

        // Act
        var result = filter.IsMatch(comparand);

        // Assert
        result.Should().BeFalse();
    }

}
