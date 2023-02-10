using FluentAssertions;

using Xunit;

namespace KafkaSnapshot.Filters.Tests;

public class StringContainsFilterTests
{
    [Fact(DisplayName = "Unable to create StringContains filter with null value.")]
    [Trait("Category", "Unit")]
    public void UnableToCreateStringContainsFilter()
    {

        // Arrange
        string value = null!;
        // Act
        var exception = Record.Exception(() => new StringContainsFilter(value));

        // Assert
        exception.Should().NotBeNull().And.BeOfType<ArgumentNullException>();
    }

    [Fact(DisplayName = "Unable to create StringContains filter with empty value.")]
    [Trait("Category", "Unit")]
    public void UnableToCreateStringContainsFilterWithEmptyString()
    {

        // Arrange
        string value = string.Empty;
        // Act
        var exception = Record.Exception(() => new StringContainsFilter(value));

        // Assert
        exception.Should().NotBeNull().And.BeOfType<ArgumentException>();
    }

    [Fact(DisplayName = "Unable to create StringContains filter with spaces only value.")]
    [Trait("Category", "Unit")]
    public void UnableToCreateStringContainsFilterWithSpacesString()
    {

        // Arrange
        string value = "    ";
        // Act
        var exception = Record.Exception(() => new StringContainsFilter(value));

        // Assert
        exception.Should().NotBeNull().And.BeOfType<ArgumentException>();
    }

    [Fact(DisplayName = "StringContains filter could be created.")]
    [Trait("Category", "Unit")]
    public void StringContainsFilterCouldBeCreated()
    {

        // Arrange
        string value = " some data ";
        // Act
        var exception = Record.Exception(() => new StringContainsFilter(value));

        // Assert
        exception.Should().BeNull();
    }

    [Fact(DisplayName = "StringContains filter can't match with null.")]
    [Trait("Category", "Unit")]
    public void StringContainsFilterCantMatchWithNull()
    {

        // Arrange
        string value = " some data ";
        var filter = new StringContainsFilter(value);
        string comparand = null!;

        // Act
        var exception = Record.Exception(() => _ = filter.IsMatch(comparand));

        // Assert
        exception.Should().NotBeNull().And.BeOfType<ArgumentNullException>();
    }

    [Fact(DisplayName = "StringContains filter returns true if contains.")]
    [Trait("Category", "Unit")]
    public void StringContainsFilterTrueForContains()
    {

        // Arrange
        string value = " some data ";
        var filter = new StringContainsFilter(value);
        string comparand = "this string has some data inside";

        // Act
        var result = filter.IsMatch(comparand);

        // Assert
        result.Should().BeTrue();
    }

    [Fact(DisplayName = "StringContains filter returns false if not scontains.")]
    [Trait("Category", "Unit")]
    public void StringContainsFilterFalseForContains()
    {

        // Arrange
        string value = " some data ";
        var filter = new StringContainsFilter(value);
        string comparand = "this string has nothing inside";

        // Act
        var result = filter.IsMatch(comparand);

        // Assert
        result.Should().BeFalse();
    }
}
