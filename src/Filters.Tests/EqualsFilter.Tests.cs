using FluentAssertions;

using Xunit;

namespace KafkaSnapshot.Filters.Tests;

public class EqualsFilterTests
{
    [Fact(DisplayName = "Unable to create Equals filter with null value.")]
    [Trait("Category", "Unit")]
    public void UnableToCreateEqualsFilter()
    {

        // Arrange
        object value = null!;
        // Act
        var exception = Record.Exception(() => new EqualsFilter<object>(value));

        // Assert
        exception.Should().NotBeNull().And.BeOfType<ArgumentNullException>();
    }

    [Fact(DisplayName = "Equals filter could be created.")]
    [Trait("Category", "Unit")]
    public void EqualsFilterCouldBeCreated()
    {

        // Arrange
        object value = new();
        // Act
        var exception = Record.Exception(() => new EqualsFilter<object>(value));

        // Assert
        exception.Should().BeNull();
    }

    [Fact(DisplayName = "Equals filter can't match with null.")]
    [Trait("Category", "Unit")]
    public void EqualsFilterCantMatchWithNull()
    {

        // Arrange
        TestedType value = new(1);
        var filter = new EqualsFilter<TestedType>(value);
        TestedType comparand = null!;

        // Act
        var exception = Record.Exception(() => _ = filter.IsMatch(comparand));

        // Assert
        exception.Should().NotBeNull().And.BeOfType<ArgumentNullException>();
    }

    [Fact(DisplayName = "Equals filter returns true for same objects.")]
    [Trait("Category", "Unit")]
    public void EqualsFilterTrueForSameObjects()
    {

        // Arrange
        TestedType value = new(1);
        var filter = new EqualsFilter<TestedType>(value);
        var comparand = new TestedType(1);
        bool result = false;

        // Act
        var exception = Record.Exception(() => result = filter.IsMatch(comparand));

        // Assert
        exception.Should().BeNull();
        result.Should().BeTrue();
    }

    [Fact(DisplayName = "Equals filter returns false for different objects.")]
    [Trait("Category", "Unit")]
    public void EqualsFilterFalseForDifferentObjects()
    {

        // Arrange
        TestedType value = new(1);
        var filter = new EqualsFilter<TestedType>(value);
        var comparand = new TestedType(2);
        bool result = true;

        // Act
        var exception = Record.Exception(() => result = filter.IsMatch(comparand));

        // Assert
        exception.Should().BeNull();
        result.Should().BeFalse();
    }
}
