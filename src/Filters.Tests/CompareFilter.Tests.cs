using FluentAssertions;

using Xunit;

namespace KafkaSnapshot.Filters.Tests;

public class CompareFilterTests
{
    [Theory(DisplayName = "Unable to create Compare filter with null value.")]
    [Trait("Category", "Unit")]
    [InlineData(true)]
    [InlineData(false)]
    public void ConstructorRejectsNullSample(bool side)
    {

        // Arrange
        string value = null!;

        // Act
        var exception = Record.Exception(() => new CompareFilter<string>(value, side));

        // Assert
        exception.Should().NotBeNull().And.BeOfType<ArgumentNullException>();
    }

    [Theory(DisplayName = "Compare filter can be created.")]
    [Trait("Category", "Unit")]
    [InlineData(true)]
    [InlineData(false)]
    public void ConstructorPreservesComparisonDirection(bool side)
    {

        // Arrange
        string value = "data";

        // Act
        var result = new CompareFilter<string>(value, side);

        // Assert
        result.IsGreaterOrEquals.Should().Be(side);
    }

    [Theory(DisplayName = "CompareFilter.IsMatch rejects null input.")]
    [Trait("Category", "Unit")]
    [InlineData(true)]
    [InlineData(false)]
    public void IsMatchRejectsNullInput(bool side)
    {

        // Arrange
        var filters = new CompareFilter<string>("test", side);

        // Act
        var exception = Record.Exception(() => _ = filters.IsMatch(null!));

        // Assert
        exception.Should().NotBeNull().And.BeOfType<ArgumentNullException>();
    }

    [Theory(DisplayName = "CompareFilter compares the input with the sample using inclusive bounds.")]
    [Trait("Category", "Unit")]
    [InlineData(true, 1, 2, true)]
    [InlineData(true, 2, 2, true)]
    [InlineData(true, 3, 2, false)]
    [InlineData(false, 1, 2, false)]
    [InlineData(false, 2, 2, true)]
    [InlineData(false, 3, 2, true)]
    public void IsMatchComparesInputWithSampleInclusively(bool isGreaterOrEquals, int sample, int data, bool targetResult)
    {

        // Arrange
        var filters = new CompareFilter<int>(sample, greaterOrEquals: isGreaterOrEquals);

        // Act
        bool result = filters.IsMatch(data);

        // Assert
        result.Should().Be(targetResult);
    }
}
