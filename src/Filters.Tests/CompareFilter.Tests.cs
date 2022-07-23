using FluentAssertions;

using Xunit;

namespace KafkaSnapshot.Filters.Tests
{
    public class CompareFilterTests
    {
        [Theory(DisplayName = "Unable to create Compare filter with null value.")]
        [Trait("Category", "Unit")]
        [InlineData(true)]
        [InlineData(false)]
        public void UnableToCreateCompareFilter(bool side)
        {

            // Arrange
            string value = null!;

            // Act
            var exception = Record.Exception(() => new CompareFilter<string>(value, side));

            // Assert
            exception.Should().NotBeNull().And.BeOfType<ArgumentNullException>();
        }

        [Theory(DisplayName = "Unable to create Compare filter with null match.")]
        [Trait("Category", "Unit")]
        [InlineData(true)]
        [InlineData(false)]
        public void UnableToCompareWithNull(bool side)
        {

            // Arrange
            var filters = new CompareFilter<string>("test", side);

            // Act
            var exception = Record.Exception(() => _ = filters.IsMatch(null!));

            // Assert
            exception.Should().NotBeNull().And.BeOfType<ArgumentNullException>();
        }

        [Theory(DisplayName = "Unable to create Compare filter with null match.")]
        [Trait("Category", "Unit")]
        [InlineData(true, 1, 2, true)]
        [InlineData(true, 2, 2, true)]
        [InlineData(true, 3, 2, false)]
        [InlineData(false, 1, 2, false)]
        [InlineData(false, 2, 2, true)]
        [InlineData(false, 3, 2, true)]
        public void FilterCanCompareData(bool isGreater, int sample, int data, bool targetResult)
        {

            // Arrange
            var filters = new CompareFilter<int>(sample, greater: isGreater);

            // Act
            bool result = filters.IsMatch(data);

            // Assert
            result.Should().Be(targetResult);
        }
    }
}
