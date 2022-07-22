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
    }
}
