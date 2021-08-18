using FluentAssertions;
using Xunit;

namespace KafkaSnapshot.Filters.Tests
{
    public class DefaultFilterTests
    {
        [Fact(DisplayName = "Default filter match is true.")]
        [Trait("Category", "Unit")]
        public void DefaultFilterMatchIsTrue()
        {

            // Arrange
            object value = null!;
            var filter = new DefaultFilter<object>();
            bool result = false;

            // Act
            var exception = Record.Exception(() => result = filter.IsMatch(value));

            // Assert
            exception.Should().BeNull();
            result.Should().BeTrue();
        }
    }
}
