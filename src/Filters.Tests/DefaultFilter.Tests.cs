using Xunit;

using FluentAssertions;

namespace KafkaSnapshot.Filters.Tests;

public class DefaultFilterTests
{
    [Fact(DisplayName = "Default filter match is true.")]
    [Trait("Category", "Unit")]
    public void DefaultFilterMatchIsTrue()
    {

        // Arrange
        object value = null!;
        var filter = new DefaultFilter<object>();

        // Act
        var result = filter.IsMatch(value);

        // Assert
        result.Should().BeTrue();
    }
}
