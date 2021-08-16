using Xunit;

using FluentAssertions;

using KafkaSnapshot.Abstractions.Filters;
using KafkaSnapshot.Models.Filters;

namespace KafkaSnapshot.Filters.Tests
{
    public class NaiveKeyFiltersFactoryTests
    {
        [Fact(DisplayName = "Default filter can be created.")]
        [Trait("Category", "Unit")]
        public void DefaultFilterCanBeCreatedByFactory()
        {
            // Arrange
            var factory = new NaiveKeyFiltersFactory<object>();
            IKeyFilter<object> result = null!;

            // Act
            var exception = Record.Exception(() => result = factory.Create(FilterType.None, new(), new()));

            // Assert
            exception.Should().BeNull();
            result.Should().BeOfType(typeof(DefaultFilter<object>));
        }

        [Theory(DisplayName = "Equals filter can be created.")]
        [Trait("Category", "Unit")]
        [InlineData(KeyType.Long)]
        [InlineData(KeyType.String)]
        public void EqualsFilterCanBeCreatedByFactory(KeyType keyType)
        {
            // Arrange
            var factory = new NaiveKeyFiltersFactory<object>();
            IKeyFilter<object> result = null!;

            // Act
            var exception = Record.Exception(() => result = factory.Create(FilterType.Equals, keyType, new()));

            // Assert
            exception.Should().BeNull();
            result.Should().BeOfType(typeof(EqualsFilter<object>));
        }

        [Fact(DisplayName = "Json Equals filter can be created.")]
        [Trait("Category", "Unit")]
        public void JsonEqualsFilterCanBeCreatedByFactory()
        {
            // Arrange
            var factory = new NaiveKeyFiltersFactory<string>();
            IKeyFilter<string> result = null!;
            string value = "{\"value\": 1 }";

            // Act
            var exception = Record.Exception(() => result = factory.Create(FilterType.Equals, KeyType.Json, value));

            // Assert
            exception.Should().BeNull();
            result.Should().BeOfType(typeof(JsonEqualsFilter));
        }
    }
}
