using Xunit;

using FluentAssertions;

using KafkaSnapshot.Abstractions.Filters;
using KafkaSnapshot.Models.Filters;

namespace KafkaSnapshot.Filters.Tests
{
    public class NaiveValueFiltersFactoryTests
    {
        [Fact(DisplayName = "Default filter can be created.")]
        [Trait("Category", "Unit")]
        public void DefaultFilterCanBeCreatedByFactory()
        {
            // Arrange
            var factory = new NaiveValueFiltersFactory<object>();
            IDataFilter<object> result = null!;

            // Act
            var exception = Record.Exception(() => result = factory.Create(FilterType.None, new(), new()));

            // Assert
            exception.Should().BeNull();
            result.Should().BeOfType(typeof(DefaultFilter<object>));
        }

        [Theory(DisplayName = "Equals filter can be created.")]
        [Trait("Category", "Unit")]
        [InlineData(ValueMessageType.Raw)]
        public void EqualsFilterCanBeCreatedByFactory(ValueMessageType messageType)
        {
            // Arrange
            var factory = new NaiveValueFiltersFactory<object>();
            IDataFilter<object> result = null!;

            // Act
            var exception = Record.Exception(() => result = factory.Create(FilterType.Equals, messageType, new()));

            // Assert
            exception.Should().BeNull();
            result.Should().BeOfType(typeof(EqualsFilter<object>));
        }

        [Fact(DisplayName = "Json Equals filter can be created.")]
        [Trait("Category", "Unit")]
        public void JsonEqualsFilterCanBeCreatedByFactory()
        {
            // Arrange
            var factory = new NaiveValueFiltersFactory<string>();
            IDataFilter<string> result = null!;
            string value = "{\"value\": 1 }";

            // Act
            var exception = Record.Exception(() => result = factory.Create(FilterType.Equals, ValueMessageType.Json, value));

            // Assert
            exception.Should().BeNull();
            result.Should().BeOfType(typeof(JsonEqualsFilter));
        }

        [Theory(DisplayName = "Equals filter can be created.")]
        [Trait("Category", "Unit")]
        [InlineData(ValueMessageType.Json)]
        public void EqualsFilterCantBeCreatedForJsonOnObject(ValueMessageType messageType)
        {
            // Arrange
            var factory = new NaiveValueFiltersFactory<object>();
            IDataFilter<object> result = null!;

            // Act
            var exception = Record.Exception(() => result = factory.Create(FilterType.Equals, messageType, new()));

            // Assert
            exception.Should().NotBeNull().And.BeOfType<ArgumentException>();
        }
    }
}
