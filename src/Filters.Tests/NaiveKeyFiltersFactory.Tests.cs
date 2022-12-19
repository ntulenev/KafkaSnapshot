using Xunit;

using FluentAssertions;

using KafkaSnapshot.Abstractions.Filters;
using KafkaSnapshot.Models.Filters;

namespace KafkaSnapshot.Filters.Tests;

public class NaiveKeyFiltersFactoryTests
{
    [Fact(DisplayName = "Default filter can be created.")]
    [Trait("Category", "Unit")]
    public void DefaultFilterCanBeCreatedByFactory()
    {
        // Arrange
        var factory = new NaiveKeyFiltersFactory<object>();
        IDataFilter<object> result = null!;

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
        IDataFilter<object> result = null!;

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
        IDataFilter<string> result = null!;
        string value = "{\"value\": 1 }";

        // Act
        var exception = Record.Exception(() => result = factory.Create(FilterType.Equals, KeyType.Json, value));

        // Assert
        exception.Should().BeNull();
        result.Should().BeOfType(typeof(JsonEqualsFilter));
    }

    [Fact(DisplayName = "Equals filter cant be created for ignored type.")]
    [Trait("Category", "Unit")]
    public void EqualsFilterCantBeCreatedForIgnoreKeyByFactory()
    {
        // Arrange
        var factory = new NaiveKeyFiltersFactory<string>();
        IDataFilter<string> result = null!;
        string value = "{\"value\": 1 }";

        // Act
        var exception = Record.Exception(() => result = factory.Create(FilterType.Equals, KeyType.Ignored, value));

        // Assert
        exception.Should().NotBeNull().And.BeOfType<ArgumentException>();
    }

    [Fact(DisplayName = "Contains filter can be created for string type.")]
    [Trait("Category", "Unit")]
    public void ContainsFilterCanBeCreatedForStringKeyByFactory()
    {
        // Arrange
        var factory = new NaiveKeyFiltersFactory<string>();
        IDataFilter<string> result = null!;
        string value = "data";

        // Act
        var exception = Record.Exception(() => result = factory.Create(FilterType.Contains, KeyType.String, value));

        // Assert
        exception.Should().BeNull();
        result.Should().BeOfType(typeof(StringContainsFilter));
    }

    [Theory(DisplayName = "Contains filter cant be created for non string type.")]
    [Trait("Category", "Unit")]
    [InlineData(KeyType.Json)]
    [InlineData(KeyType.Long)]
    [InlineData(KeyType.Ignored)]
    public void ContainsFilterCantBeCreatedForNonStringKeyByFactory(KeyType keyType)
    {
        // Arrange
        var factory = new NaiveKeyFiltersFactory<string>();
        IDataFilter<string> result = null!;
        string value = "data";

        // Act
        var exception = Record.Exception(() => result = factory.Create(FilterType.Contains, keyType, value));

        // Assert
        exception.Should().NotBeNull().And.BeOfType<ArgumentException>();
    }
}
