using Xunit;

using Microsoft.Extensions.Options;


using FluentAssertions;

using KafkaSnapshot.Processing.Configuration;
using KafkaSnapshot.Processing.Configuration.Validation;

namespace KafkaSnapshot.Processing.Tests
{
    public class LoaderToolConfigurationValidatorTests
    {
        [Fact(DisplayName = "LoaderToolConfigurationValidator can be created.")]
        [Trait("Category", "Unit")]
        public void LoaderToolConfigurationValidatorCanBeCreated()
        {

            // Arrange

            // Act
            var exception = Record.Exception(() => new LoaderToolConfigurationValidator());

            // Assert
            exception.Should().BeNull();
        }

        [Fact(DisplayName = "LoaderToolConfigurationValidator can be validated.")]
        [Trait("Category", "Unit")]
        public void LoaderToolConfigurationValidatorCanBeValidated()
        {

            // Arrange
            var validator = new LoaderToolConfigurationValidator();
            var name = "Test";
            ValidateOptionsResult result = null!;
            var options = new LoaderToolConfiguration
            {
                UseConcurrentLoad = true,
                Topics = new List<TopicConfiguration>
                 {
                      new TopicConfiguration
                      {
                          Name = "test",
                          Compacting = CompactingMode.On,
                          ExportFileName = "export",
                          ExportRawMessage = true,
                          FilterType = Models.Filters.FilterType.None,
                          KeyType = Models.Filters.KeyType.Json,
                      }
                 }
            };

            // Act
            var exception = Record.Exception(() => result = validator.Validate(name, options));

            // Assert
            exception.Should().BeNull();
            result.Succeeded.Should().BeTrue();
        }

        [Fact(DisplayName = "LoaderToolConfigurationValidator cant be validated with null options.")]
        [Trait("Category", "Unit")]
        public void LoaderToolConfigurationValidatorCantBeValidatedWithNullOptions()
        {

            // Arrange
            var validator = new LoaderToolConfigurationValidator();
            var name = "Test";
            ValidateOptionsResult result = null!;
            var options = (LoaderToolConfiguration)null!;

            // Act
            var exception = Record.Exception(() => result = validator.Validate(name, options));

            // Assert
            exception.Should().BeNull();
            result.Succeeded.Should().BeFalse();
        }

        [Fact(DisplayName = "LoaderToolConfigurationValidator cant be validated with null topics.")]
        [Trait("Category", "Unit")]
        public void LoaderToolConfigurationValidatorCantBeValidatedWithNullTopics()
        {

            // Arrange
            var validator = new LoaderToolConfigurationValidator();
            var name = "Test";
            ValidateOptionsResult result = null!;
            var options = new LoaderToolConfiguration
            {
                UseConcurrentLoad = true,
                Topics = null!
            };

            // Act
            var exception = Record.Exception(() => result = validator.Validate(name, options));

            // Assert
            exception.Should().BeNull();
            result.Succeeded.Should().BeFalse();
        }

        [Fact(DisplayName = "LoaderToolConfigurationValidator cant be validated with empty topics.")]
        [Trait("Category", "Unit")]
        public void LoaderToolConfigurationValidatorCantBeValidatedWithEmptyTopics()
        {

            // Arrange
            var validator = new LoaderToolConfigurationValidator();
            var name = "Test";
            ValidateOptionsResult result = null!;
            var options = new LoaderToolConfiguration
            {
                UseConcurrentLoad = true,
                Topics = new List<TopicConfiguration>()
            };

            // Act
            var exception = Record.Exception(() => result = validator.Validate(name, options));

            // Assert
            exception.Should().BeNull();
            result.Succeeded.Should().BeFalse();
        }
    }
}
