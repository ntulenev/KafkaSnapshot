using Xunit;

using FluentAssertions;

using Microsoft.Extensions.Options;

using KafkaSnapshot.Import.Configuration.Validation;
using KafkaSnapshot.Import.Configuration;

namespace KafkaSnapshot.Import.Tests
{
    public class TopicWatermarkLoaderConfigurationValidatorTests
    {
        [Fact(DisplayName = "TopicWatermarkLoaderConfigurationValidator can be created.")]
        [Trait("Category", "Unit")]
        public void TopicWatermarkLoaderConfigurationValidatorCanBeCreated()
        {

            // Arrange

            // Act
            var exception = Record.Exception(() => new TopicWatermarkLoaderConfigurationValidator());

            // Assert
            exception.Should().BeNull();
        }

        [Fact(DisplayName = "TopicWatermarkLoaderConfigurationValidator could be validated.")]
        [Trait("Category", "Unit")]
        public void TopicWatermarkLoaderConfigurationValidatorCouldBeValidated()
        {

            // Arrange
            var validator = new TopicWatermarkLoaderConfigurationValidator();
            var name = "test";
            var config = new TopicWatermarkLoaderConfiguration
            {
                AdminClientTimeout = TimeSpan.FromSeconds(1)
            };
            ValidateOptionsResult result = null!;

            // Act
            var exception = Record.Exception(() => result = validator.Validate(name, config));

            // Assert
            exception.Should().BeNull();
            result.Succeeded.Should().BeTrue();
        }

        [Fact(DisplayName = "TopicWatermarkLoaderConfigurationValidator time span validation.")]
        [Trait("Category", "Unit")]
        public void TopicWatermarkLoaderConfigurationValidatorTimeSpanValidation()
        {

            // Arrange
            var validator = new TopicWatermarkLoaderConfigurationValidator();
            var name = "test";
            var config = new TopicWatermarkLoaderConfiguration
            {
            };
            ValidateOptionsResult result = null!;

            // Act
            var exception = Record.Exception(() => result = validator.Validate(name, config));

            // Assert
            exception.Should().BeNull();
            result.Succeeded.Should().BeFalse();
        }
    }
}
