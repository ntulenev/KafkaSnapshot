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
    }
}
