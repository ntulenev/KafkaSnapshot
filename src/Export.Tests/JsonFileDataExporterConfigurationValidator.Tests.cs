using FluentAssertions;

using KafkaSnapshot.Export.Configuration;
using KafkaSnapshot.Export.Configuration.Validation;
using KafkaSnapshot.Models.Configuration;

using Xunit;

namespace KafkaSnapshot.Export.Tests;

public class JsonFileDataExporterConfigurationValidatorTests
{
    [Fact(DisplayName = "JsonFileDataExporterConfigurationValidator accepts default config.")]
    [Trait("Category", "Unit")]
    public void JsonFileDataExporterConfigurationValidatorAcceptsDefaultConfig()
    {
        // Arrange
        var validator = new JsonFileDataExporterConfigurationValidator();

        // Act
        var result = validator.Validate("test", new JsonFileDataExporterConfiguration());

        // Assert
        result.Succeeded.Should().BeTrue();
    }

    [Fact(DisplayName = "JsonFileDataExporterConfigurationValidator rejects whitespace output directory.")]
    [Trait("Category", "Unit")]
    public void JsonFileDataExporterConfigurationValidatorRejectsWhitespaceOutputDirectory()
    {
        // Arrange
        var validator = new JsonFileDataExporterConfigurationValidator();
        var options = new JsonFileDataExporterConfiguration
        {
            OutputDirectory = "   "
        };

        // Act
        var result = validator.Validate("test", options);

        // Assert
        result.Succeeded.Should().BeFalse();
        result.Failures.Should().ContainSingle()
            .Which.Should().StartWith(
                ConfigurationValidationErrorCodes.OutputDirectoryWhitespace);
    }
}
