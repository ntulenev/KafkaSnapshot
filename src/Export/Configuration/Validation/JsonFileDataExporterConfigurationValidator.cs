using KafkaSnapshot.Models.Configuration;

using Microsoft.Extensions.Options;

namespace KafkaSnapshot.Export.Configuration.Validation;

/// <summary>
/// Validator for <see cref="JsonFileDataExporterConfiguration"/>.
/// </summary>
public sealed class JsonFileDataExporterConfigurationValidator :
    IValidateOptions<JsonFileDataExporterConfiguration>
{
    /// <summary>
    /// Validates <see cref="JsonFileDataExporterConfiguration"/>.
    /// </summary>
    public ValidateOptionsResult Validate(
        string? name,
        JsonFileDataExporterConfiguration options)
    {
        ArgumentNullException.ThrowIfNull(options);

        if (options.OutputDirectory is not null &&
            string.IsNullOrWhiteSpace(options.OutputDirectory))
        {
            return ValidateOptionsResult.Fail(
                ConfigurationValidationError.Create(
                    ConfigurationValidationErrorCodes.OutputDirectoryWhitespace,
                    "OutputDirectory cannot be empty or consist only of whitespace."));
        }

        return ValidateOptionsResult.Success;
    }
}
