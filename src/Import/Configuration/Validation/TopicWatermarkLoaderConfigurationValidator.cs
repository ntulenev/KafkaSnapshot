using KafkaSnapshot.Models.Configuration;

using Microsoft.Extensions.Options;

namespace KafkaSnapshot.Import.Configuration.Validation;

/// <summary>
/// Validator for <see cref="TopicWatermarkLoaderConfiguration"/>.
/// </summary>
public class TopicWatermarkLoaderConfigurationValidator :
    IValidateOptions<TopicWatermarkLoaderConfiguration>
{
    /// <summary>
    /// Validates <see cref="TopicWatermarkLoaderConfiguration"/>.
    /// </summary>
    public ValidateOptionsResult Validate(string? name,
                                          TopicWatermarkLoaderConfiguration options)
    {
        ArgumentNullException.ThrowIfNull(options);

        return options.AdminClientTimeout <= TimeSpan.Zero
            ? Fail(
                ConfigurationValidationErrorCodes.AdminClientTimeoutInvalid,
                "AdminClientTimeout should be positive.")
            : ValidateOptionsResult.Success;
    }

    private static ValidateOptionsResult Fail(string code, string message)
        => ValidateOptionsResult.Fail(ConfigurationValidationError.Create(code, message));
}
