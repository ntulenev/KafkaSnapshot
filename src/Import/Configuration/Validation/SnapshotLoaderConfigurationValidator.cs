using KafkaSnapshot.Models.Configuration;

using Microsoft.Extensions.Options;

namespace KafkaSnapshot.Import.Configuration.Validation;

/// <summary>
/// Validator for <see cref="SnapshotLoaderConfiguration"/>.
/// </summary>
public class SnapshotLoaderConfigurationValidator :
    IValidateOptions<SnapshotLoaderConfiguration>
{
    /// <summary>
    /// Validates <see cref="SnapshotLoaderConfiguration"/>.
    /// </summary>
    public ValidateOptionsResult Validate(string? name, SnapshotLoaderConfiguration options)
    {
        ArgumentNullException.ThrowIfNull(options);

        if (options.DateOffsetTimeout <= TimeSpan.Zero)
        {
            return Fail(
                ConfigurationValidationErrorCodes.DateOffsetTimeoutInvalid,
                "DateOffsetTimeout should be positive.");
        }

        if (options.MaxConcurrentPartitions is <= 0)
        {
            return Fail(
                ConfigurationValidationErrorCodes.MaxConcurrentPartitionsInvalid,
                "MaxConcurrentPartitions should be positive when set.");
        }

        return ValidateOptionsResult.Success;
    }

    private static ValidateOptionsResult Fail(string code, string message)
        => ValidateOptionsResult.Fail(ConfigurationValidationError.Create(code, message));
}
