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
            return ValidateOptionsResult.Fail("DateOffsetTimeout should be positive.");
        }

        return ValidateOptionsResult.Success;
    }
}
