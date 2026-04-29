using Confluent.Kafka;

using KafkaSnapshot.Models.Configuration;

using Microsoft.Extensions.Options;

namespace KafkaSnapshot.Import.Configuration.Validation;

/// <summary>
/// Validator for <see cref="BootstrapServersConfiguration"/>.
/// </summary>
public class BootstrapServersConfigurationValidator :
    IValidateOptions<BootstrapServersConfiguration>
{
    /// <summary>
    /// Validates <see cref="BootstrapServersConfiguration"/>.
    /// </summary>
    public ValidateOptionsResult Validate(string? name,
                                          BootstrapServersConfiguration options)
    {
        ArgumentNullException.ThrowIfNull(options);

        if (options.BootstrapServers is null)
        {
            return Fail(
                ConfigurationValidationErrorCodes.BootstrapServersMissing,
                "BootstrapServers section is not set.");
        }

        if (options.BootstrapServers.Count == 0)
        {
            return Fail(
                ConfigurationValidationErrorCodes.BootstrapServersEmpty,
                "BootstrapServers section is empty.");
        }

        if (options.BootstrapServers.Any(string.IsNullOrEmpty))
        {
            return Fail(
                ConfigurationValidationErrorCodes.BootstrapServersEmptyItem,
                "BootstrapServers section contains empty string.");
        }

        if (options.BootstrapServers.Any(string.IsNullOrWhiteSpace))
        {
            return Fail(
                ConfigurationValidationErrorCodes.BootstrapServersWhitespaceItem,
                "BootstrapServers section contains empty string of whitespaces.");
        }

        if (!Enum.IsDefined(options.SecurityProtocol))
        {
            return Fail(
                ConfigurationValidationErrorCodes.SecurityProtocolUnsupported,
                $"Unsupported SecurityProtocol value {options.SecurityProtocol}.");
        }

        if (options.SASLMechanism is SaslMechanism mechanism &&
            !Enum.IsDefined(mechanism))
        {
            return Fail(
                ConfigurationValidationErrorCodes.SaslMechanismUnsupported,
                $"Unsupported SASLMechanism value {mechanism}.");
        }

        if (IsSaslProtocol(options.SecurityProtocol))
        {
            if (options.SASLMechanism is null)
            {
                return Fail(
                    ConfigurationValidationErrorCodes.SaslMechanismMissing,
                    "SASLMechanism should be set for SASL security protocols.");
            }

            if (RequiresCredentials(options.SASLMechanism.Value))
            {
                if (string.IsNullOrWhiteSpace(options.Username))
                {
                    return Fail(
                        ConfigurationValidationErrorCodes.SaslUsernameMissing,
                        "Username should be set for selected SASL mechanism.");
                }

                if (string.IsNullOrWhiteSpace(options.Password))
                {
                    return Fail(
                        ConfigurationValidationErrorCodes.SaslPasswordMissing,
                        "Password should be set for selected SASL mechanism.");
                }
            }
        }
        else if (options.SASLMechanism is not null ||
                 !string.IsNullOrWhiteSpace(options.Username) ||
                 !string.IsNullOrWhiteSpace(options.Password))
        {
            return Fail(
                ConfigurationValidationErrorCodes.SaslProtocolMissing,
                "SASL settings require SaslPlaintext or SaslSsl security protocol.");
        }

        return ValidateOptionsResult.Success;
    }

    private static bool IsSaslProtocol(SecurityProtocol protocol)
        => protocol is SecurityProtocol.SaslPlaintext or SecurityProtocol.SaslSsl;

    private static bool RequiresCredentials(SaslMechanism mechanism)
        => mechanism is SaslMechanism.Plain or
                         SaslMechanism.ScramSha256 or
                         SaslMechanism.ScramSha512;

    private static ValidateOptionsResult Fail(string code, string message)
        => ValidateOptionsResult.Fail(ConfigurationValidationError.Create(code, message));
}
