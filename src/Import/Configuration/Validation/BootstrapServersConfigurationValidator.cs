using Confluent.Kafka;

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
            return ValidateOptionsResult.Fail("BootstrapServers section is not set.");
        }

        if (!options.BootstrapServers.Any())
        {
            return ValidateOptionsResult.Fail("BootstrapServers section is empty.");
        }

        if (options.BootstrapServers.Any(string.IsNullOrEmpty))
        {
            return ValidateOptionsResult.Fail("BootstrapServers section contains empty string.");
        }

        if (options.BootstrapServers.Any(string.IsNullOrWhiteSpace))
        {
            return ValidateOptionsResult.Fail("BootstrapServers section contains " +
                "empty string of whitespaces.");
        }

        if (!Enum.IsDefined(options.SecurityProtocol))
        {
            return ValidateOptionsResult.Fail(
                $"Unsupported SecurityProtocol value {options.SecurityProtocol}.");
        }

        if (options.SASLMechanism is SaslMechanism mechanism &&
            !Enum.IsDefined(mechanism))
        {
            return ValidateOptionsResult.Fail(
                $"Unsupported SASLMechanism value {mechanism}.");
        }

        if (IsSaslProtocol(options.SecurityProtocol))
        {
            if (options.SASLMechanism is null)
            {
                return ValidateOptionsResult.Fail(
                    "SASLMechanism should be set for SASL security protocols.");
            }

            if (RequiresCredentials(options.SASLMechanism.Value))
            {
                if (string.IsNullOrWhiteSpace(options.Username))
                {
                    return ValidateOptionsResult.Fail(
                        "Username should be set for selected SASL mechanism.");
                }

                if (string.IsNullOrWhiteSpace(options.Password))
                {
                    return ValidateOptionsResult.Fail(
                        "Password should be set for selected SASL mechanism.");
                }
            }
        }
        else if (options.SASLMechanism is not null ||
                 !string.IsNullOrWhiteSpace(options.Username) ||
                 !string.IsNullOrWhiteSpace(options.Password))
        {
            return ValidateOptionsResult.Fail(
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
}
