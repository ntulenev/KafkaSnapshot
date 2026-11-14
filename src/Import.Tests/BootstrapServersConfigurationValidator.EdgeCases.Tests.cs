using Confluent.Kafka;

using FluentAssertions;

using KafkaSnapshot.Import.Configuration;
using KafkaSnapshot.Import.Configuration.Validation;
using KafkaSnapshot.Models.Configuration;

using Xunit;

namespace KafkaSnapshot.Import.Tests;

[Trait("Category", "Unit")]
public class BootstrapServersConfigurationValidatorEdgeCasesTests
{
    [Theory(DisplayName = "BootstrapServersConfigurationValidator reports the specific error for invalid security settings.")]
    [InlineData((SecurityProtocol)999, null, "KS_CFG_005")]
    [InlineData(SecurityProtocol.SaslSsl, (SaslMechanism)999, "KS_CFG_006")]
    [InlineData(SecurityProtocol.SaslSsl, null, "KS_CFG_007")]
    public void RejectsUnsupportedSecuritySettings(SecurityProtocol protocol, SaslMechanism? mechanism, string code)
    {
        var options = new BootstrapServersConfiguration
        {
            BootstrapServers = ["localhost:9092"],
            SecurityProtocol = protocol,
            SASLMechanism = mechanism
        };

        var result = new BootstrapServersConfigurationValidator().Validate(null, options);

        result.Failed.Should().BeTrue();
        result.Failures.Should().ContainSingle().Which.Should().StartWith(code);
    }

    [Theory(DisplayName = "BootstrapServersConfigurationValidator requires credentials for Plain and SCRAM authentication.")]
    [InlineData(SaslMechanism.Plain, null, "password", "KS_CFG_008")]
    [InlineData(SaslMechanism.ScramSha256, " ", "password", "KS_CFG_008")]
    [InlineData(SaslMechanism.ScramSha512, "user", null, "KS_CFG_009")]
    [InlineData(SaslMechanism.Plain, "user", " ", "KS_CFG_009")]
    public void RejectsMissingCredentials(SaslMechanism mechanism, string? username, string? password, string code)
    {
        var options = new BootstrapServersConfiguration
        {
            BootstrapServers = ["localhost:9092"],
            SecurityProtocol = SecurityProtocol.SaslSsl,
            SASLMechanism = mechanism,
            Username = username,
            Password = password
        };

        var result = new BootstrapServersConfigurationValidator().Validate(null, options);

        result.Failed.Should().BeTrue();
        result.Failures.Should().ContainSingle().Which.Should().StartWith(code);
    }

    [Theory(DisplayName = "BootstrapServersConfigurationValidator rejects null or empty entries in the broker list.")]
    [InlineData(null)]
    [InlineData("")]
    public void RejectsEmptyBrokerEntry(string? broker)
    {
        var options = new BootstrapServersConfiguration { BootstrapServers = ["localhost:9092", broker!] };

        var result = new BootstrapServersConfigurationValidator().Validate(null, options);

        result.Failures.Should().ContainSingle().Which.Should().StartWith(ConfigurationValidationErrorCodes.BootstrapServersEmptyItem);
    }

    [Fact(DisplayName = "BootstrapServersConfigurationValidator rejects null options.")]
    public void RejectsNullOptions()
    {
        Action act = () => new BootstrapServersConfigurationValidator().Validate(null, null!);
        act.Should().Throw<ArgumentNullException>().WithParameterName("options");
    }
}
