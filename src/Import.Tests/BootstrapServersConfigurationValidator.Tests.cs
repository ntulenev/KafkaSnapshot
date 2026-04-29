using Confluent.Kafka;

using FluentAssertions;

using KafkaSnapshot.Import.Configuration;
using KafkaSnapshot.Import.Configuration.Validation;
using KafkaSnapshot.Models.Configuration;

using Xunit;

namespace KafkaSnapshot.Import.Tests;

public class BootstrapServersConfigurationValidatorTests
{
    [Fact(DisplayName = "BootstrapServersConfigurationValidator can be created.")]
    [Trait("Category", "Unit")]
    public void BootstrapServersConfigurationValidatorCanBeCreated()
    {
        // Act
        var exception = Record.Exception(() => new BootstrapServersConfigurationValidator());

        // Assert
        exception.Should().BeNull();
    }

    [Fact(DisplayName = "BootstrapServersConfigurationValidator accepts valid SASL settings.")]
    [Trait("Category", "Unit")]
    public void BootstrapServersConfigurationValidatorAcceptsValidSaslSettings()
    {
        // Arrange
        var validator = new BootstrapServersConfigurationValidator();
        var options = CreateOptions();

        // Act
        var result = validator.Validate("Test", options);

        // Assert
        result.Succeeded.Should().BeTrue();
    }

    [Fact(DisplayName = "BootstrapServersConfigurationValidator accepts valid non-SASL settings.")]
    [Trait("Category", "Unit")]
    public void BootstrapServersConfigurationValidatorAcceptsValidNonSaslSettings()
    {
        // Arrange
        var validator = new BootstrapServersConfigurationValidator();
        var options = CreateOptions(
            securityProtocol: SecurityProtocol.Plaintext,
            saslMechanism: null,
            username: null,
            password: null);

        // Act
        var result = validator.Validate("Test", options);

        // Assert
        result.Succeeded.Should().BeTrue();
    }

    [Fact(DisplayName = "BootstrapServersConfigurationValidator rejects null bootstrap list.")]
    [Trait("Category", "Unit")]
    public void BootstrapServersConfigurationValidatorRejectsNullBootstrapList()
    {
        // Arrange
        var validator = new BootstrapServersConfigurationValidator();
        var options = new BootstrapServersConfiguration
        {
            BootstrapServers = null!,
            SecurityProtocol = SecurityProtocol.SaslPlaintext,
            SASLMechanism = SaslMechanism.ScramSha512,
            Username = "user",
            Password = "password"
        };

        // Act
        var result = validator.Validate("Test", options);

        // Assert
        result.Succeeded.Should().BeFalse();
        result.Failures.Should().ContainSingle()
            .Which.Should().StartWith(
                ConfigurationValidationErrorCodes.BootstrapServersMissing);
    }

    [Fact(DisplayName = "BootstrapServersConfigurationValidator rejects empty bootstrap list.")]
    [Trait("Category", "Unit")]
    public void BootstrapServersConfigurationValidatorRejectsEmptyBootstrapList()
    {
        // Arrange
        var validator = new BootstrapServersConfigurationValidator();
        var options = CreateOptions(bootstrapServers: []);

        // Act
        var result = validator.Validate("Test", options);

        // Assert
        result.Succeeded.Should().BeFalse();
    }

    [Fact(DisplayName = "BootstrapServersConfigurationValidator rejects whitespace bootstrap server.")]
    [Trait("Category", "Unit")]
    public void BootstrapServersConfigurationValidatorRejectsWhitespaceBootstrapServer()
    {
        // Arrange
        var validator = new BootstrapServersConfigurationValidator();
        var options = CreateOptions(bootstrapServers: ["kafka1:9092", "   "]);

        // Act
        var result = validator.Validate("Test", options);

        // Assert
        result.Succeeded.Should().BeFalse();
    }

    [Fact(DisplayName = "BootstrapServersConfigurationValidator rejects SASL protocol without mechanism.")]
    [Trait("Category", "Unit")]
    public void BootstrapServersConfigurationValidatorRejectsSaslProtocolWithoutMechanism()
    {
        // Arrange
        var validator = new BootstrapServersConfigurationValidator();
        var options = CreateOptions(saslMechanism: null);

        // Act
        var result = validator.Validate("Test", options);

        // Assert
        result.Succeeded.Should().BeFalse();
    }

    [Fact(DisplayName = "BootstrapServersConfigurationValidator rejects missing username for credential-based SASL mechanism.")]
    [Trait("Category", "Unit")]
    public void BootstrapServersConfigurationValidatorRejectsMissingUsernameForCredentialBasedSaslMechanism()
    {
        // Arrange
        var validator = new BootstrapServersConfigurationValidator();
        var options = CreateOptions(username: " ");

        // Act
        var result = validator.Validate("Test", options);

        // Assert
        result.Succeeded.Should().BeFalse();
    }

    [Fact(DisplayName = "BootstrapServersConfigurationValidator rejects missing password for credential-based SASL mechanism.")]
    [Trait("Category", "Unit")]
    public void BootstrapServersConfigurationValidatorRejectsMissingPasswordForCredentialBasedSaslMechanism()
    {
        // Arrange
        var validator = new BootstrapServersConfigurationValidator();
        var options = CreateOptions(password: " ");

        // Act
        var result = validator.Validate("Test", options);

        // Assert
        result.Succeeded.Should().BeFalse();
    }

    [Fact(DisplayName = "BootstrapServersConfigurationValidator accepts SASL mechanisms without username and password requirements.")]
    [Trait("Category", "Unit")]
    public void BootstrapServersConfigurationValidatorAcceptsSaslMechanismsWithoutUsernameAndPasswordRequirements()
    {
        // Arrange
        var validator = new BootstrapServersConfigurationValidator();
        var options = CreateOptions(
            saslMechanism: SaslMechanism.Gssapi,
            username: null,
            password: null);

        // Act
        var result = validator.Validate("Test", options);

        // Assert
        result.Succeeded.Should().BeTrue();
    }

    [Fact(DisplayName = "BootstrapServersConfigurationValidator rejects SASL mechanism for non-SASL protocol.")]
    [Trait("Category", "Unit")]
    public void BootstrapServersConfigurationValidatorRejectsSaslMechanismForNonSaslProtocol()
    {
        // Arrange
        var validator = new BootstrapServersConfigurationValidator();
        var options = CreateOptions(
            securityProtocol: SecurityProtocol.Plaintext,
            saslMechanism: SaslMechanism.ScramSha512,
            username: null,
            password: null);

        // Act
        var result = validator.Validate("Test", options);

        // Assert
        result.Succeeded.Should().BeFalse();
    }

    [Fact(DisplayName = "BootstrapServersConfigurationValidator rejects username for non-SASL protocol.")]
    [Trait("Category", "Unit")]
    public void BootstrapServersConfigurationValidatorRejectsUsernameForNonSaslProtocol()
    {
        // Arrange
        var validator = new BootstrapServersConfigurationValidator();
        var options = CreateOptions(
            securityProtocol: SecurityProtocol.Plaintext,
            saslMechanism: null,
            username: "user",
            password: null);

        // Act
        var result = validator.Validate("Test", options);

        // Assert
        result.Succeeded.Should().BeFalse();
    }

    [Fact(DisplayName = "BootstrapServersConfigurationValidator rejects password for non-SASL protocol.")]
    [Trait("Category", "Unit")]
    public void BootstrapServersConfigurationValidatorRejectsPasswordForNonSaslProtocol()
    {
        // Arrange
        var validator = new BootstrapServersConfigurationValidator();
        var options = CreateOptions(
            securityProtocol: SecurityProtocol.Plaintext,
            saslMechanism: null,
            username: null,
            password: "pwd");

        // Act
        var result = validator.Validate("Test", options);

        // Assert
        result.Succeeded.Should().BeFalse();
    }

    private static BootstrapServersConfiguration CreateOptions(
        IReadOnlyList<string>? bootstrapServers = null,
        SecurityProtocol securityProtocol = SecurityProtocol.SaslPlaintext,
        SaslMechanism? saslMechanism = SaslMechanism.ScramSha512,
        string? username = "user",
        string? password = "password")
        => new()
        {
            BootstrapServers = bootstrapServers ?? ["kafka1:9092"],
            SecurityProtocol = securityProtocol,
            SASLMechanism = saslMechanism,
            Username = username,
            Password = password
        };
}
