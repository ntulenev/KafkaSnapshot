using Xunit;

using FluentAssertions;

using Microsoft.Extensions.Options;

using KafkaSnapshot.Import.Configuration.Validation;
using KafkaSnapshot.Import.Configuration;

namespace KafkaSnapshot.Import.Tests
{
    public class BootstrapServersConfigurationValidatorTests
    {
        [Fact(DisplayName = "BootstrapServersConfigurationValidator can be created.")]
        [Trait("Category", "Unit")]
        public void BootstrapServersConfigurationValidatorCanBeCreated()
        {

            // Arrange

            // Act
            var exception = Record.Exception(() => new BootstrapServersConfigurationValidator());

            // Assert
            exception.Should().BeNull();
        }

        [Fact(DisplayName = "BootstrapServersConfigurationValidator can be validated.")]
        [Trait("Category", "Unit")]
        public void BootstrapServersConfigurationValidatorCanBeValidated()
        {

            // Arrange
            var validator = new BootstrapServersConfigurationValidator();
            var name = "Test";
            ValidateOptionsResult result = null!;
            var options = new BootstrapServersConfiguration
            {
                BootstrapServers = new List<string>
                {
                    "test"
                },
                Password = "password",
                SASLMechanism = Confluent.Kafka.SaslMechanism.Gssapi,
                SecurityProtocol = Confluent.Kafka.SecurityProtocol.Plaintext,
                Username = "name"
            };

            // Act
            var exception = Record.Exception(() => result = validator.Validate(name, options));

            // Assert
            exception.Should().BeNull();
            result.Succeeded.Should().BeTrue();
        }
    }
}
