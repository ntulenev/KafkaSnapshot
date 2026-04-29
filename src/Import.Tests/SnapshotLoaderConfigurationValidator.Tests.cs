using FluentAssertions;

using KafkaSnapshot.Import.Configuration;
using KafkaSnapshot.Import.Configuration.Validation;
using KafkaSnapshot.Models.Configuration;

using Xunit;

namespace KafkaSnapshot.Import.Tests;

public class SnapshotLoaderConfigurationValidatorTests
{
    [Fact(DisplayName = "SnapshotLoaderConfigurationValidator can be created.")]
    [Trait("Category", "Unit")]
    public void SnapshotLoaderConfigurationValidatorCanBeCreated()
    {
        // Act
        var exception = Record.Exception(() => new SnapshotLoaderConfigurationValidator());

        // Assert
        exception.Should().BeNull();
    }

    [Fact(DisplayName = "SnapshotLoaderConfigurationValidator could be validated.")]
    [Trait("Category", "Unit")]
    public void SnapshotLoaderConfigurationValidatorCouldBeValidated()
    {
        // Arrange
        var validator = new SnapshotLoaderConfigurationValidator();
        var config = new SnapshotLoaderConfiguration
        {
            DateOffsetTimeout = TimeSpan.FromSeconds(1),
            SearchSinglePartition = false
        };

        // Act
        var result = validator.Validate("test", config);

        // Assert
        result.Succeeded.Should().BeTrue();
    }

    [Fact(DisplayName = "SnapshotLoaderConfigurationValidator fails for zero timeout.")]
    [Trait("Category", "Unit")]
    public void SnapshotLoaderConfigurationValidatorFailsForZeroTimeout()
    {
        // Arrange
        var validator = new SnapshotLoaderConfigurationValidator();
        var config = new SnapshotLoaderConfiguration
        {
            DateOffsetTimeout = TimeSpan.Zero
        };

        // Act
        var result = validator.Validate("test", config);

        // Assert
        result.Succeeded.Should().BeFalse();
        result.Failures.Should().ContainSingle()
            .Which.Should().StartWith(
                ConfigurationValidationErrorCodes.DateOffsetTimeoutInvalid);
    }

    [Fact(DisplayName = "SnapshotLoaderConfigurationValidator fails for non-positive max concurrent partitions.")]
    [Trait("Category", "Unit")]
    public void SnapshotLoaderConfigurationValidatorFailsForNonPositiveMaxConcurrentPartitions()
    {
        // Arrange
        var validator = new SnapshotLoaderConfigurationValidator();
        var config = new SnapshotLoaderConfiguration
        {
            DateOffsetTimeout = TimeSpan.FromSeconds(1),
            MaxConcurrentPartitions = 0
        };

        // Act
        var result = validator.Validate("test", config);

        // Assert
        result.Succeeded.Should().BeFalse();
        result.Failures.Should().ContainSingle()
            .Which.Should().StartWith(
                ConfigurationValidationErrorCodes.MaxConcurrentPartitionsInvalid);
    }

    [Fact(DisplayName = "SnapshotLoaderConfigurationValidator fails for negative timeout.")]
    [Trait("Category", "Unit")]
    public void SnapshotLoaderConfigurationValidatorFailsForNegativeTimeout()
    {
        // Arrange
        var validator = new SnapshotLoaderConfigurationValidator();
        var config = new SnapshotLoaderConfiguration
        {
            DateOffsetTimeout = TimeSpan.FromSeconds(-1)
        };

        // Act
        var result = validator.Validate("test", config);

        // Assert
        result.Succeeded.Should().BeFalse();
    }
}
