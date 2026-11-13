using FluentAssertions;

using KafkaSnapshot.Import.Configuration;
using KafkaSnapshot.Import.Configuration.Validation;
using KafkaSnapshot.Models.Configuration;

using Xunit;

namespace KafkaSnapshot.Import.Tests;

[Trait("Category", "Unit")]
public class StreamingConfigurationTests
{
    [Theory(DisplayName = "Streaming buffer capacity must be positive.")]
    [InlineData(0)]
    [InlineData(-1)]
    public void InvalidCapacityIsRejected(int capacity)
    {
        var result = new SnapshotLoaderConfigurationValidator().Validate(null,
            new SnapshotLoaderConfiguration { DateOffsetTimeout = TimeSpan.FromSeconds(1), StreamBufferCapacity = capacity });
        result.Failures.Should().ContainSingle().Which.Should().StartWith(ConfigurationValidationErrorCodes.StreamBufferCapacityInvalid);
    }
}
