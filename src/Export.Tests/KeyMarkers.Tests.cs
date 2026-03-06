using FluentAssertions;

using KafkaSnapshot.Export.Markers;

using Xunit;

namespace KafkaSnapshot.Export.Tests;

public class KeyMarkersTests
{
    [Fact(DisplayName = "IgnoreKeyMarker has expected representation.")]
    [Trait("Category", "Unit")]
    public void IgnoreKeyMarkerHasExpectedRepresentation()
    {
        // Arrange
        var marker = new IgnoreKeyMarker();

        // Assert
        marker.Representation.Should().Be(nameof(IgnoreKeyMarker));
    }

    [Fact(DisplayName = "JsonKeyMarker has expected representation.")]
    [Trait("Category", "Unit")]
    public void JsonKeyMarkerHasExpectedRepresentation()
    {
        // Arrange
        var marker = new JsonKeyMarker();

        // Assert
        marker.Representation.Should().Be(nameof(JsonKeyMarker));
    }

    [Fact(DisplayName = "OriginalKeyMarker has expected representation.")]
    [Trait("Category", "Unit")]
    public void OriginalKeyMarkerHasExpectedRepresentation()
    {
        // Arrange
        var marker = new OriginalKeyMarker();

        // Assert
        marker.Representation.Should().Be(nameof(OriginalKeyMarker));
    }
}
