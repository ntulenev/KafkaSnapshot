using KafkaSnapshot.Abstractions.Export;

namespace KafkaSnapshot.Export.Markers;

/// <summary>
/// Marker for keys that should be represented by their original value.
/// </summary>
public sealed class OriginalKeyMarker : IKeyRepresentationMarker
{
    /// <inheritdoc />
    public string Representation => nameof(OriginalKeyMarker);
}
