using KafkaSnapshot.Abstractions.Export;

namespace KafkaSnapshot.Export.Markers;

/// <summary>
/// Marker for string keys that should be represented as JSON.
/// </summary>
public sealed class JsonKeyMarker : IKeyRepresentationMarker
{
    /// <inheritdoc />
    public string Representation => nameof(JsonKeyMarker);
}
