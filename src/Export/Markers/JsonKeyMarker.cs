using KafkaSnapshot.Abstractions.Export;

namespace KafkaSnapshot.Export.Markers;

/// <summary>
/// Marker for string type that should be represented as JSON.
/// </summary>
public sealed class JsonKeyMarker : IKeyRepresentationMarker
{
}
