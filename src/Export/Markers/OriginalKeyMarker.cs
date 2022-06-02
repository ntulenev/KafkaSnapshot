using KafkaSnapshot.Abstractions.Export;

namespace KafkaSnapshot.Export.Markers
{
    /// <summary>
    /// Marker for type that should be represented as original value.
    /// </summary>
    public sealed class OriginalKeyMarker : IKeyRepresentationMarker
    {
    }
}
