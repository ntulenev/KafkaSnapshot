using KafkaSnapshot.Abstractions.Export;

namespace KafkaSnapshot.Export.Markers
{
    /// <summary>
    /// Marker for not exists key type that should be ignored. 
    /// </summary>
    public sealed class IgnoreKeyMarker : IKeyRepresentationMarker
    {
    }
}