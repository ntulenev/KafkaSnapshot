using KafkaSnapshot.Abstractions.Export;

namespace KafkaSnapshot.Export.Markers;

/// <summary>
/// Marker for export formats that omit message keys.
/// </summary>
public sealed class IgnoreKeyMarker : IKeyRepresentationMarker
{
    /// <inheritdoc />
    public string Representation => nameof(IgnoreKeyMarker);
}
