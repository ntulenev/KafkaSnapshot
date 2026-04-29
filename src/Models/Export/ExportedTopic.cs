using KafkaSnapshot.Models.Names;

namespace KafkaSnapshot.Models.Export;

/// <summary>
/// Topic that can be exported.
/// </summary>
public record ExportedTopic
{
    /// <summary>
    /// Creates <see cref="ExportedTopic"/>.
    /// </summary>
    /// <param name="topicName">Topic name.</param>
    /// <param name="exportName">Export name.</param>
    /// <param name="exportRawMessage">Whether to export message values as raw strings.</param>
    public ExportedTopic(TopicName topicName, FileName exportName, bool exportRawMessage)
    {
        ArgumentNullException.ThrowIfNull(topicName);
        ArgumentNullException.ThrowIfNull(exportName);

        TopicName = topicName;
        ExportName = exportName;
        ExportRawMessage = exportRawMessage;
    }

    /// <summary>
    /// Topic name.
    /// </summary>
    public TopicName TopicName { get; init; }

    /// <summary>
    /// Export name.
    /// </summary>
    public FileName ExportName { get; init; }

    /// <summary>
    /// Whether to export message values as raw strings.
    /// </summary>
    public bool ExportRawMessage { get; init; }
}
