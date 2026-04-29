using KafkaSnapshot.Models.Names;

namespace KafkaSnapshot.Models.Export;

/// <summary>
/// Topic that can be exported.
/// </summary>
/// <param name="TopicName">Topic name.</param>
/// <param name="ExportName">Export name.</param>
/// <param name="ExportRawMessage">Whether to export message values as raw strings.</param>
public record ExportedTopic(TopicName TopicName, FileName ExportName, bool ExportRawMessage);
