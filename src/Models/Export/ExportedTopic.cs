using KafkaSnapshot.Models.Names;

namespace KafkaSnapshot.Models.Export;

/// <summary>
/// Topic that could be exported.
/// </summary>
/// <param name="Name">Topic name.</param>
/// <param name="ExportName">Export name.</param>
/// <param name="ExportRawMessage">Rule to export message as raw string.</param>
public record ExportedTopic(string Name, FileName ExportName, bool ExportRawMessage);
