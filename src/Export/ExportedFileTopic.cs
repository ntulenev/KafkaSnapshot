using KafkaSnapshot.Models.Export;

namespace KafkaSnapshot.Export
{
    /// <summary>
    /// Topic that could be exported as file.
    /// </summary>
    public record ExportedFileTopic(string Name, string FileName) : ExportedTopic(Name);
}
