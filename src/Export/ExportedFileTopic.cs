using Models.Export;

namespace KafkaSnapshot.Export
{
    public record ExportedFileTopic(string Name, string FileName) : ExportedTopic(Name);
}
