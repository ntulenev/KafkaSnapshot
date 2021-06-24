namespace KafkaSnapshot.Models.Export
{
    /// <summary>
    /// Topic that could be exported.
    /// </summary>
    public record ExportedTopic(string Name, string ExportName);
}
