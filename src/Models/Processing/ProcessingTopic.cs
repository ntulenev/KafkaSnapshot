namespace KafkaSnapshot.Models.Processing
{
    /// <summary>
    /// Topic that could be processed.
    /// </summary>
    public record ProcessingTopic(string Name, string ExportName);
}
