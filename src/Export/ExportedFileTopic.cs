namespace Export
{
    public record ExportedFileTopic(string Name, string FileName) : ExportedTopic(Name);
}
