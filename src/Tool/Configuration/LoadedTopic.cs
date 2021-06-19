namespace Tool.Configuration
{
    public class LoadedTopic
    {
        public string Name { get; set; } = default!;

        public KeyType KeyType { get; set; }

        public string ExportFileName { get; set; } = default!;
    }
}
