using KafkaSnapshot.Models.Filters;

namespace KafkaSnapshot.Models.Processing
{
    /// <summary>
    /// Topic that could be processed.
    /// </summary>
    public record ProcessingTopic<TKey>(string Name, 
                                        string ExportName, 
                                        bool LoadWithCompacting, 
                                        FilterType FilterType,
                                        KeyType KeyType,
                                        TKey FilterValue);
}
