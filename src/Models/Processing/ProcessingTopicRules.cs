using KafkaSnapshot.Models.Filters;

namespace KafkaSnapshot.Models.Processing;

/// <summary>Topic processing compatibility rules.</summary>
public static class ProcessingTopicRules
{
    /// <summary>Checks whether compaction is compatible with the key representation.</summary>
    public static bool SupportsCompacting(KeyType keyType, bool withCompacting)
        => keyType != KeyType.Ignored || !withCompacting;

}
