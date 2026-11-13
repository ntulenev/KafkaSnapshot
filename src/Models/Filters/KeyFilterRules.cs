namespace KafkaSnapshot.Models.Filters;

/// <summary>Key filtering compatibility rules.</summary>
public static class KeyFilterRules
{
    /// <summary>Checks compatibility of the operation and key representation.</summary>
    public static bool IsSupported(FilterType type, KeyType keyType)
        => type switch
        {
            FilterType.None => true,
            FilterType.Equals => keyType != KeyType.Ignored,
            FilterType.Contains => keyType == KeyType.String,
            FilterType.GreaterOrEquals or FilterType.LessOrEquals => keyType == KeyType.Long,
            _ => false
        };
}
