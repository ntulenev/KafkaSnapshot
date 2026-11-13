namespace KafkaSnapshot.Models.Filters;

/// <summary>
/// Validated key filter definition.
/// </summary>
/// <typeparam name="TKey">Filter value type.</typeparam>
public sealed class KeyFilterDefinition<TKey>
{
    /// <summary>
    /// Creates a filter with a compatible operation and key representation.
    /// </summary>
    public KeyFilterDefinition(FilterType type, KeyType keyType, TKey value)
    {
        if (!Enum.IsDefined(type))
        {
            throw new ArgumentOutOfRangeException(nameof(type), type, "Invalid filter type.");
        }

        if (!Enum.IsDefined(keyType))
        {
            throw new ArgumentOutOfRangeException(nameof(keyType), keyType, "Invalid key type.");
        }

        if (!KeyFilterRules.IsSupported(type, keyType))
        {
            throw new ArgumentException("Filter operation is incompatible with key type.", nameof(type));
        }

        if (type != FilterType.None)
        {
            ArgumentNullException.ThrowIfNull(value);
            if (keyType == KeyType.Long ? value is not long : value is not string)
            {
                throw new ArgumentException("Filter value does not match the key representation.", nameof(value));
            }
        }

        Type = type;
        KeyType = keyType;
        Value = value;
    }

    /// <summary>Filter operation.</summary>
    public FilterType Type { get; }

    /// <summary>Key representation.</summary>
    public KeyType KeyType { get; }

    /// <summary>Filter value; unused when filtering is disabled.</summary>
    public TKey Value { get; }

}
