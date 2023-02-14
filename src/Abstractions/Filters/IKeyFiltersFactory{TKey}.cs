using KafkaSnapshot.Models.Filters;

namespace KafkaSnapshot.Abstractions.Filters;

/// <summary>
/// Defines an interface for creating filters on message keys.
/// </summary>
/// <typeparam name="TKey">The type of the message key.</typeparam>
public interface IKeyFiltersFactory<TKey> where TKey : notnull
{
    /// <summary>
    /// Creates a filter for the given key and filter types.
    /// </summary>
    /// <param name="filterKeyType">The type of filter to create.</param>
    /// <param name="keyType">The type of key being filtered.</param>
    /// <param name="sample">A sample key value to use for creating the filter.</param>
    /// <returns>A filter instance for the given key and filter types.</returns>
    public IDataFilter<TKey> Create(FilterType filterKeyType, KeyType keyType, TKey sample);
}
