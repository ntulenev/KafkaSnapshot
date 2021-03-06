using KafkaSnapshot.Models.Filters;

namespace KafkaSnapshot.Abstractions.Filters
{
    /// <summary>
    /// Creates KeyFilter by condition.
    /// </summary>
    /// <typeparam name="TKey">Message key type.</typeparam>
    public interface IKeyFiltersFactory<TKey> where TKey : notnull
    {
        /// <summary>
        /// Creates suitable filter for filter type and key type.
        /// </summary>
        /// <param name="filterKeyType">Filter type.</param>
        /// <param name="keyType">Key type.</param>
        /// <param name="sample">Key value sample.</param>
        /// <returns>Filter for this key type and filter type.</returns>
        public IDataFilter<TKey> Create(FilterType filterKeyType, KeyType keyType, TKey sample);
    }
}
