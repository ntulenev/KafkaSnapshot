using KafkaSnapshot.Abstractions.Filters;
using KafkaSnapshot.Models.Filters;

namespace KafkaSnapshot.Filters
{
    /// <summary>
    /// Simple filter factory.
    /// </summary>
    /// <typeparam name="TKey">Message key type.</typeparam>
    public class NaiveKeyFiltersFactory<TKey> : IKeyFiltersFactory<TKey> where TKey : notnull
    {
        /// <inheritdoc/>
        public IKeyFilter<TKey> Create(FilterType filterKeyType, KeyType keyType, TKey sample)
        {
            return (filterKeyType, keyType, sample) switch
            {
                (FilterType.None, _, _) => _default,
                (FilterType.Equals, KeyType.Long or KeyType.String, _) => new EqualsFilter<TKey>(sample),
                (FilterType.Equals, KeyType.Json, string json) => (IKeyFilter<TKey>)new JsonEqualsFilter(json),
                _ => throw new ArgumentException($"Invalid filter type {filterKeyType} for key type {keyType} with sample type {typeof(TKey).Name}.", nameof(filterKeyType)),
            };
        }

        private readonly DefaultFilter<TKey> _default = new();
    }
}
