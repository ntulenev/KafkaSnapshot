using System;

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
        public IKeyFilter<TKey> Create(FilterType filterType, KeyType keyType, TKey sample)
        {
            return (filterType, keyType, sample) switch
            {
                (FilterType.None, _, _) => _default,
                (FilterType.Equals, KeyType.Long or KeyType.String, _) => new EqualsFilter<TKey>(sample),
                (FilterType.Equals, KeyType.Json, string _) => throw new NotImplementedException("Json filter not implemented yet."),
                _ => throw new ArgumentException($"Invalid filter type {filterType}", nameof(filterType)),
            };
        }

        private readonly DefaultFilter<TKey> _default = new();
    }
}
