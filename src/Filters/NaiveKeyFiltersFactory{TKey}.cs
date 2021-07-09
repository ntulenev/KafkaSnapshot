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
        public IKeyFilter<TKey> Create(FilterType type, KeyType keyType, TKey sample)
        {
            return (type, keyType) switch
            {
                (FilterType.None, _) => _default,
                (FilterType.Equals, KeyType.Long or KeyType.String) => new EqualsFilter<TKey>(sample),
                (FilterType.Equals, KeyType.Json) => throw new NotImplementedException("Json filter not implemented yet."),
                _ => throw new ArgumentException($"Invalid filter type {type}", nameof(type)),
            };
        }

        private readonly DefaultFilter<TKey> _default = new();
    }
}
