using System;

using KafkaSnapshot.Abstractions.Filters;
using KafkaSnapshot.Models.Filters;

namespace KafkaSnapshot.Import.Filters
{
    /// <summary>
    /// Simple factory filter factory.
    /// </summary>
    /// <typeparam name="TKey">Message key type.</typeparam>
    public class NaiveKeyFiltersFactory<TKey> : IKeyFiltersFactory<TKey> where TKey : notnull
    {
        /// <inheritdoc/>
        public IKeyFilter<TKey> Create(FilterType type, TKey sample)
        {
            return type switch
            {
                FilterType.None => _default,
                FilterType.Equals => new EqualsFilter<TKey>(sample),
                _ => throw new ArgumentException($"Invalid filter type {type}", nameof(type)),
            };
        }

        private readonly DefaultFilter<TKey> _default = new();
    }
}
