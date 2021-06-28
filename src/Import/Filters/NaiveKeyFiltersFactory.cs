using System;

using KafkaSnapshot.Abstractions.Filters;

namespace KafkaSnapshot.Import.Filters
{
    /// <summary>
    /// Simple factory for primitive types.
    /// </summary>
    /// <typeparam name="TKey">Message key type.</typeparam>
    class NaiveKeyFiltersFactory<TKey> : IKeyFiltersFactory<TKey> where TKey : notnull
    {
        /// <inheritdoc/>
        public IKeyFilter<TKey> Create(FilterType type, TKey sample)
        {
            if (sample is null)
            {
                throw new ArgumentNullException(nameof(sample));
            }

            return type switch
            {
                FilterType.Default => _default,
                FilterType.Equals => new EqualsFilter<TKey>(sample),
                _ => throw new ArgumentException($"Invalid filter type {type}", nameof(type)),
            };
        }

        private readonly DefaultFilter<TKey> _default = new();
    }
}
