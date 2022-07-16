using KafkaSnapshot.Abstractions.Filters;
using KafkaSnapshot.Models.Filters;

namespace KafkaSnapshot.Filters
{
    /// <summary>
    /// Simple value filter factory.
    /// </summary>
    /// <typeparam name="TKey">Message value type.</typeparam>
    public class NaiveValueFiltersFactory<TValue> : IValueFilterFactory<TValue> where TValue : notnull
    {
        ///// <inheritdoc/>
        public IKeyFilter<TValue> Create(FilterType filterValueType, ValueMessageType valueType, TValue sample)
        {
            return (filterValueType, valueType, sample) switch
            {
                (FilterType.None, _, _) => _default,
                (FilterType.Equals, ValueMessageType.Raw, _) => new EqualsFilter<TValue>(sample),
                (FilterType.Equals, ValueMessageType.Json, string json) => (IKeyFilter<TValue>)new JsonEqualsFilter(json),
                _ => throw new ArgumentException($"Invalid filter type {filterValueType} for value type {valueType} with sample type {typeof(TValue).Name}.", nameof(filterValueType)),
            };
        }

        private readonly DefaultFilter<TValue> _default = new();
    }
}
