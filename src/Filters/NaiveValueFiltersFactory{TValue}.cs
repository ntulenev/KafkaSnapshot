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
        public IDataFilter<TValue> Create(FilterType filterValueType, ValueMessageType valueType, TValue sample)
        {
            IDataFilter<TValue> filter = (filterValueType, valueType, sample) switch
            {
                (FilterType.None, _, _) => _default,
                (FilterType.Equals, ValueMessageType.Raw, _) => new EqualsFilter<TValue>(sample),
                (FilterType.Equals, ValueMessageType.Json, string json) => (IDataFilter<TValue>)new JsonEqualsFilter(json),
                _ => throw new ArgumentException($"Invalid filter type {filterValueType} for value type {valueType} with sample type {typeof(TValue).Name}.", nameof(filterValueType)),
            };

            return filter;
        }

        private readonly DefaultFilter<TValue> _default = new();
    }
}
