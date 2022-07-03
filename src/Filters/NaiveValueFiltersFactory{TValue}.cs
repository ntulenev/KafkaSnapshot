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
            //TODO Add cases
            return _default;
        }

        private readonly DefaultFilter<TValue> _default = new();


    }
}
