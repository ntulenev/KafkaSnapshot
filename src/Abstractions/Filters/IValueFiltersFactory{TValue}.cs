using KafkaSnapshot.Models.Filters;

namespace KafkaSnapshot.Abstractions.Filters
{
    /// <summary>
    /// Creates ValueFilter by condition.
    /// </summary>
    /// <typeparam name="TValue">Message value type.</typeparam>
    public interface IValueFilterFactory<TValue> where TValue : notnull
    {
        /// <summary>
        /// Creates suitable filter for filter type and value type.
        /// </summary>
        /// <param name="filterValueType">Filter type.</param>
        /// <param name="valueType">Value type.</param>
        /// <param name="sample">Value sample.</param>
        /// <returns>Filter for this value type and filter type.</returns>
        public IDataFilter<TValue> Create(FilterType filterValueType, ValueMessageType valueType, TValue sample);
    }
}
