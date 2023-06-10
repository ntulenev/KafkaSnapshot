using KafkaSnapshot.Models.Filters;

namespace KafkaSnapshot.Abstractions.Filters;

/// <summary>
/// Interface for creating a value filter based on a condition.
/// </summary>
/// <typeparam name="TValue">The type of message value.</typeparam>
public interface IValueFilterFactory<TValue> where TValue : notnull
{
    /// <summary>
    /// Creates a filter for the given value and filter types.
    /// </summary>
    /// <param name="filterValueType">The type of filter to create.</param>
    /// <param name="valueType">The type of value being filtered.</param>
    /// <param name="sample">A sample of the value being filtered.</param>
    /// <returns>A filter for the given value and filter types.</returns>
    public IDataFilter<TValue> Create(
            FilterType filterValueType, 
            ValueMessageType valueType, 
            TValue sample);
}
