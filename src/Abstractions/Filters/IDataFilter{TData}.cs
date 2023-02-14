namespace KafkaSnapshot.Abstractions.Filters;

/// <summary>
/// Interface for filtering data loaded from Kafka.
/// </summary>
/// <typeparam name="TData">The type of data being filtered.</typeparam>
public interface IDataFilter<TData> where TData : notnull
{
    /// <summary>
    /// Determines if the provided data matches the filter criteria.
    /// </summary>
    /// <param name="data">The data to check against the filter.</param>
    /// <returns>true if the data matches the filter criteria, false otherwise.</returns>
    public bool IsMatch(TData data);
}
