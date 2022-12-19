namespace KafkaSnapshot.Abstractions.Filters;

/// <summary>
/// Filter for loading data from Kafka.
/// </summary>
/// <typeparam name="TKey">Filtering data type type.</typeparam>
public interface IDataFilter<TData> where TData : notnull
{
    /// <summary>
    /// Check data on matching some creteria.
    /// </summary>
    /// <param name="data">Message data.</param>
    /// <returns>true is match.</returns>
    public bool IsMatch(TData data);
}
