using KafkaSnapshot.Abstractions.Filters;

namespace KafkaSnapshot.Filters;

/// <summary>
/// Default filter for message (match for any data).
/// </summary>
/// <typeparam name="TData">Data type.</typeparam>
public sealed class DefaultFilter<TData> : 
    IDataFilter<TData> 
    where TData : notnull
{
    /// <inheritdoc/>
    public bool IsMatch(TData data) => true;
}
