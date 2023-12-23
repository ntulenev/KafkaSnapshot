using KafkaSnapshot.Abstractions.Filters;

namespace KafkaSnapshot.Filters;

/// <summary>
/// Check data on equality with sample.
/// </summary>
/// <typeparam name="TKey">Message data type.</typeparam>
/// <remarks>
/// Creates <see cref="EqualsFilter{TKey}"/>.
/// </remarks>
/// <param name="sample">Data sample.</param>
public sealed class EqualsFilter<TData>(TData sample) : 
    IDataFilter<TData> 
    where TData : notnull
{

    /// <inheritdoc/>
    public bool IsMatch(TData data)
    {
        ArgumentNullException.ThrowIfNull(data);

        return data.Equals(_sample);
    }

    private readonly TData _sample = sample ?? 
        throw new ArgumentNullException(nameof(sample));
}
