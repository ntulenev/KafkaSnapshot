using KafkaSnapshot.Abstractions.Filters;

namespace KafkaSnapshot.Filters;

/// <summary>
/// Check data on equality with sample.
/// </summary>
/// <typeparam name="TKey">Message data type.</typeparam>
public class EqualsFilter<TData> : 
    IDataFilter<TData> 
    where TData : notnull
{
    /// <summary>
    /// Creates <see cref="EqualsFilter{TKey}"/>.
    /// </summary>
    /// <param name="sample">Data sample.</param>
    public EqualsFilter(TData sample)
    {
        _sample = sample ?? throw new ArgumentNullException(nameof(sample));
    }

    /// <inheritdoc/>
    public bool IsMatch(TData data)
    {
        ArgumentNullException.ThrowIfNull(data);

        return data.Equals(_sample);
    }

    private readonly TData _sample;
}
