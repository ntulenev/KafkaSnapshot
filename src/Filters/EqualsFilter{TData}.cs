using KafkaSnapshot.Abstractions.Filters;

namespace KafkaSnapshot.Filters;

/// <summary>
/// Check data on equality with sample.
/// </summary>
/// <typeparam name="TData">Message data type.</typeparam>
/// <remarks>
/// Creates <see cref="EqualsFilter{TData}"/>.
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

    private readonly TData _sample = ValidateSample(sample);

    private static TData ValidateSample(TData sample)
    {
        ArgumentNullException.ThrowIfNull(sample);

        return sample;
    }
}
