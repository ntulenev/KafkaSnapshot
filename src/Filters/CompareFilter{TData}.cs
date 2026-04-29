using KafkaSnapshot.Abstractions.Filters;

namespace KafkaSnapshot.Filters;

/// <summary>
/// Compares input values with a sample value.
/// </summary>
/// <typeparam name="TData">Comparable data type.</typeparam>
/// <param name="sample">Sample value for comparison.</param>
/// <param name="greaterOrEquals">
/// If true, matches values greater than or equal to sample; otherwise less than or equal.
/// </param>
public class CompareFilter<TData>(TData sample, bool greaterOrEquals) :
    IDataFilter<TData>
    where TData : IComparable<TData>
{
    /// <summary>
    /// Checks whether the data matches the configured comparison rule.
    /// </summary>
    /// <param name="data">Data to compare.</param>
    /// <returns>True if data matches comparison rule; otherwise false.</returns>
    public bool IsMatch(TData data)
    {
        ArgumentNullException.ThrowIfNull(data);

        var result = data.CompareTo(_sample);

        if (IsGreaterOrEquals)
        {
            return result >= 0;
        }
        else
        {
            return result <= 0;
        }
    }

    /// <summary>
    /// Gets whether comparison is greater-or-equal mode.
    /// </summary>
    public bool IsGreaterOrEquals { get; } = greaterOrEquals;

    private readonly TData _sample = ValidateSample(sample);

    private static TData ValidateSample(TData sample)
    {
        ArgumentNullException.ThrowIfNull(sample);

        return sample;
    }
}
