using KafkaSnapshot.Abstractions.Filters;

namespace KafkaSnapshot.Filters;

public class CompareFilter<TData> : 
    IDataFilter<TData> 
    where TData : IComparable<TData>
{
    public CompareFilter(TData sample, bool greater)
    {
        _sample = sample ?? throw new ArgumentNullException(nameof(sample));
        _greater = greater;
    }

    public bool IsMatch(TData data)
    {
        ArgumentNullException.ThrowIfNull(data);

        var result = data.CompareTo(_sample);

        if (_greater)
        {
            return result >= 0;
        }
        else
        {
            return result <= 0;
        }
    }

    public bool IsGreaterWay => _greater;

    private readonly bool _greater;
    private readonly TData _sample;

}
