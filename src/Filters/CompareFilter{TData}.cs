using KafkaSnapshot.Abstractions.Filters;

namespace KafkaSnapshot.Filters;

public class CompareFilter<TData> : 
    IDataFilter<TData> 
    where TData : IComparable<TData>
{
    public CompareFilter(TData sample, bool greaterOrEquals)
    {
        _sample = sample ?? throw new ArgumentNullException(nameof(sample));
        _greaterOrEquals = greaterOrEquals;
    }

    public bool IsMatch(TData data)
    {
        ArgumentNullException.ThrowIfNull(data);

        var result = data.CompareTo(_sample);

        if (_greaterOrEquals)
        {
            return result >= 0;
        }
        else
        {
            return result <= 0;
        }
    }

    public bool IsGreaterOrEquals => _greaterOrEquals;

    private readonly bool _greaterOrEquals;
    private readonly TData _sample;

}
