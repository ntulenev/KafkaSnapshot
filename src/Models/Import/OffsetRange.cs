namespace KafkaSnapshot.Models.Import;

/// <summary>A captured offset interval with an inclusive start and exclusive end.</summary>
public sealed record OffsetRange
{
    /// <summary>Creates an ordered, non-negative snapshot interval.</summary>
    public OffsetRange(long low, long high)
    {
        ArgumentOutOfRangeException.ThrowIfNegative(low);
        ArgumentOutOfRangeException.ThrowIfLessThan(high, low);
        Low = low;
        High = high;
    }

    /// <summary>First offset in the interval.</summary>
    public long Low { get; }

    /// <summary>Exclusive upper offset.</summary>
    public long High { get; }

    /// <summary>Whether the interval contains no offsets.</summary>
    public bool IsEmpty => Low == High;

    /// <summary>Checks whether an offset belongs to the captured interval.</summary>
    public bool Contains(long offset) => offset >= Low && offset < High;

    /// <summary>Checks whether more offsets remain after the consumed offset.</summary>
    public bool ShouldContinueReadingAfter(long offset) => !IsEmpty && offset < High - 1;
}
