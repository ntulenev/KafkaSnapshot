namespace KafkaSnapshot.Models.Sorting;

/// <summary>
/// Sorting parameters.
/// </summary>
public sealed record SortingParams
{
    /// <summary>
    /// Creates <see cref="SortingParams"/>.
    /// </summary>
    /// <param name="type">Message attribute for sorting.</param>
    /// <param name="order">Sort order.</param>
    public SortingParams(SortingType type, SortingOrder order)
    {
        if (!Enum.IsDefined(type))
        {
            throw new ArgumentOutOfRangeException(
                nameof(type),
                type,
                $"Invalid SortingType value {type}.");
        }

        if (!Enum.IsDefined(order))
        {
            throw new ArgumentOutOfRangeException(
                nameof(order),
                order,
                $"Invalid SortingOrder value {order}.");
        }

        Type = type;
        Order = order;
    }

    /// <summary>
    /// Message attribute for sorting.
    /// </summary>
    public SortingType Type { get; init; }

    /// <summary>
    /// Sort order.
    /// </summary>
    public SortingOrder Order { get; init; }
}
