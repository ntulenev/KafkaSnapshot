namespace KafkaSnapshot.Models.Sorting;

/// <summary>
/// Kafka items sort order.
/// </summary>
public enum SortingOrder
{
    /// <summary>
    /// Sort in ascending order.
    /// </summary>
    Ascending,

    /// <summary>
    /// Sort in descending order.
    /// </summary>
    Descending,

    /// <summary>
    /// Do not apply sorting.
    /// </summary>
    None,

    /// <summary>
    /// Sort in ascending order.
    /// </summary>
    [Obsolete("Use Ascending instead.")]
    Ask = Ascending,

    /// <summary>
    /// Sort in descending order.
    /// </summary>
    [Obsolete("Use Descending instead.")]
    Desk = Descending,

    /// <summary>
    /// Do not apply sorting.
    /// </summary>
    [Obsolete("Use None instead.")]
    No = None
}
