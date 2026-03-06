namespace KafkaSnapshot.Models.Sorting;

/// <summary>
/// Kafka items sort order.
/// </summary>
public enum SortingOrder
{
    /// <summary>
    /// Sort in ascending order.
    /// </summary>
    Ask,

    /// <summary>
    /// Sort in descending order.
    /// </summary>
    Desk,

    /// <summary>
    /// Do not apply sorting.
    /// </summary>
    No
}
