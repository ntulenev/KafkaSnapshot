namespace KafkaSnapshot.Models.Sorting;

/// <summary>
/// Sorting parameters.
/// </summary>
/// <param name="Type">Message attribute for sorting.</param>
/// <param name="Order">Order side.</param>
public sealed record SortingParams(SortingType Type, SortingOrder Order);
