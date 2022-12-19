namespace KafkaSnapshot.Models.Sorting;

/// <summary>
/// Sorting parameters.
/// </summary>
/// <param name="Type">Message arrtibute for sorting.</param>
/// <param name="Order">Order side.</param>
public record SortingParams(SortingType Type, SortingOrder Order);
