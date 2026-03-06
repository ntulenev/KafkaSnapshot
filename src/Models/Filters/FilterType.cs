namespace KafkaSnapshot.Models.Filters;

/// <summary>
/// Type of data filter.
/// </summary>
public enum FilterType
{
    /// <summary>
    /// No filtering is applied.
    /// </summary>
    None,

    /// <summary>
    /// Filter by exact equality.
    /// </summary>
    Equals,

    /// <summary>
    /// Filter by substring inclusion.
    /// </summary>
    Contains,

    /// <summary>
    /// Filter values less than or equal to the filter key.
    /// </summary>
    LessOrEquals,

    /// <summary>
    /// Filter values greater than or equal to the filter key.
    /// </summary>
    GreaterOrEquals
}
