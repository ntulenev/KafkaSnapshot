namespace KafkaSnapshot.Models.Filters
{
    /// <summary>
    /// Type of data filter.
    /// </summary>
    public enum FilterType
    {
        None,
        Equals,
        Contains,
        LessOrEquals,
        GreaterOrEquals
    }
}
