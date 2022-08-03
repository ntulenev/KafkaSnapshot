namespace KafkaSnapshot.Models.Filters
{
    /// <summary>
    /// Message offset date filtration range.
    /// </summary>
    /// <param name="StartDate">Start offset date.</param>
    /// <param name="EndDate">End offset date.</param>
    public record DateFilterRange(DateTime? StartDate, DateTime? EndDate);
}