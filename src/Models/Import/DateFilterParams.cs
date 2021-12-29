namespace KafkaSnapshot.Models.Import
{
    /// <summary>
    /// Message offset date filtration range.
    /// </summary>
    /// <param name="StartDate">Start offset date.</param>
    /// <param name="EndDate">End offset date.</param>
    public record DateFilterParams(DateTime? StartDate, DateTime? EndDate);
}