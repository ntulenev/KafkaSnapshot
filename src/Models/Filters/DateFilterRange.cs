namespace KafkaSnapshot.Models.Filters;

/// <summary>
/// Message offset date filtration range.
/// </summary>
/// <param name="StartDate">Start offset date.</param>
/// <param name="EndDate">End offset date.</param>
public class DateFilterRange
{
    /// <summary>
    /// Start offset date.
    /// </summary>
    public DateTime? StartDate { get; }

    /// <summary>
    /// End offset date.
    /// </summary>
    public DateTime? EndDate { get; }

    /// <summary>
    /// Creates <see cref="DateFilterRange"/>.
    /// </summary>
    /// <param name="startDate">Start offset date.</param>
    /// <param name="endDate">End offset date.</param>
    /// <exception cref="ArgumentException">Throws if <paramref name="startDate"/> 
    /// greater then <paramref name="endDate"/>.</exception>
    public DateFilterRange(DateTime? startDate, DateTime? endDate)
    {
        if (startDate is not null && endDate is not null)
        {
            if (startDate > endDate)
            {
                throw new ArgumentException($"Start date {startDate} should " +
                    $"be less or equals then end date {endDate}.");
            }
        }

        StartDate = startDate;
        EndDate = endDate;
    }
}