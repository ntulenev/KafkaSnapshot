namespace KafkaSnapshot.Models.Filters;

/// <summary>
/// Message offset date filtration range.
/// </summary>
public sealed class DateFilterRange
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
        if (startDate is DateTime start && endDate is DateTime end)
        {
            ArgumentOutOfRangeException.ThrowIfGreaterThan(start, end, nameof(startDate));
        }

        StartDate = startDate;
        EndDate = endDate;
    }
}
