namespace KafkaSnapshot.Models.Filters;

/// <summary>
/// Message offset date filtration range.
/// </summary>
public sealed class DateFilterRange
{
    /// <summary>
    /// Start offset date in UTC.
    /// </summary>
    public DateTime? StartDate { get; }

    /// <summary>
    /// End offset date in UTC.
    /// </summary>
    public DateTime? EndDate { get; }

    /// <summary>
    /// Creates <see cref="DateFilterRange"/>.
    /// </summary>
    /// <param name="startDate">Start offset date. Local dates are converted to UTC, unspecified dates are treated as UTC.</param>
    /// <param name="endDate">End offset date. Local dates are converted to UTC, unspecified dates are treated as UTC.</param>
    /// <exception cref="ArgumentException">Throws if <paramref name="startDate"/> 
    /// greater then <paramref name="endDate"/>.</exception>
    public DateFilterRange(DateTime? startDate, DateTime? endDate)
    {
        var utcStartDate = NormalizeToUtc(startDate);
        var utcEndDate = NormalizeToUtc(endDate);

        if (utcStartDate is DateTime start && utcEndDate is DateTime end)
        {
            ArgumentOutOfRangeException.ThrowIfGreaterThan(start, end, nameof(startDate));
        }

        StartDate = utcStartDate;
        EndDate = utcEndDate;
    }

    private static DateTime? NormalizeToUtc(DateTime? date)
        =>
        date?.Kind switch
        {
            null => null,
            DateTimeKind.Utc => date.Value,
            DateTimeKind.Local => date.Value.ToUniversalTime(),
            DateTimeKind.Unspecified => DateTime.SpecifyKind(date.Value, DateTimeKind.Utc),
            _ => date.Value
        };
}
