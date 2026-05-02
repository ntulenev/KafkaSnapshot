namespace KafkaSnapshot.Models.Filters;

/// <summary>
/// Message offset date filtration range.
/// </summary>
public sealed class DateFilterRange
{
    /// <summary>
    /// Start offset date in UTC.
    /// </summary>
    public DateTimeOffset? StartDate { get; }

    /// <summary>
    /// End offset date in UTC.
    /// </summary>
    public DateTimeOffset? EndDate { get; }

    /// <summary>
    /// Creates <see cref="DateFilterRange"/>.
    /// </summary>
    /// <param name="startDate">Start offset date. Dates are converted to UTC.</param>
    /// <param name="endDate">End offset date. Dates are converted to UTC.</param>
    /// <exception cref="ArgumentException">Throws if <paramref name="startDate"/> 
    /// greater than <paramref name="endDate"/>.</exception>
    public DateFilterRange(DateTimeOffset? startDate, DateTimeOffset? endDate)
    {
        var utcStartDate = NormalizeToUtc(startDate);
        var utcEndDate = NormalizeToUtc(endDate);

        if (utcStartDate is DateTimeOffset start && utcEndDate is DateTimeOffset end)
        {
            ArgumentOutOfRangeException.ThrowIfGreaterThan(start, end, nameof(startDate));
        }

        StartDate = utcStartDate;
        EndDate = utcEndDate;
    }

    private static DateTimeOffset? NormalizeToUtc(DateTimeOffset? date)
        => date?.ToUniversalTime();
}
