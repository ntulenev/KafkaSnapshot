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

        if (!IsValid(utcStartDate, utcEndDate))
        {
            throw new ArgumentOutOfRangeException(nameof(startDate), "Start date exceeds end date.");
        }

        StartDate = utcStartDate;
        EndDate = utcEndDate;
    }

    /// <summary>Checks whether the timestamp exceeds the inclusive upper boundary.</summary>
    public bool IsAfterEnd(DateTimeOffset timestamp) => EndDate is DateTimeOffset end && timestamp > end;

    /// <summary>Checks that the date boundaries are ordered.</summary>
    public static bool IsValid(DateTimeOffset? start, DateTimeOffset? end)
        => !start.HasValue || !end.HasValue || start.Value <= end.Value;

    private static DateTimeOffset? NormalizeToUtc(DateTimeOffset? date)
        => date?.ToUniversalTime();
}
