using FluentAssertions;

using KafkaSnapshot.Models.Filters;

using Xunit;

namespace KafkaSnapshot.Models.Tests;

public class DateFilterRangeTests
{
    [Fact(DisplayName = "DateFilterRange can be created with null dates.")]
    [Trait("Category", "Unit")]
    public void CanCreateDateFilterRangeWithNulls()
    {
        // Act
        var exception = Record.Exception(() => new DateFilterRange(null!, null!));

        // Assert
        exception.Should().BeNull();
    }

    [Fact(DisplayName = "DateFilterRange can be created with start null date.")]
    [Trait("Category", "Unit")]
    public void CanCreateDateFilterRangeStartNull()
    {
        // Act
        var exception = Record.Exception(() => new DateFilterRange(null!, DateTime.Now));

        // Assert
        exception.Should().BeNull();
    }

    [Fact(DisplayName = "DateFilterRange can be created with end null date.")]
    [Trait("Category", "Unit")]
    public void CanCreateDateFilterRangeEndNull()
    {
        // Act
        var exception = Record.Exception(() => new DateFilterRange(DateTime.Now, null!));

        // Assert
        exception.Should().BeNull();
    }

    [Fact(DisplayName = "DateFilterRange can be created with end both dates.")]
    [Trait("Category", "Unit")]
    public void CanCreateDateFilterRangeBothDates()
    {
        // Arrange
        var dtStart = new DateTime(2012, 1, 1);
        var dtEnd = new DateTime(2013, 1, 1);

        // Act
        var exception = Record.Exception(() => new DateFilterRange(dtStart, dtEnd));

        // Assert
        exception.Should().BeNull();
    }

    [Fact(DisplayName = "DateFilterRange can be created with end both dates.")]
    [Trait("Category", "Unit")]
    public void CanCreateDateFilterRangeBothSameDates()
    {
        // Arrange
        var date = new DateTime(2012, 1, 1);

        // Act
        var exception = Record.Exception(() => new DateFilterRange(date, date));

        // Assert
        exception.Should().BeNull();
    }

    [Fact(DisplayName = "DateFilterRange converts local dates to UTC.")]
    [Trait("Category", "Unit")]
    public void DateFilterRangeConvertsLocalDatesToUtc()
    {
        // Arrange
        var start = new DateTime(2024, 1, 1, 12, 0, 0, DateTimeKind.Local);
        var end = start.AddHours(1);

        // Act
        var result = new DateFilterRange(start, end);

        // Assert
        result.StartDate.Should().Be(start.ToUniversalTime());
        result.StartDate!.Value.Kind.Should().Be(DateTimeKind.Utc);
        result.EndDate.Should().Be(end.ToUniversalTime());
        result.EndDate!.Value.Kind.Should().Be(DateTimeKind.Utc);
    }

    [Fact(DisplayName = "DateFilterRange treats unspecified dates as UTC.")]
    [Trait("Category", "Unit")]
    public void DateFilterRangeTreatsUnspecifiedDatesAsUtc()
    {
        // Arrange
        var start = new DateTime(2024, 1, 1, 12, 0, 0, DateTimeKind.Unspecified);
        var end = start.AddHours(1);

        // Act
        var result = new DateFilterRange(start, end);

        // Assert
        result.StartDate.Should().Be(DateTime.SpecifyKind(start, DateTimeKind.Utc));
        result.StartDate!.Value.Kind.Should().Be(DateTimeKind.Utc);
        result.EndDate.Should().Be(DateTime.SpecifyKind(end, DateTimeKind.Utc));
        result.EndDate!.Value.Kind.Should().Be(DateTimeKind.Utc);
    }

    [Fact(DisplayName = "DateFilterRange cant be created when start bigger then end.")]
    [Trait("Category", "Unit")]
    public void CantCreateDateFilterRangeWhenStardDateBigger()
    {
        // Arrange
        var dtStart = new DateTime(2013, 1, 1);
        var dtEnd = new DateTime(2012, 1, 1);

        // Act
        var exception = Record.Exception(() => new DateFilterRange(dtStart, dtEnd));

        // Assert
        exception.Should().NotBeNull().And.BeOfType<ArgumentOutOfRangeException>();
    }
}
