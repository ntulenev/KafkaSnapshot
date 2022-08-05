using FluentAssertions;

using KafkaSnapshot.Models.Filters;

using Xunit;

namespace KafkaSnapshot.Models.Tests
{
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
            exception.Should().NotBeNull().And.BeOfType<ArgumentException>();
        }
    }
}
