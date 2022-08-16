using FluentAssertions;

using KafkaSnapshot.Models.Sorting;

namespace KafkaSnapshot.Sorting.Tests
{
    public class MessageSorterTests
    {
        [Fact(DisplayName = "MessageSorter can't be created without rules.")]
        [Trait("Category", "Unit")]
        public void MessageSorterCantBeCreatedWithoutRules()
        {
            // Act
            var exception = Record.Exception(() => new MessageSorter<object, object>(null!));

            // Assert
            exception.Should().NotBeNull().And.BeOfType<ArgumentNullException>();
        }

        [Theory(DisplayName = "MessageSorter can't sort null data.")]
        [Trait("Category", "Unit")]
        [InlineData(SortingType.Partition, SortingOrder.No)]
        [InlineData(SortingType.Time, SortingOrder.No)]
        [InlineData(SortingType.Partition, SortingOrder.Ask)]
        [InlineData(SortingType.Time, SortingOrder.Ask)]
        [InlineData(SortingType.Partition, SortingOrder.Desk)]
        [InlineData(SortingType.Time, SortingOrder.Desk)]
        public void MessageSorterCantSortNullData(SortingType type, SortingOrder order)
        {
            // Arrange
            var sorter = new MessageSorter<object, object>(new Models.Sorting.SortingParams(type, order));

            // Act
            var exception = Record.Exception(() => sorter.Sort(null!));

            // Assert
            exception.Should().NotBeNull().And.BeOfType<ArgumentNullException>();
        }

    }
}
