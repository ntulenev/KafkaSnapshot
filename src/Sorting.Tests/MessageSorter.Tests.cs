using FluentAssertions;

using KafkaSnapshot.Models.Message;
using KafkaSnapshot.Models.Sorting;

namespace KafkaSnapshot.Sorting.Tests;

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

    [Theory(DisplayName = "MessageSorter can skip sort data.")]
    [Trait("Category", "Unit")]
    [InlineData(SortingType.Partition)]
    [InlineData(SortingType.Time)]
    public void MessageSorterCanSkipSorting(SortingType type)
    {
        // Arrange 
        IEnumerable<KeyValuePair<int, KafkaMessage<int>>> data = new[]
        {
             new KeyValuePair<int, KafkaMessage<int>>(1,new KafkaMessage<int>(1,new KafkaMetadata(DateTime.UtcNow,1,1))),
             new KeyValuePair<int, KafkaMessage<int>>(2,new KafkaMessage<int>(2,new KafkaMetadata(DateTime.UtcNow,1,1))),
             new KeyValuePair<int, KafkaMessage<int>>(3,new KafkaMessage<int>(3,new KafkaMetadata(DateTime.UtcNow,1,1))),
        };

        var sorter = new MessageSorter<int, int>(new Models.Sorting.SortingParams(type, SortingOrder.No));

        // Act
        var result = sorter.Sort(data);

        // Assert
        result.Should().BeSameAs(data);
    }

    [Fact(DisplayName = "MessageSorter can sort partition ask.")]
    [Trait("Category", "Unit")]
    public void MessageSorterCanSortingPartitionAsk()
    {
        // Arrange 
        var date = DateTime.Now;

        var msg3 = new KeyValuePair<int, KafkaMessage<int>>(1, new KafkaMessage<int>(1, new KafkaMetadata(date, 3, 1)));
        var msg1 = new KeyValuePair<int, KafkaMessage<int>>(1, new KafkaMessage<int>(1, new KafkaMetadata(date, 1, 1)));
        var msg2 = new KeyValuePair<int, KafkaMessage<int>>(1, new KafkaMessage<int>(1, new KafkaMetadata(date, 2, 1)));

        var sorter = new MessageSorter<int, int>(new Models.Sorting.SortingParams(SortingType.Partition, SortingOrder.Ask));

        // Act
        var result = sorter.Sort(new[] { msg3, msg1, msg2 });

        // Assert
        result.Should().BeEquivalentTo(new[] { msg1, msg2, msg3 });
    }

    [Fact(DisplayName = "MessageSorter can sort partition desc.")]
    [Trait("Category", "Unit")]
    public void MessageSorterCanSortingPartitionDesc()
    {
        // Arrange 
        var date = DateTime.Now;

        var msg3 = new KeyValuePair<int, KafkaMessage<int>>(1, new KafkaMessage<int>(1, new KafkaMetadata(date, 3, 1)));
        var msg1 = new KeyValuePair<int, KafkaMessage<int>>(1, new KafkaMessage<int>(1, new KafkaMetadata(date, 1, 1)));
        var msg2 = new KeyValuePair<int, KafkaMessage<int>>(1, new KafkaMessage<int>(1, new KafkaMetadata(date, 2, 1)));

        var sorter = new MessageSorter<int, int>(new Models.Sorting.SortingParams(SortingType.Partition, SortingOrder.Ask));

        // Act
        var result = sorter.Sort(new[] { msg3, msg1, msg2 });

        // Assert
        result.Should().BeEquivalentTo(new[] { msg3, msg2, msg1 });
    }

    [Fact(DisplayName = "MessageSorter can sort date ask.")]
    [Trait("Category", "Unit")]
    public void MessageSorterCanSortingDateAsk()
    {
        // Arrange 
        var date1 = DateTime.Now;
        var date2 = date1.AddDays(1);
        var date3 = date2.AddDays(1);

        var msg3 = new KeyValuePair<int, KafkaMessage<int>>(1, new KafkaMessage<int>(1, new KafkaMetadata(date3, 1, 1)));
        var msg1 = new KeyValuePair<int, KafkaMessage<int>>(1, new KafkaMessage<int>(1, new KafkaMetadata(date2, 1, 1)));
        var msg2 = new KeyValuePair<int, KafkaMessage<int>>(1, new KafkaMessage<int>(1, new KafkaMetadata(date1, 1, 1)));

        var sorter = new MessageSorter<int, int>(new Models.Sorting.SortingParams(SortingType.Partition, SortingOrder.Ask));

        // Act
        var result = sorter.Sort(new[] { msg3, msg1, msg2 });

        // Assert
        result.Should().BeEquivalentTo(new[] { msg1, msg2, msg3 });
    }

    [Fact(DisplayName = "MessageSorter can sort date desc.")]
    [Trait("Category", "Unit")]
    public void MessageSorterCanSortingDateDesc()
    {
        // Arrange 
        var date1 = DateTime.Now;
        var date2 = date1.AddDays(1);
        var date3 = date2.AddDays(1);

        var msg3 = new KeyValuePair<int, KafkaMessage<int>>(1, new KafkaMessage<int>(1, new KafkaMetadata(date3, 1, 1)));
        var msg1 = new KeyValuePair<int, KafkaMessage<int>>(1, new KafkaMessage<int>(1, new KafkaMetadata(date2, 1, 1)));
        var msg2 = new KeyValuePair<int, KafkaMessage<int>>(1, new KafkaMessage<int>(1, new KafkaMetadata(date1, 1, 1)));

        var sorter = new MessageSorter<int, int>(new Models.Sorting.SortingParams(SortingType.Partition, SortingOrder.Ask));

        // Act
        var result = sorter.Sort(new[] { msg3, msg1, msg2 });

        // Assert
        result.Should().BeEquivalentTo(new[] { msg3, msg2, msg1 });
    }

}
