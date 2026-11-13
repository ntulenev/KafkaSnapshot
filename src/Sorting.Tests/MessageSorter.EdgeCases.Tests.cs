using FluentAssertions;

using KafkaSnapshot.Models.Message;
using KafkaSnapshot.Models.Sorting;

namespace KafkaSnapshot.Sorting.Tests;

[Trait("Category", "Unit")]
public class MessageSorterEdgeCasesTests
{
    [Theory(DisplayName = "MessageSorter orders timestamps ascending within each partition for either partition sort direction.")]
    [InlineData(SortingOrder.Ascending)]
    [InlineData(SortingOrder.Descending)]
    public void PartitionSortUsesAscendingTimestampWithinEachPartition(SortingOrder order)
    {
        var firstEarly = Message(1, 0, 1);
        var firstLate = Message(2, 0, 2);
        var secondEarly = Message(3, 1, 1);
        var secondLate = Message(4, 1, 2);
        var sorter = new MessageSorter<int, string>(new SortingParams(SortingType.Partition, order));

        var result = sorter.Sort([secondLate, firstLate, secondEarly, firstEarly]);

        result.Should().Equal(order == SortingOrder.Ascending
            ? [firstEarly, firstLate, secondEarly, secondLate]
            : [secondEarly, secondLate, firstEarly, firstLate]);
    }

    [Theory(DisplayName = "MessageSorter preserves input order for equal sort keys and accepts empty input.")]
    [InlineData(SortingType.Time, SortingOrder.Ascending)]
    [InlineData(SortingType.Time, SortingOrder.Descending)]
    [InlineData(SortingType.Partition, SortingOrder.Ascending)]
    [InlineData(SortingType.Partition, SortingOrder.Descending)]
    public void EqualSortKeysPreserveInputOrder(SortingType type, SortingOrder order)
    {
        var messages = new[] { Message(3, 0, 1), Message(1, 0, 1), Message(2, 0, 1) };
        var sorter = new MessageSorter<int, string>(new SortingParams(type, order));

        sorter.Sort(messages).Should().Equal(messages);
        sorter.Sort([]).Should().BeEmpty();
    }

    private static KeyValuePair<int, KafkaMessage<string>> Message(int key, int partition, int seconds)
        => new(key, new KafkaMessage<string>(key.ToString(System.Globalization.CultureInfo.InvariantCulture), new KafkaMetadata(DateTime.UnixEpoch.AddSeconds(seconds), partition, key)));
}
