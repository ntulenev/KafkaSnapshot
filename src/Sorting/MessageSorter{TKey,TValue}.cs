using KafkaSnapshot.Abstractions.Sorting;
using KafkaSnapshot.Models.Message;
using KafkaSnapshot.Models.Sorting;

namespace KafkaSnapshot.Sorting;

/// <summary>
/// Creates <see cref="MessageSorter{TKey, TValue}"/>.
/// </summary>
/// <typeparam name="TKey">Kafka message key type.</typeparam>
/// <typeparam name="TValue">Kafka message value type.</typeparam>
public sealed class MessageSorter<TKey, TValue> : IMessageSorter<TKey, TValue>
                                         where TKey : notnull
                                         where TValue : notnull
{
    /// <summary>
    /// Creates <see cref="MessageSorter{TKey, TValue}"/>.
    /// </summary>
    /// <param name="sortingRules">Sorting parameters.</param>
    /// <exception cref="ArgumentNullException">Thrown when <paramref name="sortingRules"/>
    /// is null.</exception>
    public MessageSorter(SortingParams sortingRules)
    {
        ArgumentNullException.ThrowIfNull(sortingRules);

        _sortingRules = sortingRules;
    }

    /// <inheritdoc/>
    public bool IsEnabled => _sortingRules.IsEnabled;

    /// <inheritdoc/>
    public IEnumerable<KeyValuePair<TKey, KafkaMessage<TValue>>> Sort(
        IEnumerable<KeyValuePair<TKey, KafkaMessage<TValue>>> source)
    {
        ArgumentNullException.ThrowIfNull(source);

        return (_sortingRules.Type, _sortingRules.Order) switch
        {
            (_, SortingOrder.None) => source,
            (SortingType.Time, SortingOrder.Descending) => source.OrderByDescending(x => x.Value.Meta.Timestamp),
            (SortingType.Time, _) => source.OrderBy(x => x.Value.Meta.Timestamp),
            (_, SortingOrder.Descending) => source.OrderByDescending(x => x.Value.Meta.Partition)
                .ThenBy(x => x.Value.Meta.Timestamp),
            _ => source.OrderBy(x => x.Value.Meta.Partition)
                .ThenBy(x => x.Value.Meta.Timestamp)
        };
    }

    private readonly SortingParams _sortingRules;
}
