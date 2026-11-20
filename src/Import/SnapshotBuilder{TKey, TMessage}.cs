using System.Collections.ObjectModel;

using KafkaSnapshot.Abstractions.Sorting;
using KafkaSnapshot.Models.Message;

using Microsoft.Extensions.Logging;

namespace KafkaSnapshot.Import;

/// <summary>
/// Creates a final snapshot from consumed Kafka messages.
/// </summary>
/// <typeparam name="TKey">Kafka message key type.</typeparam>
/// <typeparam name="TMessage">Decoded Kafka message value type.</typeparam>
public class SnapshotBuilder<TKey, TMessage>
    : ISnapshotBuilder<TKey, TMessage>
    where TKey : notnull
    where TMessage : notnull
{
    /// <summary>
    /// Creates <see cref="SnapshotBuilder{TKey, TMessage}"/>.
    /// </summary>
    /// <param name="logger">Logger.</param>
    /// <param name="sorter">Message sorter used when compaction is disabled.</param>
    public SnapshotBuilder(
        ILogger<SnapshotBuilder<TKey, TMessage>> logger,
        IMessageSorter<TKey, TMessage> sorter)
    {
        ArgumentNullException.ThrowIfNull(logger);
        ArgumentNullException.ThrowIfNull(sorter);

        _logger = logger;
        _sorter = sorter;
    }

    /// <inheritdoc/>
    public bool RequiresBuffering(bool withCompacting) => withCompacting || _sorter.IsEnabled;

    /// <summary>
    /// Creates a snapshot from consumed messages.
    /// </summary>
    /// <param name="items">Consumed messages.</param>
    /// <param name="withCompacting">Whether duplicated keys should be compacted.</param>
    public IEnumerable<KeyValuePair<TKey, KafkaMessage<TMessage>>> CreateSnapshot(
        IEnumerable<KeyValuePair<TKey, KafkaMessage<TMessage>>> items,
        bool withCompacting)
    {
        ArgumentNullException.ThrowIfNull(items);

        IEnumerable<KeyValuePair<TKey, KafkaMessage<TMessage>>> result;

        if (withCompacting)
        {
            _logger.LogDebug("Compacting data");

            var latest = new Dictionary<TKey, KafkaMessage<TMessage>>();
            foreach (var item in items)
            {
                if (item.Key is not null)
                {
                    latest[item.Key] = item.Value;
                }
            }

            result = new ReadOnlyDictionary<TKey, KafkaMessage<TMessage>>(latest);

            if (_logger.IsEnabled(LogLevel.Debug))
            {
                _logger.LogDebug("Created compacting state for {ItemCount} item(s)", latest.Count);
            }
        }
        else
        {
            result = _sorter.Sort(items);

            _logger.LogDebug("Created state without compacting");
        }

        return result;
    }

    private readonly ILogger _logger;
    private readonly IMessageSorter<TKey, TMessage> _sorter;
}
