using Microsoft.Extensions.Logging;

using KafkaSnapshot.Abstractions.Import;
using KafkaSnapshot.Abstractions.Export;
using KafkaSnapshot.Abstractions.Processing;
using KafkaSnapshot.Models.Processing;
using KafkaSnapshot.Models.Export;
using KafkaSnapshot.Models.Import;
using KafkaSnapshot.Abstractions.Filters;
using KafkaSnapshot.Models.Message;
using KafkaSnapshot.Models.Sorting;
using KafkaSnapshot.Models.Filters;

namespace KafkaSnapshot.Processing
{
    /// <summary>
    /// Single topic processor that loads data from Apache Kafka and exports to file.
    /// </summary>
    /// <typeparam name="TKey">Message Key.</typeparam>
    /// <typeparam name="TKeyMarker">Message Key marker.</typeparam>
    /// <typeparam name="TValue">Message Value.</typeparam>
    public class ProcessingUnit<TKey, TKeyMarker, TValue> : IProcessingUnit where TKey : notnull
                                                                            where TKeyMarker : IKeyRepresentationMarker
                                                                            where TValue : notnull
    {
        /// <summary>
        /// Creates <see cref="ProcessingUnit{TKey,TKeyMarker, TValue}"/>.
        /// </summary>
        /// <param name="logger">Creates logger for <see cref="ProcessingUnit{TKey, TKeyMarker, TValue}"/>.</param>
        /// <param name="topic">Apahe Kafka topic.</param>
        /// <param name="kafkaLoader">Kafka topic loader.</param>
        /// <param name="exporter">Data exporter.</param>
        public ProcessingUnit(ILogger<ProcessingUnit<TKey, TKeyMarker, TValue>> logger,
                              ProcessingTopic<TKey> topic,
                              ISnapshotLoader<TKey, TValue> kafkaLoader,
                              IDataExporter<TKey, TKeyMarker, TValue, ExportedTopic> exporter,
                              IKeyFiltersFactory<TKey> keyFilterFactory,
                              IValueFilterFactory<TValue> valueFilterFactory
                              )
        {
            _logger = logger ?? throw new ArgumentNullException(nameof(logger));
            _kafkaLoader = kafkaLoader ?? throw new ArgumentNullException(nameof(kafkaLoader));
            _exporter = exporter ?? throw new ArgumentNullException(nameof(exporter));

            ArgumentNullException.ThrowIfNull(topic);
            ArgumentNullException.ThrowIfNull(keyFilterFactory);
            ArgumentNullException.ThrowIfNull(valueFilterFactory);

            _keyFilter = keyFilterFactory.Create(topic.FilterKeyType, topic.KeyType, topic.FilterKeyValue);

            //Stub for value filters
            //TODO Add params later
            _valueFilter = valueFilterFactory.Create(Models.Filters.FilterType.None, ValueMessageType.Raw, default!);

            //Stub for getting sort from topic
            //TODO Add params later
            var sort = new SortingParams(SortingType.Time, SortingOrder.No);

            _topicParams = new LoadingTopic(
                                topic.Name,
                                topic.LoadWithCompacting,
                                topic.DateRange,
                                topic.PartitionIdsFilter,
                                sort
                                );

            _exportedTopic = new ExportedTopic(topic.Name, topic.ExportName, topic.ExportRawMessage);

            _logger.LogDebug("Instance created for topic {@topic}.", topic);
        }

        /// <inheritdoc/>
        public async Task ProcessAsync(CancellationToken ct)
        {
            _logger.LogDebug("Start loading data from Kafka.");
            var items = await _kafkaLoader.LoadCompactSnapshotAsync(
                _topicParams,
                _keyFilter,
                _valueFilter,
                ct).ConfigureAwait(false);

            items = SortData(items);

            _logger.LogDebug("Start exporting data to file.");
            await _exporter.ExportAsync(items, _exportedTopic, ct).ConfigureAwait(false);
        }

        private IEnumerable<KeyValuePair<TKey, KafkaMessage<TValue>>> SortData(
            IEnumerable<KeyValuePair<TKey, KafkaMessage<TValue>>> items)
        {
            // TODO Move to separate class
            return (_topicParams.Sorting) switch
            {
                { Order: SortingOrder.No, Type: _ } => items,
                { Order: SortingOrder.Ask, Type: SortingType.Time } => items.OrderBy(x => x.Value.Meta.Timestamp).ToList(),
                { Order: SortingOrder.Desk, Type: SortingType.Time } => items.OrderByDescending(x => x.Value.Meta.Timestamp).ToList(),
                { Order: SortingOrder.Ask, Type: SortingType.Partition } => items.OrderBy(x => x.Value.Meta.Partition).ThenBy(x => x.Value.Meta.Timestamp).ToList(),
                { Order: SortingOrder.Desk, Type: SortingType.Partition } => items.OrderByDescending(x => x.Value.Meta.Partition).ThenBy(x => x.Value.Meta.Timestamp).ToList(),
                _ => throw new NotImplementedException("Sort type not implemented")
            };
        }

        /// <inheritdoc/>
        public string TopicName => _topicParams.Value;

        private readonly ISnapshotLoader<TKey, TValue> _kafkaLoader;
        private readonly IDataExporter<TKey, TKeyMarker, TValue, ExportedTopic> _exporter;
        private readonly ILogger<ProcessingUnit<TKey, TKeyMarker, TValue>> _logger;
        private readonly IDataFilter<TKey> _keyFilter;
        private readonly IDataFilter<TValue> _valueFilter;
        private readonly LoadingTopic _topicParams;
        private readonly ExportedTopic _exportedTopic;
    }
}
