using System;
using System.Threading;
using System.Threading.Tasks;

using KafkaSnapshot.Abstractions.Import;
using KafkaSnapshot.Abstractions.Export;
using KafkaSnapshot.Abstractions.Processing;
using KafkaSnapshot.Export;
using KafkaSnapshot.Models.Processing;
using Microsoft.Extensions.Logging;

namespace KafkaSnapshot.Processing
{
    /// <summary>
    /// Single topic processor that loads data from Apache Kafka and exports to file.
    /// </summary>
    /// <typeparam name="TKey">Message Key.</typeparam>
    /// <typeparam name="TValue">Message Value.</typeparam>
    public class ProcessingUnit<TKey, TValue> : IProcessingUnit where TKey : notnull
    {
        /// <summary>
        /// Creates <see cref="ProcessingUnit{TKey, TValue}"/>.
        /// </summary>
        /// <param name="topic">Apahe Kafka topic.</param>
        /// <param name="kafkaLoader">Kafka topic loader.</param>
        /// <param name="exporter">Data exporter.</param>
        public ProcessingUnit(ILogger<ProcessingUnit<TKey, TValue>> logger,
                              ProcessingTopic topic,
                              ISnapshotLoader<TKey, TValue> kafkaLoader,
                              IDataExporter<TKey, TValue,
                              ExportedFileTopic> exporter)
        {
            _logger = logger ?? throw new ArgumentNullException(nameof(logger));
            _topic = topic ?? throw new ArgumentNullException(nameof(topic));
            _kafkaLoader = kafkaLoader ?? throw new ArgumentNullException(nameof(kafkaLoader));
            _exporter = exporter ?? throw new ArgumentNullException(nameof(exporter));

            _logger.LogDebug("Instance created for topic {topic}.", _topic);
        }

        /// <inheritdoc/>
        public async Task ProcessAsync(CancellationToken ct)
        {
            _logger.LogDebug("Start loading data from Kafka.");
            var items = await _kafkaLoader.LoadCompactSnapshotAsync(ct).ConfigureAwait(false);

            _logger.LogDebug("Start exporting data to file.");
            await _exporter.ExportAsync(items, new ExportedFileTopic(_topic.Name, _topic.ExportName), ct).ConfigureAwait(false);
        }

        /// <inheritdoc/>
        public ProcessingTopic Topic => _topic;

        private readonly ISnapshotLoader<TKey, TValue> _kafkaLoader;
        private readonly IDataExporter<TKey, TValue, ExportedFileTopic> _exporter;
        private readonly ProcessingTopic _topic;
        private readonly ILogger<ProcessingUnit<TKey, TValue>> _logger;
    }
}
