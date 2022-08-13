using Microsoft.Extensions.Logging;

using KafkaSnapshot.Abstractions.Processing;

namespace KafkaSnapshot.Processing
{
    /// <summary>
    /// Tool that process topics from Apache Kafka.
    /// </summary>
    public class LoaderTool : ILoaderTool
    {
        /// <summary>
        /// Creates <see cref="LoaderTool"/>.
        /// </summary>
        /// <param name="logger">Logger.</param>
        /// <param name="units">Processors for topics.</param>
        /// <exception cref="ArgumentNullException"></exception>
        public LoaderTool(ILogger<LoaderTool> logger, IReadOnlyCollection<IProcessingUnit> units)
        {
            _logger = logger ?? throw new ArgumentNullException(nameof(logger));
            _units = units ?? throw new ArgumentNullException(nameof(units));

            _logger.LogDebug("Instance created for {count} unit(s).", _units.Count);
        }

        /// <summary>
        /// Runs processing of topics.
        /// </summary>
        /// <param name="ct">Token for cancelling.</param>
        public async Task ProcessAsync(CancellationToken ct)
        {
            _logger.LogInformation("The utility starts loading {count} Apache Kafka topics...", _units.Count);

            int indexer = 0;

            foreach (var unit in _units)
            {
                using var _ = _logger.BeginScope("topic {topic}", unit.TopicName);

                ct.ThrowIfCancellationRequested();

                _logger.LogInformation("{indexer}/{count} Processing topic {topicName}", ++indexer, _units.Count, unit.TopicName);

                try
                {
                    await unit.ProcessAsync(ct).ConfigureAwait(false);

                    _logger.LogDebug("Finish processing topic.");
                }
                catch (OperationCanceledException)
                {

                }
                catch (Exception ex)
                {
                    _logger.LogError(ex, "Error on processing topic {name}", unit.TopicName);
                }

            }

            _logger.LogInformation("Done.");
        }

        private readonly IReadOnlyCollection<IProcessingUnit> _units;
        private readonly ILogger<LoaderTool> _logger;

    }
}
