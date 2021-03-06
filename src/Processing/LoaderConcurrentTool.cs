using Microsoft.Extensions.Logging;

using KafkaSnapshot.Abstractions.Processing;

namespace KafkaSnapshot.Processing
{
    /// <summary>
    /// Tool that process topics from Apache Kafka (in concurrent mode).
    /// </summary>
    public class LoaderConcurrentTool : ILoaderTool
    {
        /// <summary>
        /// Creates <see cref="LoaderConcurrentTool"/>.
        /// </summary>
        /// <param name="units">Processors for topics.</param>
        public LoaderConcurrentTool(ILogger<LoaderConcurrentTool> logger, ICollection<IProcessingUnit> units)
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
            _logger.LogInformation("The utility starts loading {count} Apache Kafka topics concurrently...", _units.Count);

            await Task.WhenAll(_units.Select(async unit =>
            {
                using var _ = _logger.BeginScope("Topic {topic}", unit.TopicName);

                _logger.LogInformation("Start processing topic.");

                try
                {
                    await unit.ProcessAsync(ct).ConfigureAwait(false);

                    _logger.LogInformation("Finish processing topic.");
                }
                catch (OperationCanceledException)
                {

                }
                catch (Exception ex)
                {
                    _logger.LogError(ex, "Error on processing topic");
                }

            }));

            _logger.LogInformation("Done.");
        }

        private readonly ICollection<IProcessingUnit> _units;
        private readonly ILogger<LoaderConcurrentTool> _logger;

    }
}
