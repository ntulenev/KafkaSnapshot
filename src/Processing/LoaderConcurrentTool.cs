using System;
using System.Threading;
using System.Threading.Tasks;
using System.Collections.Generic;
using System.Linq;

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
            Console.WriteLine($"The utility starts loading {_units.Count} Apache Kafka topics concurrently...");

            await Task.WhenAll(_units.Select(async unit =>
            {
                using var _ = _logger.BeginScope("topic {topic}", unit.TopicName);

                _logger.LogDebug("Start processing topic.");

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
                    _logger.LogError(ex, "Error on processing topic");
                    Console.WriteLine($"Unable to load data for topic {unit.TopicName}");
                }

            }));

            Console.WriteLine("Done.");
        }

        private readonly ICollection<IProcessingUnit> _units;
        private readonly ILogger<LoaderConcurrentTool> _logger;

    }
}
