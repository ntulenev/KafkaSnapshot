using System;
using System.Threading;
using System.Threading.Tasks;
using System.Collections.Generic;

using KafkaSnapshot.Abstractions.Processing;

namespace KafkaSnapshot.Processing
{
    /// <summary>
    /// Tool that process topics from Apache Kafka.
    /// </summary>
    public class LoaderTool
    {
        /// <summary>
        /// Creates <see cref="LoaderTool"/>.
        /// </summary>
        /// <param name="units">Processors for topics.</param>
        public LoaderTool(ICollection<IProcessingUnit> units)
        {
            _units = units ?? throw new ArgumentNullException(nameof(units));
        }

        /// <summary>
        /// Runs processing of topics.
        /// </summary>
        /// <param name="ct">Token for cancelling.</param>
        public async Task ProcessAsync(CancellationToken ct)
        {
            Console.WriteLine("The utility starts loading Apache Kafka topics...");

            int indexer = 0;

            foreach (var unit in _units)
            {
                Console.WriteLine($"{++indexer}/{_units.Count} Processing topic {unit.Topic.Name}.");

                try
                {
                    await unit.ProcessAsync(ct).ConfigureAwait(false);
                }
                catch (OperationCanceledException)
                {

                }
                catch
                {
                    //TODO Add err logs.
                    Console.WriteLine($"Unable to load data for topic {unit.Topic.Name}");
                }

            }

            Console.WriteLine("Done.");
        }

        private readonly ICollection<IProcessingUnit> _units;

    }
}
