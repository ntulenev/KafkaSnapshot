using System;
using System.Threading;
using System.Threading.Tasks;
using System.Collections.Generic;

using KafkaSnapshot.Abstractions.Processing;

namespace KafkaSnapshot.Processing
{
    public class LoaderTool
    {
        public LoaderTool(ICollection<IProcessingUnit> units)
        {
            _units = units ?? throw new ArgumentNullException(nameof(units));
        }

        public async Task ProcessAsync(CancellationToken ct)
        {
            Console.WriteLine("The utility starts loading Apache Kafka topics...");

            int indexer = 0;

            foreach (var unit in _units)
            {
                Console.WriteLine($"{++indexer}/{_units.Count} Processing topic {unit.Topic.Name}.");

                try
                {
                    await unit.ProcessAsync(ct);
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
