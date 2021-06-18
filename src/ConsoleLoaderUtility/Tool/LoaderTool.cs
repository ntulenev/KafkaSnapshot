using System;
using System.Threading;
using System.Threading.Tasks;
using System.Collections.Generic;

namespace ConsoleLoaderUtility.Tool
{
    public class LoaderTool
    {
        public LoaderTool(ICollection<IProcessingUnit> units)
        {

            _units = units ?? throw new ArgumentNullException(nameof(units));
        }

        public async Task ProcessAsync(CancellationToken ct)
        {
            int indexer = 0;

            foreach (var unit in _units)
            {
                Console.WriteLine($"{++indexer}/{_units.Count} Processing topic {unit.Topic}.");

                await unit.ProcessAsync(ct);

            }

            Console.WriteLine("Done.");
        }

        private readonly ICollection<IProcessingUnit> _units;

    }
}
