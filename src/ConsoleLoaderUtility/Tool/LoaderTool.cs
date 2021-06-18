using System;
using System.Threading;
using System.Threading.Tasks;
using System.Collections.Generic;
using System.Linq;

using KafkaSnapshot;

using Export;


namespace ConsoleLoaderUtility.Tool
{
    public class LoaderTool<Key, Value> where Key : notnull
    {
        public LoaderTool(IDictionary<string, ISnapshotLoader<Key, Value>> processors, IDataExporter<Key, Value> exporter)
        {
            if (processors is null)
            {
                throw new ArgumentNullException(nameof(processors));
            }

            if (!processors.Any())
            {
                throw new ArgumentException("Processors for topics are not set.", nameof(processors));
            }

            _processors = processors;
            _exporter = exporter ?? throw new ArgumentNullException(nameof(exporter));
        }

        public async Task ProcessAsync(CancellationToken ct)
        {
            int indexer = 0;

            foreach (var (topic, processor) in _processors)
            {
                Console.WriteLine($"{++indexer}/{_processors.Count} Processing topic {topic}.");

                var items = await processor.LoadCompactSnapshotAsync(ct);
                await _exporter.ExportAsync(items, topic, ct);

            }

            Console.WriteLine("Done.");
        }

        private readonly IDictionary<string, ISnapshotLoader<Key, Value>> _processors;
        private readonly IDataExporter<Key, Value> _exporter;
    }
}
