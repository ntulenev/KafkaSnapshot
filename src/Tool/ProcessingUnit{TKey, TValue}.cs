using System;
using System.Threading;
using System.Threading.Tasks;

using KafkaSnapshot.Abstractions.Import;
using KafkaSnapshot.Abstractions.Export;
using KafkaSnapshot.Abstractions.Processing;

using Export;

using Models.Processing;

namespace Tool
{
    public class ProcessingUnit<TKey, TValue> : IProcessingUnit where TKey : notnull
    {
        public ProcessingUnit(ProcessingTopic topic, ISnapshotLoader<TKey, TValue> processor, IDataExporter<TKey, TValue, ExportedFileTopic> exporter)
        {
            _topic = topic ?? throw new ArgumentNullException(nameof(topic));
            _processor = processor ?? throw new ArgumentNullException(nameof(processor));
            _exporter = exporter ?? throw new ArgumentNullException(nameof(exporter));
        }

        public async Task ProcessAsync(CancellationToken ct)
        {

            var items = await _processor.LoadCompactSnapshotAsync(ct);
            await _exporter.ExportAsync(items, new ExportedFileTopic(_topic.Name, _topic.ExportName), ct);
        }

        public ProcessingTopic Topic => _topic;

        private readonly ISnapshotLoader<TKey, TValue> _processor;
        private readonly IDataExporter<TKey, TValue, ExportedFileTopic> _exporter;
        private readonly ProcessingTopic _topic;
    }
}
