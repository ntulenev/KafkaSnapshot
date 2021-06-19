using System.Threading;
using System.Threading.Tasks;

using KafkaSnapshot.Models.Processing;

namespace KafkaSnapshot.Abstractions.Processing
{
    public interface IProcessingUnit
    {
        public Task ProcessAsync(CancellationToken ct);

        public ProcessingTopic Topic { get; }
    }
}
