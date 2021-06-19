using System.Threading;
using System.Threading.Tasks;

namespace KafkaSnapshot.Abstractions.Processing
{
    public interface ITopicProcessor
    {
        public Task ProcessTopicAsync(string name, CancellationToken ct);
    }
}
