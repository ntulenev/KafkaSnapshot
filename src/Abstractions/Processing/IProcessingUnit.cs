using System.Threading;
using System.Threading.Tasks;

namespace KafkaSnapshot.Abstractions.Processing
{
    /// <summary>
    /// Single topic processor that loads data from Apache Kafka and exports to file.
    /// </summary>
    public interface IProcessingUnit
    {
        /// <summary>
        /// Start processing topic.
        /// </summary>
        /// <param name="ct">Token for canelling operation.</param>
        public Task ProcessAsync(CancellationToken ct);

        /// <summary>
        /// Processing topic information.
        /// </summary>
        public string TopicName { get; }
    }
}
