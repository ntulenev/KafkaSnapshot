using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;

using KafkaSnapshot.Models.Export;
using KafkaSnapshot.Models.Message;

namespace KafkaSnapshot.Abstractions.Export
{
    /// <summary>
    /// Abstraction for exporting topic as file.
    /// </summary>
    /// <typeparam name="TKey">Message Key</typeparam>
    /// <typeparam name="TKeyMarker">Key marker.</typeparam>
    /// <typeparam name="TValue">Message Value</typeparam>
    /// <typeparam name="TTopic">Topic object</typeparam>
    public interface IDataExporter<TKey, TKeyMarker, TMessage, TTopic> where TTopic : ExportedTopic
                                                                       where TKeyMarker : IKeyRepresentationMarker
                                                                       where TMessage : notnull
    {
        /// <summary>
        /// Exports <paramref name="data"/> to file.
        /// </summary>
        /// <param name="data">Data to be exported.</param>
        /// <param name="topic">topic description.</param>
        /// <param name="ct">Token for cancelling operation.</param>
        /// <returns></returns>
        public Task ExportAsync(IEnumerable<KeyValuePair<TKey, DatedMessage<TMessage>>> data, TTopic topic, CancellationToken ct);
    }
}
