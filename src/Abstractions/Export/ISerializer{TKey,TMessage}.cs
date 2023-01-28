﻿using KafkaSnapshot.Models.Message;

namespace KafkaSnapshot.Abstractions.Export;

/// <summary>
/// Export data serializer.
/// </summary>
/// <typeparam name="TKey">Message key.</typeparam>
/// <typeparam name="TMessage">Message value.</typeparam>
/// <typeparam name="TKeyMarker">Key Interpretation.</typeparam>
public interface ISerializer<TKey, TMessage, TKeyMarker> where TMessage : notnull
                                                         where TKeyMarker : IKeyRepresentationMarker
{
    /// <summary>
    /// Serializes data as string.
    /// </summary>
    /// <param name="data">Data for serialization.</param>
    /// <param name="exportRawMessage">Rule for message serialization.</param>
    /// <returns>String data representation.</returns>
    public string Serialize(IEnumerable<KeyValuePair<TKey, KafkaMessage<TMessage>>> data, bool exportRawMessage);

    /// <summary>
    ///  Serializes to stream.
    /// </summary>
    /// <param name="data">Data for serialization.</param>
    /// <param name="exportRawMessage">Rule for message serialization.</param>
    /// <param name="stream">Stream to store data.</param>
    public void Serialize(IEnumerable<KeyValuePair<TKey, KafkaMessage<TMessage>>> data, bool exportRawMessage, Stream stream);
}
