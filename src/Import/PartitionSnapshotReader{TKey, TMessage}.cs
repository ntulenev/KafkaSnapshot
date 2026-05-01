using Confluent.Kafka;

using KafkaSnapshot.Abstractions.Filters;
using KafkaSnapshot.Abstractions.Import;
using KafkaSnapshot.Import.Configuration;
using KafkaSnapshot.Import.Watermarks;
using KafkaSnapshot.Models.Import;
using KafkaSnapshot.Models.Message;

using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;

namespace KafkaSnapshot.Import;

/// <summary>
/// Reads Kafka messages from a single topic partition up to its watermark.
/// </summary>
/// <typeparam name="TKey">Kafka message key type.</typeparam>
/// <typeparam name="TMessage">Decoded Kafka message value type.</typeparam>
public class PartitionSnapshotReader<TKey, TMessage>
    : IPartitionSnapshotReader<TKey, TMessage>
    where TKey : notnull
    where TMessage : notnull
{
    /// <summary>
    /// Creates <see cref="PartitionSnapshotReader{TKey, TMessage}"/>.
    /// </summary>
    /// <param name="logger">Logger.</param>
    /// <param name="config">Snapshot loader configuration.</param>
    /// <param name="consumerFactory">Kafka consumer factory.</param>
    /// <param name="encoder">Raw message value encoder.</param>
    public PartitionSnapshotReader(
        ILogger<PartitionSnapshotReader<TKey, TMessage>> logger,
        IOptions<SnapshotLoaderConfiguration> config,
        Func<IConsumer<TKey, byte[]>> consumerFactory,
        IMessageEncoder<byte[], TMessage> encoder)
    {
        ArgumentNullException.ThrowIfNull(logger);
        ArgumentNullException.ThrowIfNull(config);
        ArgumentNullException.ThrowIfNull(config.Value, nameof(config));
        ArgumentNullException.ThrowIfNull(consumerFactory);
        ArgumentNullException.ThrowIfNull(encoder);

        _logger = logger;
        _config = config.Value;
        _consumerFactory = consumerFactory;
        _encoder = encoder;
    }

    /// <summary>
    /// Reads messages from a single partition watermark.
    /// </summary>
    /// <param name="watermark">Partition watermark.</param>
    /// <param name="topicParams">Topic loading parameters.</param>
    /// <param name="keyFilter">Message key filter.</param>
    /// <param name="valueFilter">Message value filter.</param>
    /// <param name="ct">Cancellation token.</param>
    public IEnumerable<KeyValuePair<TKey, KafkaMessage<TMessage>>> Read(
        PartitionWatermark watermark,
        LoadingTopic topicParams,
        IDataFilter<TKey> keyFilter,
        IDataFilter<TMessage> valueFilter,
        CancellationToken ct)
    {
        ArgumentNullException.ThrowIfNull(watermark);
        ArgumentNullException.ThrowIfNull(topicParams);
        ArgumentNullException.ThrowIfNull(keyFilter);
        ArgumentNullException.ThrowIfNull(valueFilter);

        var logScope = _logger.IsEnabled(LogLevel.Information)
            ? _logger.BeginScope("Partition {Partition}", watermark.Partition.Value)
            : null;

        using (logScope)
        {
            if (_logger.IsEnabled(LogLevel.Information))
            {
                _logger.LogInformation("Watermarks: Low {Low}, High {High}",
                    watermark.Offset.Low,
                    watermark.Offset.High);
            }

            using var consumer = _consumerFactory();

            try
            {
                if (topicParams.HasOffsetDate)
                {
                    if (_logger.IsEnabled(LogLevel.Information))
                    {
                        _logger.LogInformation(
                            "Searching offset for topic {Topic}, partition {Partition}, date {Date}",
                            topicParams.Value.Name,
                            watermark.Partition.Value,
                            topicParams.OffsetDate);
                    }

                    bool offsetAssigned;
                    TopicPartitionOffset assignedOffset;

                    try
                    {
                        offsetAssigned = watermark.AssignWithConsumer(
                            consumer,
                            topicParams.OffsetDate,
                            _config.DateOffsetTimeout,
                            out assignedOffset);
                    }
                    catch (Exception exception) when (exception is not OperationCanceledException)
                    {
                        _logger.LogError(
                            exception,
                            "Failed to resolve offset for topic {Topic}, partition {Partition}, date {Date}",
                            topicParams.Value.Name,
                            watermark.Partition.Value,
                            topicParams.OffsetDate);
                        throw;
                    }

                    if (!offsetAssigned)
                    {
                        if (_logger.IsEnabled(LogLevel.Warning))
                        {
                            _logger.LogWarning(
                                "No actual offset for topic {Topic}, partition {Partition}, date {Date}",
                                topicParams.Value.Name,
                                watermark.Partition.Value,
                                topicParams.OffsetDate);
                        }

                        yield break;
                    }

                    if (_logger.IsEnabled(LogLevel.Information))
                    {
                        _logger.LogInformation(
                            "Resolved offset {Offset} for topic {Topic}, partition {Partition}, date {Date}",
                            assignedOffset.Offset.Value,
                            topicParams.Value.Name,
                            watermark.Partition.Value,
                            topicParams.OffsetDate);
                    }
                }
                else
                {
                    watermark.AssignWithConsumer(consumer);
                }

                ConsumeResult<TKey, byte[]> result;

                DateTimeOffset getResultTimestamp()
                {
                    return new DateTimeOffset(result.Message.Timestamp.UtcDateTime, TimeSpan.Zero);
                }

                bool isFinalOffsetDateReached()
                {
                    return topicParams.HasEndOffsetDate &&
                           getResultTimestamp() > topicParams.EndOffsetDate;
                }

                do
                {
                    try
                    {
                        result = consumer.Consume(ct);
                    }
                    catch (Exception exception) when (exception is not OperationCanceledException)
                    {
                        _logger.LogError(
                            exception,
                            "Failed to consume message from topic {Topic}, partition {Partition}",
                            topicParams.Value.Name,
                            watermark.Partition.Value);
                        throw;
                    }

                    if (isFinalOffsetDateReached())
                    {
                        if (_logger.IsEnabled(LogLevel.Information))
                        {
                            _logger.LogInformation(
                                "Final date offset {Date} reached by message timestamp {MessageTimestamp} at partition {Partition}, offset {Offset}",
                                topicParams.EndOffsetDate,
                                getResultTimestamp(),
                                watermark.Partition.Value,
                                result.Offset.Value);
                        }

                        break;
                    }

                    var messageValue = _encoder.Encode(result.Message.Value, topicParams.TopicValueEncoderRule);

                    if (keyFilter.IsMatch(result.Message.Key) &&
                        valueFilter.IsMatch(messageValue))
                    {
                        if (_logger.IsEnabled(LogLevel.Trace))
                        {
                            _logger.LogTrace("Loading {Key} - {Value}",
                                result.Message.Key,
                                messageValue);
                        }

                        var meta = new KafkaMetadata(
                            new DateTimeOffset(result.Message.Timestamp.UtcDateTime, TimeSpan.Zero),
                            watermark.Partition.Value,
                            result.Offset.Value);

                        var message = new KafkaMessage<TMessage>(messageValue, meta);

                        yield return new KeyValuePair<TKey, KafkaMessage<TMessage>>(
                            result.Message.Key,
                            message);
                    }

                } while (watermark.IsWatermarkAchievedBy(result));
            }
            finally
            {
                consumer?.Close();
            }
        }
    }

    private readonly SnapshotLoaderConfiguration _config;
    private readonly Func<IConsumer<TKey, byte[]>> _consumerFactory;
    private readonly ILogger _logger;
    private readonly IMessageEncoder<byte[], TMessage> _encoder;
}
