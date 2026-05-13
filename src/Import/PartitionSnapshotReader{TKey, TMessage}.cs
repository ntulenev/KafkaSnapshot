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
                if (!TryAssignConsumerToStart(consumer, watermark, topicParams))
                {
                    yield break;
                }

                foreach (var item in ConsumeAssignedPartition(
                    consumer,
                    watermark,
                    topicParams,
                    keyFilter,
                    valueFilter,
                    ct))
                {
                    yield return item;
                }
            }
            finally
            {
                consumer?.Close();
            }
        }
    }

    private bool TryAssignConsumerToStart(
        IConsumer<TKey, byte[]> consumer,
        PartitionWatermark watermark,
        LoadingTopic topicParams)
    {
        if (!topicParams.HasOffsetDate)
        {
            watermark.AssignWithConsumer(consumer);
            return true;
        }

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

            return false;
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

        return true;
    }

    private IEnumerable<KeyValuePair<TKey, KafkaMessage<TMessage>>> ConsumeAssignedPartition(
        IConsumer<TKey, byte[]> consumer,
        PartitionWatermark watermark,
        LoadingTopic topicParams,
        IDataFilter<TKey> keyFilter,
        IDataFilter<TMessage> valueFilter,
        CancellationToken ct)
    {
        ConsumeResult<TKey, byte[]> result;

        do
        {
            result = ConsumeNext(consumer, topicParams, watermark, ct);

            if (IsFinalOffsetDateReached(topicParams, result))
            {
                LogFinalOffsetDateReached(topicParams, watermark, result);
                break;
            }

            var messageValue = _encoder.Encode(result.Message.Value, topicParams.TopicValueEncoderRule);

            if (!keyFilter.IsMatch(result.Message.Key) ||
                !valueFilter.IsMatch(messageValue))
            {
                continue;
            }

            yield return CreateMessage(result, messageValue, watermark);

        } while (watermark.ShouldContinueReadingAfter(result));
    }

    private ConsumeResult<TKey, byte[]> ConsumeNext(
        IConsumer<TKey, byte[]> consumer,
        LoadingTopic topicParams,
        PartitionWatermark watermark,
        CancellationToken ct)
    {
        try
        {
            return consumer.Consume(ct);
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
    }

    private static bool IsFinalOffsetDateReached(
        LoadingTopic topicParams,
        ConsumeResult<TKey, byte[]> result)
        => topicParams.HasEndOffsetDate &&
           GetResultTimestamp(result) > topicParams.EndOffsetDate;

    private void LogFinalOffsetDateReached(
        LoadingTopic topicParams,
        PartitionWatermark watermark,
        ConsumeResult<TKey, byte[]> result)
    {
        if (!_logger.IsEnabled(LogLevel.Information))
        {
            return;
        }

        _logger.LogInformation(
            "Final date offset {Date} reached by message timestamp {MessageTimestamp} at partition {Partition}, offset {Offset}",
            topicParams.EndOffsetDate,
            GetResultTimestamp(result),
            watermark.Partition.Value,
            result.Offset.Value);
    }

    private KeyValuePair<TKey, KafkaMessage<TMessage>> CreateMessage(
        ConsumeResult<TKey, byte[]> result,
        TMessage messageValue,
        PartitionWatermark watermark)
    {
        if (_logger.IsEnabled(LogLevel.Trace))
        {
            _logger.LogTrace("Loading {Key} - {Value}",
                result.Message.Key,
                messageValue);
        }

        var meta = new KafkaMetadata(
            GetResultTimestamp(result),
            watermark.Partition.Value,
            result.Offset.Value);

        var message = new KafkaMessage<TMessage>(messageValue, meta);

        return new KeyValuePair<TKey, KafkaMessage<TMessage>>(
            result.Message.Key,
            message);
    }

    private static DateTimeOffset GetResultTimestamp(ConsumeResult<TKey, byte[]> result)
        => new(result.Message.Timestamp.UtcDateTime, TimeSpan.Zero);

    private readonly SnapshotLoaderConfiguration _config;
    private readonly Func<IConsumer<TKey, byte[]>> _consumerFactory;
    private readonly ILogger _logger;
    private readonly IMessageEncoder<byte[], TMessage> _encoder;
}
