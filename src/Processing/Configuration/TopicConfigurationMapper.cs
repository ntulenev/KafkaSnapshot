using System.Globalization;

using KafkaSnapshot.Models.Filters;
using KafkaSnapshot.Models.Names;
using KafkaSnapshot.Models.Processing;

namespace KafkaSnapshot.Processing.Configuration;

/// <summary>
/// Maps topic configuration to processing domain objects.
/// </summary>
public static class TopicConfigurationMapper
{
    /// <summary>
    /// Converts configuration to <see cref="ProcessingTopic{TKey}"/>.
    /// </summary>
    public static ProcessingTopic<TKey> ToProcessingTopic<TKey>(TopicConfiguration configuration)
    {
        ArgumentNullException.ThrowIfNull(configuration);

        var filterKeyValue = CreateKeyFilter(configuration).Value;
        var typedFilterKeyValue = filterKeyValue is null ? default : (TKey)filterKeyValue;
        var dateRange = new DateFilterRange(configuration.OffsetStartDate, configuration.OffsetEndDate);

        return new ProcessingTopic<TKey>(
            new TopicName(configuration.Name),
            new FileName(configuration.ExportFileName),
            configuration.Compacting == CompactingMode.On,
            configuration.FilterKeyType,
            configuration.KeyType,
            typedFilterKeyValue!,
            dateRange,
            configuration.ExportRawMessage,
            configuration.MessageEncoderRule,
            configuration.PartitionsIds);
    }

    internal static KeyFilterDefinition<object?> CreateKeyFilter(TopicConfiguration configuration)
        => new(configuration.FilterKeyType, configuration.KeyType,
            ParseFilterKeyValue(configuration));

    private static object? ParseFilterKeyValue(TopicConfiguration configuration)
    {
        if (configuration.FilterKeyType == FilterType.None || configuration.FilterKeyValue is null)
        {
            return default;
        }

        return configuration.KeyType switch
        {
            KeyType.Long => long.Parse(
                configuration.FilterKeyValue,
                NumberStyles.Integer,
                CultureInfo.InvariantCulture),
            KeyType.Json or KeyType.String => configuration.FilterKeyValue,
            KeyType.Ignored => default,
            _ => throw new InvalidOperationException(
                $"Invalid Key type {configuration.KeyType} for processing.")
        };
    }
}
