using System.Diagnostics.CodeAnalysis;

using KafkaSnapshot.Models.Configuration;
using KafkaSnapshot.Models.Filters;
using KafkaSnapshot.Models.Import;
using KafkaSnapshot.Models.Names;
using KafkaSnapshot.Models.Processing;
using KafkaSnapshot.Models.Sorting;

using Microsoft.Extensions.Options;

namespace KafkaSnapshot.Processing.Configuration.Validation;

/// <summary>
/// Validator for <see cref="LoaderToolConfiguration"/>.
/// </summary>
public sealed partial class LoaderToolConfigurationValidator :
    IValidateOptions<LoaderToolConfiguration>
{
    private static bool TryFailOnTopicRules(
        TopicConfiguration topic,
        [NotNullWhen(returnValue: true)] out ValidateOptionsResult result)
    {
        return TryFailOnTopicName(topic, out result) ||
               TryFailOnExportFileName(topic, out result) ||
               TryFailOnTopicEnums(topic, out result) ||
               TryFailOnTopicCompacting(topic, out result) ||
               TryFailOnTopicFilter(topic, out result) ||
               TryFailOnTopicPartitions(topic, out result) ||
               TryFailOnTopicDateRange(topic, out result);
    }

    private static bool TryFailOnTopicName(
        TopicConfiguration topic,
        [NotNullWhen(returnValue: true)] out ValidateOptionsResult result)
    {
        var error = KafkaTopicNameRules.Validate(topic.Name);
        if (error == TopicNameValidationError.None)
        {
            result = null!;
            return false;
        }

        var code = _topicNameErrorCodes[error];
        result = Fail(code, $"Invalid topic name '{topic.Name}': {error}.");
        return true;
    }

    private static bool TryFailOnExportFileName(
        TopicConfiguration topic,
        [NotNullWhen(returnValue: true)] out ValidateOptionsResult result)
    {
        try
        {
            _ = new FileName(topic.ExportFileName);
            result = null!;
            return false;
        }
        catch (ArgumentException exception)
        {
            var code = exception is ArgumentNullException
                ? ConfigurationValidationErrorCodes.ExportFileNameMissing
                : ConfigurationValidationErrorCodes.ExportFileNameEmpty;
            result = Fail(code, $"Invalid export file name for topic {topic.Name}: {exception.Message}");
            return true;
        }
    }

    private static bool TryFailOnTopicEnums(
        TopicConfiguration topic,
        [NotNullWhen(returnValue: true)] out ValidateOptionsResult result)
    {
        if (!Enum.IsDefined(topic.KeyType))
        {
            result = Fail(
                ConfigurationValidationErrorCodes.KeyTypeUnsupported,
                $"Unsupported KeyType value {topic.KeyType}.");
            return true;
        }

        if (!Enum.IsDefined(topic.FilterKeyType))
        {
            result = Fail(
                ConfigurationValidationErrorCodes.FilterTypeUnsupported,
                $"Unsupported FilterKeyType value {topic.FilterKeyType}.");
            return true;
        }

        if (!Enum.IsDefined(topic.Compacting))
        {
            result = Fail(
                ConfigurationValidationErrorCodes.CompactingModeUnsupported,
                $"Unsupported Compacting value {topic.Compacting}.");
            return true;
        }

        if (!Enum.IsDefined(topic.MessageEncoderRule))
        {
            result = Fail(
                ConfigurationValidationErrorCodes.EncoderRuleUnsupported,
                $"Unsupported MessageEncoderRule value {topic.MessageEncoderRule}.");
            return true;
        }

        result = null!;
        return false;
    }

    private static bool TryFailOnTopicFilter(
        TopicConfiguration topic,
        [NotNullWhen(returnValue: true)] out ValidateOptionsResult result)
    {
        try
        {
            // Representation conversion belongs to the configuration boundary.
            // Compatibility and required-value rules belong to the domain model.
            if (!KeyFilterRules.IsSupported(topic.FilterKeyType, topic.KeyType))
            {
                result = Fail(ConfigurationValidationErrorCodes.FilterKeyIncompatible,
                    $"Filter {topic.FilterKeyType} is incompatible with key type {topic.KeyType}.");
                return true;
            }

            _ = TopicConfigurationMapper.CreateKeyFilter(topic);
            result = null!;
            return false;
        }
        catch (ArgumentNullException exception)
        {
            result = Fail(ConfigurationValidationErrorCodes.FilterValueMissing,
                $"Filter value is not set for topic {topic.Name}: {exception.Message}");
            return true;
        }
        catch (Exception exception) when (exception is FormatException or OverflowException)
        {
            result = Fail(ConfigurationValidationErrorCodes.FilterValueInvalid,
                $"Invalid filter value for topic {topic.Name}: {exception.Message}");
            return true;
        }
    }

    private static bool TryFailOnTopicPartitions(
        TopicConfiguration topic,
        [NotNullWhen(returnValue: true)] out ValidateOptionsResult result)
    {
        try
        {
            _ = new PartitionSelection(topic.PartitionsIds);
            result = null!;
            return false;
        }
        catch (ArgumentOutOfRangeException exception)
        {
            result = Fail(ConfigurationValidationErrorCodes.PartitionFilterInvalid,
                $"Invalid partition selection for topic {topic.Name}: {exception.Message}");
            return true;
        }
    }

    private static bool TryFailOnTopicCompacting(
        TopicConfiguration topic,
        [NotNullWhen(returnValue: true)] out ValidateOptionsResult result)
    {
        if (!ProcessingTopicRules.SupportsCompacting(topic.KeyType, topic.Compacting == CompactingMode.On))
        {
            result = Fail(
                ConfigurationValidationErrorCodes.CompactingIgnoredKey,
                $"Compacting is not supported for ignored keys. Topic {topic.Name}.");
            return true;
        }

        result = null!;
        return false;
    }

    private static bool TryFailOnTopicDateRange(
        TopicConfiguration topic,
        [NotNullWhen(returnValue: true)] out ValidateOptionsResult result)
    {
        try
        {
            _ = new DateFilterRange(topic.OffsetStartDate, topic.OffsetEndDate);
            result = null!;
            return false;
        }
        catch (ArgumentOutOfRangeException exception)
        {
            result = Fail(ConfigurationValidationErrorCodes.TopicDateRangeInvalid,
                $"Invalid date range for topic {topic.Name}: {exception.Message}");
            return true;
        }
    }

    private static bool TryFailOnEmptyConfig(
        LoaderToolConfiguration options,
        [NotNullWhen(returnValue: true)] out ValidateOptionsResult result)
    {
        if (options.Topics is null)
        {
            result = Fail(
                ConfigurationValidationErrorCodes.TopicsMissing,
                "Topics section is not set.");
            return true;
        }

        if (options.Topics.Count == 0)
        {
            result = Fail(
                ConfigurationValidationErrorCodes.TopicsEmpty,
                "Topics section is empty.");
            return true;
        }

        result = null!;
        return false;
    }

    private static bool TryFailOnDuplicateFiles(
        LoaderToolConfiguration options,
        [NotNullWhen(returnValue: true)] out ValidateOptionsResult result)
    {
        var fileDuplicates = options.Topics
                                    .GroupBy(x => x.ExportFileName,
                                             StringComparer.CurrentCultureIgnoreCase)
                                    .Where(x => x.Count() > 1)
                                    .Select(x => x.Key)
                                    .ToList();

        if (fileDuplicates.Count > 0)
        {
            var duplicates = string.Join(",", fileDuplicates);
            result = Fail(
                     ConfigurationValidationErrorCodes.ExportFileNameDuplicate,
                     $"File names are duplicated in several topics ({duplicates}).");
            return true;
        }

        result = null!;
        return false;
    }

    /// <summary>
    /// Validates <see cref="LoaderToolConfiguration"/>.
    /// </summary>
    public ValidateOptionsResult Validate(string? name, LoaderToolConfiguration options)
    {
        ArgumentNullException.ThrowIfNull(options);

        if (TryFailOnEmptyConfig(options, out var error))
        {
            return error;
        }

        try
        {
            _ = new SortingParams(options.GlobalMessageSort, options.GlobalSortOrder);
        }
        catch (ArgumentOutOfRangeException exception)
        {
            var code = exception.ParamName == "type"
                ? ConfigurationValidationErrorCodes.SortingTypeUnsupported
                : ConfigurationValidationErrorCodes.SortingOrderUnsupported;
            return Fail(code, exception.Message);
        }

        foreach (var topic in options.Topics)
        {
            if (TryFailOnTopicRules(topic, out var topicError))
            {
                return topicError;
            }
        }

        return TryFailOnDuplicateFiles(options, out var duplicateError) ? duplicateError : ValidateOptionsResult.Success;
    }

    private static readonly Dictionary<TopicNameValidationError, string> _topicNameErrorCodes =
        new()
        {
            [TopicNameValidationError.Missing] = ConfigurationValidationErrorCodes.TopicNameMissing,
            [TopicNameValidationError.Empty] = ConfigurationValidationErrorCodes.TopicNameEmpty,
            [TopicNameValidationError.Whitespace] = ConfigurationValidationErrorCodes.TopicNameWhitespace,
            [TopicNameValidationError.TooLong] = ConfigurationValidationErrorCodes.TopicNameTooLong,
            [TopicNameValidationError.InvalidCharacters] = ConfigurationValidationErrorCodes.TopicNameInvalid
        };

    private static ValidateOptionsResult Fail(string code, string message)
        => ValidateOptionsResult.Fail(ConfigurationValidationError.Create(code, message));
}
