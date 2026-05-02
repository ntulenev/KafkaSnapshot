using System.Diagnostics.CodeAnalysis;

using KafkaSnapshot.Models.Configuration;
using KafkaSnapshot.Models.Filters;
using KafkaSnapshot.Models.Names;

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
        if (topic.Name is null)
        {
            result = Fail(
                ConfigurationValidationErrorCodes.TopicNameMissing,
                "Topic name is not set.");
            return true;
        }

        if (string.IsNullOrWhiteSpace(topic.Name))
        {
            result = Fail(
                ConfigurationValidationErrorCodes.TopicNameEmpty,
                "The topic name cannot be empty or consist only of whitespace.");
            return true;
        }

        if (topic.Name.Any(char.IsWhiteSpace))
        {
            result = Fail(
                ConfigurationValidationErrorCodes.TopicNameWhitespace,
                $"The topic name {topic.Name} cannot contain whitespace.");
            return true;
        }

        if (topic.Name.Length > KafkaTopicNameRules.MaxLength)
        {
            result = Fail(
                ConfigurationValidationErrorCodes.TopicNameTooLong,
                $"The topic name {topic.Name} is too long.");
            return true;
        }

        if (!KafkaTopicNameRules.IsValid(topic.Name))
        {
            result = Fail(
               ConfigurationValidationErrorCodes.TopicNameInvalid,
               $"Incorrect topic name {topic.Name}. " +
               $"The topic name may consist of characters 'a' to 'z', 'A' to 'Z', " +
               $"digits, dots, underscores, and minus signs.");
            return true;
        }

        if (topic.ExportFileName is null)
        {
            result = Fail(
                ConfigurationValidationErrorCodes.ExportFileNameMissing,
                "Topic export name is not set.");
            return true;
        }

        if (string.IsNullOrWhiteSpace(topic.ExportFileName))
        {
            result = Fail(
                ConfigurationValidationErrorCodes.ExportFileNameEmpty,
                "The topic export name cannot be empty or consist only of whitespace.");
            return true;
        }

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

        if (topic.FilterKeyType is not FilterType.None)
        {
            if (topic.FilterKeyValue is null)
            {
                result = Fail(
                    ConfigurationValidationErrorCodes.FilterValueMissing,
                    $"Filter value is not set for topic {topic.Name}.");
                return true;
            }
        }

        if (topic.KeyType == KeyType.Ignored &&
            topic.Compacting == CompactingMode.On)
        {
            result = Fail(
                ConfigurationValidationErrorCodes.CompactingIgnoredKey,
                $"Compacting is not supported for ignored keys. Topic {topic.Name}.");
            return true;
        }

        if (topic.OffsetStartDate is DateTimeOffset startDate &&
            topic.OffsetEndDate is DateTimeOffset endDate &&
            startDate > endDate)
        {
            result = Fail(
                ConfigurationValidationErrorCodes.TopicDateRangeInvalid,
                $"Topic start date ({topic.OffsetStartDate}) is greater than " +
                $"end date ({topic.OffsetEndDate}). Topic {topic.Name}.");
            return true;
        }

        result = null!;
        return false;
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

        if (!Enum.IsDefined(options.GlobalMessageSort))
        {
            return Fail(
                ConfigurationValidationErrorCodes.SortingTypeUnsupported,
                $"Unsupported GlobalMessageSort value {options.GlobalMessageSort}.");
        }

        if (!Enum.IsDefined(options.GlobalSortOrder))
        {
            return Fail(
                ConfigurationValidationErrorCodes.SortingOrderUnsupported,
                $"Unsupported GlobalSortOrder value {options.GlobalSortOrder}.");
        }

        foreach (var topic in options.Topics)
        {
            if (TryFailOnTopicRules(topic, out var topicError))
            {
                return topicError;
            }
        }

        if (TryFailOnDuplicateFiles(options, out var duplicateError))
        {
            return duplicateError;
        }

        return ValidateOptionsResult.Success;
    }

    private static ValidateOptionsResult Fail(string code, string message)
        => ValidateOptionsResult.Fail(ConfigurationValidationError.Create(code, message));
}
