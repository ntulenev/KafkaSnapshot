using System.Diagnostics.CodeAnalysis;

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
            result = ValidateOptionsResult.Fail("Topic name is not set.");
            return true;
        }

        if (string.IsNullOrWhiteSpace(topic.Name))
        {
            result = ValidateOptionsResult.Fail(
                "The topic name cannot be empty or consist of whitespaces.");
            return true;
        }

        if (topic.Name.Any(char.IsWhiteSpace))
        {
            result = ValidateOptionsResult.Fail(
                $"The topic name {topic.Name} cannot contain whitespaces.");
            return true;
        }

        if (topic.Name.Length > KafkaTopicNameRules.MaxLength)
        {
            result = ValidateOptionsResult.Fail(
                $"The name of a topic {topic.Name} is too long.");
            return true;
        }

        if (!KafkaTopicNameRules.IsValid(topic.Name))
        {
            result = ValidateOptionsResult.Fail(
               $"Incorrect topic name {topic.Name}. " +
               $"The topic name may consist of characters 'a' to 'z', 'A' to 'Z', " +
               $"digits, dots, underscores, and minus signs.");
            return true;
        }

        if (topic.ExportFileName is null)
        {
            result = ValidateOptionsResult.Fail("Topic export name is not set.");
            return true;
        }

        if (string.IsNullOrWhiteSpace(topic.ExportFileName))
        {
            result = ValidateOptionsResult.Fail(
                "The topic export name cannot be empty or consist of whitespaces.");
            return true;
        }

        if (topic.FilterKeyType is not Models.Filters.FilterType.None)
        {
            if (topic.FilterKeyValue is null)
            {
                result = ValidateOptionsResult.Fail(
                    $"Filter value does not set for topic {topic.Name}.");
                return true;
            }
        }

        if (topic.KeyType == Models.Filters.KeyType.Ignored &&
            topic.Compacting == CompactingMode.On)
        {
            result = ValidateOptionsResult.Fail(
                $"Compacting is not supported for ignored keys. Topic {topic.Name}.");
            return true;
        }

        if (topic.OffsetStartDate is DateTime startDate &&
            topic.OffsetEndDate is DateTime endDate &&
            startDate > endDate)
        {
            result = ValidateOptionsResult.Fail(
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
            result = ValidateOptionsResult.Fail("Topics section is not set.");
            return true;
        }

        if (options.Topics.Count == 0)
        {
            result = ValidateOptionsResult.Fail("Topics section is empty.");
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
            result = ValidateOptionsResult.Fail(
                     $"Files names duplicate in several topics ({duplicates}).");
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

}
