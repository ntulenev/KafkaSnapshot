using System.Text.RegularExpressions;

using Microsoft.Extensions.Options;

namespace KafkaSnapshot.Processing.Configuration.Validation
{
    /// <summary>
    /// Validator for <see cref="LoaderToolConfiguration"/>.
    /// </summary>
    public class LoaderToolConfigurationValidator : IValidateOptions<LoaderToolConfiguration>
    {
        /// <summary>
        /// Validates <see cref="LoaderToolConfiguration"/>.
        /// </summary>
        public ValidateOptionsResult Validate(string name, LoaderToolConfiguration options)
        {
            if (options is null)
            {
                return ValidateOptionsResult.Fail("Configuration object is null.");
            }

            if (options.Topics is null)
            {
                return ValidateOptionsResult.Fail("Topics section is not set.");
            }

            if (!options.Topics.Any())
            {
                return ValidateOptionsResult.Fail("Topics section is empty.");
            }

            foreach (var topic in options.Topics)
            {
                if (topic.Name is null)
                {
                    return ValidateOptionsResult.Fail("Topic name is not set.");
                }

                if (string.IsNullOrWhiteSpace(topic.Name))
                {
                    return ValidateOptionsResult.Fail(
                        "The topic name cannot be empty or consist of whitespaces.");
                }

                if (topic.Name.Any(character => char.IsWhiteSpace(character)))
                {
                    return ValidateOptionsResult.Fail(
                        $"The topic name {topic.Name} cannot contain whitespaces.");
                }

                if (topic.Name.Length > MAX_TOPIC_NAME_LENGTH)
                {
                    return ValidateOptionsResult.Fail(
                        $"The name of a topic {topic.Name} is too long.");
                }

                if (!_topicNameCharacters.IsMatch(topic.Name))
                {
                    return ValidateOptionsResult.Fail(
                       $"Incorrect topic name {topic.Name}. The topic name may consist of characters 'a' to 'z', 'A' to 'Z', digits, and minus signs.");
                }

                if (topic.ExportFileName is null)
                {
                    return ValidateOptionsResult.Fail("Topic export name is not set.");
                }

                if (string.IsNullOrWhiteSpace(topic.ExportFileName))
                {
                    return ValidateOptionsResult.Fail(
                        "The topic export name cannot be empty or consist of whitespaces.");
                }

                if (topic.FilterKeyType is not Models.Filters.FilterType.None)
                {
                    if (topic.FilterKeyValue is null)
                    {
                        return ValidateOptionsResult.Fail($"Filter value does not set for topic {topic.Name}.");
                    }
                }

                if (topic.KeyType == Models.Filters.KeyType.Ignored && topic.Compacting == CompactingMode.On)
                {
                    return ValidateOptionsResult.Fail($"Compacting is not supported for ignored keys. Topic {topic.Name}.");
                }

                if (topic.OffsetStartDate is not null && topic.OffsetEndDate is not null)
                {
                    if (topic.OffsetStartDate > topic.OffsetEndDate)
                    {
                        return ValidateOptionsResult.Fail($"Topic start date ({topic.OffsetStartDate}) is greater than end date ({topic.OffsetEndDate}). Topic {topic.Name}.");
                    }
                }
            }

            return ValidateOptionsResult.Success;
        }

        private static readonly Regex _topicNameCharacters = new(
            "^[a-zA-Z0-9\\-]*$",
            RegexOptions.Compiled);

        private const int MAX_TOPIC_NAME_LENGTH = 249;
    }
}
