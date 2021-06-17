using System;
using System.Linq;
using System.Text.RegularExpressions;

namespace KafkaSnapshot.Metadata
{
    /// <summary>
    /// Represents kafka topic name
    /// </summary>
    public class TopicName
    {
        /// <summary>
        /// Validated tipic name
        /// </summary>
        public string Value { get; }

        /// <summary>
        /// Creates topic name
        /// </summary>
        public TopicName(string name)
        {
            ValidateTopicName(name);
            Value = name;
        }

        /// <inheritdoc/>
        public override bool Equals(object? obj)
        {
            if (obj is TopicName tName)
            {
                return Value.Equals(tName.Value);
            }

            return false;
        }

        /// <inheritdoc/>
        public override int GetHashCode() => HashCode.Combine(Value);

        /// <summary>
        /// Validates topic name.
        /// </summary>
        /// <param name="topicName">Topic name.</param>
        private static void ValidateTopicName(string topicName)
        {
            if (topicName is null)
            {
                throw new ArgumentNullException(nameof(topicName));
            }

            if (string.IsNullOrWhiteSpace(topicName))
            {
                throw new ArgumentException(
                    "The topic name cannot be empty or consist of whitespaces.", nameof(topicName));
            }

            if (topicName.Any(character => char.IsWhiteSpace(character)))
            {
                throw new ArgumentException(
                    "The topic name cannot contain whitespaces.", nameof(topicName));
            }

            if (topicName.Length > MAX_TOPIC_NAME_LENGTH)
            {
                throw new ArgumentException(
                    "The name of a topic is too long.", nameof(topicName));
            }

            if (!_topicNameCharacters.IsMatch(topicName))
            {
                throw new ArgumentException(
                    "The topic name may consist of characters 'a' to 'z', 'A' to 'Z', digits, and minus signs.", nameof(topicName));
            }
        }

        private static readonly Regex _topicNameCharacters = new Regex(
           "^[a-zA-Z0-9\\-]*$",
           RegexOptions.Compiled);

        private const int MAX_TOPIC_NAME_LENGTH = 249;
    }
}
