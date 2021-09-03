using System;
using System.Linq;
using System.Text.RegularExpressions;

namespace KafkaSnapshot.Models.Import
{
    /// <summary>
    /// Represents kafka topic name.
    /// </summary>
    public class LoadingTopic
    {
        /// <summary>
        /// Validated topic name.
        /// </summary>
        public string Value { get; }

        /// <summary>
        /// Compact topic results.
        /// </summary>
        public bool LoadWithCompacting { get; }

        public DateTime OffsetDate => _offsetDate.HasValue ? _offsetDate!.Value :
            throw new InvalidOperationException("Topic params does not have date offset.");

        public bool HasOffsetDate => _offsetDate.HasValue;

        /// <summary>
        /// Creates topic name.
        /// </summary>
        public LoadingTopic(string name, bool loadWithCompacting)
        {
            ValidateTopicName(name);
            Value = name;
            LoadWithCompacting = loadWithCompacting;
        }

        public LoadingTopic(string name, bool loadWithCompacting, DateTime offsetDate) : this(name, loadWithCompacting)
        {
            _offsetDate = offsetDate;
        }

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

        private readonly DateTime? _offsetDate;

        private static readonly Regex _topicNameCharacters = new(
           "^[a-zA-Z0-9\\-]*$",
           RegexOptions.Compiled);

        private const int MAX_TOPIC_NAME_LENGTH = 249;
    }
}
