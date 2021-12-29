using System.Text.RegularExpressions;

namespace KafkaSnapshot.Models.Import
{
    /// <summary>
    /// Represents Kafka topics attributes.
    /// </summary>
    public class LoadingTopic
    {
        /// <summary>
        /// Validated topic name.
        /// </summary>
        public string Value { get; }

        /// <summary>
        /// Need to compact results by key.
        /// </summary>
        public bool LoadWithCompacting { get; }

        /// <summary>
        /// Date and time of staring offset.
        /// </summary>
        public DateTime OffsetDate => _offsetDate.HasValue ? _offsetDate!.Value :
            throw new InvalidOperationException("Topic params does not have date offset.");

        /// <summary>
        /// Date and time of end offset.
        /// </summary>
        public DateTime EndOffsetDate => _endOffsetDate.HasValue ? _endOffsetDate!.Value :
            throw new InvalidOperationException("Topic params does not have end date offset.");

        /// <summary>
        /// Does offset trimmed with start date.
        /// </summary>
        public bool HasOffsetDate => _offsetDate.HasValue;

        /// <summary>
        /// Does offset trimmed with end date.
        /// </summary>
        public bool HasEndOffsetDate => _endOffsetDate.HasValue;

        /// <summary>
        /// Creates <see cref="LoadingTopic"/>.
        /// </summary>
        /// <param name="name">topic name.</param>
        /// <param name="loadWithCompacting">Flag for compacting.</param>
        public LoadingTopic(string name, bool loadWithCompacting)
        {
            ValidateTopicName(name);
            Value = name;
            LoadWithCompacting = loadWithCompacting;
        }

        /// <summary>
        /// Creates <see cref="LoadingTopic"/>.
        /// </summary>
        /// <param name="name">topic name.</param>
        /// <param name="loadWithCompacting">Flag for compacting.</param>
        /// <param name="offsetDate">date for initial offset.</param>
        public LoadingTopic(string name, bool loadWithCompacting, DateTime offsetDate) : this(name, loadWithCompacting)
        {
            _offsetDate = offsetDate;
        }

        /// <summary>
        /// Creates <see cref="LoadingTopic"/>.
        /// </summary>
        /// <param name="name">topic name.</param>
        /// <param name="loadWithCompacting">Flag for compacting.</param>
        /// <param name="offsetDate">date for initial offset.</param>
        public LoadingTopic(string name, bool loadWithCompacting, DateTime offsetDate, DateTime endOffsetDate) :
            this(name, loadWithCompacting, offsetDate)
        {
            _endOffsetDate = endOffsetDate;
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

        private readonly DateTime? _endOffsetDate;

        private static readonly Regex _topicNameCharacters = new(
           "^[a-zA-Z0-9\\-]*$",
           RegexOptions.Compiled);

        private const int MAX_TOPIC_NAME_LENGTH = 249;
    }
}
