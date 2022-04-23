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
        /// Topic's interested partitions.
        /// </summary>
        public IReadOnlySet<int> PartitionFilter => _partitionFilter;

        /// <summary>
        /// Checks if <see cref="PartitionFilter"/> contains any items.
        /// </summary>
        public bool HasPartitionFilter => _partitionFilter.Any();

        /// <summary>
        /// Creates <see cref="LoadingTopic"/>.
        /// </summary>
        /// <param name="name">topic name.</param>
        /// <param name="loadWithCompacting">Flag for compacting.</param>
        /// <param name="dateParams">date filter for initial offset.</param>
        /// <param name="partitionFilter">filtered partition ids.</param>
        public LoadingTopic(string name, bool loadWithCompacting, DateFilterParams dateParams, HashSet<int> partitionFilter = null!)
        {
            ValidateTopicName(name);
            Value = name;
            LoadWithCompacting = loadWithCompacting;

            ArgumentNullException.ThrowIfNull(dateParams);

            _offsetDate = dateParams.StartDate;
            _endOffsetDate = dateParams.EndDate;

            if (partitionFilter != null)
            {
                if (!partitionFilter.Any())
                {
                    throw new ArgumentException("Filter is not set", nameof(partitionFilter));
                }

                _partitionFilter = new HashSet<int>(partitionFilter);
            }
            else
            {
                _partitionFilter = new HashSet<int>();
            }
        }

        /// <summary>
        /// Validates topic name.
        /// </summary>
        /// <param name="topicName">Topic name.</param>
        private static void ValidateTopicName(string topicName)
        {
            ArgumentNullException.ThrowIfNull(topicName);

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

        private readonly IReadOnlySet<int> _partitionFilter;

        private static readonly Regex _topicNameCharacters = new(
           "^[a-zA-Z0-9\\-]*$",
           RegexOptions.Compiled);

        private const int MAX_TOPIC_NAME_LENGTH = 249;
    }
}
