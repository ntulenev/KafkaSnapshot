using KafkaSnapshot.Models.Filters;
using KafkaSnapshot.Models.Names;

using System.Collections.Frozen;

namespace KafkaSnapshot.Models.Import;

/// <summary>
/// Represents Kafka topics attributes.
/// </summary>
public sealed class LoadingTopic
{
    /// <summary>
    /// Topic name.
    /// </summary>
    public TopicName Value { get; }

    /// <summary>
    /// Specifies the rules for encoding messages.
    /// </summary>
    public EncoderRules TopicValueEncoderRule { get; }

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
    public IReadOnlySet<int> PartitionFilter { get; }

    /// <summary>
    /// Checks if <see cref="PartitionFilter"/> contains any items.
    /// </summary>
    public bool HasPartitionFilter => PartitionFilter.Any();

    /// <summary>
    /// Creates <see cref="LoadingTopic"/>.
    /// </summary>
    /// <param name="topicName">Topic name.</param>
    /// <param name="loadWithCompacting">Flag for compacting.</param>
    /// <param name="dateParams">Date filter for initial offset.</param>
    /// <param name="valueEncoderRule">Encoding rule for topic values.</param>
    /// <param name="partitionFilter">Filtered partition ids.</param>
    public LoadingTopic(TopicName topicName,
                        bool loadWithCompacting,
                        DateFilterRange dateParams,
                        EncoderRules valueEncoderRule,
                        IReadOnlySet<int>? partitionFilter
                        )
    {
        Value = topicName ?? throw new ArgumentNullException(nameof(topicName));
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

            PartitionFilter = partitionFilter.ToFrozenSet();
        }
        else
        {
            PartitionFilter = new HashSet<int>().ToFrozenSet();
        }

        if (!Enum.IsDefined(valueEncoderRule))
        {
            throw new ArgumentException(
                $"Invalid EncoderRules value {valueEncoderRule}", nameof(valueEncoderRule));
        }

        TopicValueEncoderRule = valueEncoderRule;
    }

    private readonly DateTime? _offsetDate;
    private readonly DateTime? _endOffsetDate;
}
