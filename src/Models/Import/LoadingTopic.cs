using KafkaSnapshot.Models.Filters;
using KafkaSnapshot.Models.Names;

using System.Collections.Frozen;

namespace KafkaSnapshot.Models.Import;

/// <summary>
/// Represents Kafka topic loading attributes.
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
    /// Indicates whether results should be compacted by key.
    /// </summary>
    public bool LoadWithCompacting { get; }

    /// <summary>
    /// Date and time of the starting offset.
    /// </summary>
    public DateTimeOffset OffsetDate => _offsetDate.HasValue ? _offsetDate.Value :
        throw new InvalidOperationException("Topic params does not have date offset.");

    /// <summary>
    /// Date and time of the ending offset.
    /// </summary>
    public DateTimeOffset EndOffsetDate => _endOffsetDate.HasValue ? _endOffsetDate.Value :
        throw new InvalidOperationException("Topic params does not have end date offset.");

    /// <summary>
    /// Indicates whether the starting offset date is set.
    /// </summary>
    public bool HasOffsetDate => _offsetDate.HasValue;

    /// <summary>
    /// Indicates whether the ending offset date is set.
    /// </summary>
    public bool HasEndOffsetDate => _endOffsetDate.HasValue;

    /// <summary>
    /// Topic partitions to read from.
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
        ArgumentNullException.ThrowIfNull(topicName);
        ArgumentNullException.ThrowIfNull(dateParams);

        Value = topicName;
        LoadWithCompacting = loadWithCompacting;
        _offsetDate = dateParams.StartDate;
        _endOffsetDate = dateParams.EndDate;

        if (partitionFilter != null)
        {
            ArgumentOutOfRangeException.ThrowIfZero(partitionFilter.Count, nameof(partitionFilter));

            PartitionFilter = partitionFilter.ToFrozenSet();
        }
        else
        {
            PartitionFilter = new HashSet<int>().ToFrozenSet();
        }

        if (!Enum.IsDefined(valueEncoderRule))
        {
            throw new ArgumentOutOfRangeException(
                nameof(valueEncoderRule),
                valueEncoderRule,
                $"Invalid EncoderRules value {valueEncoderRule}.");
        }

        TopicValueEncoderRule = valueEncoderRule;
    }

    private readonly DateTimeOffset? _offsetDate;
    private readonly DateTimeOffset? _endOffsetDate;
}
