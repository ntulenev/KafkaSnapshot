using KafkaSnapshot.Models.Filters;
using KafkaSnapshot.Models.Names;

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
    public DateTimeOffset OffsetDate
    {
        get
        {
            return _offsetDate ?? throw new InvalidOperationException(
                "Topic parameters do not have a date offset.");
        }
    }

    /// <summary>
    /// Date and time of the ending offset.
    /// </summary>
    public DateTimeOffset EndOffsetDate
    {
        get
        {
            return _endOffsetDate ?? throw new InvalidOperationException(
                "Topic parameters do not have an end date offset.");
        }
    }

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
    public IReadOnlySet<int> PartitionFilter => _partitionSelection.Partitions;

    /// <summary>
    /// Checks if <see cref="PartitionFilter"/> contains any items.
    /// </summary>
    public bool HasPartitionFilter => _partitionSelection.IsRestricted;

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
        DateRange = dateParams;
        _offsetDate = dateParams.StartDate;
        _endOffsetDate = dateParams.EndDate;

        _partitionSelection = new PartitionSelection(partitionFilter);

        if (!Enum.IsDefined(valueEncoderRule))
        {
            throw new ArgumentOutOfRangeException(
                nameof(valueEncoderRule),
                valueEncoderRule,
                $"Invalid EncoderRules value {valueEncoderRule}.");
        }

        TopicValueEncoderRule = valueEncoderRule;
    }

    /// <summary>Validated date boundaries.</summary>
    public DateFilterRange DateRange { get; }

    /// <summary>Checks whether a partition is included in the selection.</summary>
    public bool IncludesPartition(int partition) => _partitionSelection.Includes(partition);

    private readonly PartitionSelection _partitionSelection;
    private readonly DateTimeOffset? _offsetDate;
    private readonly DateTimeOffset? _endOffsetDate;
}
