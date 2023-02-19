using System.Text.RegularExpressions;

using KafkaSnapshot.Models.Filters;
using KafkaSnapshot.Models.Names;

namespace KafkaSnapshot.Models.Import;

/// <summary>
/// Represents Kafka topics attributes.
/// </summary>
public class LoadingTopic
{
    /// <summary>
    /// Topic name.
    /// </summary>
    public TopicName Value { get; }

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
    public LoadingTopic(TopicName topicName,
                        bool loadWithCompacting,
                        DateFilterRange dateParams,
                        HashSet<int>? partitionFilter)
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

            _partitionFilter = new HashSet<int>(partitionFilter);
        }
        else
        {
            _partitionFilter = new HashSet<int>();
        }
    }

    private readonly DateTime? _offsetDate;
    private readonly DateTime? _endOffsetDate;
    private readonly IReadOnlySet<int> _partitionFilter;
}
