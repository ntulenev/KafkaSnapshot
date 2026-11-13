using System.Collections.Frozen;

namespace KafkaSnapshot.Models.Import;

/// <summary>An immutable selection of non-negative partitions; null selects all partitions.</summary>
public sealed class PartitionSelection
{
    /// <summary>Creates a selection, rejecting empty explicit selections and negative identifiers.</summary>
    public PartitionSelection(IReadOnlySet<int>? partitions)
    {
        if (partitions is not null)
        {
            ArgumentOutOfRangeException.ThrowIfZero(partitions.Count, nameof(partitions));
            foreach (var partition in partitions)
            {
                ArgumentOutOfRangeException.ThrowIfNegative(partition, nameof(partitions));
            }
        }

        Partitions = partitions is null ? new HashSet<int>().ToFrozenSet() : partitions.ToFrozenSet();
    }

    /// <summary>Explicit partition identifiers, or an empty set for all partitions.</summary>
    public IReadOnlySet<int> Partitions { get; }

    /// <summary>Whether this selection restricts reading to explicit partitions.</summary>
    public bool IsRestricted => Partitions.Count > 0;

    /// <summary>Checks whether the selection includes a partition.</summary>
    public bool Includes(int partition) => !IsRestricted || Partitions.Contains(partition);
}
