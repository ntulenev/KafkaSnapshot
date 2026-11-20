using System.Collections;

namespace KafkaSnapshot.Import;

/// <summary>Enumerates completed partition reads in traversal order without copying their records.</summary>
internal sealed class PartitionResults<T>(List<T>[] partitions) : IReadOnlyCollection<T>
{
    public int Count { get; } = partitions.Sum(partition => partition.Count);

    public IEnumerator<T> GetEnumerator()
        => partitions.SelectMany(partition => partition).GetEnumerator();

    IEnumerator IEnumerable.GetEnumerator() => GetEnumerator();
}
