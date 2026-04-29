using KafkaSnapshot.Models.Sorting;

namespace KafkaSnapshot.Processing.Configuration;

/// <summary>
/// Application configuration.
/// </summary>
public sealed class LoaderToolConfiguration
{
    /// <summary>
    /// Topics to process.
    /// </summary>
    public IReadOnlyList<TopicConfiguration> Topics { get; init; } = [];

    /// <summary>
    /// Use <see cref="LoaderConcurrentTool"/> to process topics concurrently.
    /// </summary>
    public bool UseConcurrentLoad { get; init; }

    /// <summary>
    /// Field used to sort messages.
    /// </summary>
    public SortingType GlobalMessageSort { get; init; } = SortingType.Time;

    /// <summary>
    /// Message sort order.
    /// </summary>
    public SortingOrder GlobalSortOrder { get; init; } = SortingOrder.None;
}
