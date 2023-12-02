using KafkaSnapshot.Models.Sorting;

namespace KafkaSnapshot.Processing.Configuration;

/// <summary>
/// Application configuration.
/// </summary>
public class LoaderToolConfiguration
{
    /// <summary>
    /// List of  topics with string Key.
    /// </summary>
    public required List<TopicConfiguration> Topics { get; init; }

    /// <summary>
    /// User <see cref="LoaderConcurrentTool"/> to process topics in concurrent mode.
    /// </summary>
    public bool UseConcurrentLoad { get; init; }

    /// <summary>
    /// Message sorting field.
    /// </summary>
    public SortingType GlobalMessageSort { get; init; } = SortingType.Time;

    /// <summary>
    /// Message sorting type.
    /// </summary>
    public SortingOrder GlobalSortOrder { get; init; } = SortingOrder.No;
}
