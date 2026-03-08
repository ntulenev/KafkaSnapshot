using KafkaSnapshot.Abstractions.Export;
using KafkaSnapshot.Abstractions.Filters;
using KafkaSnapshot.Abstractions.Processing;
using KafkaSnapshot.Export.Markers;
using KafkaSnapshot.Filters;
using KafkaSnapshot.Models.Filters;
using KafkaSnapshot.Processing;
using KafkaSnapshot.Processing.Configuration;

using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Options;

namespace KafkaSnapshot.Utility.Helpers;

/// <summary>
/// Registers topic loader services.
/// </summary>
internal static class TopicLoadersRegistrationHelper
{
    internal static void Register(
        IServiceCollection services,
        Func<IServiceProvider, IReadOnlyCollection<IProcessingUnit>> topicLoadersFactory)
    {
        ArgumentNullException.ThrowIfNull(services);
        ArgumentNullException.ThrowIfNull(topicLoadersFactory);

        _ = services.AddSingleton<IKeyFiltersFactory<long>, NaiveKeyFiltersFactory<long>>();
        _ = services.AddSingleton<IKeyFiltersFactory<string>, NaiveKeyFiltersFactory<string>>();
        _ = services.AddSingleton<IValueFilterFactory<string>, NaiveValueFiltersFactory<string>>();
        _ = services.AddSingleton(topicLoadersFactory);
    }

    internal static IReadOnlyCollection<IProcessingUnit> CreateTopicLoaders(IServiceProvider serviceProvider)
    {
        var config = serviceProvider.GetRequiredService<IOptions<LoaderToolConfiguration>>().Value;

        return [.. config.Topics.Select(topic => topic.KeyType switch
        {
            KeyType.Ignored => (IProcessingUnit)InitUnit<string, IgnoreKeyMarker>(topic, serviceProvider),
            KeyType.Json => InitUnit<string, JsonKeyMarker>(topic, serviceProvider),
            KeyType.String => InitUnit<string, OriginalKeyMarker>(topic, serviceProvider),
            KeyType.Long => InitUnit<long, OriginalKeyMarker>(topic, serviceProvider),
            _ => throw new InvalidOperationException(
                $"Invalid Key type {topic.KeyType} for processing.")
        })];
    }

    private static ProcessingUnit<TKey, TMarker, string> InitUnit<TKey, TMarker>(
        TopicConfiguration topic,
        IServiceProvider provider)
        where TKey : notnull
        where TMarker : IKeyRepresentationMarker
        => ActivatorUtilities.CreateInstance<ProcessingUnit<TKey, TMarker, string>>(
            provider,
            topic.ConvertToProcess<TKey>());
}
