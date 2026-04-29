using Confluent.Kafka;

using KafkaSnapshot.Abstractions.Import;
using KafkaSnapshot.Abstractions.Sorting;
using KafkaSnapshot.Import.Encoders;
using KafkaSnapshot.Import.Kafka;
using KafkaSnapshot.Import.Metadata;
using KafkaSnapshot.Processing.Configuration;
using KafkaSnapshot.Sorting;

using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Options;

namespace KafkaSnapshot.Utility.Helpers;

/// <summary>
/// Registers import services.
/// </summary>
internal static class ImportRegistrationHelper
{
    internal static IServiceCollection Register(IServiceCollection services, IConfiguration configuration)
    {
        ArgumentNullException.ThrowIfNull(services);
        ArgumentNullException.ThrowIfNull(configuration);

        _ = services.ConfigureImport(configuration);
        _ = services.AddSingleton<IMessageEncoder<byte[], string>, ByteMessageEncoder>();
        _ = services.AddSingleton<IKafkaClientFactory, KafkaClientFactory>();
        _ = services.AddSingleton<ITopicWatermarkLoader, TopicWatermarkLoader>();
        _ = services.AddSingleton<Func<IConsumer<string, byte[]>>>(
            serviceProvider => serviceProvider.GetRequiredService<IKafkaClientFactory>().CreateConsumer<string>);
        _ = services.AddSingleton<Func<IConsumer<long, byte[]>>>(
            serviceProvider => serviceProvider.GetRequiredService<IKafkaClientFactory>().CreateConsumer<long>);
        _ = services.AddSingleton(
            typeof(ISnapshotLoader<,>),
            typeof(KafkaSnapshot.Import.SnapshotLoader<,>));
        _ = services.AddSingleton<IMessageSorter<string, string>>(CreateStringSorter);
        _ = services.AddSingleton<IMessageSorter<long, string>>(CreateLongSorter);

        return services;
    }

    private static MessageSorter<TKey, TValue> CreateSorter<TKey, TValue>(
        IServiceProvider serviceProvider)
        where TKey : notnull
        where TValue : notnull
    {
        var config = serviceProvider.GetRequiredService<IOptions<LoaderToolConfiguration>>().Value;
        var sortingParams = new Models.Sorting.SortingParams(
            config.GlobalMessageSort,
            config.GlobalSortOrder);

        return new MessageSorter<TKey, TValue>(sortingParams);
    }

    private static MessageSorter<string, string> CreateStringSorter(IServiceProvider serviceProvider)
        => CreateSorter<string, string>(serviceProvider);

    private static MessageSorter<long, string> CreateLongSorter(IServiceProvider serviceProvider)
        => CreateSorter<long, string>(serviceProvider);
}
