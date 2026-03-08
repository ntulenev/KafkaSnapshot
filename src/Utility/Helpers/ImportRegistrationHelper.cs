using Confluent.Kafka;

using KafkaSnapshot.Abstractions.Import;
using KafkaSnapshot.Abstractions.Sorting;
using KafkaSnapshot.Import;
using KafkaSnapshot.Import.Configuration;
using KafkaSnapshot.Import.Encoders;
using KafkaSnapshot.Import.Metadata;
using KafkaSnapshot.Processing.Configuration;
using KafkaSnapshot.Sorting;

using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Options;

namespace KafkaSnapshot.Utility.Helpers;

/// <summary>
/// Registers import services.
/// </summary>
internal static class ImportRegistrationHelper
{
    internal static void Register(IServiceCollection services, HostBuilderContext hostContext)
    {
        ArgumentNullException.ThrowIfNull(services);
        ArgumentNullException.ThrowIfNull(hostContext);

        _ = services.ConfigureImport(hostContext);
        _ = services.AddSingleton<IMessageEncoder<byte[], string>, ByteMessageEncoder>();
        _ = services.AddSingleton(CreateAdminClient);
        _ = services.AddSingleton<ITopicWatermarkLoader, TopicWatermarkLoader>();
        _ = services.AddSingleton<Func<IConsumer<string, byte[]>>>(
            serviceProvider => () => CreateConsumer<string>(serviceProvider));
        _ = services.AddSingleton<Func<IConsumer<long, byte[]>>>(
            serviceProvider => () => CreateConsumer<long>(serviceProvider));
        _ = services.AddSingleton(typeof(ISnapshotLoader<,>), typeof(SnapshotLoader<,>));
        _ = services.AddSingleton<IMessageSorter<string, string>>(CreateStringSorter);
        _ = services.AddSingleton<IMessageSorter<long, string>>(CreateLongSorter);
    }

    private static IAdminClient CreateAdminClient(IServiceProvider serviceProvider)
    {
        var config = serviceProvider.GetRequiredService<IOptions<BootstrapServersConfiguration>>().Value;
        var servers = string.Join(",", config.BootstrapServers);
        var adminConfig = new AdminClientConfig
        {
            BootstrapServers = servers,
            SecurityProtocol = config.SecurityProtocol,
            SaslMechanism = config.SASLMechanism,
            SaslUsername = config.Username,
            SaslPassword = config.Password
        };

        return new AdminClientBuilder(adminConfig).Build();
    }

    private static IConsumer<TKey, byte[]> CreateConsumer<TKey>(IServiceProvider serviceProvider)
    {
        var config = serviceProvider.GetRequiredService<IOptions<BootstrapServersConfiguration>>().Value;
        var servers = string.Join(",", config.BootstrapServers);
        var consumerConfig = new ConsumerConfig
        {
            BootstrapServers = servers,
            AutoOffsetReset = AutoOffsetReset.Earliest,
            GroupId = Guid.NewGuid().ToString(),
            EnableAutoCommit = false,
            SecurityProtocol = config.SecurityProtocol,
            SaslMechanism = config.SASLMechanism,
            SaslUsername = config.Username,
            SaslPassword = config.Password
        };

        return new ConsumerBuilder<TKey, byte[]>(consumerConfig).Build();
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
