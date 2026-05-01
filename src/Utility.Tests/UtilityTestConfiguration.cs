using System.Globalization;

using KafkaSnapshot.Models.Filters;
using KafkaSnapshot.Processing.Configuration;

using Microsoft.Extensions.Configuration;

namespace KafkaSnapshot.Utility.Tests;

internal static class UtilityTestConfiguration
{
    public static IConfiguration BuildLoaderConfig(bool useConcurrentLoad, KeyType keyType)
        => new ConfigurationBuilder()
            .AddInMemoryCollection(new Dictionary<string, string?>
            {
                ["LoaderToolConfiguration:UseConcurrentLoad"] =
                    useConcurrentLoad.ToString(CultureInfo.InvariantCulture),
                ["LoaderToolConfiguration:Topics:0:Name"] = "topic-1",
                ["LoaderToolConfiguration:Topics:0:ExportFileName"] = "topic-1.json",
                ["LoaderToolConfiguration:Topics:0:KeyType"] =
                    ((int)keyType).ToString(CultureInfo.InvariantCulture),
                ["LoaderToolConfiguration:Topics:0:Compacting"] = nameof(CompactingMode.Off),
                ["LoaderToolConfiguration:Topics:0:FilterKeyType"] = nameof(FilterType.None)
            })
            .Build();

    public static Dictionary<string, string?> CreateValidConfig()
        => new()
        {
            ["BootstrapServersConfiguration:BootstrapServers:0"] = "kafka1:9092",
            ["TopicWatermarkLoaderConfiguration:AdminClientTimeout"] = "00:00:05",
            ["SnapshotLoaderConfiguration:DateOffsetTimeout"] = "00:00:05",
            ["SnapshotLoaderConfiguration:SearchSinglePartition"] = "false",
            ["JsonFileDataExporterConfiguration:UseFileStreaming"] = "true",
            ["LoaderToolConfiguration:UseConcurrentLoad"] = "false",
            ["LoaderToolConfiguration:Topics:0:Name"] = "topic-1",
            ["LoaderToolConfiguration:Topics:0:ExportFileName"] = "topic-1.json",
            ["LoaderToolConfiguration:Topics:0:KeyType"] = nameof(KeyType.String),
            ["LoaderToolConfiguration:Topics:0:Compacting"] = nameof(CompactingMode.Off),
            ["LoaderToolConfiguration:Topics:0:FilterKeyType"] = nameof(FilterType.None)
        };
}

