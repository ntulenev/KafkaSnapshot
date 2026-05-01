using System.Globalization;

using KafkaSnapshot.Import.Kafka;
using KafkaSnapshot.Utility.DependencyInjection;

using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;

namespace KafkaSnapshot.Utility.Tests;

internal sealed class FakeKafkaPipelineHostBuilder
{
    private FakeKafkaPipelineHostBuilder(
        string outputDirectory,
        IKafkaClientFactory kafkaClientFactory)
    {
        _kafkaClientFactory = kafkaClientFactory;
        _configuration = new Dictionary<string, string?>
        {
            ["LoaderToolConfiguration:UseConcurrentLoad"] = "false",
            ["JsonFileDataExporterConfiguration:OutputDirectory"] = outputDirectory,
            ["BootstrapServersConfiguration:BootstrapServers:0"] = "fake-kafka:9092",
            ["TopicWatermarkLoaderConfiguration:AdminClientTimeout"] = "00:00:01",
            ["SnapshotLoaderConfiguration:DateOffsetTimeout"] = "00:00:01",
            ["SnapshotLoaderConfiguration:MaxConcurrentPartitions"] = "1"
        };
    }

    public static FakeKafkaPipelineHostBuilder Create(
        string outputDirectory,
        IKafkaClientFactory kafkaClientFactory)
        => new(outputDirectory, kafkaClientFactory);

    public FakeKafkaPipelineHostBuilder WithConcurrentLoad()
    {
        _configuration["LoaderToolConfiguration:UseConcurrentLoad"] = "true";
        return this;
    }

    public FakeKafkaPipelineHostBuilder WithTimeSortAscending()
    {
        _configuration["LoaderToolConfiguration:GlobalSortOrder"] = "Ascending";
        _configuration["LoaderToolConfiguration:GlobalMessageSort"] = "Time";
        return this;
    }

    public FakeKafkaPipelineHostBuilder WithTopic(
        int index,
        string name,
        string keyType,
        string compacting,
        string exportFileName,
        bool exportRawMessage,
        string filterKeyType = "None",
        string? filterKeyValue = null,
        string? offsetStartDate = null,
        string? offsetEndDate = null,
        IReadOnlyCollection<int>? partitionIds = null)
    {
        var prefix = $"LoaderToolConfiguration:Topics:{index}";
        _configuration[$"{prefix}:Name"] = name;
        _configuration[$"{prefix}:KeyType"] = keyType;
        _configuration[$"{prefix}:Compacting"] = compacting;
        _configuration[$"{prefix}:ExportFileName"] = exportFileName;
        _configuration[$"{prefix}:ExportRawMessage"] = exportRawMessage.ToString();
        _configuration[$"{prefix}:FilterKeyType"] = filterKeyType;

        if (filterKeyValue is not null)
        {
            _configuration[$"{prefix}:FilterKeyValue"] = filterKeyValue;
        }

        if (offsetStartDate is not null)
        {
            _configuration[$"{prefix}:OffsetStartDate"] = offsetStartDate;
        }

        if (offsetEndDate is not null)
        {
            _configuration[$"{prefix}:OffsetEndDate"] = offsetEndDate;
        }

        if (partitionIds is not null)
        {
            var partitionIndex = 0;
            foreach (var partitionId in partitionIds)
            {
                _configuration[$"{prefix}:PartitionsIds:{partitionIndex}"] =
                    partitionId.ToString(CultureInfo.InvariantCulture);
                partitionIndex++;
            }
        }

        return this;
    }

    public IHost Build()
    {
        var builder = Host.CreateApplicationBuilder();
        builder.Configuration.Sources.Clear();
        builder.Configuration.AddInMemoryCollection(_configuration);

        _ = builder.Services.AddKafkaSnapshotUtility(builder.Configuration);
        _ = builder.Services.AddSingleton(_kafkaClientFactory);

        return builder.Build();
    }

    private readonly IKafkaClientFactory _kafkaClientFactory;
    private readonly Dictionary<string, string?> _configuration;
}
