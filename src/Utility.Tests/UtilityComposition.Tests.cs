using FluentAssertions;

using KafkaSnapshot.Abstractions.Import;
using KafkaSnapshot.Abstractions.Processing;
using KafkaSnapshot.Import;
using KafkaSnapshot.Models.Filters;
using KafkaSnapshot.Processing;
using KafkaSnapshot.Utility.DependencyInjection;

using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;

using Xunit;

namespace KafkaSnapshot.Utility.Tests;

public class UtilityCompositionTests
{
    [Fact(DisplayName = "AddKafkaSnapshotProcessing resolves LoaderTool for sequential mode.")]
    [Trait("Category", "Unit")]
    public void AddKafkaSnapshotProcessingResolvesSequentialLoader()
    {
        // Arrange
        var configuration = UtilityTestConfiguration.BuildLoaderConfig(
            useConcurrentLoad: false,
            keyType: KeyType.String);
        var services = new ServiceCollection();
        _ = services.AddLogging();
        _ = services.AddSingleton<IReadOnlyCollection<IProcessingUnit>>([]);
        _ = services.AddKafkaSnapshotProcessing(configuration);

        using var provider = services.BuildServiceProvider();
        using var scope = provider.CreateScope();

        // Act
        var tool = scope.ServiceProvider.GetRequiredService<ILoaderTool>();

        // Assert
        tool.Should().BeOfType<LoaderTool>();
    }

    [Fact(DisplayName = "AddKafkaSnapshotUtility resolves snapshot import composition.")]
    [Trait("Category", "Unit")]
    public void AddKafkaSnapshotUtilityResolvesSnapshotImportComposition()
    {
        // Arrange
        var configuration = new ConfigurationBuilder()
            .AddInMemoryCollection(UtilityTestConfiguration.CreateValidConfig())
            .Build();
        var services = new ServiceCollection();
        _ = services.AddKafkaSnapshotUtility(configuration);
        using var provider = services.BuildServiceProvider();

        // Act
        var snapshotLoader = provider.GetRequiredService<ISnapshotLoader<string, string>>();
        var partitionReader = provider.GetRequiredService<IPartitionSnapshotReader<string, string>>();
        var partitionBatchReader = provider.GetRequiredService<IPartitionSnapshotBatchReader<string, string>>();
        var snapshotCompactor = provider.GetRequiredService<ISnapshotCompactor<string, string>>();

        // Assert
        snapshotLoader.Should().BeOfType<SnapshotLoader<string, string>>();
        partitionReader.Should().BeOfType<PartitionSnapshotReader<string, string>>();
        partitionBatchReader.Should().BeOfType<PartitionSnapshotBatchReader<string, string>>();
        snapshotCompactor.Should().BeOfType<SnapshotCompactor<string, string>>();
    }
}

