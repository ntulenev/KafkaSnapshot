using System.Globalization;
using System.Reflection;

using FluentAssertions;

using KafkaSnapshot.Abstractions.Processing;
using KafkaSnapshot.Import.Configuration;
using KafkaSnapshot.Models.Configuration;
using KafkaSnapshot.Models.Filters;
using KafkaSnapshot.Processing;
using KafkaSnapshot.Processing.Configuration;
using KafkaSnapshot.Processing.Configuration.Validation;
using KafkaSnapshot.Utility.DependencyInjection;

using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;

using Xunit;

namespace KafkaSnapshot.Utility.Tests;

public class UtilityInternalsTests
{
    [Fact(DisplayName = "ConfigHelper throws on invalid bootstrap config.")]
    [Trait("Category", "Unit")]
    public void ConfigHelperThrowsOnInvalidBootstrapConfig()
    {
        // Arrange
        var services = new ServiceCollection();
        _ = services.AddSingleton<IValidateOptions<BootstrapServersConfiguration>>(
            new FailBootstrapValidator());
        var configuration = new ConfigurationBuilder()
            .AddInMemoryCollection(new Dictionary<string, string?>
            {
                ["BootstrapServersConfiguration:BootstrapServers:0"] = "server1"
            })
            .Build();
        _ = services.AddOptions<BootstrapServersConfiguration>()
            .Bind(configuration.GetSection(nameof(BootstrapServersConfiguration)));
        using var provider = services.BuildServiceProvider();

        // Act
        Action act = () => _ = provider.GetRequiredService<IOptions<BootstrapServersConfiguration>>().Value;

        // Assert
        act.Should().Throw<OptionsValidationException>();
    }

    [Fact(DisplayName = "ConfigHelper throws on invalid loader config.")]
    [Trait("Category", "Unit")]
    public void ConfigHelperThrowsOnInvalidLoaderConfig()
    {
        // Arrange
        var services = new ServiceCollection();
        _ = services.AddSingleton<IValidateOptions<LoaderToolConfiguration>>(
            new FailLoaderValidator());
        var configuration = BuildLoaderConfig(useConcurrentLoad: true, keyType: KeyType.String);
        _ = services.AddOptions<LoaderToolConfiguration>()
            .Bind(configuration.GetSection(nameof(LoaderToolConfiguration)));
        using var provider = services.BuildServiceProvider();

        // Act
        Action act = () => _ = provider.GetRequiredService<IOptions<LoaderToolConfiguration>>().Value;

        // Assert
        act.Should().Throw<OptionsValidationException>();
    }

    [Fact(DisplayName = "Host validates options on start.")]
    [Trait("Category", "Unit")]
    public async Task HostValidatesOptionsOnStart()
    {
        // Arrange
        var builder = Host.CreateApplicationBuilder([]);
        builder.Configuration.AddInMemoryCollection(CreateValidConfig());
        builder.Configuration["JsonFileDataExporterConfiguration:OutputDirectory"] = "   ";
        _ = builder.Services.AddKafkaSnapshotUtility(builder.Configuration);
        using var host = builder.Build();

        // Act
        var exception = await Record.ExceptionAsync(
            () => host.StartAsync(CancellationToken.None));

        // Assert
        exception.Should().BeOfType<OptionsValidationException>()
            .Which.Failures.Should().ContainSingle()
            .Which.Should().StartWith(
                ConfigurationValidationErrorCodes.OutputDirectoryWhitespace);
    }

    [Fact(DisplayName = "AddKafkaSnapshotProcessing resolves LoaderTool for sequential mode.")]
    [Trait("Category", "Unit")]
    public void AddKafkaSnapshotProcessingResolvesSequentialLoader()
    {
        // Arrange
        var configuration = BuildLoaderConfig(useConcurrentLoad: false, keyType: KeyType.String);
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

    [Fact(DisplayName = "TopicLoadersRegistrationHelper throws options validation for unsupported key type.")]
    [Trait("Category", "Unit")]
    public void TopicLoadersRegistrationHelperCreateTopicLoadersThrowsForUnsupportedKeyType()
    {
        // Arrange
        var configuration = BuildLoaderConfig(useConcurrentLoad: true, keyType: (KeyType)999);
        var services = new ServiceCollection();
        _ = services.AddOptions<LoaderToolConfiguration>()
            .Bind(configuration.GetSection(nameof(LoaderToolConfiguration)));
        _ = services.AddSingleton<IValidateOptions<LoaderToolConfiguration>>(
            new LoaderToolConfigurationValidator());
        using var provider = services.BuildServiceProvider();

        var method = GetStaticMethod(
            "KafkaSnapshot.Utility.Helpers.TopicLoadersRegistrationHelper",
            "CreateTopicLoaders",
            BindingFlags.NonPublic | BindingFlags.Static);

        // Act
        Action act = () => _ = method.Invoke(null, [provider]);

        // Assert
        var exception = act.Should().Throw<TargetInvocationException>().Which;
        var validationException = exception.InnerException.Should()
            .BeOfType<OptionsValidationException>()
            .Subject;
        validationException.Failures.Should().ContainSingle()
            .Which.Should().StartWith(
                ConfigurationValidationErrorCodes.KeyTypeUnsupported);
    }

    private static MethodInfo GetStaticMethod(
        string typeName,
        string methodName,
        BindingFlags flags = BindingFlags.Public | BindingFlags.Static)
    {
        var type = typeof(HostBuildHelper).Assembly.GetType(typeName);
        type.Should().NotBeNull();

        var method = type.GetMethod(methodName, flags);
        method.Should().NotBeNull();

        return method;
    }

    private static IConfiguration BuildLoaderConfig(bool useConcurrentLoad, KeyType keyType)
        => new ConfigurationBuilder()
            .AddInMemoryCollection(new Dictionary<string, string?>
            {
                ["LoaderToolConfiguration:UseConcurrentLoad"] = useConcurrentLoad.ToString(CultureInfo.InvariantCulture),
                ["LoaderToolConfiguration:Topics:0:Name"] = "topic-1",
                ["LoaderToolConfiguration:Topics:0:ExportFileName"] = "topic-1.json",
                ["LoaderToolConfiguration:Topics:0:KeyType"] = ((int)keyType).ToString(CultureInfo.InvariantCulture),
                ["LoaderToolConfiguration:Topics:0:Compacting"] = nameof(CompactingMode.Off),
                ["LoaderToolConfiguration:Topics:0:FilterKeyType"] = nameof(FilterType.None)
            })
            .Build();

    private static Dictionary<string, string?> CreateValidConfig()
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

    private sealed class FailBootstrapValidator :
        IValidateOptions<BootstrapServersConfiguration>
    {
        public ValidateOptionsResult Validate(string? name, BootstrapServersConfiguration options)
            => ValidateOptionsResult.Fail("forced bootstrap failure");
    }

    private sealed class FailLoaderValidator :
        IValidateOptions<LoaderToolConfiguration>
    {
        public ValidateOptionsResult Validate(string? name, LoaderToolConfiguration options)
            => ValidateOptionsResult.Fail("forced loader failure");
    }
}
