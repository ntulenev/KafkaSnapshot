using System.Reflection;

using FluentAssertions;

using KafkaSnapshot.Abstractions.Processing;
using KafkaSnapshot.Import.Configuration;
using KafkaSnapshot.Models.Filters;
using KafkaSnapshot.Processing;
using KafkaSnapshot.Processing.Configuration;
using KafkaSnapshot.Processing.Configuration.Validation;

using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Logging.Abstractions;
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

    [Fact(DisplayName = "StartupHelper AddTools resolves LoaderTool for sequential mode.")]
    [Trait("Category", "Unit")]
    public void StartupHelperAddToolsResolvesSequentialLoader()
    {
        // Arrange
        var configuration = BuildLoaderConfig(useConcurrentLoad: false, keyType: KeyType.String);
        var context = new HostBuilderContext(new Dictionary<object, object?>())
        {
            Configuration = configuration
        };
        var services = new ServiceCollection();
        _ = services.AddSingleton<ILogger<LoaderTool>>(NullLogger<LoaderTool>.Instance);
        _ = services.AddSingleton<ILogger<LoaderConcurrentTool>>(NullLogger<LoaderConcurrentTool>.Instance);
        _ = services.AddSingleton<IReadOnlyCollection<IProcessingUnit>>([]);

        var method = GetStaticMethod("KafkaSnapshot.Utility.StartupHelper", "AddTools");
        _ = method.Invoke(null, [services, context]);

        using var provider = services.BuildServiceProvider();
        using var scope = provider.CreateScope();

        // Act
        var tool = scope.ServiceProvider.GetRequiredService<ILoaderTool>();

        // Assert
        tool.Should().BeOfType<LoaderTool>();
    }

    [Fact(DisplayName = "StartupHelper throws for unsupported key type in topic loader creation.")]
    [Trait("Category", "Unit")]
    public void StartupHelperCreateTopicLoadersThrowsForUnsupportedKeyType()
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
            "KafkaSnapshot.Utility.StartupHelper",
            "CreateTopicLoaders",
            BindingFlags.NonPublic | BindingFlags.Static);

        // Act
        Action act = () => _ = method.Invoke(null, [provider]);

        // Assert
        act.Should().Throw<TargetInvocationException>()
            .WithInnerException<InvalidOperationException>()
            .WithMessage("*Invalid Key type*");
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
                ["LoaderToolConfiguration:UseConcurrentLoad"] = useConcurrentLoad.ToString(),
                ["LoaderToolConfiguration:Topics:0:Name"] = "topic-1",
                ["LoaderToolConfiguration:Topics:0:ExportFileName"] = "topic-1.json",
                ["LoaderToolConfiguration:Topics:0:KeyType"] = ((int)keyType).ToString(),
                ["LoaderToolConfiguration:Topics:0:Compacting"] = nameof(CompactingMode.Off),
                ["LoaderToolConfiguration:Topics:0:FilterKeyType"] = nameof(FilterType.None)
            })
            .Build();

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
