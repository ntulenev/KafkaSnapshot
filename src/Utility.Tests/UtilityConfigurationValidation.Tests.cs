using FluentAssertions;

using KafkaSnapshot.Import.Configuration;
using KafkaSnapshot.Models.Configuration;
using KafkaSnapshot.Models.Filters;
using KafkaSnapshot.Processing.Configuration;
using KafkaSnapshot.Utility.DependencyInjection;

using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Options;

using Xunit;

namespace KafkaSnapshot.Utility.Tests;

public class UtilityConfigurationValidationTests
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
        var configuration = UtilityTestConfiguration.BuildLoaderConfig(
            useConcurrentLoad: true,
            keyType: KeyType.String);
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
        builder.Configuration.AddInMemoryCollection(UtilityTestConfiguration.CreateValidConfig());
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

