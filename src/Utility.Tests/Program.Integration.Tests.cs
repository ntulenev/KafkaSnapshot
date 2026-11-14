using FluentAssertions;

using KafkaSnapshot.Abstractions.Processing;

using Microsoft.Extensions.DependencyInjection;

using Xunit;

namespace KafkaSnapshot.Utility.Tests;

public class ProgramTests
{
    [Fact(DisplayName = "HostBuildHelper creates a host that resolves the configured loader tool.")]
    [Trait("Category", "Integration")]
    public void CreateHostResolvesConfiguredLoaderTool()
    {
        // Act
        var exception = Record.Exception(() =>
        {
            using var host = HostBuildHelper.CreateHost();
            using var serviceScope = host.Services.CreateScope();
            var services = serviceScope.ServiceProvider;
            _ = services.GetRequiredService<ILoaderTool>();
        });

        // Assert
        exception.Should().BeNull();
    }

    [Fact(DisplayName = "HostBuildHelper applies command-line overrides to loader configuration.")]
    [Trait("Category", "Integration")]
    public void CreateHostAppliesCommandLineOverrides()
    {
        using var host = HostBuildHelper.CreateHost(["--LoaderToolConfiguration:UseConcurrentLoad=true"]);

        var options = host.Services.GetRequiredService<Microsoft.Extensions.Options.IOptions<KafkaSnapshot.Processing.Configuration.LoaderToolConfiguration>>();

        options.Value.UseConcurrentLoad.Should().BeTrue();
    }
}
