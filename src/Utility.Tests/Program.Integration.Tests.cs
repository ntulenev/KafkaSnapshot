using Microsoft.Extensions.DependencyInjection;

using Xunit;

using FluentAssertions;

using KafkaSnapshot.Abstractions.Processing;
using KafkaSnapshot.Utility;

namespace Utility.Tests;

public class ProgramTests
{
    [Fact(DisplayName = "Utility can be created.")]
    [Trait("Category", "Integration")]
    public void UtilityCanBeCreated()
    {
        // Act
        var exception = Record.Exception(() =>
        {
            using var serviceScope = HostBuildHelper.CreateHost().Services.CreateScope();
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
