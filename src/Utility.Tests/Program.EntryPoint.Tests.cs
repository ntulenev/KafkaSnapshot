using System.Reflection;

using FluentAssertions;

using KafkaSnapshot.Models.Configuration;

using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.Options;

using Xunit;

namespace KafkaSnapshot.Utility.Tests;

public class ProgramEntryPointTests
{
    [Fact(DisplayName = "Utility entry point rejects invalid command-line configuration before connecting to Kafka.")]
    [Trait("Category", "Integration")]
    public async Task EntryPointValidatesConfigurationBeforeLoading()
    {
        using var directory = new TestDirectory();
        var settings = UtilityTestConfiguration.CreateValidConfig();
        settings["LoaderToolConfiguration:Topics:0:Name"] = "invalid topic";
        settings["JsonFileDataExporterConfiguration:OutputDirectory"] = directory.Path;
        var args = settings.Select(x => $"--{x.Key}={x.Value}").ToArray();
        var entryPoint = typeof(HostBuildHelper).Assembly.EntryPoint!;

        var exception = await Record.ExceptionAsync(async () =>
        {
            await Task.Run(async () =>
            {
                var result = entryPoint.Invoke(null, [args]);
                if (result is Task task)
                {
                    await task;
                }
            }).WaitAsync(TestTimeouts.Wait);
        });

        var failure = exception is TargetInvocationException invocation ? invocation.InnerException : exception;
        failure.Should().BeOfType<OptionsValidationException>().Which.Failures.Should()
            .ContainSingle().Which.Should().StartWith(ConfigurationValidationErrorCodes.TopicNameWhitespace);
        Directory.EnumerateFiles(directory.Path).Should().BeEmpty();
    }

    [Fact(DisplayName = "Application settings registration loads appsettings.json when the file is present.")]
    [Trait("Category", "Unit")]
    public void RegisterApplicationSettingsLoadsJsonConfiguration()
    {
        using var directory = new TestDirectory();
        File.WriteAllText(Path.Combine(directory.Path, "appsettings.json"), "{\"TestSetting\":\"loaded\"}");
        var builder = new ConfigurationBuilder().SetBasePath(directory.Path);
        var helper = typeof(HostBuildHelper).Assembly.GetType("KafkaSnapshot.Utility.Helpers.ConfigHelper")!;

        helper.GetMethod("RegisterApplicationSettings")!.Invoke(null, [builder]);
        var configuration = builder.Build();
        using var cleanup = (IDisposable)configuration;

        configuration["TestSetting"].Should().Be("loaded");
    }
}
