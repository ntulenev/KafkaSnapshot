using FluentAssertions;

using KafkaSnapshot.Export.Configuration;
using KafkaSnapshot.Export.File.Common;
using KafkaSnapshot.Models.Names;

using Microsoft.Extensions.Options;

using Xunit;

namespace KafkaSnapshot.Export.Tests;

[Trait("Category", "Unit")]
public class ExportFileStorageTests
{
    [Fact(DisplayName = "Export storage rejects missing configuration.")]
    public void MissingOptionsAreRejected()
    {
        Action missing = () => _ = new ExportFileStorage(null!);
        Action missingValue = () => _ = new ExportFileStorage(Options.Create<JsonFileDataExporterConfiguration>(null!));
        missing.Should().Throw<ArgumentNullException>();
        missingValue.Should().Throw<ArgumentNullException>();
    }

    [Theory(DisplayName = "Export storage creates the configured directory for text and stream writes.")]
    [InlineData(false)]
    [InlineData(true)]
    public async Task CreatesConfiguredDirectory(bool streaming)
    {
        var directory = Path.Combine(Path.GetTempPath(), Guid.NewGuid().ToString("N"));
        var storage = new ExportFileStorage(Options.Create(new JsonFileDataExporterConfiguration
        {
            OutputDirectory = directory
        }));
        try
        {
            if (streaming)
            {
                await using var stream = storage.CreateFileStream(new FileName("orders.json"));
                await stream.WriteAsync(new byte[] { 49 }, TestContext.Current.CancellationToken);
            }
            else
            {
                await storage.SaveAsync(new FileName("orders.json"), "1", CancellationToken.None);
            }

            (await System.IO.File.ReadAllTextAsync(Path.Combine(directory, "orders.json"), TestContext.Current.CancellationToken)).Should().Be("1");
        }
        finally
        {
            if (Directory.Exists(directory))
            {
                System.IO.File.Delete(Path.Combine(directory, "orders.json"));
                Directory.Delete(directory);
            }
        }
    }

    [Fact(DisplayName = "Canceled export does not create the output directory.")]
    public async Task CanceledWriteHasNoSideEffects()
    {
        var directory = Path.Combine(Path.GetTempPath(), Guid.NewGuid().ToString("N"));
        var storage = new ExportFileStorage(Options.Create(new JsonFileDataExporterConfiguration { OutputDirectory = directory }));
        using var cts = new CancellationTokenSource();
        await cts.CancelAsync();
        Func<Task> act = () => storage.SaveAsync(new FileName("orders.json"), "[]", cts.Token);
        await act.Should().ThrowAsync<OperationCanceledException>();
        Directory.Exists(directory).Should().BeFalse();
    }
}
