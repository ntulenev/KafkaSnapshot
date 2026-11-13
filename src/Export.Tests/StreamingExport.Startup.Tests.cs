using System.Runtime.CompilerServices;

using FluentAssertions;

using KafkaSnapshot.Abstractions.Export;
using KafkaSnapshot.Export.Configuration;
using KafkaSnapshot.Export.File.Common;
using KafkaSnapshot.Export.File.Output;
using KafkaSnapshot.Export.Markers;
using KafkaSnapshot.Export.Serialization;
using KafkaSnapshot.Models.Export;
using KafkaSnapshot.Models.Message;
using KafkaSnapshot.Models.Names;

using Microsoft.Extensions.Logging.Abstractions;
using Microsoft.Extensions.Options;

using Moq;

using Xunit;

namespace KafkaSnapshot.Export.Tests;

[Trait("Category", "Unit")]
public class StreamingExportStartupTests
{
    [Theory(DisplayName = "An import failure or cancellation before the first message preserves the previous export.")]
    [InlineData(false)]
    [InlineData(true)]
    public async Task FailedStartupPreservesPreviousExport(bool cancel)
    {
        var directory = Path.Combine(Path.GetTempPath(), Guid.NewGuid().ToString("N"));
        Directory.CreateDirectory(directory);
        var path = Path.Combine(directory, "orders.json");
        const string previous = "[\"previous snapshot\"]";
        await System.IO.File.WriteAllTextAsync(path, previous);
        var options = Options.Create(new JsonFileDataExporterConfiguration { UseFileStreaming = true, OutputDirectory = directory });
        var exporter = new JsonFileDataExporter<string, OriginalKeyMarker, string>(options,
            NullLogger<JsonFileDataExporter<string, OriginalKeyMarker, string>>.Instance,
            new ExportFileStorage(options), new OriginalKeySerializer<string>(NullLogger<OriginalKeySerializer<string>>.Instance));
        var started = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var closed = false;
        var failure = new InvalidOperationException("metadata failed");
        async IAsyncEnumerable<KeyValuePair<string, KafkaMessage<string>>> Source(
            [EnumeratorCancellation] CancellationToken ct = default)
        {
            try
            {
                await Task.Yield();
                started.SetResult();
                if (cancel)
                {
                    await Task.Delay(Timeout.InfiniteTimeSpan, ct);
                }

                yield return await Task.FromException<KeyValuePair<string, KafkaMessage<string>>>(failure);
            }
            finally
            {
                closed = true;
            }
        }

        using var cts = new CancellationTokenSource(TimeSpan.FromSeconds(10));
        try
        {
            var pending = exporter.ExportAsync(Source(), new ExportedTopic(new TopicName("orders"), new FileName("orders.json"), true), cts.Token);
            await started.Task.WaitAsync(TimeSpan.FromSeconds(5));
            if (cancel)
            {
                await cts.CancelAsync();
            }

            Func<Task> act = () => pending;
            if (cancel)
            {
                await act.Should().ThrowAsync<OperationCanceledException>();
            }
            else
            {
                (await act.Should().ThrowAsync<InvalidOperationException>()).Which.Should().BeSameAs(failure);
            }

            closed.Should().BeTrue();
            (await System.IO.File.ReadAllTextAsync(path)).Should().Be(previous);
        }
        finally
        {
            System.IO.File.Delete(path);
            Directory.Delete(directory);
        }
    }
    [Fact(DisplayName = "An empty successful stream produces an empty JSON array.")]
    public async Task EmptyStreamWritesEmptyArray()
    {
        using var stream = new MemoryStream();
        var storage = new Mock<IExportFileStorage>(MockBehavior.Strict);
        storage.Setup(x => x.CreateFileStream(It.IsAny<FileName>())).Returns(stream);
        var exporter = new JsonFileDataExporter<string, OriginalKeyMarker, string>(
            Options.Create(new JsonFileDataExporterConfiguration { UseFileStreaming = true }),
            NullLogger<JsonFileDataExporter<string, OriginalKeyMarker, string>>.Instance, storage.Object,
            new OriginalKeySerializer<string>(NullLogger<OriginalKeySerializer<string>>.Instance));
        async IAsyncEnumerable<KeyValuePair<string, KafkaMessage<string>>> Source()
        {
            await Task.Yield();
            yield break;
        }

        await exporter.ExportAsync(Source(), new ExportedTopic(new TopicName("orders"), new FileName("orders.json"), true), CancellationToken.None);
        System.Text.Encoding.UTF8.GetString(stream.ToArray()).Should().Be("[]");
    }

    [Fact(DisplayName = "Failure to open the output disposes an already-started source.")]
    public async Task OutputOpenFailureClosesSource()
    {
        var storage = new Mock<IExportFileStorage>(MockBehavior.Strict);
        storage.Setup(x => x.CreateFileStream(It.IsAny<FileName>())).Throws(new IOException("cannot open"));
        var exporter = new JsonFileDataExporter<string, OriginalKeyMarker, string>(
            Options.Create(new JsonFileDataExporterConfiguration { UseFileStreaming = true }),
            NullLogger<JsonFileDataExporter<string, OriginalKeyMarker, string>>.Instance, storage.Object,
            new OriginalKeySerializer<string>(NullLogger<OriginalKeySerializer<string>>.Instance));
        var closed = false;
        async IAsyncEnumerable<KeyValuePair<string, KafkaMessage<string>>> Source()
        {
            try
            {
                await Task.Yield();
                yield return new("key", new("value", new(DateTimeOffset.UnixEpoch, 0, 0)));
            }
            finally
            {
                closed = true;
            }
        }

        Func<Task> act = () => exporter.ExportAsync(Source(), new ExportedTopic(new TopicName("orders"), new FileName("orders.json"), true), CancellationToken.None);
        await act.Should().ThrowAsync<IOException>().WithMessage("cannot open");
        closed.Should().BeTrue();
    }
}
