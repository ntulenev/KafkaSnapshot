using System.Text.Json;

using FluentAssertions;

using KafkaSnapshot.Abstractions.Export;
using KafkaSnapshot.Export.Configuration;
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
public class StreamingExportTests
{
    [Fact(DisplayName = "Async export writes output before the source completes.")]
    public async Task FirstWriteDoesNotWaitForCompleteInput()
    {
        var wrote = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        using var stream = new ObservedStream(() => wrote.TrySetResult());
        var closed = false;
        async IAsyncEnumerable<KeyValuePair<string, KafkaMessage<string>>> Source()
        {
            try
            {
                yield return Message(new string('x', 100000));
                await wrote.Task.WaitAsync(TestTimeouts.Wait, TestContext.Current.CancellationToken);
                yield return Message("last");
            }
            finally
            {
                closed = true;
            }
        }

        await Exporter(stream).ExportAsync(Source(), Topic(), CancellationToken.None);
        closed.Should().BeTrue();
        stream.Disposed.Should().BeTrue();
        using var document = JsonDocument.Parse(stream.ToArray());
        document.RootElement.GetArrayLength().Should().Be(2);
    }

    [Theory(DisplayName = "Writer failure or cancellation disposes the asynchronous source and output stream.")]
    [InlineData(false)]
    [InlineData(true)]
    public async Task FailedOrCanceledWriterDisposesInput(bool cancel)
    {
        var wrote = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        using var stream = new ObservedStream(() => wrote.TrySetResult(), cancel ? "wait" : "fail");
        var closed = false;
        async IAsyncEnumerable<KeyValuePair<string, KafkaMessage<string>>> Source()
        {
            try
            {
                for (var i = 0; i < 10; i++)
                {
                    yield return Message(new string('x', 100000));
                    await Task.Yield();
                }
            }
            finally
            {
                closed = true;
            }
        }

        using var cts = TestTimeouts.CreateScenarioCancellation();
        var exporting = Exporter(stream).ExportAsync(Source(), Topic(), cts.Token);
        await wrote.Task.WaitAsync(TestTimeouts.Wait, TestContext.Current.CancellationToken);
        if (cancel)
        {
            await cts.CancelAsync();
        }

        Func<Task> act = () => exporting;
        if (cancel)
        {
            await act.Should().ThrowAsync<OperationCanceledException>();
        }
        else
        {
            await act.Should().ThrowAsync<IOException>().WithMessage("writer failed");
        }

        closed.Should().BeTrue();
        stream.Disposed.Should().BeTrue();
    }

    [Fact(DisplayName = "Async export honors buffered file mode.")]
    public async Task BufferedFileModeCollectsAsyncInput()
    {
        var storage = new Mock<IExportFileStorage>(MockBehavior.Strict);
        string? saved = null;
        storage.Setup(x => x.SaveAsync(It.IsAny<FileName>(), It.IsAny<string>(), It.IsAny<CancellationToken>()))
            .Callback<FileName, string, CancellationToken>((_, json, _) => saved = json).Returns(Task.CompletedTask);
        var exporter = new JsonFileDataExporter<string, OriginalKeyMarker, string>(
            Options.Create(new JsonFileDataExporterConfiguration()),
            NullLogger<JsonFileDataExporter<string, OriginalKeyMarker, string>>.Instance, storage.Object,
            new OriginalKeySerializer<string>(NullLogger<OriginalKeySerializer<string>>.Instance));
        async IAsyncEnumerable<KeyValuePair<string, KafkaMessage<string>>> Source()
        {
            await Task.Yield();
            yield return Message("first");
            yield return Message("second");
        }

        exporter.UseStreaming.Should().BeFalse();
        await exporter.ExportAsync(Source(), Topic(), CancellationToken.None);
        using var document = JsonDocument.Parse(saved!);
        document.RootElement.GetArrayLength().Should().Be(2);
    }

    private static JsonFileDataExporter<string, OriginalKeyMarker, string> Exporter(Stream stream)
    {
        var storage = new Mock<IExportFileStorage>(MockBehavior.Strict);
        storage.Setup(x => x.CreateFileStream(It.IsAny<FileName>())).Returns(stream);
        return new(Options.Create(new JsonFileDataExporterConfiguration { UseFileStreaming = true }),
            NullLogger<JsonFileDataExporter<string, OriginalKeyMarker, string>>.Instance, storage.Object,
            new OriginalKeySerializer<string>(NullLogger<OriginalKeySerializer<string>>.Instance));
    }

    private static ExportedTopic Topic() => new(new TopicName("orders"), new FileName("orders.json"), true);
    private static KeyValuePair<string, KafkaMessage<string>> Message(string value)
        => new("key", new(value, new(DateTimeOffset.UnixEpoch, 0, 0)));

    private sealed class ObservedStream(Action onWrite, string? mode = null) : MemoryStream
    {
        public bool Disposed { get; private set; }
        public override async ValueTask WriteAsync(ReadOnlyMemory<byte> buffer, CancellationToken ct = default)
        {
            onWrite();
            if (mode == "fail")
            {
                throw new IOException("writer failed");
            }

            if (mode == "wait")
            {
                await Task.Delay(Timeout.InfiniteTimeSpan, ct);
            }

            await base.WriteAsync(buffer, ct);
        }

        protected override void Dispose(bool disposing)
        {
            Disposed = true;
            base.Dispose(disposing);
        }
    }
}
