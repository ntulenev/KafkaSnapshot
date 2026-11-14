using FluentAssertions;

using KafkaSnapshot.Abstractions.Processing;

using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Logging.Abstractions;

using Moq;

using Xunit;

namespace KafkaSnapshot.Utility.Tests;

[Trait("Category", "Unit")]
public class LoaderServiceLifecycleTests
{
    [Theory(DisplayName = "LoaderService stops the application after success, cancellation, or failure.")]
    [InlineData(0)]
    [InlineData(1)]
    [InlineData(2)]
    public async Task ProcessingOutcomeAlwaysStopsApplication(int outcome)
    {
        var failure = new InvalidOperationException("processing failed");
        var tool = new Mock<ILoaderTool>(MockBehavior.Strict);
        tool.Setup(x => x.ProcessAsync(It.Is<CancellationToken>(ct => ct.CanBeCanceled)))
            .Returns<CancellationToken>(ct => outcome switch
            {
                1 => Task.FromException(new OperationCanceledException(ct)),
                2 => Task.FromException(failure),
                _ => Task.CompletedTask
            });
        var lifetime = new Mock<IHostApplicationLifetime>(MockBehavior.Strict);
        lifetime.Setup(x => x.StopApplication());
        var logger = new Mock<ILogger<LoaderService>>();
        using var loader = new LoaderService(tool.Object, lifetime.Object, logger.Object);

        await loader.StartAsync(CancellationToken.None);
        Func<Task> completed = () => loader.ExecuteTask!.WaitAsync(TestTimeouts.Wait, TestContext.Current.CancellationToken);

        if (outcome == 2)
        {
            (await completed.Should().ThrowAsync<InvalidOperationException>()).Which.Should().BeSameAs(failure);
        }
        else
        {
            await completed.Should().NotThrowAsync();
        }
        tool.VerifyAll();
        lifetime.Verify(x => x.StopApplication(), Times.Once);
        var expectedLevel = outcome == 2 ? LogLevel.Error : LogLevel.Information;
        var expectedText = outcome switch { 1 => "was canceled", 2 => "failed", _ => "completed" };
        logger.Verify(x => x.Log(
            expectedLevel, It.IsAny<EventId>(),
            It.Is<It.IsAnyType>((state, _) => state != null && state.ToString()!.Contains(expectedText)),
            outcome == 2 ? failure : null,
            It.IsAny<Func<It.IsAnyType, Exception?, string>>()), Times.Once);
    }

    [Fact(DisplayName = "LoaderService forwards shutdown cancellation to the running loader tool.")]
    public async Task StopAsyncCancelsProcessingAndStopsApplication()
    {
        var started = new TaskCompletionSource<CancellationToken>(TaskCreationOptions.RunContinuationsAsynchronously);
        var tool = new Mock<ILoaderTool>(MockBehavior.Strict);
        tool.Setup(x => x.ProcessAsync(It.IsAny<CancellationToken>())).Returns<CancellationToken>(async ct =>
        {
            started.SetResult(ct);
            await Task.Delay(Timeout.InfiniteTimeSpan, ct);
        });
        var lifetime = new Mock<IHostApplicationLifetime>(MockBehavior.Strict);
        lifetime.Setup(x => x.StopApplication());
        using var loader = new LoaderService(tool.Object, lifetime.Object, NullLogger<LoaderService>.Instance);
        using var timeout = TestTimeouts.CreateScenarioCancellation();

        await loader.StartAsync(timeout.Token);
        var processingToken = await started.Task.WaitAsync(timeout.Token);
        await loader.StopAsync(timeout.Token);
        await loader.ExecuteTask!.WaitAsync(timeout.Token);

        processingToken.IsCancellationRequested.Should().BeTrue();
        lifetime.Verify(x => x.StopApplication(), Times.Once);
    }

    [Fact(DisplayName = "LoaderService rejects a null logger.")]
    public void ConstructorRejectsNullLogger()
    {
        Action act = () => _ = new LoaderService(Mock.Of<ILoaderTool>(), Mock.Of<IHostApplicationLifetime>(), null!);
        act.Should().Throw<ArgumentNullException>().WithParameterName("logger");
    }
}
