using FluentAssertions;

using KafkaSnapshot.Abstractions.Processing;
using KafkaSnapshot.Models.Names;

using Microsoft.Extensions.Logging.Abstractions;

using Moq;

using Xunit;

namespace KafkaSnapshot.Processing.Tests;

[Trait("Category", "Unit")]
public class LoaderConcurrentToolFailureTests
{
    [Fact(DisplayName = "A topic failure cancels and joins a sibling topic awaiting cancellation.")]
    public async Task FailureStopsSiblingTopic()
    {
        var started = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var closed = false;
        var failure = new InvalidOperationException("export failed");
        var first = new Mock<IProcessingUnit>(MockBehavior.Strict);
        first.SetupGet(x => x.TopicName).Returns(new TopicName("first"));
        first.Setup(x => x.ProcessAsync(It.IsAny<CancellationToken>())).Returns<CancellationToken>(async ct =>
        {
            started.SetResult();
            try
            {
                await Task.Delay(Timeout.InfiniteTimeSpan, ct);
            }
            finally
            {
                closed = true;
            }
        });
        var second = new Mock<IProcessingUnit>(MockBehavior.Strict);
        second.SetupGet(x => x.TopicName).Returns(new TopicName("second"));
        second.Setup(x => x.ProcessAsync(It.IsAny<CancellationToken>())).Returns(async () =>
        {
            await started.Task;
            throw failure;
        });
        var tool = new LoaderConcurrentTool(NullLogger<LoaderConcurrentTool>.Instance, [first.Object, second.Object]);
        using var cts = new CancellationTokenSource(TestTimeouts.Scenario);
        var pending = tool.ProcessAsync(cts.Token);
        try
        {
            Func<Task> act = () => pending.WaitAsync(TestTimeouts.Wait);
            (await act.Should().ThrowAsync<InvalidOperationException>()).Which.Should().BeSameAs(failure);
            closed.Should().BeTrue();
        }
        finally
        {
            await cts.CancelAsync();
            _ = await Record.ExceptionAsync(() => pending);
        }
    }
}
