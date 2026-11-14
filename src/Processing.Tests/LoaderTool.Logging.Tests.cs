using FluentAssertions;

using KafkaSnapshot.Abstractions.Processing;
using KafkaSnapshot.Models.Names;

using Microsoft.Extensions.Logging;

using Moq;

using Xunit;

namespace KafkaSnapshot.Processing.Tests;

public class LoaderToolLoggingTests
{
    [Fact(DisplayName = "LoaderTool processes topics in sequence and disposes the information log scope after failure.")]
    [Trait("Category", "Unit")]
    public async Task ProcessingFailureDisposesInformationLogScope()
    {
        var logger = new Mock<ILogger<LoaderTool>>();
        logger.Setup(x => x.IsEnabled(LogLevel.Information)).Returns(true);
        var scope = new Mock<IDisposable>(MockBehavior.Strict);
        scope.Setup(x => x.Dispose());
        logger.Setup(x => x.BeginScope(It.IsAny<It.IsAnyType>())).Returns(scope.Object);
        var failure = new InvalidOperationException("processing failed");
        var first = new Mock<IProcessingUnit>(MockBehavior.Strict);
        first.SetupGet(x => x.TopicName).Returns(new TopicName("customers"));
        first.Setup(x => x.ProcessAsync(CancellationToken.None)).ThrowsAsync(failure);
        var second = new Mock<IProcessingUnit>(MockBehavior.Strict);
        var tool = new LoaderTool(logger.Object, [first.Object, second.Object]);

        Func<Task> act = () => tool.ProcessAsync(CancellationToken.None);

        (await act.Should().ThrowAsync<InvalidOperationException>()).Which.Should().BeSameAs(failure);
        first.Verify(x => x.ProcessAsync(CancellationToken.None), Times.Once);
        second.VerifyNoOtherCalls();
        scope.Verify(x => x.Dispose(), Times.Once);
    }
}
