using FluentAssertions;

using KafkaSnapshot.Abstractions.Processing;
using KafkaSnapshot.Models.Names;

using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Logging.Abstractions;

using Moq;

using Xunit;

namespace KafkaSnapshot.Processing.Tests;

public class LoaderConcurrentToolTests
{
    [Fact(DisplayName = "LoaderConcurrentTool can't be created without logger.")]
    [Trait("Category", "Unit")]
    public void LoaderConcurrentToolCannotBeCreatedWithoutLogger()
    {
        // Arrange
        var logger = (ILogger<LoaderConcurrentTool>)null!;
        var items = new[]
        {
            new Mock<IProcessingUnit>(MockBehavior.Strict).Object
        };

        // Act
        var exception = Record.Exception(() => new LoaderConcurrentTool(logger, items));

        // Assert
        exception.Should().NotBeNull().And.BeOfType<ArgumentNullException>();
    }

    [Fact(DisplayName = "LoaderConcurrentTool can't be created without unit collection.")]
    [Trait("Category", "Unit")]
    public void LoaderConcurrentToolCannotBeCreatedWithoutNullCollection()
    {
        // Arrange
        var loggerMock = new Mock<ILogger<LoaderConcurrentTool>>();
        var logger = loggerMock.Object;
        var items = (IReadOnlyCollection<IProcessingUnit>)null!;

        // Act
        var exception = Record.Exception(() => new LoaderConcurrentTool(logger, items));

        // Assert
        exception.Should().NotBeNull().And.BeOfType<ArgumentNullException>();
    }

    [Fact(DisplayName = "LoaderConcurrentTool can be created with valid parameters.")]
    [Trait("Category", "Unit")]
    public void LoaderConcurrentToolCanBeCreatedWithValidParams()
    {
        // Arrange
        var loggerMock = new Mock<ILogger<LoaderConcurrentTool>>();
        var logger = loggerMock.Object;
        var items = new[]
        {
            new Mock<IProcessingUnit>(MockBehavior.Strict).Object
        };

        // Act
        var exception = Record.Exception(() => new LoaderConcurrentTool(logger, items));

        // Assert
        exception.Should().BeNull();
    }

    [Fact(DisplayName = "LoaderConcurrentTool can be created with debug logger.")]
    [Trait("Category", "Unit")]
    public void LoaderConcurrentToolCanBeCreatedWithDebugLogger()
    {
        // Arrange
        var loggerMock = new Mock<ILogger<LoaderConcurrentTool>>();
        loggerMock.Setup(x => x.IsEnabled(LogLevel.Debug)).Returns(true);
        var logger = loggerMock.Object;
        var items = new[]
        {
            new Mock<IProcessingUnit>(MockBehavior.Strict).Object
        };

        // Act
        var exception = Record.Exception(() => new LoaderConcurrentTool(logger, items));

        // Assert
        exception.Should().BeNull();
    }

    [Fact(DisplayName = "LoaderConcurrentTool can process units.")]
    [Trait("Category", "Unit")]
    public async Task LoaderConcurrentToolProcessUnits()
    {
        // Arrange
        using var cts = new CancellationTokenSource();
        var loggerMock = new Mock<ILogger<LoaderConcurrentTool>>();
        var logger = loggerMock.Object;
        var unit1 = new Mock<IProcessingUnit>(MockBehavior.Strict);
        var unit2 = new Mock<IProcessingUnit>(MockBehavior.Strict);
        unit1.Setup(x => x.TopicName).Returns(() => new TopicName("unit1"));
        unit2.Setup(x => x.TopicName).Returns(() => new TopicName("unit2"));
        var unit1ProcessCalls = 0;
        var unit2ProcessCalls = 0;
        unit1.Setup(x => x.ProcessAsync(It.Is<CancellationToken>(current => current.CanBeCanceled)))
            .Callback(() => unit1ProcessCalls++)
            .Returns(Task.CompletedTask);
        unit2.Setup(x => x.ProcessAsync(It.Is<CancellationToken>(current => current.CanBeCanceled)))
            .Callback(() => unit2ProcessCalls++)
            .Returns(Task.CompletedTask);
        var items = new[]
        {
            unit1.Object,unit2.Object
        };
        var tool = new LoaderConcurrentTool(logger, items);

        // Act
        await tool.ProcessAsync(cts.Token);

        // Assert
        unit1ProcessCalls.Should().Be(1);
        unit2ProcessCalls.Should().Be(1);
    }

    [Fact(DisplayName = "Concurrent topics can enter blocking reads together.")]
    public async Task BlockingTopicReadsCanOverlap()
    {
        using var barrier = new Barrier(2);
        var unit = new Mock<IProcessingUnit>(MockBehavior.Strict);
        unit.SetupGet(x => x.TopicName).Returns(new TopicName("orders"));
        unit.Setup(x => x.ProcessAsync(It.IsAny<CancellationToken>()))
            .Returns<CancellationToken>(ct =>
            {
                barrier.SignalAndWait(TestTimeouts.Wait, ct).Should().BeTrue();
                return Task.CompletedTask;
            });

        var tool = new LoaderConcurrentTool(NullLogger<LoaderConcurrentTool>.Instance,
            [unit.Object, unit.Object]);
        await tool.ProcessAsync(TestContext.Current.CancellationToken);
    }

    [Fact(DisplayName = "LoaderConcurrentTool can't process units if token is cancelled.")]
    [Trait("Category", "Unit")]
    public async Task LoaderConcurrentToolCannotProcessUnitsWithCancelToken()
    {
        // Arrange
        using var cts = new CancellationTokenSource();
        var loggerMock = new Mock<ILogger<LoaderConcurrentTool>>();
        var logger = loggerMock.Object;
        var unit1 = new Mock<IProcessingUnit>(MockBehavior.Strict);
        var unit2 = new Mock<IProcessingUnit>(MockBehavior.Strict);
        unit1.Setup(x => x.TopicName).Returns(() => new TopicName("unit1"));
        unit2.Setup(x => x.TopicName).Returns(() => new TopicName("unit2"));
        var unit1ProcessCalls = 0;
        var unit2ProcessCalls = 0;
        unit1.Setup(x => x.ProcessAsync(It.Is<CancellationToken>(current => current.CanBeCanceled)))
            .Callback(() => unit1ProcessCalls++)
            .Returns(Task.CompletedTask);
        unit2.Setup(x => x.ProcessAsync(It.Is<CancellationToken>(current => current.CanBeCanceled)))
            .Callback(() => unit2ProcessCalls++)
            .Returns(Task.CompletedTask);
        var items = new[]
        {
            unit1.Object,unit2.Object
        };
        var tool = new LoaderConcurrentTool(logger, items);
        await cts.CancelAsync();

        // Act
        var exception = await Record.ExceptionAsync(() => tool.ProcessAsync(cts.Token));

        // Assert
        exception.Should().NotBeNull().And.BeOfType<OperationCanceledException>();
        unit1ProcessCalls.Should().Be(0);
        unit2ProcessCalls.Should().Be(0);
    }
}
