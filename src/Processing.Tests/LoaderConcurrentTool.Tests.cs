using FluentAssertions;

using Microsoft.Extensions.Logging;

using Moq;

using Xunit;

using KafkaSnapshot.Abstractions.Processing;
using KafkaSnapshot.Models.Names;

namespace KafkaSnapshot.Processing.Tests;

public class LoaderConcurrentToolTests
{
    [Fact(DisplayName = "LoaderConcurrentTool can't be created without logger.")]
    [Trait("Category", "Unit")]
    public void LoaderConcurrentToolCantBeCreatedWithoutLogger()
    {
        // Arrange
        var logger = (ILogger<LoaderConcurrentTool>)null!;
        var items = new[]
        {
            (new Mock<IProcessingUnit>(MockBehavior.Strict)).Object
        };

        // Act
        var exception = Record.Exception(() => new LoaderConcurrentTool(logger, items));

        // Assert
        exception.Should().NotBeNull().And.BeOfType<ArgumentNullException>();
    }

    [Fact(DisplayName = "LoaderConcurrentTool can't be created without unit collection.")]
    [Trait("Category", "Unit")]
    public void LoaderConcurrentToolCantBeCreatedWithoutNullCollection()
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

    [Fact(DisplayName = "LoaderConcurrentTool can be created with valid params.")]
    [Trait("Category", "Unit")]
    public void LoaderConcurrentToolCanBeCreatedWithValidParams()
    {
        // Arrange
        var loggerMock = new Mock<ILogger<LoaderConcurrentTool>>();
        var logger = loggerMock.Object;
        var items = new[]
        {
            (new Mock<IProcessingUnit>(MockBehavior.Strict)).Object
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
            (new Mock<IProcessingUnit>(MockBehavior.Strict)).Object
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
        unit1.Setup(x => x.ProcessAsync(cts.Token))
            .Callback(() => unit1ProcessCalls++)
            .Returns(Task.CompletedTask);
        unit2.Setup(x => x.ProcessAsync(cts.Token))
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

    [Fact(DisplayName = "LoaderConcurrentTool can process units if any thorws error.")]
    [Trait("Category", "Unit")]
    public async Task LoaderConcurrentToolProcessUnitsWithError()
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
        unit1.Setup(x => x.ProcessAsync(cts.Token))
            .Callback(() => unit1ProcessCalls++)
            .Throws<Exception>();
        unit2.Setup(x => x.ProcessAsync(cts.Token))
            .Callback(() => unit2ProcessCalls++)
            .Returns(Task.CompletedTask);
        var items = new[]
        {
            unit1.Object,unit2.Object
        };
        var tool = new LoaderConcurrentTool(logger, items);

        // Act
        var exception = await Record.ExceptionAsync(() => tool.ProcessAsync(cts.Token));

        // Assert
        exception.Should().NotBeNull().And.BeOfType<Exception>();
        unit1ProcessCalls.Should().Be(1);
        unit2ProcessCalls.Should().Be(1);
    }

    [Fact(DisplayName = "LoaderConcurrentTool cant process units if token is cancelled.")]
    [Trait("Category", "Unit")]
    public async Task LoaderConcurrentToolCantProcessUnitsWithCancelToken()
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
        unit1.Setup(x => x.ProcessAsync(cts.Token))
            .Callback(() => unit1ProcessCalls++)
            .Returns(Task.CompletedTask);
        unit2.Setup(x => x.ProcessAsync(cts.Token))
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
