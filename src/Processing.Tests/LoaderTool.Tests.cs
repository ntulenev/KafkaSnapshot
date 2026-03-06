using FluentAssertions;

using Microsoft.Extensions.Logging;

using Moq;

using Xunit;

using KafkaSnapshot.Abstractions.Processing;
using KafkaSnapshot.Models.Names;

namespace KafkaSnapshot.Processing.Tests;

public class LoaderToolTests
{
    [Fact(DisplayName = "LoaderTool can't be created without logger.")]
    [Trait("Category", "Unit")]
    public void LoaderToolCantBeCreatedWithoutLogger()
    {
        // Arrange
        var logger = (ILogger<LoaderTool>)null!;
        var items = new[]
        {
            (new Mock<IProcessingUnit>(MockBehavior.Strict)).Object
        };

        // Act
        var exception = Record.Exception(() => new LoaderTool(logger, items));

        // Assert
        exception.Should().NotBeNull().And.BeOfType<ArgumentNullException>();
    }

    [Fact(DisplayName = "LoaderTool can't be created without unit collection.")]
    [Trait("Category", "Unit")]
    public void LoaderToolCantBeCreatedWithoutNullCollection()
    {
        // Arrange
        var loggerMock = new Mock<ILogger<LoaderTool>>();
        var logger = loggerMock.Object;
        var items = (IReadOnlyCollection<IProcessingUnit>)null!;

        // Act
        var exception = Record.Exception(() => new LoaderTool(logger, items));

        // Assert
        exception.Should().NotBeNull().And.BeOfType<ArgumentNullException>();
    }

    [Fact(DisplayName = "LoaderTool can be created with valid params.")]
    [Trait("Category", "Unit")]
    public void LoaderToolCanBeCreatedWithValidParams()
    {
        // Arrange
        var loggerMock = new Mock<ILogger<LoaderTool>>();
        var logger = loggerMock.Object;
        var items = new[]
        {
            (new Mock<IProcessingUnit>(MockBehavior.Strict)).Object
        };

        // Act
        var exception = Record.Exception(() => new LoaderTool(logger, items));

        // Assert
        exception.Should().BeNull();
    }

    [Fact(DisplayName = "LoaderTool can be created with debug logger.")]
    [Trait("Category", "Unit")]
    public void LoaderToolCanBeCreatedWithDebugLogger()
    {
        // Arrange
        var loggerMock = new Mock<ILogger<LoaderTool>>();
        loggerMock.Setup(x => x.IsEnabled(LogLevel.Debug)).Returns(true);
        var logger = loggerMock.Object;
        var items = new[]
        {
            (new Mock<IProcessingUnit>(MockBehavior.Strict)).Object
        };

        // Act
        var exception = Record.Exception(() => new LoaderTool(logger, items));

        // Assert
        exception.Should().BeNull();
    }

    [Fact(DisplayName = "LoaderTool can process units.")]
    [Trait("Category", "Unit")]
    public async Task LoaderToolProcessUnits()
    {
        // Arrange
        using var cts = new CancellationTokenSource();
        var loggerMock = new Mock<ILogger<LoaderTool>>();
        var logger = loggerMock.Object;
        var unit1 = new Mock<IProcessingUnit>(MockBehavior.Strict);
        var unit2 = new Mock<IProcessingUnit>(MockBehavior.Strict);
        var unit1ProcessCalls = 0;
        var unit2ProcessCalls = 0;
        unit1.Setup(x => x.ProcessAsync(cts.Token))
            .Callback(() => unit1ProcessCalls++)
            .Returns(Task.CompletedTask);
        unit2.Setup(x => x.ProcessAsync(cts.Token))
            .Callback(() => unit2ProcessCalls++)
            .Returns(Task.CompletedTask);
        unit1.Setup(x => x.TopicName).Returns(() => new TopicName("unit1"));
        unit2.Setup(x => x.TopicName).Returns(() => new TopicName("unit2"));
        var items = new[]
        {
            unit1.Object,unit2.Object
        };
        var tool = new LoaderTool(logger, items);

        // Act
        await tool.ProcessAsync(cts.Token);

        // Assert
        unit1ProcessCalls.Should().Be(1);
        unit2ProcessCalls.Should().Be(1);
    }

    [Fact(DisplayName = "LoaderTool can process units if error.")]
    [Trait("Category", "Unit")]
    public async Task LoaderToolProcessUnitsIfError()
    {
        // Arrange
        using var cts = new CancellationTokenSource();
        var loggerMock = new Mock<ILogger<LoaderTool>>();
        var logger = loggerMock.Object;
        var unit1 = new Mock<IProcessingUnit>(MockBehavior.Strict);
        unit1.Setup(x => x.TopicName).Returns(() => new TopicName("unit1"));
        var unit1ProcessCalls = 0;
        unit1.Setup(x => x.ProcessAsync(cts.Token))
            .Callback(() => unit1ProcessCalls++)
            .Throws(new Exception());
        var unit2 = new Mock<IProcessingUnit>(MockBehavior.Strict);
        unit2.Setup(x => x.TopicName).Returns(() => new TopicName("unit2"));
        var unit2ProcessCalls = 0;
        unit2.Setup(x => x.ProcessAsync(cts.Token))
            .Callback(() => unit2ProcessCalls++)
            .Returns(Task.CompletedTask);
        var items = new[]
        {
            unit1.Object,unit2.Object
        };
        var tool = new LoaderTool(logger, items);

        // Act
        var exception = await Record.ExceptionAsync(() => tool.ProcessAsync(cts.Token));

        // Assert
        exception.Should().NotBeNull().And.BeOfType<Exception>();
        unit1ProcessCalls.Should().Be(1);
        unit2ProcessCalls.Should().Be(0);
    }

    [Fact(DisplayName = "LoaderTool can't process units if token is canceled.")]
    [Trait("Category", "Unit")]
    public async Task LoaderToolCantProcessUnitsIfCancelled()
    {
        // Arrange
        using var cts = new CancellationTokenSource();
        var loggerMock = new Mock<ILogger<LoaderTool>>();
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
        var tool = new LoaderTool(logger, items);
        cts.Cancel();

        // Act
        var exception = await Record.ExceptionAsync(() => tool.ProcessAsync(cts.Token));

        // Assert
        exception.Should().NotBeNull().And.BeOfType<OperationCanceledException>();
        unit1ProcessCalls.Should().Be(0);
        unit2ProcessCalls.Should().Be(0);
    }
}
