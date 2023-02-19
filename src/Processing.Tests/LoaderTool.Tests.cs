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
        unit1.Setup(x => x.ProcessAsync(cts.Token));
        unit2.Setup(x => x.ProcessAsync(cts.Token));
        unit1.Setup(x => x.TopicName).Returns(() => new TopicName("unit1"));
        unit2.Setup(x => x.TopicName).Returns(() => new TopicName("unit2"));
        var items = new[]
        {
            unit1.Object,unit2.Object
        };
        var tool = new LoaderTool(logger, items);

        // Act
        await tool.ProcessAsync(cts.Token).ConfigureAwait(false);

        // Assert
        unit1.Verify(x => x.ProcessAsync(cts.Token), Times.Once);
        unit2.Verify(x => x.ProcessAsync(cts.Token), Times.Once);
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
        unit1.Setup(x => x.ProcessAsync(cts.Token)).Throws(new Exception());
        var unit2 = new Mock<IProcessingUnit>(MockBehavior.Strict);
        unit2.Setup(x => x.TopicName).Returns(() => new TopicName("unit2"));
        unit2.Setup(x => x.ProcessAsync(cts.Token));
        var items = new[]
        {
            unit1.Object,unit2.Object
        };
        var tool = new LoaderTool(logger, items);

        // Act
        await tool.ProcessAsync(cts.Token).ConfigureAwait(false);

        // Assert
        unit1.Verify(x => x.ProcessAsync(cts.Token), Times.Once);
        unit2.Verify(x => x.ProcessAsync(cts.Token), Times.Once);
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
        var items = new[]
        {
            unit1.Object,unit2.Object
        };
        var tool = new LoaderTool(logger, items);
        cts.Cancel();

        // Act
        await tool.ProcessAsync(cts.Token).ConfigureAwait(false);

        // Assert
        unit1.Verify(x => x.ProcessAsync(cts.Token), Times.Never);
        unit2.Verify(x => x.ProcessAsync(cts.Token), Times.Never);
    }
}
