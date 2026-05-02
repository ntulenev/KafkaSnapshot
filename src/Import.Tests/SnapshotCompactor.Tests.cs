using FluentAssertions;

using KafkaSnapshot.Abstractions.Sorting;
using KafkaSnapshot.Models.Message;

using Microsoft.Extensions.Logging;

using Moq;

using Xunit;

namespace KafkaSnapshot.Import.Tests;

public class SnapshotCompactorTests
{
    [Fact(DisplayName = "SnapshotCompactor can't be created with null logger.")]
    [Trait("Category", "Unit")]
    public void SnapshotCompactorCannotBeCreatedWithNullLogger()
    {
        // Arrange
        var sorterMock = new Mock<IMessageSorter<object, object>>(MockBehavior.Strict);

        // Act
        var exception = Record.Exception(() =>
            new SnapshotCompactor<object, object>(null!, sorterMock.Object));

        // Assert
        exception.Should().NotBeNull().And.BeOfType<ArgumentNullException>();
    }

    [Fact(DisplayName = "SnapshotCompactor can't be created with null sorter.")]
    [Trait("Category", "Unit")]
    public void SnapshotCompactorCannotBeCreatedWithNullSorter()
    {
        // Arrange
        var loggerMock = CreateLoggerMock();

        // Act
        var exception = Record.Exception(() =>
            new SnapshotCompactor<object, object>(loggerMock.Object, null!));

        // Assert
        exception.Should().NotBeNull().And.BeOfType<ArgumentNullException>();
    }

    [Fact(DisplayName = "SnapshotCompactor can't create snapshot from null items.")]
    [Trait("Category", "Unit")]
    public void SnapshotCompactorCannotCreateSnapshotFromNullItems()
    {
        // Arrange
        var loggerMock = CreateLoggerMock();
        var sorterMock = new Mock<IMessageSorter<object, object>>(MockBehavior.Strict);
        var compactor = new SnapshotCompactor<object, object>(loggerMock.Object, sorterMock.Object);

        // Act
        var exception = Record.Exception(() => compactor.CreateSnapshot(null!, withCompacting: false));

        // Assert
        exception.Should().NotBeNull().And.BeOfType<ArgumentNullException>();
    }

    [Fact(DisplayName = "SnapshotCompactor sorts snapshot without compacting.")]
    [Trait("Category", "Unit")]
    public void SnapshotCompactorSortsSnapshotWithoutCompacting()
    {
        // Arrange
        var loggerMock = CreateLoggerMock();
        var items = new[]
        {
            CreateMessage("b", "2", 1),
            CreateMessage("a", "1", 0)
        };
        var expected = items.Reverse().ToList();
        var sorterMock = new Mock<IMessageSorter<object, object>>(MockBehavior.Strict);
        sorterMock.Setup(x => x.Sort(items)).Returns(expected);
        var compactor = new SnapshotCompactor<object, object>(loggerMock.Object, sorterMock.Object);

        // Act
        var result = compactor.CreateSnapshot(items, withCompacting: false);

        // Assert
        result.Should().BeEquivalentTo(expected, options => options.WithStrictOrdering());
    }

    [Fact(DisplayName = "SnapshotCompactor keeps last message for each key.")]
    [Trait("Category", "Unit")]
    public void SnapshotCompactorKeepsLastMessageForEachKey()
    {
        // Arrange
        var loggerMock = CreateLoggerMock();
        var sorterMock = new Mock<IMessageSorter<object, object>>(MockBehavior.Strict);
        var items = new[]
        {
            CreateMessage("key1", "old", 0),
            CreateMessage("key2", "value", 1),
            CreateMessage("key1", "new", 2)
        };
        var compactor = new SnapshotCompactor<object, object>(loggerMock.Object, sorterMock.Object);

        // Act
        var result = compactor.CreateSnapshot(items, withCompacting: true).ToDictionary();

        // Assert
        result.Should().HaveCount(2);
        result["key1"].Message.Should().Be("new");
        result["key2"].Message.Should().Be("value");
    }

    private static KeyValuePair<object, KafkaMessage<object>> CreateMessage(
        object key,
        object message,
        long offset)
        =>
        new(
            key,
            new KafkaMessage<object>(
                message,
                new KafkaMetadata(DateTime.UtcNow, 0, offset)));

    private static Mock<ILogger<SnapshotCompactor<object, object>>> CreateLoggerMock()
    {
        var loggerMock = new Mock<ILogger<SnapshotCompactor<object, object>>>(MockBehavior.Strict);
        loggerMock.Setup(x => x.IsEnabled(It.IsAny<LogLevel>())).Returns(false);
        loggerMock.Setup(
            x => x.Log(
                It.IsAny<LogLevel>(),
                It.IsAny<EventId>(),
                It.IsAny<It.IsAnyType>(),
                It.IsAny<Exception?>(),
                It.IsAny<Func<It.IsAnyType, Exception?, string>>()));

        return loggerMock;
    }
}
