using FluentAssertions;
using KafkaSnapshot.Abstractions.Sorting;
using KafkaSnapshot.Models.Message;
using Microsoft.Extensions.Logging;
using Moq;
using Xunit;

namespace KafkaSnapshot.Import.Tests;

public class SnapshotCompactorLoggingTests
{
    [Fact(DisplayName = "SnapshotCompactor logs the number of unique keys after compaction and ignores null keys.")]
    [Trait("Category", "Unit")]
    public void DebugLogReportsCompactedCount()
    {
        var logger = new Mock<ILogger<SnapshotCompactor<string, string>>>();
        logger.Setup(x => x.IsEnabled(LogLevel.Debug)).Returns(true);
        var sorter = new Mock<IMessageSorter<string, string>>(MockBehavior.Strict);
        var compactor = new SnapshotCompactor<string, string>(logger.Object, sorter.Object);
        var message = new KafkaMessage<string>("value", new KafkaMetadata(DateTime.UnixEpoch, 0, 0));

        var snapshot = compactor.CreateSnapshot([
            new("key", message), new(null!, message), new("key", message)
        ], withCompacting: true).ToList();

        snapshot.Should().ContainSingle().Which.Key.Should().Be("key");
        logger.Verify(x => x.Log(LogLevel.Debug, It.IsAny<EventId>(),
            It.Is<It.IsAnyType>((state, _) => state != null && state.ToString() == "Created compacting state for 1 item(s)"),
            null, It.IsAny<Func<It.IsAnyType, Exception?, string>>()), Times.Once);
        sorter.VerifyNoOtherCalls();
    }
}
