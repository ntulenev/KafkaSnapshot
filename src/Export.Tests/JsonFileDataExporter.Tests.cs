using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;

using Moq;

using Xunit;

using FluentAssertions;

using KafkaSnapshot.Abstractions.Export;
using KafkaSnapshot.Export.File.Output;
using KafkaSnapshot.Export.Markers;
using KafkaSnapshot.Models.Export;
using KafkaSnapshot.Models.Message;
using KafkaSnapshot.Export.Configuration;
using KafkaSnapshot.Models.Names;

namespace KafkaSnapshot.Export.Tests;

public class JsonFileDataExporterTests
{
    [Fact(DisplayName = "JsonFileDataExporter can't be created without logger.")]
    [Trait("Category", "Unit")]
    public void JsonFileDataExporterCantBeCreatedWithoutLogger()
    {
        // Arrange
        var config = new Mock<IOptions<JsonFileDataExporterConfiguration>>(MockBehavior.Strict);
        config.Setup(x => x.Value).Returns(new JsonFileDataExporterConfiguration());
        var logger = (ILogger<JsonFileDataExporter<object, OriginalKeyMarker, object, ExportedTopic>>)null!;
        var fileSaver = new Mock<IFileSaver>(MockBehavior.Strict);
        var serializer = new Mock<ISerializer<object, object, OriginalKeyMarker>>(MockBehavior.Strict);
        var fileStreamProvider = new Mock<IFileStreamProvider>(MockBehavior.Strict);

        // Act
        var exception = Record.Exception(() =>
        _ = new JsonFileDataExporter<object, OriginalKeyMarker, object, ExportedTopic>(
            config.Object, logger, fileSaver.Object, fileStreamProvider.Object, serializer.Object));

        // Assert
        exception.Should().NotBeNull().And.BeOfType<ArgumentNullException>();
    }

    [Fact(DisplayName = "JsonFileDataExporter can't be created without serializer.")]
    [Trait("Category", "Unit")]
    public void JsonFileDataExporterCantBeCreatedWithoutSerializer()
    {
        // Arrange
        var config = new Mock<IOptions<JsonFileDataExporterConfiguration>>(MockBehavior.Strict);
        config.Setup(x => x.Value).Returns(new JsonFileDataExporterConfiguration());
        var logger = new Mock<ILogger<JsonFileDataExporter<object, OriginalKeyMarker, object, ExportedTopic>>>();
        var fileSaver = new Mock<IFileSaver>(MockBehavior.Strict);
        var serializer = (ISerializer<object, object, OriginalKeyMarker>)null!;
        var fileStreamProvider = new Mock<IFileStreamProvider>(MockBehavior.Strict);

        // Act
        var exception = Record.Exception(() =>
        _ = new JsonFileDataExporter<object, OriginalKeyMarker, object, ExportedTopic>(
                config.Object, 
                logger.Object, 
                fileSaver.Object, 
                fileStreamProvider.Object, 
                serializer));

        // Assert
        exception.Should().NotBeNull().And.BeOfType<ArgumentNullException>();
    }

    [Fact(DisplayName = "JsonFileDataExporter can't be created without fileSaver.")]
    [Trait("Category", "Unit")]
    public void JsonFileDataExporterCantBeCreatedWithoutSaver()
    {
        // Arrange
        var config = new Mock<IOptions<JsonFileDataExporterConfiguration>>(MockBehavior.Strict);
        config.Setup(x => x.Value).Returns(new JsonFileDataExporterConfiguration());
        var logger = new Mock<ILogger<JsonFileDataExporter<object, OriginalKeyMarker, object, ExportedTopic>>>();
        var fileSaver = (IFileSaver)null!;
        var serializer = new Mock<ISerializer<object, object, OriginalKeyMarker>>(MockBehavior.Strict);
        var fileStreamProvider = new Mock<IFileStreamProvider>(MockBehavior.Strict);

        // Act
        var exception = Record.Exception(() =>
        _ = new JsonFileDataExporter<object, OriginalKeyMarker, object, ExportedTopic>(
                config.Object, 
                logger.Object, 
                fileSaver, 
                fileStreamProvider.Object, 
                serializer.Object));

        // Assert
        exception.Should().NotBeNull().And.BeOfType<ArgumentNullException>();
    }

    [Fact(DisplayName = "JsonFileDataExporter can't be created without fileStreamProvider.")]
    [Trait("Category", "Unit")]
    public void JsonFileDataExporterCantBeCreatedWithoutStreamProvider()
    {
        // Arrange
        var config = new Mock<IOptions<JsonFileDataExporterConfiguration>>(MockBehavior.Strict);
        config.Setup(x => x.Value).Returns(new JsonFileDataExporterConfiguration());
        var logger = new Mock<ILogger<JsonFileDataExporter<object, OriginalKeyMarker, object, ExportedTopic>>>();
        var fileSaver = new Mock<IFileSaver>(MockBehavior.Strict);
        var serializer = new Mock<ISerializer<object, object, OriginalKeyMarker>>(MockBehavior.Strict);
        var fileStreamProvider = (IFileStreamProvider)null!;

        // Act
        var exception = Record.Exception(() =>
        _ = new JsonFileDataExporter<object, OriginalKeyMarker, object, ExportedTopic>(
                config.Object, 
                logger.Object, 
                fileSaver.Object, 
                fileStreamProvider, 
                serializer.Object));

        // Assert
        exception.Should().NotBeNull().And.BeOfType<ArgumentNullException>();
    }

    [Fact(DisplayName = "JsonFileDataExporter could be creates with valid params.")]
    [Trait("Category", "Unit")]
    public void JsonFileDataExporterCanBeCreated()
    {
        // Arrange
        var config = new Mock<IOptions<JsonFileDataExporterConfiguration>>(MockBehavior.Strict);
        config.Setup(x => x.Value).Returns(new JsonFileDataExporterConfiguration());
        var logger = new Mock<ILogger<JsonFileDataExporter<object, OriginalKeyMarker, object, ExportedTopic>>>();
        var fileSaver = new Mock<IFileSaver>(MockBehavior.Strict);
        var serializer = new Mock<ISerializer<object, object, OriginalKeyMarker>>(MockBehavior.Strict);
        var fileStreamProvider = new Mock<IFileStreamProvider>(MockBehavior.Strict);

        // Act
        var exception = Record.Exception(() =>
        _ = new JsonFileDataExporter<object, OriginalKeyMarker, object, ExportedTopic>(
                config.Object, 
                logger.Object, 
                fileSaver.Object, 
                fileStreamProvider.Object, 
                serializer.Object));

        // Assert
        exception.Should().BeNull();
    }

    [Theory(DisplayName = "JsonFileDataExporter can't export null data.")]
    [Trait("Category", "Unit")]
    [InlineData(true)]
    [InlineData(false)]
    public async Task JsonFileDataExporterCanExportNullDataAsync(bool isStreaming)
    {
        // Arrange
        var config = new Mock<IOptions<JsonFileDataExporterConfiguration>>(MockBehavior.Strict);
        config.Setup(x => x.Value).Returns(new JsonFileDataExporterConfiguration() { UseFileStreaming = isStreaming });
        var logger = new Mock<ILogger<JsonFileDataExporter<object, OriginalKeyMarker, object, ExportedTopic>>>();
        var fileSaver = new Mock<IFileSaver>(MockBehavior.Strict);
        var serializer = new Mock<ISerializer<object, object, OriginalKeyMarker>>(MockBehavior.Strict);
        var fileStreamProvider = new Mock<IFileStreamProvider>(MockBehavior.Strict);
        var exporter = new JsonFileDataExporter<object, OriginalKeyMarker, object, ExportedTopic>
            (config.Object, logger.Object, fileSaver.Object, fileStreamProvider.Object, serializer.Object);
        var topic = new ExportedTopic(new TopicName("name"), new FileName("filename"), true);
        var data = (IEnumerable<KeyValuePair<object, KafkaMessage<object>>>)null!;

        // Act
        var exception = await Record.ExceptionAsync(async () =>
            await exporter.ExportAsync(data, topic, CancellationToken.None));

        // Assert
        exception.Should().NotBeNull().And.BeOfType<ArgumentNullException>();
    }

    [Theory(DisplayName = "JsonFileDataExporter can't export for null topic.")]
    [Trait("Category", "Unit")]
    [InlineData(true)]
    [InlineData(false)]
    public async Task JsonFileDataExporterCanExportNullTopic(bool isStreaming)
    {
        // Arrange
        var config = new Mock<IOptions<JsonFileDataExporterConfiguration>>(MockBehavior.Strict);
        config.Setup(x => x.Value).Returns(new JsonFileDataExporterConfiguration() { UseFileStreaming = isStreaming });
        var logger = new Mock<ILogger<JsonFileDataExporter<object, OriginalKeyMarker, object, ExportedTopic>>>();
        var fileSaver = new Mock<IFileSaver>(MockBehavior.Strict);
        var serializer = new Mock<ISerializer<object, object, OriginalKeyMarker>>(MockBehavior.Strict);
        var fileStreamProvider = new Mock<IFileStreamProvider>(MockBehavior.Strict);
        var exporter = new JsonFileDataExporter<object, OriginalKeyMarker, object, ExportedTopic>
            (config.Object, logger.Object, fileSaver.Object, fileStreamProvider.Object, serializer.Object);
        var topic = (ExportedTopic)null!;
        var data = Enumerable.Empty<KeyValuePair<object, KafkaMessage<object>>>();

        // Act
        var exception = await Record.ExceptionAsync(async () =>
            await exporter.ExportAsync(data, topic, CancellationToken.None));

        // Assert
        exception.Should().NotBeNull().And.BeOfType<ArgumentNullException>();
    }

    [Theory(DisplayName = "JsonFileDataExporter can export data with non stream mode.")]
    [Trait("Category", "Unit")]
    [InlineData(true)]
    [InlineData(false)]
    public async Task JsonFileDataExporterCanExportDataNonStream(bool isRawMessage)
    {
        // Arrange
        var config = new Mock<IOptions<JsonFileDataExporterConfiguration>>(MockBehavior.Strict);
        config.Setup(x => x.Value).Returns(new JsonFileDataExporterConfiguration() { UseFileStreaming = false });
        var logger = new Mock<ILogger<JsonFileDataExporter<object, OriginalKeyMarker, object, ExportedTopic>>>();
        var fileSaver = new Mock<IFileSaver>(MockBehavior.Strict);

        var serializer = new Mock<ISerializer<object, object, OriginalKeyMarker>>(MockBehavior.Strict);
        var fileStreamProvider = new Mock<IFileStreamProvider>(MockBehavior.Strict);
        var exporter = new JsonFileDataExporter<object, OriginalKeyMarker, object, ExportedTopic>
            (config.Object, logger.Object, fileSaver.Object, fileStreamProvider.Object, serializer.Object);
        var topic = new ExportedTopic(new TopicName("name"), new FileName("filename"), isRawMessage);
        var data = new KeyValuePair<object, KafkaMessage<object>>[]
        {
            new("test",
                new KafkaMessage<object>("value", 
                    new KafkaMetadata(DateTime.UtcNow,1,2)))
        };
        var jsonData = "testJson";
        serializer.Setup(x => x.Serialize(data, isRawMessage)).Returns(jsonData);
        using var tcs = new CancellationTokenSource();
        var token = tcs.Token;
        var fileSaverRunCount = 0;
        fileSaver.Setup(x => x.SaveAsync(topic.ExportName, jsonData, token))
            .Returns(Task.CompletedTask)
            .Callback(() => fileSaverRunCount++);

        // Act
        await exporter.ExportAsync(data, topic, token);

        // Assert
        fileSaverRunCount.Should().Be(1);
    }

    [Theory(DisplayName = "JsonFileDataExporter can export data with stream mode.")]
    [Trait("Category", "Unit")]
    [InlineData(true)]
    [InlineData(false)]
    public async Task JsonFileDataExporterCanExportDataWithStream(bool isRawMessage)
    {
        // Arrange
        var config = new Mock<IOptions<JsonFileDataExporterConfiguration>>(MockBehavior.Strict);
        config.Setup(x => x.Value).Returns(new JsonFileDataExporterConfiguration() { UseFileStreaming = true });
        var logger = new Mock<ILogger<JsonFileDataExporter<object, OriginalKeyMarker, object, ExportedTopic>>>();
        var fileSaver = new Mock<IFileSaver>(MockBehavior.Strict);

        var serializer = new Mock<ISerializer<object, object, OriginalKeyMarker>>(MockBehavior.Strict);
        var fileStreamProvider = new Mock<IFileStreamProvider>(MockBehavior.Strict);
        var exporter = new JsonFileDataExporter<object, OriginalKeyMarker, object, ExportedTopic>
            (config.Object, logger.Object, fileSaver.Object, fileStreamProvider.Object, serializer.Object);
        var topic = new ExportedTopic(new TopicName("name"), new FileName("filename"), isRawMessage);
        var data = new KeyValuePair<object, KafkaMessage<object>>[]
        {
            new("test",
                new KafkaMessage<object>("value", 
                    new KafkaMetadata(DateTime.UtcNow,1,2)))
        };
        var testStream = new Mock<Stream>().Object;
        fileStreamProvider.Setup(x => x.CreateFileStream(topic.ExportName)).Returns(() => testStream);
        var fileSaverRunCount = 0;
        serializer.Setup(x => x.Serialize(data, isRawMessage, testStream)).Callback(() => fileSaverRunCount++);
        using var tcs = new CancellationTokenSource();
        var token = tcs.Token;

        // Act
        await exporter.ExportAsync(data, topic, token);

        // Assert
        fileSaverRunCount.Should().Be(1);
    }
}
