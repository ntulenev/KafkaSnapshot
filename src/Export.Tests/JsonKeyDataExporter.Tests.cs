using System;
using System.Threading;
using System.Threading.Tasks;
using System.Linq;
using System.Collections.Generic;

using Microsoft.Extensions.Logging;

using Moq;

using Xunit;

using FluentAssertions;

using Newtonsoft.Json;

using KafkaSnapshot.Abstractions.Export;
using KafkaSnapshot.Export.File.Json;
using KafkaSnapshot.Models.Export;
using KafkaSnapshot.Models.Message;

namespace KafkaSnapshot.Export.Tests
{
    public class JsonKeyDataExporterTests
    {
        [Fact(DisplayName = "JsonKeyDataExporter can't be created without logger.")]
        [Trait("Category", "Unit")]
        public void JsonKeyDataExporterCantBeCreatedWithoutLogger()
        {
            // Arrange
            var logger = (ILogger<JsonKeyDataExporter>)null!;
            var fileSaver = new Mock<IFileSaver>();

            // Act
            var exception = Record.Exception(() =>
            _ = new JsonKeyDataExporter(logger, fileSaver.Object));

            // Assert
            exception.Should().NotBeNull().And.BeOfType<ArgumentNullException>();
        }

        [Fact(DisplayName = "JsonKeyDataExporter can't be created without fileSaver.")]
        [Trait("Category", "Unit")]
        public void JsonKeyDataExporterCantBeCreatedWithoutSaver()
        {
            // Arrange
            var logger = new Mock<ILogger<JsonKeyDataExporter>>();
            var fileSaver = (IFileSaver)null!;

            // Act
            var exception = Record.Exception(() =>
            _ = new JsonKeyDataExporter(logger.Object, fileSaver));

            // Assert
            exception.Should().NotBeNull().And.BeOfType<ArgumentNullException>();
        }

        [Fact(DisplayName = "JsonKeyDataExporter could be creates with valid params.")]
        [Trait("Category", "Unit")]
        public void JsonKeyDataExporterCanBeCreated()
        {
            // Arrange
            var logger = new Mock<ILogger<JsonKeyDataExporter>>();
            var fileSaver = new Mock<IFileSaver>();

            // Act
            var exception = Record.Exception(() =>
            _ = new JsonKeyDataExporter(logger.Object, fileSaver.Object));

            // Assert
            exception.Should().BeNull();
        }

        [Fact(DisplayName = "JsonKeyDataExporter can't export null data.")]
        [Trait("Category", "Unit")]
        public async Task JsonKeyDataExporterCanExportNullDataAsync()
        {
            // Arrange
            var logger = new Mock<ILogger<JsonKeyDataExporter>>();
            var fileSaver = new Mock<IFileSaver>();
            var exporter = new JsonKeyDataExporter(logger.Object, fileSaver.Object);
            var topic = new ExportedTopic("name", "filename", true);
            var data = (IEnumerable<KeyValuePair<string, DatedMessage<string>>>)null!;

            // Act
            var exception = await Record.ExceptionAsync(async () =>
            await exporter.ExportAsync(data, topic, CancellationToken.None)
                );

            // Assert
            exception.Should().NotBeNull().And.BeOfType<ArgumentNullException>();
        }

        [Fact(DisplayName = "JsonKeyDataExporter can't export for null topic.")]
        [Trait("Category", "Unit")]
        public async Task JsonKeyDataExporterCanExportNullTopic()
        {
            // Arrange
            var logger = new Mock<ILogger<JsonKeyDataExporter>>();
            var fileSaver = new Mock<IFileSaver>();
            var exporter = new JsonKeyDataExporter(logger.Object, fileSaver.Object);
            var topic = (ExportedTopic)null!;
            var data = Enumerable.Empty<KeyValuePair<string, DatedMessage<string>>>();

            // Act
            var exception = await Record.ExceptionAsync(async () =>
            await exporter.ExportAsync(data, topic, CancellationToken.None)
                );

            // Assert
            exception.Should().NotBeNull().And.BeOfType<ArgumentNullException>();
        }


        [Fact(DisplayName = "JsonKeyDataExporter can't export non json value data if not raw mode.")]
        [Trait("Category", "Unit")]
        public async Task JsonKeyDataExporterCantExportNonDataIfNoRawMode()
        {
            // Arrange
            var logger = new Mock<ILogger<JsonKeyDataExporter>>();
            var fileSaver = new Mock<IFileSaver>();
            var exporter = new JsonKeyDataExporter(logger.Object, fileSaver.Object);
            var topic = new ExportedTopic("name", "filename", false);
            var data = new KeyValuePair<string, DatedMessage<string>>[]
            {
                new KeyValuePair<string, DatedMessage<string>>("{\"id\": 1 }",new DatedMessage<string>("test", DateTime.UtcNow))
            };
            var token = CancellationToken.None;

            // Act
            var exception = await Record.ExceptionAsync(async () =>
            await exporter.ExportAsync(data, topic, token)
                );

            // Assert
            exception.Should().NotBeNull().And.BeOfType<JsonReaderException>();
        }

        [Fact(DisplayName = "JsonKeyDataExporter can export non json value data if raw mode.")]
        [Trait("Category", "Unit")]
        public async Task JsonKeyDataExporterCanExportNonDataInRawMode()
        {
            // Arrange
            var logger = new Mock<ILogger<JsonKeyDataExporter>>();
            var fileSaver = new Mock<IFileSaver>();
            var exporter = new JsonKeyDataExporter(logger.Object, fileSaver.Object);
            var topic = new ExportedTopic("name", "filename", true);
            var data = new KeyValuePair<string, DatedMessage<string>>[]
            {
                new KeyValuePair<string, DatedMessage<string>>("{\"id\": 1 }",new DatedMessage<string>("test", DateTime.UtcNow))
            };
            var token = CancellationToken.None;

            // Act
            var exception = await Record.ExceptionAsync(async () =>
            await exporter.ExportAsync(data, topic, token)
                );

            // Assert
            exception.Should().BeNull();
            fileSaver.Verify(x => x.SaveAsync(topic.ExportName, It.IsAny<string>(), token), Times.Once);
        }

        [Fact(DisplayName = "JsonKeyDataExporter can't export non json key data.")]
        [Trait("Category", "Unit")]
        public async Task JsonKeyDataExporterCantExportNonJsonKeyData()
        {
            // Arrange
            var logger = new Mock<ILogger<JsonKeyDataExporter>>();
            var fileSaver = new Mock<IFileSaver>();
            var exporter = new JsonKeyDataExporter(logger.Object, fileSaver.Object);
            var topic = new ExportedTopic("name", "filename", true);
            var data = new KeyValuePair<string, DatedMessage<string>>[]
            {
                new KeyValuePair<string, DatedMessage<string>>("test",new DatedMessage<string>("{\"value\": 1 }", DateTime.UtcNow))
            };
            var token = CancellationToken.None;

            // Act
            var exception = await Record.ExceptionAsync(async () =>
            await exporter.ExportAsync(data, topic, token)
                );

            // Assert
            exception.Should().NotBeNull().And.BeOfType<JsonReaderException>();
        }

        [Theory(DisplayName = "JsonKeyDataExporter can export json data for any mode.")]
        [Trait("Category", "Unit")]
        [InlineData(true)]
        [InlineData(false)]
        public async Task JsonKeyDataExporterCanExportData(bool isRawMessage)
        {
            // Arrange
            var logger = new Mock<ILogger<JsonKeyDataExporter>>();
            var fileSaver = new Mock<IFileSaver>();
            var exporter = new JsonKeyDataExporter(logger.Object, fileSaver.Object);
            var topic = new ExportedTopic("name", "filename", isRawMessage);
            var data = new KeyValuePair<string, DatedMessage<string>>[]
            {
                new KeyValuePair<string, DatedMessage<string>>("{\"id\": 1 }",new DatedMessage<string>("{\"value\": 1 }", DateTime.UtcNow))
            };
            var token = CancellationToken.None;

            // Act
            var exception = await Record.ExceptionAsync(async () =>
            await exporter.ExportAsync(data, topic, token)
                );

            // Assert
            exception.Should().BeNull();
            fileSaver.Verify(x => x.SaveAsync(topic.ExportName, It.IsAny<string>(), token), Times.Once);
        }
    }
}
