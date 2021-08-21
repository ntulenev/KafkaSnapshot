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
    public class JsonKeyJsonValueDataExporterTests
    {
        [Fact(DisplayName = "JsonKeyJsonValueDataExporter can't be created without logger.")]
        [Trait("Category", "Unit")]
        public void JsonKeyJsonValueDataExporterCantBeCreatedWithoutLogger()
        {
            // Arrange
            var logger = (ILogger<JsonKeyJsonValueDataExporter>)null!;
            var fileSaver = new Mock<IFileSaver>();

            // Act
            var exception = Record.Exception(() =>
            _ = new JsonKeyJsonValueDataExporter(logger, fileSaver.Object));

            // Assert
            exception.Should().NotBeNull().And.BeOfType<ArgumentNullException>();
        }

        [Fact(DisplayName = "JsonKeyJsonValueDataExporter can't be created without fileSaver.")]
        [Trait("Category", "Unit")]
        public void JsonKeyJsonValueDataExporterCantBeCreatedWithoutSaver()
        {
            // Arrange
            var logger = new Mock<ILogger<JsonKeyJsonValueDataExporter>>();
            var fileSaver = (IFileSaver)null!;

            // Act
            var exception = Record.Exception(() =>
            _ = new JsonKeyJsonValueDataExporter(logger.Object, fileSaver));

            // Assert
            exception.Should().NotBeNull().And.BeOfType<ArgumentNullException>();
        }

        [Fact(DisplayName = "JsonKeyJsonValueDataExporter could be creates with valid params.")]
        [Trait("Category", "Unit")]
        public void JsonKeyJsonValueDataExporterCanBeCreated()
        {
            // Arrange
            var logger = new Mock<ILogger<JsonKeyJsonValueDataExporter>>();
            var fileSaver = new Mock<IFileSaver>();

            // Act
            var exception = Record.Exception(() =>
            _ = new JsonKeyJsonValueDataExporter(logger.Object, fileSaver.Object));

            // Assert
            exception.Should().BeNull();
        }

        [Fact(DisplayName = "JsonKeyJsonValueDataExporter can't export null data.")]
        [Trait("Category", "Unit")]
        public async Task JsonKeyJsonValueDataExporterCanExportNullDataAsync()
        {
            // Arrange
            var logger = new Mock<ILogger<JsonKeyJsonValueDataExporter>>();
            var fileSaver = new Mock<IFileSaver>();
            var exporter = new JsonKeyJsonValueDataExporter(logger.Object, fileSaver.Object);
            var topic = new ExportedTopic("name", "filename");
            var data = (IEnumerable<KeyValuePair<string, DatedMessage<string>>>)null!;

            // Act
            var exception = await Record.ExceptionAsync(async () =>
            await exporter.ExportAsync(data, topic, CancellationToken.None)
                );

            // Assert
            exception.Should().NotBeNull().And.BeOfType<ArgumentNullException>();
        }

        [Fact(DisplayName = "JsonKeyJsonValueDataExporter can't export for null topic.")]
        [Trait("Category", "Unit")]
        public async Task JsonKeyJsonValueDataExporterCanExportNullTopic()
        {
            // Arrange
            var logger = new Mock<ILogger<JsonKeyJsonValueDataExporter>>();
            var fileSaver = new Mock<IFileSaver>();
            var exporter = new JsonKeyJsonValueDataExporter(logger.Object, fileSaver.Object);
            var topic = (ExportedTopic)null!;
            var data = Enumerable.Empty<KeyValuePair<string, DatedMessage<string>>>();

            // Act
            var exception = await Record.ExceptionAsync(async () =>
            await exporter.ExportAsync(data, topic, CancellationToken.None)
                );

            // Assert
            exception.Should().NotBeNull().And.BeOfType<ArgumentNullException>();
        }


        [Fact(DisplayName = "JsonKeyJsonValueDataExporter can't export non json value data.")]
        [Trait("Category", "Unit")]
        public async Task JsonKeyJsonValueDataExporterCantExportNonJsonValueData()
        {
            // Arrange
            var logger = new Mock<ILogger<JsonKeyJsonValueDataExporter>>();
            var fileSaver = new Mock<IFileSaver>();
            var exporter = new JsonKeyJsonValueDataExporter(logger.Object, fileSaver.Object);
            var topic = new ExportedTopic("name", "filename");
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

        [Fact(DisplayName = "JsonKeyJsonValueDataExporter can't export non json key data.")]
        [Trait("Category", "Unit")]
        public async Task JsonKeyJsonValueDataExporterCantExportNonJsonKeyData()
        {
            // Arrange
            var logger = new Mock<ILogger<JsonKeyJsonValueDataExporter>>();
            var fileSaver = new Mock<IFileSaver>();
            var exporter = new JsonKeyJsonValueDataExporter(logger.Object, fileSaver.Object);
            var topic = new ExportedTopic("name", "filename");
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

        [Fact(DisplayName = "JsonKeyJsonValueDataExporter can export data.")]
        [Trait("Category", "Unit")]
        public async Task JsonKeyJsonValueDataExporterCanExportData()
        {
            // Arrange
            var logger = new Mock<ILogger<JsonKeyJsonValueDataExporter>>();
            var fileSaver = new Mock<IFileSaver>();
            var exporter = new JsonKeyJsonValueDataExporter(logger.Object, fileSaver.Object);
            var topic = new ExportedTopic("name", "filename");
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
