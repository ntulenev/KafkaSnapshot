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
    public class OriginalKeyJsonValueDataExporterTests
    {
        [Fact(DisplayName = "OriginalKeyJsonValueDataExporter can't be created without logger.")]
        [Trait("Category", "Unit")]
        public void OriginalKeyJsonValueDataExporterCantBeCreatedWithoutLogger()
        {
            // Arrange
            var logger = (ILogger<OriginalKeyJsonValueDataExporter<string>>)null!;
            var fileSaver = new Mock<IFileSaver>();

            // Act
            var exception = Record.Exception(() =>
            _ = new OriginalKeyJsonValueDataExporter<string>(logger, fileSaver.Object));

            // Assert
            exception.Should().NotBeNull().And.BeOfType<ArgumentNullException>();
        }

        [Fact(DisplayName = "OriginalKeyJsonValueDataExporter can't be created without fileSaver.")]
        [Trait("Category", "Unit")]
        public void OriginalKeyJsonValueDataExporterCantBeCreatedWithoutSaver()
        {
            // Arrange
            var logger = new Mock<ILogger<OriginalKeyJsonValueDataExporter<string>>>();
            var fileSaver = (IFileSaver)null!;

            // Act
            var exception = Record.Exception(() =>
            _ = new OriginalKeyJsonValueDataExporter<string>(logger.Object, fileSaver));

            // Assert
            exception.Should().NotBeNull().And.BeOfType<ArgumentNullException>();
        }

        [Fact(DisplayName = "OriginalKeyJsonValueDataExporter could be creates with valid params.")]
        [Trait("Category", "Unit")]
        public void OriginalKeyJsonValueDataExporterCanBeCreated()
        {
            // Arrange
            var logger = new Mock<ILogger<OriginalKeyJsonValueDataExporter<string>>>();
            var fileSaver = new Mock<IFileSaver>();

            // Act
            var exception = Record.Exception(() =>
            _ = new OriginalKeyJsonValueDataExporter<string>(logger.Object, fileSaver.Object));

            // Assert
            exception.Should().BeNull();
        }

        [Fact(DisplayName = "OriginalKeyJsonValueDataExporter can't export null data.")]
        [Trait("Category", "Unit")]
        public async Task OriginalKeyJsonValueDataExporterCanExportNullDataAsync()
        {
            // Arrange
            var logger = new Mock<ILogger<OriginalKeyJsonValueDataExporter<string>>>();
            var fileSaver = new Mock<IFileSaver>();
            var exporter = new OriginalKeyJsonValueDataExporter<string>(logger.Object, fileSaver.Object);
            var topic = new ExportedTopic("name", "filename");
            var data = (IEnumerable<KeyValuePair<string, DatedMessage<string>>>)null!;

            // Act
            var exception = await Record.ExceptionAsync(async () =>
            await exporter.ExportAsync(data, topic, CancellationToken.None)
                );

            // Assert
            exception.Should().NotBeNull().And.BeOfType<ArgumentNullException>();
        }

        [Fact(DisplayName = "OriginalKeyJsonValueDataExporter can't export for null topic.")]
        [Trait("Category", "Unit")]
        public async Task OriginalKeyJsonValueDataExporterCanExportNullTopic()
        {
            // Arrange
            var logger = new Mock<ILogger<OriginalKeyJsonValueDataExporter<string>>>();
            var fileSaver = new Mock<IFileSaver>();
            var exporter = new OriginalKeyJsonValueDataExporter<string>(logger.Object, fileSaver.Object);
            var topic = (ExportedTopic)null!;
            var data = Enumerable.Empty<KeyValuePair<string, DatedMessage<string>>>();

            // Act
            var exception = await Record.ExceptionAsync(async () =>
            await exporter.ExportAsync(data, topic, CancellationToken.None)
                );

            // Assert
            exception.Should().NotBeNull().And.BeOfType<ArgumentNullException>();
        }

        [Fact(DisplayName = "OriginalKeyJsonValueDataExporter can't export non json value data.")]
        [Trait("Category", "Unit")]
        public async Task OriginalKeyJsonValueDataExporterCantExportNonJsonValueData()
        {
            // Arrange
            var logger = new Mock<ILogger<OriginalKeyJsonValueDataExporter<string>>>();
            var fileSaver = new Mock<IFileSaver>();
            var exporter = new OriginalKeyJsonValueDataExporter<string>(logger.Object, fileSaver.Object);
            var topic = new ExportedTopic("name", "filename");
            var data = new KeyValuePair<string, DatedMessage<string>>[]
            {
                new KeyValuePair<string, DatedMessage<string>>("test",new DatedMessage<string>("value", DateTime.UtcNow))
            };
            var jsonData = JsonConvert.SerializeObject(data, Formatting.Indented);
            var token = CancellationToken.None;
            // Act
            var exception = await Record.ExceptionAsync(async () =>
            await exporter.ExportAsync(data, topic, token)
                );

            // Assert
            exception.Should().NotBeNull().And.BeOfType<JsonReaderException>();
        }

        [Fact(DisplayName = "OriginalKeyJsonValueDataExporter can export data.")]
        [Trait("Category", "Unit")]
        public async Task OriginalKeyJsonValueDataExporterCanExportData()
        {
            // Arrange
            var logger = new Mock<ILogger<OriginalKeyJsonValueDataExporter<string>>>();
            var fileSaver = new Mock<IFileSaver>();
            var exporter = new OriginalKeyJsonValueDataExporter<string>(logger.Object, fileSaver.Object);
            var topic = new ExportedTopic("name", "filename");
            var data = new KeyValuePair<string, DatedMessage<string>>[]
            {
                new KeyValuePair<string, DatedMessage<string>>("test",new DatedMessage<string>("{\"value\": 1 }", DateTime.UtcNow))
            };
            var jsonData = JsonConvert.SerializeObject(data, Formatting.Indented);
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
