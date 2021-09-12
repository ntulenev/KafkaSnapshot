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
    public class OriginalKeyDataExporterTests
    {
        [Fact(DisplayName = "OriginalKeyDataExporter can't be created without logger.")]
        [Trait("Category", "Unit")]
        public void OriginalKeyDataExporterCantBeCreatedWithoutLogger()
        {
            // Arrange
            var logger = (ILogger<OriginalKeyDataExporter<string>>)null!;
            var fileSaver = new Mock<IFileSaver>();

            // Act
            var exception = Record.Exception(() =>
            _ = new OriginalKeyDataExporter<string>(logger, fileSaver.Object));

            // Assert
            exception.Should().NotBeNull().And.BeOfType<ArgumentNullException>();
        }

        [Fact(DisplayName = "OriginalKeyDataExporter can't be created without fileSaver.")]
        [Trait("Category", "Unit")]
        public void OriginalKeyDataExporterCantBeCreatedWithoutSaver()
        {
            // Arrange
            var logger = new Mock<ILogger<OriginalKeyDataExporter<string>>>();
            var fileSaver = (IFileSaver)null!;

            // Act
            var exception = Record.Exception(() =>
            _ = new OriginalKeyDataExporter<string>(logger.Object, fileSaver));

            // Assert
            exception.Should().NotBeNull().And.BeOfType<ArgumentNullException>();
        }

        [Fact(DisplayName = "OriginalKeyDataExporter could be creates with valid params.")]
        [Trait("Category", "Unit")]
        public void OriginalKeyDataExporterCanBeCreated()
        {
            // Arrange
            var logger = new Mock<ILogger<OriginalKeyDataExporter<string>>>();
            var fileSaver = new Mock<IFileSaver>();

            // Act
            var exception = Record.Exception(() =>
            _ = new OriginalKeyDataExporter<string>(logger.Object, fileSaver.Object));

            // Assert
            exception.Should().BeNull();
        }

        [Fact(DisplayName = "OriginalKeyDataExporter can't export null data.")]
        [Trait("Category", "Unit")]
        public async Task OriginalKeyDataExporterCanExportNullDataAsync()
        {
            // Arrange
            var logger = new Mock<ILogger<OriginalKeyDataExporter<string>>>();
            var fileSaver = new Mock<IFileSaver>();
            var exporter = new OriginalKeyDataExporter<string>(logger.Object, fileSaver.Object);
            var topic = new ExportedTopic("name", "filename", true);
            var data = (IEnumerable<KeyValuePair<string, DatedMessage<string>>>)null!;

            // Act
            var exception = await Record.ExceptionAsync(async () =>
            await exporter.ExportAsync(data, topic, CancellationToken.None)
                );

            // Assert
            exception.Should().NotBeNull().And.BeOfType<ArgumentNullException>();
        }

        [Fact(DisplayName = "OriginalKeyDataExporter can't export for null topic.")]
        [Trait("Category", "Unit")]
        public async Task OriginalKeyDataExporterCanExportNullTopic()
        {
            // Arrange
            var logger = new Mock<ILogger<OriginalKeyDataExporter<string>>>();
            var fileSaver = new Mock<IFileSaver>();
            var exporter = new OriginalKeyDataExporter<string>(logger.Object, fileSaver.Object);
            var topic = (ExportedTopic)null!;
            var data = Enumerable.Empty<KeyValuePair<string, DatedMessage<string>>>();

            // Act
            var exception = await Record.ExceptionAsync(async () =>
            await exporter.ExportAsync(data, topic, CancellationToken.None)
                );

            // Assert
            exception.Should().NotBeNull().And.BeOfType<ArgumentNullException>();
        }

        [Fact(DisplayName = "OriginalKeyDataExporter can't export non json value data for json mode.")]
        [Trait("Category", "Unit")]
        public async Task OriginalKeyDataExporterCantExportNonDataForJsonMode()
        {
            // Arrange
            var logger = new Mock<ILogger<OriginalKeyDataExporter<string>>>();
            var fileSaver = new Mock<IFileSaver>();
            var exporter = new OriginalKeyDataExporter<string>(logger.Object, fileSaver.Object);
            var topic = new ExportedTopic("name", "filename", false);
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

        [Fact(DisplayName = "OriginalKeyDataExporter can export non json value data for raw mode.")]
        [Trait("Category", "Unit")]
        public async Task OriginalKeyDataExporterCanExportNonJsonDataForRawMode()
        {
            // Arrange
            var logger = new Mock<ILogger<OriginalKeyDataExporter<string>>>();
            var fileSaver = new Mock<IFileSaver>();
            var exporter = new OriginalKeyDataExporter<string>(logger.Object, fileSaver.Object);
            var topic = new ExportedTopic("name", "filename", true);
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
            exception.Should().BeNull();
            fileSaver.Verify(x => x.SaveAsync(topic.ExportName, It.IsAny<string>(), token), Times.Once);
        }


        [Theory(DisplayName = "OriginalKeyDataExporter can export json data for any mode.")]
        [Trait("Category", "Unit")]
        [InlineData(true)]
        [InlineData(false)]
        public async Task OriginalKeyDataExporterCanExportData(bool isRawMessage)
        {
            // Arrange
            var logger = new Mock<ILogger<OriginalKeyDataExporter<string>>>();
            var fileSaver = new Mock<IFileSaver>();
            var exporter = new OriginalKeyDataExporter<string>(logger.Object, fileSaver.Object);
            var topic = new ExportedTopic("name", "filename", isRawMessage);
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
