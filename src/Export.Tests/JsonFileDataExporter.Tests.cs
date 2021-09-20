//using System;
//using System.Threading;
//using System.Threading.Tasks;
//using System.Linq;
//using System.Collections.Generic;

//using Microsoft.Extensions.Logging;

//using Moq;

//using Xunit;

//using FluentAssertions;

//using Newtonsoft.Json;

//using KafkaSnapshot.Abstractions.Export;
//using KafkaSnapshot.Export.File.Output;
//using KafkaSnapshot.Export.Markers;
//using KafkaSnapshot.Models.Export;
//using KafkaSnapshot.Models.Message;

//namespace KafkaSnapshot.Export.Tests
//{
//    public class JsonFileDataExporterTests
//    {
//        [Fact(DisplayName = "JsonFileDataExporter can't be created without logger.")]
//        [Trait("Category", "Unit")]
//        public void JsonFileDataExporterCantBeCreatedWithoutLogger()
//        {
//            // Arrange
//            var logger = (ILogger<JsonFileDataExporter<object, OriginalKeyMarker, object, ExportedTopic>>)null!;
//            var fileSaver = new Mock<IFileSaver>();

//            // Act
//            var exception = Record.Exception(() =>
//            _ = new JsonFileDataExporter<object, OriginalKeyMarker, object, ExportedTopic>(logger, fileSaver.Object));

//            // Assert
//            exception.Should().NotBeNull().And.BeOfType<ArgumentNullException>();
//        }

//        [Fact(DisplayName = "JsonFileDataExporter can't be created without fileSaver.")]
//        [Trait("Category", "Unit")]
//        public void JsonFileDataExporterCantBeCreatedWithoutSaver()
//        {
//            // Arrange
//            var logger = new Mock<ILogger<JsonFileDataExporter<object, OriginalKeyMarker, object, ExportedTopic>>>();
//            var fileSaver = (IFileSaver)null!;

//            // Act
//            var exception = Record.Exception(() =>
//            _ = new JsonFileDataExporter<object, OriginalKeyMarker, object, ExportedTopic>(logger.Object, fileSaver));

//            // Assert
//            exception.Should().NotBeNull().And.BeOfType<ArgumentNullException>();
//        }

//        [Fact(DisplayName = "JsonFileDataExporter could be creates with valid params.")]
//        [Trait("Category", "Unit")]
//        public void JsonFileDataExporterCanBeCreated()
//        {
//            // Arrange
//            var logger = new Mock<ILogger<JsonFileDataExporter<object, OriginalKeyMarker, object, ExportedTopic>>>();
//            var fileSaver = new Mock<IFileSaver>();

//            // Act
//            var exception = Record.Exception(() =>
//            _ = new JsonFileDataExporter<object, OriginalKeyMarker, object, ExportedTopic>(logger.Object, fileSaver.Object));

//            // Assert
//            exception.Should().BeNull();
//        }

//        [Fact(DisplayName = "JsonFileDataExporter can't export null data.")]
//        [Trait("Category", "Unit")]
//        public async Task JsonFileDataExporterCanExportNullDataAsync()
//        {
//            // Arrange
//            var logger = new Mock<ILogger<JsonFileDataExporter<object, OriginalKeyMarker, object, ExportedTopic>>>();
//            var fileSaver = new Mock<IFileSaver>();
//            var exporter = new JsonFileDataExporter<object, OriginalKeyMarker, object, ExportedTopic>(logger.Object, fileSaver.Object);
//            var topic = new ExportedTopic("name", "filename", true);
//            var data = (IEnumerable<KeyValuePair<object, DatedMessage<object>>>)null!;

//            // Act
//            var exception = await Record.ExceptionAsync(async () =>
//            await exporter.ExportAsync(data, topic, CancellationToken.None)
//                );

//            // Assert
//            exception.Should().NotBeNull().And.BeOfType<ArgumentNullException>();
//        }

//        [Fact(DisplayName = "JsonFileDataExporter can't export for null topic.")]
//        [Trait("Category", "Unit")]
//        public async Task JsonFileDataExporterCanExportNullTopic()
//        {
//            // Arrange
//            var logger = new Mock<ILogger<JsonFileDataExporter<object, OriginalKeyMarker, object, ExportedTopic>>>();
//            var fileSaver = new Mock<IFileSaver>();
//            var exporter = new JsonFileDataExporter<object, OriginalKeyMarker, object, ExportedTopic>(logger.Object, fileSaver.Object);
//            var topic = (ExportedTopic)null!;
//            var data = Enumerable.Empty<KeyValuePair<object, DatedMessage<object>>>();

//            // Act
//            var exception = await Record.ExceptionAsync(async () =>
//            await exporter.ExportAsync(data, topic, CancellationToken.None)
//                );

//            // Assert
//            exception.Should().NotBeNull().And.BeOfType<ArgumentNullException>();
//        }

//        [Theory(DisplayName = "JsonFileDataExporter can export data for any mode.")]
//        [Trait("Category", "Unit")]
//        [InlineData(true)]
//        [InlineData(false)]
//        public async Task JsonFileDataExporterCanExportData(bool isRawMessage)
//        {
//            // Arrange
//            var logger = new Mock<ILogger<JsonFileDataExporter<object, OriginalKeyMarker, object, ExportedTopic>>>();
//            var fileSaver = new Mock<IFileSaver>();
//            var exporter = new JsonFileDataExporter<object, OriginalKeyMarker, object, ExportedTopic>(logger.Object, fileSaver.Object);
//            var topic = new ExportedTopic("name", "filename", isRawMessage);
//            var data = new KeyValuePair<object, DatedMessage<object>>[]
//            {
//                new KeyValuePair<object, DatedMessage<object>>("test",new DatedMessage<object>("value", DateTime.UtcNow))
//            };
//            var jsonData = JsonConvert.SerializeObject(data, Formatting.Indented);
//            var token = CancellationToken.None;
//            // Act
//            var exception = await Record.ExceptionAsync(async () =>
//            await exporter.ExportAsync(data, topic, token)
//                );

//            // Assert
//            exception.Should().BeNull();
//            fileSaver.Verify(x => x.SaveAsync(topic.ExportName, jsonData, token), Times.Once);
//        }
//    }
//}
