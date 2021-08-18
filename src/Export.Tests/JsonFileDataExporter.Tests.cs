using System;

using Microsoft.Extensions.Logging;

using Moq;

using Xunit;

using FluentAssertions;

using KafkaSnapshot.Abstractions.Export;
using KafkaSnapshot.Export.File.Json;
using KafkaSnapshot.Export.Markers;
using KafkaSnapshot.Models.Export;

namespace KafkaSnapshot.Export.Tests
{
    public class JsonFileDataExporterTests
    {
        [Fact(DisplayName = "JsonFileDataExporter can't be created without logger.")]
        [Trait("Category", "Unit")]
        public void JsonFileDataExporterCantBeCreatedWithoutLogger()
        {
            // Arrange
            var logger = (ILogger<JsonFileDataExporter<object, OriginalKeyMarker, object, ExportedTopic>>)null!;
            var fileSaver = new Mock<IFileSaver>();

            // Act
            var exception = Record.Exception(() =>
            _ = new JsonFileDataExporter<object, OriginalKeyMarker, object, ExportedTopic>(logger, fileSaver.Object));

            // Assert
            exception.Should().NotBeNull().And.BeOfType<ArgumentNullException>();
        }

        [Fact(DisplayName = "JsonFileDataExporter can't be created without fileSaver.")]
        [Trait("Category", "Unit")]
        public void JsonFileDataExporterCantBeCreatedWithoutSaver()
        {
            // Arrange
            var logger = new Mock<ILogger<JsonFileDataExporter<object, OriginalKeyMarker, object, ExportedTopic>>>();
            var fileSaver = (IFileSaver)null!;

            // Act
            var exception = Record.Exception(() =>
            _ = new JsonFileDataExporter<object, OriginalKeyMarker, object, ExportedTopic>(logger.Object, fileSaver));

            // Assert
            exception.Should().NotBeNull().And.BeOfType<ArgumentNullException>();
        }

        [Fact(DisplayName = "JsonFileDataExporter could be creates with valid params.")]
        [Trait("Category", "Unit")]
        public void JsonFileDataExporterCanBeCreated()
        {
            // Arrange
            var logger = new Mock<ILogger<JsonFileDataExporter<object, OriginalKeyMarker, object, ExportedTopic>>>();
            var fileSaver = new Mock<IFileSaver>();

            // Act
            var exception = Record.Exception(() =>
            _ = new JsonFileDataExporter<object, OriginalKeyMarker, object, ExportedTopic>(logger.Object, fileSaver.Object));

            // Assert
            exception.Should().BeNull();
        }
    }
}
