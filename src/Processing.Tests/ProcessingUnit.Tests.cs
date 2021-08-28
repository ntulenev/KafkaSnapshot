using System;

using Microsoft.Extensions.Logging;

using Moq;

using Xunit;

using FluentAssertions;

using KafkaSnapshot.Abstractions.Export;
using KafkaSnapshot.Abstractions.Filters;
using KafkaSnapshot.Abstractions.Import;
using KafkaSnapshot.Models.Export;
using KafkaSnapshot.Models.Processing;

namespace KafkaSnapshot.Processing.Tests
{
    public class ProcessingUnitTests
    {
        [Fact(DisplayName = "ProcessingUnit can't be created without logger.")]
        [Trait("Category", "Unit")]
        public void ProcessingUnitCantBeCreatedWithoutLogger()
        {
            var markerMoq = new Mock<IKeyRepresentationMarker>();
            var marker = markerMoq.Object;

            // Arrange
            var logger = (ILogger<ProcessingUnit<object, IKeyRepresentationMarker, object>>)null!;
            var topic = new ProcessingTopic<object>
                                ("test", "test", true, Models.Filters.FilterType.None, Models.Filters.KeyType.String, null!);
            var loaderMock = new Mock<ISnapshotLoader<object, object>>();
            var loader = loaderMock.Object;
            var exporterMock = new Mock<IDataExporter<object, IKeyRepresentationMarker, object, ExportedTopic>>();
            var exporter = exporterMock.Object;
            var factoryMock = new Mock<IKeyFiltersFactory<object>>();
            var factory = factoryMock.Object;

            // Act
            var exception = Record.Exception(() =>
                new ProcessingUnit<object, IKeyRepresentationMarker, object>(logger, topic, loader, exporter, factory));

            // Assert
            exception.Should().NotBeNull().And.BeOfType<ArgumentNullException>();
        }

        [Fact(DisplayName = "ProcessingUnit can't be created without topic.")]
        [Trait("Category", "Unit")]
        public void ProcessingUnitCantBeCreatedWithoutTopic()
        {
            var markerMoq = new Mock<IKeyRepresentationMarker>();
            var marker = markerMoq.Object;

            // Arrange
            var loggerMock = new Mock<ILogger<ProcessingUnit<object, IKeyRepresentationMarker, object>>>();
            var logger = loggerMock.Object;
            var topic = (ProcessingTopic<object>)null!;
            var loaderMock = new Mock<ISnapshotLoader<object, object>>();
            var loader = loaderMock.Object;
            var exporterMock = new Mock<IDataExporter<object, IKeyRepresentationMarker, object, ExportedTopic>>();
            var exporter = exporterMock.Object;
            var factoryMock = new Mock<IKeyFiltersFactory<object>>();
            var factory = factoryMock.Object;

            // Act
            var exception = Record.Exception(() =>
                new ProcessingUnit<object, IKeyRepresentationMarker, object>(logger, topic, loader, exporter, factory));

            // Assert
            exception.Should().NotBeNull().And.BeOfType<ArgumentNullException>();
        }

        [Fact(DisplayName = "ProcessingUnit can't be created without loader.")]
        [Trait("Category", "Unit")]
        public void ProcessingUnitCantBeCreatedWithoutLoader()
        {
            var markerMoq = new Mock<IKeyRepresentationMarker>();
            var marker = markerMoq.Object;

            // Arrange
            var loggerMock = new Mock<ILogger<ProcessingUnit<object, IKeyRepresentationMarker, object>>>();
            var logger = loggerMock.Object;
            var topic = new ProcessingTopic<object>
                                ("test", "test", true, Models.Filters.FilterType.None, Models.Filters.KeyType.String, null!);
            var loader = (ISnapshotLoader<object, object>)null!;
            var exporterMock = new Mock<IDataExporter<object, IKeyRepresentationMarker, object, ExportedTopic>>();
            var exporter = exporterMock.Object;
            var factoryMock = new Mock<IKeyFiltersFactory<object>>();
            var factory = factoryMock.Object;

            // Act
            var exception = Record.Exception(() =>
                new ProcessingUnit<object, IKeyRepresentationMarker, object>(logger, topic, loader, exporter, factory));

            // Assert
            exception.Should().NotBeNull().And.BeOfType<ArgumentNullException>();
        }

        [Fact(DisplayName = "ProcessingUnit can't be created without exporter.")]
        [Trait("Category", "Unit")]
        public void ProcessingUnitCantBeCreatedWithoutExporter()
        {
            var markerMoq = new Mock<IKeyRepresentationMarker>();
            var marker = markerMoq.Object;

            // Arrange
            var loggerMock = new Mock<ILogger<ProcessingUnit<object, IKeyRepresentationMarker, object>>>();
            var logger = loggerMock.Object;
            var topic = new ProcessingTopic<object>
                                ("test", "test", true, Models.Filters.FilterType.None, Models.Filters.KeyType.String, null!);
            var loaderMock = new Mock<ISnapshotLoader<object, object>>();
            var loader = loaderMock.Object;
            var exporter = (IDataExporter<object, IKeyRepresentationMarker, object, ExportedTopic>)null!;
            var factoryMock = new Mock<IKeyFiltersFactory<object>>();
            var factory = factoryMock.Object;

            // Act
            var exception = Record.Exception(() =>
                new ProcessingUnit<object, IKeyRepresentationMarker, object>(logger, topic, loader, exporter, factory));

            // Assert
            exception.Should().NotBeNull().And.BeOfType<ArgumentNullException>();
        }

        [Fact(DisplayName = "ProcessingUnit can't be created without filters factory.")]
        [Trait("Category", "Unit")]
        public void ProcessingUnitCantBeCreatedWithoutFiltersFactory()
        {
            var markerMoq = new Mock<IKeyRepresentationMarker>();
            var marker = markerMoq.Object;

            // Arrange
            var loggerMock = new Mock<ILogger<ProcessingUnit<object, IKeyRepresentationMarker, object>>>();
            var logger = loggerMock.Object;
            var topic = new ProcessingTopic<object>
                                ("test", "test", true, Models.Filters.FilterType.None, Models.Filters.KeyType.String, null!);
            var loaderMock = new Mock<ISnapshotLoader<object, object>>();
            var loader = loaderMock.Object;
            var exporterMock = new Mock<IDataExporter<object, IKeyRepresentationMarker, object, ExportedTopic>>();
            var exporter = exporterMock.Object;
            var factory = (IKeyFiltersFactory<object>)null!;

            // Act
            var exception = Record.Exception(() =>
                new ProcessingUnit<object, IKeyRepresentationMarker, object>(logger, topic, loader, exporter, factory));

            // Assert
            exception.Should().NotBeNull().And.BeOfType<ArgumentNullException>();
        }

        [Fact(DisplayName = "ProcessingUnit could be created with valid params.")]
        [Trait("Category", "Unit")]
        public void ProcessingUnitCouldBeCreatedWithValidParams()
        {
            var markerMoq = new Mock<IKeyRepresentationMarker>();
            var marker = markerMoq.Object;

            // Arrange
            var loggerMock = new Mock<ILogger<ProcessingUnit<object, IKeyRepresentationMarker, object>>>();
            var logger = loggerMock.Object;
            var topic = new ProcessingTopic<object>
                                ("test", "test", true, Models.Filters.FilterType.None, Models.Filters.KeyType.String, null!);
            var loaderMock = new Mock<ISnapshotLoader<object, object>>();
            var loader = loaderMock.Object;
            var exporterMock = new Mock<IDataExporter<object, IKeyRepresentationMarker, object, ExportedTopic>>();
            var exporter = exporterMock.Object;
            var factoryMock = new Mock<IKeyFiltersFactory<object>>();
            var factory = factoryMock.Object;

            // Act
            var exception = Record.Exception(() =>
                new ProcessingUnit<object, IKeyRepresentationMarker, object>(logger, topic, loader, exporter, factory));

            // Assert
            exception.Should().BeNull();
        }
    }
}
