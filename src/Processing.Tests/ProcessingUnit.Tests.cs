using Microsoft.Extensions.Logging;

using Moq;

using Xunit;

using FluentAssertions;

using KafkaSnapshot.Abstractions.Export;
using KafkaSnapshot.Abstractions.Filters;
using KafkaSnapshot.Abstractions.Import;
using KafkaSnapshot.Models.Export;
using KafkaSnapshot.Models.Processing;
using KafkaSnapshot.Models.Import;
using KafkaSnapshot.Models.Message;

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
                                ("test", "test", true, Models.Filters.FilterType.None, Models.Filters.KeyType.String, null!, null!, null!, false);
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
                                ("test", "test", true, Models.Filters.FilterType.None, Models.Filters.KeyType.String, null!, null!, null!, false);
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
                                ("test", "test", true, Models.Filters.FilterType.None, Models.Filters.KeyType.String, null!, null!, null!, false);
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
                                ("test", "test", true, Models.Filters.FilterType.None, Models.Filters.KeyType.String, null!, null!, null!, false);
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
                                ("test", "test", true, Models.Filters.FilterType.None, Models.Filters.KeyType.String, null!, null!, null!, false);
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

        [Fact(DisplayName = "ProcessingUnit can process topuc.")]
        [Trait("Category", "Unit")]
        public async Task ProcessingUnitCanProcessTopic()
        {
            var markerMoq = new Mock<IKeyRepresentationMarker>();
            var marker = markerMoq.Object;

            // Arrange
            var loggerMock = new Mock<ILogger<ProcessingUnit<object, IKeyRepresentationMarker, object>>>();
            var logger = loggerMock.Object;
            var valueObj = "test value";
            var topic = new ProcessingTopic<object>
                                ("test", "exportTest", true, Models.Filters.FilterType.None, Models.Filters.KeyType.String, valueObj, null!, null!, false);
            var loaderMock = new Mock<ISnapshotLoader<object, object>>();
            var exporterMock = new Mock<IDataExporter<object, IKeyRepresentationMarker, object, ExportedTopic>>();
            var exporter = exporterMock.Object;
            var factoryMock = new Mock<IKeyFiltersFactory<object>>();
            var filterMock = new Mock<IKeyFilter<object>>();
            var filter = filterMock.Object;
            var snapshotMock = new Mock<IEnumerable<KeyValuePair<object, MetaMessage<object>>>>();
            var snapshot = snapshotMock.Object;
            factoryMock.Setup(x => x.Create(topic.FilterType, topic.KeyType, topic.FilterValue)).Returns(filter);
            loaderMock.Setup(x => x.LoadCompactSnapshotAsync(
                        It.Is<LoadingTopic>(n => n.Value == topic.Name),
                        filter, CancellationToken.None)).Returns(Task.FromResult(snapshot));
            var factory = factoryMock.Object;
            var loader = loaderMock.Object;
            var unit = new ProcessingUnit<object, IKeyRepresentationMarker, object>(logger, topic, loader, exporter, factory);

            // Act
            var exception = await Record.ExceptionAsync(async () =>
                await unit.ProcessAsync(CancellationToken.None)).ConfigureAwait(false);

            // Assert
            exception.Should().BeNull();
            exporterMock.Verify
                (x => x.ExportAsync(snapshot,
                                    It.Is<ExportedTopic>(e => e.Name == topic.Name && e.ExportName == topic.ExportName),
                                    CancellationToken.None), Times.Once);

        }
    }
}
