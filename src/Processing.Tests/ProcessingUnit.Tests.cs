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
using KafkaSnapshot.Models.Filters;
using KafkaSnapshot.Models.Names;

namespace KafkaSnapshot.Processing.Tests;

public class ProcessingUnitTests
{
    [Fact(DisplayName = "ProcessingUnit can't be created without logger.")]
    [Trait("Category", "Unit")]
    public void ProcessingUnitCantBeCreatedWithoutLogger()
    {

        // Arrange
        var markerMoq = new Mock<IKeyRepresentationMarker>();
        var marker = markerMoq.Object;
        var logger = (ILogger<ProcessingUnit<object, IKeyRepresentationMarker, object>>)null!;
        var topic = new ProcessingTopic<object>
                            (new TopicName("test"), new FileName("test"), true, Models.Filters.FilterType.None, Models.Filters.KeyType.String, null!,
                            new DateFilterRange(null!, null!), false, EncoderRules.String);
        var loaderMock = new Mock<ISnapshotLoader<object, object>>(MockBehavior.Strict);
        var loader = loaderMock.Object;
        var exporterMock = new Mock<IDataExporter<object, IKeyRepresentationMarker, object, ExportedTopic>>(MockBehavior.Strict);
        var exporter = exporterMock.Object;
        var factoryMock = new Mock<IKeyFiltersFactory<object>>(MockBehavior.Strict);
        var factory = factoryMock.Object;
        var valueFactoryMock = new Mock<IValueFilterFactory<object>>(MockBehavior.Strict);
        var valueFactory = valueFactoryMock.Object;

        // Act
        var exception = Record.Exception(() =>
            new ProcessingUnit<object, IKeyRepresentationMarker, object>(logger, topic, loader, exporter, factory, valueFactory));

        // Assert
        exception.Should().NotBeNull().And.BeOfType<ArgumentNullException>();
    }

    [Fact(DisplayName = "ProcessingUnit can't be created without topic.")]
    [Trait("Category", "Unit")]
    public void ProcessingUnitCantBeCreatedWithoutTopic()
    {

        // Arrange
        var markerMoq = new Mock<IKeyRepresentationMarker>(MockBehavior.Strict);
        var marker = markerMoq.Object;
        var loggerMock = new Mock<ILogger<ProcessingUnit<object, IKeyRepresentationMarker, object>>>();
        var logger = loggerMock.Object;
        var topic = (ProcessingTopic<object>)null!;
        var loaderMock = new Mock<ISnapshotLoader<object, object>>(MockBehavior.Strict);
        var loader = loaderMock.Object;
        var exporterMock = new Mock<IDataExporter<object, IKeyRepresentationMarker, object, ExportedTopic>>(MockBehavior.Strict);
        var exporter = exporterMock.Object;
        var factoryMock = new Mock<IKeyFiltersFactory<object>>(MockBehavior.Strict);
        var factory = factoryMock.Object;
        var valueFactoryMock = new Mock<IValueFilterFactory<object>>(MockBehavior.Strict);
        var valueFactory = valueFactoryMock.Object;

        // Act
        var exception = Record.Exception(() =>
            new ProcessingUnit<object, IKeyRepresentationMarker, object>(logger, topic, loader, exporter, factory, valueFactory));

        // Assert
        exception.Should().NotBeNull().And.BeOfType<ArgumentNullException>();
    }

    [Fact(DisplayName = "ProcessingUnit can't be created without loader.")]
    [Trait("Category", "Unit")]
    public void ProcessingUnitCantBeCreatedWithoutLoader()
    {

        // Arrange
        var markerMoq = new Mock<IKeyRepresentationMarker>(MockBehavior.Strict);
        var marker = markerMoq.Object;
        var loggerMock = new Mock<ILogger<ProcessingUnit<object, IKeyRepresentationMarker, object>>>();
        var logger = loggerMock.Object;
        var topic = new ProcessingTopic<object>
                            (new TopicName("test"), new FileName("test"), true, Models.Filters.FilterType.None,
                            Models.Filters.KeyType.String, null!, new DateFilterRange(null!, null!), false, EncoderRules.String);
        var loader = (ISnapshotLoader<object, object>)null!;
        var exporterMock = new Mock<IDataExporter<object, IKeyRepresentationMarker, object, ExportedTopic>>(MockBehavior.Strict);
        var exporter = exporterMock.Object;
        var factoryMock = new Mock<IKeyFiltersFactory<object>>(MockBehavior.Strict);
        var factory = factoryMock.Object;
        var valueFactoryMock = new Mock<IValueFilterFactory<object>>(MockBehavior.Strict);
        var valueFactory = valueFactoryMock.Object;

        // Act
        var exception = Record.Exception(() =>
            new ProcessingUnit<object, IKeyRepresentationMarker, object>(logger, topic, loader, exporter, factory, valueFactory));

        // Assert
        exception.Should().NotBeNull().And.BeOfType<ArgumentNullException>();
    }

    [Fact(DisplayName = "ProcessingUnit can't be created without exporter.")]
    [Trait("Category", "Unit")]
    public void ProcessingUnitCantBeCreatedWithoutExporter()
    {

        // Arrange
        var markerMoq = new Mock<IKeyRepresentationMarker>(MockBehavior.Strict);
        var marker = markerMoq.Object;
        var loggerMock = new Mock<ILogger<ProcessingUnit<object, IKeyRepresentationMarker, object>>>();
        var logger = loggerMock.Object;
        var topic = new ProcessingTopic<object>
                            (new TopicName("test"), new FileName("test"), true, Models.Filters.FilterType.None, Models.Filters.KeyType.String, null!,
                            new DateFilterRange(null!, null!), false, EncoderRules.String);
        var loaderMock = new Mock<ISnapshotLoader<object, object>>(MockBehavior.Strict);
        var loader = loaderMock.Object;
        var exporter = (IDataExporter<object, IKeyRepresentationMarker, object, ExportedTopic>)null!;
        var factoryMock = new Mock<IKeyFiltersFactory<object>>(MockBehavior.Strict);
        var factory = factoryMock.Object;
        var valueFactoryMock = new Mock<IValueFilterFactory<object>>();
        var valueFactory = valueFactoryMock.Object;

        // Act
        var exception = Record.Exception(() =>
            new ProcessingUnit<object, IKeyRepresentationMarker, object>(logger, topic, loader, exporter, factory, valueFactory));

        // Assert
        exception.Should().NotBeNull().And.BeOfType<ArgumentNullException>();
    }

    [Fact(DisplayName = "ProcessingUnit can't be created without filters factory.")]
    [Trait("Category", "Unit")]
    public void ProcessingUnitCantBeCreatedWithoutFiltersFactory()
    {

        // Arrange
        var markerMoq = new Mock<IKeyRepresentationMarker>(MockBehavior.Strict);
        var marker = markerMoq.Object;
        var loggerMock = new Mock<ILogger<ProcessingUnit<object, IKeyRepresentationMarker, object>>>();
        var logger = loggerMock.Object;
        var topic = new ProcessingTopic<object>
                            (new TopicName("test"), new FileName("test"), true, Models.Filters.FilterType.None, Models.Filters.KeyType.String, null!,
                            new DateFilterRange(null!, null!), false, EncoderRules.String);
        var loaderMock = new Mock<ISnapshotLoader<object, object>>(MockBehavior.Strict);
        var loader = loaderMock.Object;
        var exporterMock = new Mock<IDataExporter<object, IKeyRepresentationMarker, object, ExportedTopic>>(MockBehavior.Strict);
        var exporter = exporterMock.Object;
        var factory = (IKeyFiltersFactory<object>)null!;
        var valueFactoryMock = new Mock<IValueFilterFactory<object>>(MockBehavior.Strict);
        var valueFactory = valueFactoryMock.Object;

        // Act
        var exception = Record.Exception(() =>
            new ProcessingUnit<object, IKeyRepresentationMarker, object>(logger, topic, loader, exporter, factory, valueFactory));

        // Assert
        exception.Should().NotBeNull().And.BeOfType<ArgumentNullException>();
    }

    [Fact(DisplayName = "ProcessingUnit can't be created without value filters factory.")]
    [Trait("Category", "Unit")]
    public void ProcessingUnitCantBeCreatedWithoutValueFiltersFactory()
    {

        // Arrange
        var markerMoq = new Mock<IKeyRepresentationMarker>(MockBehavior.Strict);
        var marker = markerMoq.Object;
        var loggerMock = new Mock<ILogger<ProcessingUnit<object, IKeyRepresentationMarker, object>>>();
        var logger = loggerMock.Object;
        var topic = new ProcessingTopic<object>
                            (new TopicName("test"), new FileName("test"), true, Models.Filters.FilterType.None, Models.Filters.KeyType.String, null!,
                            new DateFilterRange(null!, null!), false, EncoderRules.String);
        var loaderMock = new Mock<ISnapshotLoader<object, object>>(MockBehavior.Strict);
        var loader = loaderMock.Object;
        var exporterMock = new Mock<IDataExporter<object, IKeyRepresentationMarker, object, ExportedTopic>>(MockBehavior.Strict);
        var exporter = exporterMock.Object;
        var factoryMock = new Mock<IKeyFiltersFactory<object>>(MockBehavior.Strict);
        var factory = factoryMock.Object;
        var valueFactory = (IValueFilterFactory<object>)null!;

        // Act
        var exception = Record.Exception(() =>
            new ProcessingUnit<object, IKeyRepresentationMarker, object>(logger, topic, loader, exporter, factory, valueFactory));

        // Assert
        exception.Should().NotBeNull().And.BeOfType<ArgumentNullException>();
    }

    [Fact(DisplayName = "ProcessingUnit could be created with valid params.")]
    [Trait("Category", "Unit")]
    public void ProcessingUnitCouldBeCreatedWithValidParams()
    {
        // Arrange
        var factoryMock = new Mock<IKeyFiltersFactory<object>>(MockBehavior.Strict);
        var keyFilterMock = new Mock<IDataFilter<object>>(MockBehavior.Strict);
        var keyFilter = keyFilterMock.Object;
        var valueFactoryMock = new Mock<IValueFilterFactory<object>>(MockBehavior.Strict);
        var valueFilterMock = new Mock<IDataFilter<object>>(MockBehavior.Strict);
        var valueFilter = valueFilterMock.Object;
        var markerMoq = new Mock<IKeyRepresentationMarker>(MockBehavior.Strict);
        var marker = markerMoq.Object;
        var loggerMock = new Mock<ILogger<ProcessingUnit<object, IKeyRepresentationMarker, object>>>();
        var logger = loggerMock.Object;
        var topic = new ProcessingTopic<object>
                            (new TopicName("test"), new FileName("test"), true, Models.Filters.FilterType.None, Models.Filters.KeyType.String,
                            null!, new DateFilterRange(null!, null!), false, EncoderRules.String);
        var loaderMock = new Mock<ISnapshotLoader<object, object>>(MockBehavior.Strict);
        var loader = loaderMock.Object;
        var exporterMock = new Mock<IDataExporter<object, IKeyRepresentationMarker, object, ExportedTopic>>(MockBehavior.Strict);
        var exporter = exporterMock.Object;
        factoryMock.Setup(x => x.Create(topic.FilterKeyType, topic.KeyType, topic.FilterKeyValue)).Returns(keyFilter);
        var factory = factoryMock.Object;
        valueFactoryMock.Setup(x => x.Create(Models.Filters.FilterType.None, ValueMessageType.Raw, default!)).Returns(valueFilter);
        var valueFactory = valueFactoryMock.Object;

        // Act
        var exception = Record.Exception(() =>
            new ProcessingUnit<object, IKeyRepresentationMarker, object>(logger, topic, loader, exporter, factory, valueFactory));

        // Assert
        exception.Should().BeNull();
    }

    [Fact(DisplayName = "ProcessingUnit can process topic.")]
    [Trait("Category", "Unit")]
    public async Task ProcessingUnitCanProcessTopic()
    {

        // Arrange
        using var cts = new CancellationTokenSource();
        var markerMoq = new Mock<IKeyRepresentationMarker>(MockBehavior.Strict);
        var marker = markerMoq.Object;
        var loggerMock = new Mock<ILogger<ProcessingUnit<object, IKeyRepresentationMarker, object>>>();
        var logger = loggerMock.Object;
        var valueObj = "test value";
        var topic = new ProcessingTopic<object>
                            (new TopicName("test"), new FileName("exportTest"), true, Models.Filters.FilterType.None, Models.Filters.KeyType.String,
                            valueObj, new DateFilterRange(null!, null!), false, EncoderRules.String);
        var loaderMock = new Mock<ISnapshotLoader<object, object>>(MockBehavior.Strict);
        var factoryMock = new Mock<IKeyFiltersFactory<object>>(MockBehavior.Strict);
        var keyFilterMock = new Mock<IDataFilter<object>>(MockBehavior.Strict);
        var keyFilter = keyFilterMock.Object;
        var valueFactoryMock = new Mock<IValueFilterFactory<object>>(MockBehavior.Strict);
        var valueFilterMock = new Mock<IDataFilter<object>>(MockBehavior.Strict);
        var valueFilter = valueFilterMock.Object;
        var snapshotMock = new Mock<IEnumerable<KeyValuePair<object, KafkaMessage<object>>>>(MockBehavior.Strict);
        var snapshot = snapshotMock.Object;
        factoryMock.Setup(x => x.Create(topic.FilterKeyType, topic.KeyType, topic.FilterKeyValue)).Returns(keyFilter);
        valueFactoryMock.Setup(x => x.Create(Models.Filters.FilterType.None, ValueMessageType.Raw, default!)).Returns(valueFilter);
        var valueFactory = valueFactoryMock.Object;
        loaderMock.Setup(x => x.LoadSnapshotAsync(
                    It.Is<LoadingTopic>(n => n.Value.Name == topic.TopicName.Name),
                    keyFilter, valueFilter, cts.Token)).Returns(Task.FromResult(snapshot));
        var factory = factoryMock.Object;
        var loader = loaderMock.Object;
        var exporterMock = new Mock<IDataExporter<object, IKeyRepresentationMarker, object, ExportedTopic>>(MockBehavior.Strict);
        exporterMock.Setup(x => x.ExportAsync(snapshot,
                                It.Is<ExportedTopic>(e => e.TopicName.Name == topic.TopicName.Name && e.ExportName == topic.ExportName),
                                cts.Token)).Returns(() => Task.CompletedTask);
        var exporter = exporterMock.Object;
        var unit = new ProcessingUnit<object, IKeyRepresentationMarker, object>(logger, topic, loader, exporter, factory, valueFactory);

        // Act
        await unit.ProcessAsync(cts.Token).ConfigureAwait(false);

        // Assert
        exporterMock.Verify
            (x => x.ExportAsync(snapshot,
                                It.Is<ExportedTopic>(e => e.TopicName.Name == topic.TopicName.Name && e.ExportName == topic.ExportName),
                                cts.Token), Times.Once);

    }
}
