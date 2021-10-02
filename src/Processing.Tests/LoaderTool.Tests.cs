using System;
using System.Collections.Generic;
using System.Threading.Tasks;
using System.Threading;

using FluentAssertions;

using Microsoft.Extensions.Logging;

using Moq;

using Xunit;

using KafkaSnapshot.Abstractions.Processing;

namespace KafkaSnapshot.Processing.Tests
{
    public class LoaderToolTests
    {
        [Fact(DisplayName = "LoaderTool can't be created without logger.")]
        [Trait("Category", "Unit")]
        public void LoaderToolCantBeCreatedWithoutLogger()
        {
            // Arrange
            var logger = (ILogger<LoaderTool>)null!;
            var items = new[]
            {
                (new Mock<IProcessingUnit>()).Object
            };

            // Act
            var exception = Record.Exception(() => new LoaderTool(logger, items));

            // Assert
            exception.Should().NotBeNull().And.BeOfType<ArgumentNullException>();
        }

        [Fact(DisplayName = "LoaderTool can't be created without unit collection.")]
        [Trait("Category", "Unit")]
        public void LoaderToolCantBeCreatedWithoutNullCollection()
        {
            // Arrange
            var loggerMock = new Mock<ILogger<LoaderTool>>();
            var logger = loggerMock.Object;
            var items = (ICollection<IProcessingUnit>)null!;

            // Act
            var exception = Record.Exception(() => new LoaderTool(logger, items));

            // Assert
            exception.Should().NotBeNull().And.BeOfType<ArgumentNullException>();
        }

        [Fact(DisplayName = "LoaderTool can be created with valid params.")]
        [Trait("Category", "Unit")]
        public void LoaderToolCanBeCreatedWithValidParams()
        {
            // Arrange
            var loggerMock = new Mock<ILogger<LoaderTool>>();
            var logger = loggerMock.Object;
            var items = new[]
            {
                (new Mock<IProcessingUnit>()).Object
            };

            // Act
            var exception = Record.Exception(() => new LoaderTool(logger, items));

            // Assert
            exception.Should().BeNull();
        }

        [Fact(DisplayName = "LoaderTool can process units.")]
        [Trait("Category", "Unit")]
        public async Task LoaderToolProcessUnits()
        {
            // Arrange
            var loggerMock = new Mock<ILogger<LoaderTool>>();
            var logger = loggerMock.Object;
            var unit1 = new Mock<IProcessingUnit>();
            var unit2 = new Mock<IProcessingUnit>();
            var items = new[]
            {
                unit1.Object,unit2.Object
            };
            var tool = new LoaderTool(logger, items);

            // Act
            var exception = await Record.ExceptionAsync(async () => await tool.ProcessAsync(CancellationToken.None));

            // Assert
            exception.Should().BeNull();
            unit1.Verify(x => x.ProcessAsync(CancellationToken.None), Times.Once);
            unit2.Verify(x => x.ProcessAsync(CancellationToken.None), Times.Once);
        }

        [Fact(DisplayName = "LoaderTool can process units if error.")]
        [Trait("Category", "Unit")]
        public async Task LoaderToolProcessUnitsIfError()
        {
            // Arrange
            var loggerMock = new Mock<ILogger<LoaderTool>>();
            var logger = loggerMock.Object;
            var unit1 = new Mock<IProcessingUnit>();
            unit1.Setup(x => x.ProcessAsync(CancellationToken.None)).Throws(new Exception());
            var unit2 = new Mock<IProcessingUnit>();
            var items = new[]
            {
                unit1.Object,unit2.Object
            };
            var tool = new LoaderTool(logger, items);

            // Act
            var exception = await Record.ExceptionAsync(async () => await tool.ProcessAsync(CancellationToken.None));

            // Assert
            exception.Should().BeNull();
            unit1.Verify(x => x.ProcessAsync(CancellationToken.None), Times.Once);
            unit2.Verify(x => x.ProcessAsync(CancellationToken.None), Times.Once);
        }
    }
}
