using FluentAssertions;
using KafkaSnapshot.Abstractions.Processing;
using Microsoft.Extensions.Logging;
using Moq;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Xunit;

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

        [Fact(DisplayName = "LoaderTool can't be created without unit collestion.")]
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
    }
}
