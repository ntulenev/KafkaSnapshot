using System;
using System.Collections.Generic;

using FluentAssertions;

using Microsoft.Extensions.Logging;

using Moq;

using Xunit;

using KafkaSnapshot.Abstractions.Processing;

namespace KafkaSnapshot.Processing.Tests
{
    public class LoaderConcurrentToolTests
    {
        [Fact(DisplayName = "LoaderConcurrentTool can't be created without logger.")]
        [Trait("Category", "Unit")]
        public void LoaderConcurrentToolCantBeCreatedWithoutLogger()
        {
            // Arrange
            var logger = (ILogger<LoaderConcurrentTool>)null!;
            var items = new[]
            {
                (new Mock<IProcessingUnit>()).Object
            };

            // Act
            var exception = Record.Exception(() => new LoaderConcurrentTool(logger, items));

            // Assert
            exception.Should().NotBeNull().And.BeOfType<ArgumentNullException>();
        }

        [Fact(DisplayName = "LoaderConcurrentTool can't be created without unit collestion.")]
        [Trait("Category", "Unit")]
        public void LoaderConcurrentToolCantBeCreatedWithoutNullCollection()
        {
            // Arrange
            var loggerMock = new Mock<ILogger<LoaderConcurrentTool>>();
            var logger = loggerMock.Object;
            var items = (ICollection<IProcessingUnit>)null!;

            // Act
            var exception = Record.Exception(() => new LoaderConcurrentTool(logger, items));

            // Assert
            exception.Should().NotBeNull().And.BeOfType<ArgumentNullException>();
        }

        [Fact(DisplayName = "LoaderConcurrentTool can be created with valid params.")]
        [Trait("Category", "Unit")]
        public void LoaderConcurrentToolCanBeCreatedWithValidParams()
        {
            // Arrange
            var loggerMock = new Mock<ILogger<LoaderConcurrentTool>>();
            var logger = loggerMock.Object;
            var items = new[]
            {
                (new Mock<IProcessingUnit>()).Object
            };

            // Act
            var exception = Record.Exception(() => new LoaderConcurrentTool(logger, items));

            // Assert
            exception.Should().BeNull();
        }
    }
}
