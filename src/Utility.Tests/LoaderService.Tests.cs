using FluentAssertions;

using KafkaSnapshot.Abstractions.Processing;

using Microsoft.Extensions.Hosting;

using Moq;

using Xunit;

namespace KafkaSnapshot.Utility.Tests
{
    public class LoaderServiceTests
    {
        [Fact(DisplayName = "LoaderService can be created.")]
        [Trait("Category", "Unit")]
        public void LoaderServiceCanBeCreated()
        {
            // Arrange
            var tool = new Mock<ILoaderTool>(MockBehavior.Strict).Object;
            var lifetime = new Mock<IHostApplicationLifetime>(MockBehavior.Strict).Object;

            // Act
            var exception = Record.Exception(() => new LoaderService(tool, lifetime));

            // Assert
            exception.Should().BeNull();
        }

        [Fact(DisplayName = "LoaderService cant be created with null lifetime.")]
        [Trait("Category", "Unit")]
        public void LoaderServiceCantBeCreatedWithNullLifetime()
        {
            // Arrange
            var tool = new Mock<ILoaderTool>(MockBehavior.Strict).Object;

            // Act
            var exception = Record.Exception(() => new LoaderService(tool, null!));

            // Assert
            exception.Should().NotBeNull().And.BeOfType<ArgumentNullException>();
        }

        [Fact(DisplayName = "LoaderService cant be created with null tool.")]
        [Trait("Category", "Unit")]
        public void LoaderServiceCantBeCreatedWithNullTool()
        {
            // Arrange
            var lifetime = new Mock<IHostApplicationLifetime>(MockBehavior.Strict).Object;

            // Act
            var exception = Record.Exception(() => new LoaderService(null!, lifetime));

            // Assert
            exception.Should().NotBeNull().And.BeOfType<ArgumentNullException>();
        }

        [Fact(DisplayName = "LoaderService can be started.")]
        [Trait("Category", "Unit")]
        public async Task LoaderServiceCanBeStarted()
        {
            // Arrange
            using var token = new CancellationTokenSource();
            var toolMock = new Mock<ILoaderTool>(MockBehavior.Strict);
            var processCount = 0;
            toolMock.Setup(x => x.ProcessAsync(It.IsAny<CancellationToken>()))
                .Returns(() => Task.CompletedTask)
                .Callback(() => processCount++);
            var lifetimeMock = new Mock<IHostApplicationLifetime>(MockBehavior.Strict);
            var stopCount = 0;
            lifetimeMock.Setup(x => x.StopApplication()).Callback(() => stopCount++);
            var lifetime = lifetimeMock.Object;
            var tool = toolMock.Object;
            using var loader = new LoaderService(tool, lifetime);

            // Act
            await loader.StartAsync(token.Token);

            // Assert
            processCount.Should().Be(1);
            stopCount.Should().Be(1);
        }
    }
}
