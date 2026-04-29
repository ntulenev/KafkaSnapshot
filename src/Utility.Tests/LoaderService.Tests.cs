using FluentAssertions;

using KafkaSnapshot.Abstractions.Processing;

using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;

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
            var logger = new Mock<ILogger<LoaderService>>(MockBehavior.Strict).Object;

            // Act
            var exception = Record.Exception(() => new LoaderService(tool, lifetime, logger));

            // Assert
            exception.Should().BeNull();
        }

        [Fact(DisplayName = "LoaderService cant be created with null lifetime.")]
        [Trait("Category", "Unit")]
        public void LoaderServiceCantBeCreatedWithNullLifetime()
        {
            // Arrange
            var tool = new Mock<ILoaderTool>(MockBehavior.Strict).Object;
            var logger = new Mock<ILogger<LoaderService>>(MockBehavior.Strict).Object;

            // Act
            var exception = Record.Exception(() => new LoaderService(tool, null!, logger));

            // Assert
            exception.Should().NotBeNull().And.BeOfType<ArgumentNullException>();
        }

        [Fact(DisplayName = "LoaderService cant be created with null tool.")]
        [Trait("Category", "Unit")]
        public void LoaderServiceCantBeCreatedWithNullTool()
        {
            // Arrange
            var lifetime = new Mock<IHostApplicationLifetime>(MockBehavior.Strict).Object;
            var logger = new Mock<ILogger<LoaderService>>(MockBehavior.Strict).Object;

            // Act
            var exception = Record.Exception(() => new LoaderService(null!, lifetime, logger));

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
            var processStarted = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
            toolMock.Setup(x => x.ProcessAsync(It.Is<CancellationToken>(token => token.CanBeCanceled)))
                .Returns(() => Task.CompletedTask)
                .Callback(() =>
                {
                    processCount++;
                    processStarted.SetResult();
                });
            var lifetimeMock = new Mock<IHostApplicationLifetime>(MockBehavior.Strict);
            var stopCount = 0;
            var stopRequested = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
            lifetimeMock.Setup(x => x.StopApplication()).Callback(() =>
            {
                stopCount++;
                stopRequested.SetResult();
            });
            var lifetime = lifetimeMock.Object;
            var tool = toolMock.Object;
            var logger = new Mock<ILogger<LoaderService>>().Object;
            using var loader = new LoaderService(tool, lifetime, logger);

            // Act
            await loader.StartAsync(token.Token);
            await processStarted.Task.WaitAsync(TimeSpan.FromSeconds(1));
            await stopRequested.Task.WaitAsync(TimeSpan.FromSeconds(1));

            // Assert
            processCount.Should().Be(1);
            stopCount.Should().Be(1);
        }
    }
}
