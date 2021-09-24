using Microsoft.Extensions.DependencyInjection;

using Xunit;

using FluentAssertions;

using KafkaSnapshot.Abstractions.Processing;
using KafkaSnapshot.Utility;

namespace Utility.Tests
{
    public class ProgramTests
    {
        [Fact(DisplayName = "Utility can be created.")]
        [Trait("Category", "Integration")]
        public void UtilityCanBeCreated()
        {
            // Act
            var exception = Record.Exception(() =>
            {
                using var serviceScope = HostBuildHelper.CreateHost().Services.CreateScope();
                var services = serviceScope.ServiceProvider;
                _ = services.GetRequiredService<ILoaderTool>();
            });

            // Assert
            exception.Should().BeNull();
        }
    }
}
