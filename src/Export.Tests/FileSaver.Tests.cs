using FluentAssertions;

using Xunit;

using KafkaSnapshot.Export.File.Common;
using KafkaSnapshot.Models.Names;

namespace KafkaSnapshot.Export.Tests;

public class FileSaverTests
{
    [Fact(DisplayName = "File saver could be created.")]
    [Trait("Category", "Unit")]
    public void FileSaverCouldBeCreated()
    {
        // Act
        var exception = Record.Exception(() => _ = new FileSaver());

        // Assert
        exception.Should().BeNull();
    }

    [Fact(DisplayName = "File saver cant save file with null name.")]
    [Trait("Category", "Unit")]
    public async Task CantSaveWithNullFileName()
    {
        // Arrange
        var fileSaver = new FileSaver();
        var fileName = (FileName)null!;
        var content = "123";

        // Act
        var exception = await Record.ExceptionAsync(
            async () => await fileSaver.SaveAsync(fileName, content, CancellationToken.None));

        // Assert
        exception.Should().NotBeNull().And.BeOfType<ArgumentNullException>();
    }

    [Fact(DisplayName = "File saver cant save file with null content.")]
    [Trait("Category", "Unit")]
    public async Task CantSaveWithNullContent()
    {
        // Arrange
        var fileSaver = new FileSaver();
        var fileName = new FileName("123");
        var content = (string)null!;

        // Act
        var exception = await Record.ExceptionAsync(
            async () => await fileSaver.SaveAsync(fileName, content, CancellationToken.None));

        // Assert
        exception.Should().NotBeNull().And.BeOfType<ArgumentNullException>();
    }
}
