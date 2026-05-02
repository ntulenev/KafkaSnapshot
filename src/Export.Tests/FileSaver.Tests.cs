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

    [Fact(DisplayName = "File saver can't save file with null name.")]
    [Trait("Category", "Unit")]
    public async Task CannotSaveWithNullFileName()
    {
        // Arrange
        using var tokenSource = new CancellationTokenSource();
        var token = tokenSource.Token;
        var fileSaver = new FileSaver();
        var fileName = (FileName)null!;
        var content = "123";

        // Act
        var exception = await Record.ExceptionAsync(
            async () => await fileSaver.SaveAsync(fileName, content, token));

        // Assert
        exception.Should().NotBeNull().And.BeOfType<ArgumentNullException>();
    }

    [Fact(DisplayName = "File saver can't save file with null content.")]
    [Trait("Category", "Unit")]
    public async Task CannotSaveWithNullContent()
    {
        // Arrange
        using var tokenSource = new CancellationTokenSource();
        var token = tokenSource.Token;
        var fileSaver = new FileSaver();
        var fileName = new FileName("123");
        var content = (string)null!;

        // Act
        var exception = await Record.ExceptionAsync(
            async () => await fileSaver.SaveAsync(fileName, content, token));

        // Assert
        exception.Should().NotBeNull().And.BeOfType<ArgumentNullException>();
    }

    [Fact(DisplayName = "File saver can save file with valid data.")]
    [Trait("Category", "Unit")]
    public async Task CanSaveWithValidData()
    {
        // Arrange
        using var tokenSource = new CancellationTokenSource();
        var token = tokenSource.Token;
        var fileSaver = new FileSaver();
        var tempPath = Path.Combine(Path.GetTempPath(), $"{Guid.NewGuid():N}.txt");
        var fileName = new FileName(tempPath);
        var content = "test content";

        try
        {
            // Act
            await fileSaver.SaveAsync(fileName, content, token);

            // Assert
            System.IO.File.Exists(tempPath).Should().BeTrue();
            (await System.IO.File.ReadAllTextAsync(tempPath)).Should().Be(content);
        }
        finally
        {
            if (System.IO.File.Exists(tempPath))
            {
                System.IO.File.Delete(tempPath);
            }
        }
    }
}
