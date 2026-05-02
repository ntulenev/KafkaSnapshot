using Xunit;

using FluentAssertions;

using KafkaSnapshot.Models.Names;

namespace KafkaSnapshot.Models.Tests;

public class FileNameTests
{
    [Theory(DisplayName = "FileName constructor throws exception with empty or whitespace input")]
    [InlineData("")]
    [InlineData("    ")]
    [Trait("Category", "Unit")]
    public void ConstructorThrowsExceptionWithEmptyOrSpacesInInput(string input)
    {
        // Act
        var exception = Record.Exception(() => new FileName(input));

        // Assert
        exception.Should().NotBeNull().And.BeOfType<ArgumentException>();
    }

    [Fact(DisplayName = "FileName constructor throws exception with null input")]
    [Trait("Category", "Unit")]
    public void ConstructorThrowsExceptionWithNullInput()
    {
        // Act
        var exception = Record.Exception(() => new FileName(null!));

        // Assert
        exception.Should().NotBeNull().And.BeOfType<ArgumentNullException>();
    }

    [Fact(DisplayName = "FileName constructor returns expected file name and extension")]
    [Trait("Category", "Unit")]
    public void ConstructorReturnsExpectedFileNameAndExtension()
    {
        // Arrange
        var fileName = "testFile.txt";
        var expectedFullName = fileName;
        var expectedExtension = ".txt";

        // Act
        var result = new FileName(fileName);

        // Assert
        result.FullName.Should().Be(expectedFullName);
        result.Extension.Should().Be(expectedExtension);
    }

    [Fact(DisplayName = "FileName constructor returns expected file and empty extension")]
    [Trait("Category", "Unit")]
    public void ConstructorReturnsExpectedFileNameAndEmptyExtension()
    {
        // Arrange
        var fileName = "testFile";
        var expectedFullName = fileName;
        var expectedExtension = "";

        // Act
        var result = new FileName(fileName);

        // Assert
        result.FullName.Should().Be(expectedFullName);
        result.Extension.Should().Be(expectedExtension);
    }
}
