using Xunit;

using FluentAssertions;

using KafkaSnapshot.Models.Names;

namespace KafkaSnapshot.Models.Tests
{
    public class FileNameTests
    {
        [Theory(DisplayName = "FileName constructor throws exception with emtpy or spaces in input")]
        [InlineData("")]
        [InlineData("    ")]
        public void ConstructorThrowsExceptionWithEmptyOrSpacesInInput(string input)
        {
            // Act
            var exception = Record.Exception(() => new FileName(input));

            // Assert
            exception.Should().NotBeNull().And.BeOfType<ArgumentException>();
        }

        [Fact(DisplayName = "FileName constructor throws exception with null input")]
        public void ConstructorThrowsExceptionWithNullInput()
        {
            // Act
            var exception = Record.Exception(() => new FileName(null!));

            // Assert
            exception.Should().NotBeNull().And.BeOfType<ArgumentNullException>();
        }

        [Fact(DisplayName = "FileName constructor returns expected file name and extension")]
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
}
