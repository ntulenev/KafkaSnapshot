using FluentAssertions;

using KafkaSnapshot.Export.File.Common;

using Xunit;

namespace KafkaSnapshot.Export.Tests
{
    public class FileStreamProviderTests
    {
        [Fact(DisplayName = "FileStreamProvider could be created.")]
        [Trait("Category", "Unit")]
        public void FileStreamProviderCouldBeCreated()
        {
            // Act
            var exception = Record.Exception(() => _ = new FileStreamProvider());

            // Assert
            exception.Should().BeNull();
        }

        [Fact(DisplayName = "FileStreamProvider cant create stream for null name.")]
        [Trait("Category", "Unit")]
        public void CantCreateStreamWithNullFileName()
        {
            // Arrange
            var provider = new FileStreamProvider();
            var fileName = (string)null!;

            // Act
            var exception = Record.Exception(() => provider.CreateFileStream(fileName));

            // Assert
            exception.Should().NotBeNull().And.BeOfType<ArgumentNullException>();
        }

        [Fact(DisplayName = "FileStreamProvider cant create stream for empty name.")]
        [Trait("Category", "Unit")]
        public void CantCreateStreamWithEmptyFileName()
        {
            // Arrange
            var provider = new FileStreamProvider();
            var fileName = string.Empty;

            // Act
            var exception = Record.Exception(() => provider.CreateFileStream(fileName));

            // Assert
            exception.Should().NotBeNull().And.BeOfType<ArgumentException>();
        }

        [Fact(DisplayName = "FileStreamProvider cant create stream for spaces name.")]
        [Trait("Category", "Unit")]
        public void CantCreateStreamWithSpacesFileName()
        {
            // Arrange
            var provider = new FileStreamProvider();
            var fileName = "   ";

            // Act
            var exception = Record.Exception(() => provider.CreateFileStream(fileName));

            // Assert
            exception.Should().NotBeNull().And.BeOfType<ArgumentException>();
        }
    }
}
