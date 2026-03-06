using FluentAssertions;

using KafkaSnapshot.Export.File.Common;
using KafkaSnapshot.Models.Names;

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
            var fileName = (FileName)null!;

            // Act
            var exception = Record.Exception(() => provider.CreateFileStream(fileName));

            // Assert
            exception.Should().NotBeNull().And.BeOfType<ArgumentNullException>();
        }

        [Fact(DisplayName = "FileStreamProvider can create stream with valid file name.")]
        [Trait("Category", "Unit")]
        public void CanCreateStreamWithValidFileName()
        {
            // Arrange
            var provider = new FileStreamProvider();
            var tempPath = Path.Combine(Path.GetTempPath(), $"{Guid.NewGuid():N}.tmp");
            var fileName = new FileName(tempPath);

            try
            {
                // Act
                using var stream = provider.CreateFileStream(fileName);

                // Assert
                stream.Should().NotBeNull();
                stream.CanWrite.Should().BeTrue();
                System.IO.File.Exists(tempPath).Should().BeTrue();
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
}
