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
    }
}
