using FluentAssertions;

using KafkaSnapshot.Export.File.Common;
using KafkaSnapshot.Export.Configuration;
using Microsoft.Extensions.Options;
using KafkaSnapshot.Models.Names;

using Xunit;

namespace KafkaSnapshot.Export.Tests
{
    public class ExportFileStorageStreamTests
    {
        [Fact(DisplayName = "ExportFileStorage can be created.")]
        [Trait("Category", "Unit")]
        public void StorageCanBeCreated()
        {
            // Act
            var exception = Record.Exception(() => _ = new ExportFileStorage(Options.Create(new JsonFileDataExporterConfiguration())));

            // Assert
            exception.Should().BeNull();
        }

        [Fact(DisplayName = "ExportFileStorage can't create stream for null name.")]
        [Trait("Category", "Unit")]
        public void CannotCreateStreamWithNullFileName()
        {
            // Arrange
            var provider = new ExportFileStorage(Options.Create(new JsonFileDataExporterConfiguration()));
            var fileName = (FileName)null!;

            // Act
            var exception = Record.Exception(() => provider.CreateFileStream(fileName));

            // Assert
            exception.Should().NotBeNull().And.BeOfType<ArgumentNullException>();
        }

        [Fact(DisplayName = "ExportFileStorage can create stream with valid file name.")]
        [Trait("Category", "Unit")]
        public void CanCreateStreamWithValidFileName()
        {
            // Arrange
            var provider = new ExportFileStorage(Options.Create(new JsonFileDataExporterConfiguration()));
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
