using FluentAssertions;

using Moq;

using Xunit;

using KafkaSnapshot.Import.Watermarks;

namespace KafkaAsTable.Tests
{
    public class TopicWatermarkTests
    {
        [Fact(DisplayName = "TopicWatermark can't be created with null partitions")]
        [Trait("Category", "Unit")]
        public void CantCreateTopicWatermarkWithInvalidParams()
        {

            // Arrange
            IEnumerable<PartitionWatermark> partitionWatermarks = null!;

            // Act
            var exception = Record.Exception(() => new TopicWatermark(partitionWatermarks));

            // Assert
            exception.Should().NotBeNull().And.BeOfType<ArgumentNullException>();
        }

        [Fact(DisplayName = "TopicWatermark could be created.")]
        [Trait("Category", "Unit")]
        public void CanCreateTopicWatermarkWithValidParams()
        {

            // Arrange
            var partitionWatermarks = (new Mock<IEnumerable<PartitionWatermark>>()).Object;

            // Act
            TopicWatermark result = null!;
            var exception = Record.Exception(() => result = new TopicWatermark(partitionWatermarks));

            // Assert
            exception.Should().BeNull();
            result.Watermarks.Should().BeEquivalentTo(partitionWatermarks);
        }
    }
}
