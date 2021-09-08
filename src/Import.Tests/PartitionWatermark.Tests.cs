using System;

using Confluent.Kafka;

using FluentAssertions;

using Moq;

using Xunit;

using KafkaSnapshot.Import.Watermarks;
using KafkaSnapshot.Models.Import;
using System.Collections.Generic;

namespace KafkaAsTable.Tests
{
    public class PartitionWatermarkTests
    {
        [Fact(DisplayName = "PartitionWatermark can't be created with null name.")]
        [Trait("Category", "Unit")]
        public void CantCreatePartitionWatermarkWithNullName()
        {

            // Arrange
            LoadingTopic topicName = null!;
            var offsets = new WatermarkOffsets(new Offset(1), new Offset(2));
            var partition = new Partition(1);

            // Act
            var exception = Record.Exception(() => new PartitionWatermark(topicName, offsets, partition));

            // Assert
            exception.Should().NotBeNull().And.BeOfType<ArgumentNullException>();
        }

        [Fact(DisplayName = "PartitionWatermark can't be created with null offets.")]
        [Trait("Category", "Unit")]
        public void CantCreatePartitionWatermarkWithNullWatermarkOffsets()
        {

            // Arrange
            var topicName = new LoadingTopic("Test", true);
            WatermarkOffsets offsets = null!;
            var partition = new Partition(1);

            // Act
            var exception = Record.Exception(() => new PartitionWatermark(topicName, offsets, partition));

            // Assert
            exception.Should().NotBeNull().And.BeOfType<ArgumentNullException>();
        }

        [Fact(DisplayName = "PartitionWatermark can be created with valid params.")]
        [Trait("Category", "Unit")]
        public void PartitionWatermarkCanBeCreated()
        {

            // Arrange
            var topicName = new LoadingTopic("Test", true);
            var offsets = new WatermarkOffsets(new Offset(1), new Offset(2));
            var partition = new Partition(1);

            PartitionWatermark result = null!;

            // Act
            var exception = Record.Exception(() => result = new PartitionWatermark(topicName, offsets, partition));

            // Assert
            exception.Should().BeNull();

            result.Partition.Should().Be(partition);
            result.Offset.Should().Be(offsets);
            result.TopicName.Should().Be(topicName);
        }

        [Theory(DisplayName = "PartitionWatermark is ready to read only valid offsets.")]
        [InlineData(0, 1, true)]
        [InlineData(1, 0, false)]
        [Trait("Category", "Unit")]
        public void IsReadyToReadReactsCorrectlyOnOffsetValues(int startOffset, int endOffset, bool condition)
        {

            // Arrange
            var topicName = new LoadingTopic("Test", true);
            var offsets = new WatermarkOffsets(new Offset(startOffset), new Offset(endOffset));
            var partition = new Partition(1);

            var pw = new PartitionWatermark(topicName, offsets, partition);
            var result = false;

            // Act
            var exception = Record.Exception(() => result = pw.IsReadyToRead());

            // Assert
            exception.Should().BeNull();

            result.Should().Be(condition);
        }

        [Fact(DisplayName = "IsWatermarkAchievedBy faild on null result.")]
        [Trait("Category", "Unit")]
        public void IsWatermarkAchievedByFailsOnNullResult()
        {

            // Arrange
            var topicName = new LoadingTopic("Test", true);
            var offsets = new WatermarkOffsets(new Offset(1), new Offset(2));
            var partition = new Partition(1);

            ConsumeResult<object, object> result = null!;

            var pw = new PartitionWatermark(topicName, offsets, partition);

            // Act
            var exception = Record.Exception(() => pw.IsWatermarkAchievedBy(result));

            // Assert
            exception.Should().NotBeNull().And.BeOfType<ArgumentNullException>();
        }

        [Fact(DisplayName = "IsWatermarkAchievedBy reacts on offset's end.")]
        [Trait("Category", "Unit")]
        public void IsWatermarkAchievedByReatcsOnRightOffset()
        {

            // Arrange
            var topicName = new LoadingTopic("Test", true);
            var offsets = new WatermarkOffsets(new Offset(1), new Offset(2));
            var partition = new Partition(1);

            var result = new ConsumeResult<object, object>()
            {
                Offset = offsets.High
            };

            var pw = new PartitionWatermark(topicName, offsets, partition);
            var status = false;

            // Act
            var exception = Record.Exception(() => status = pw.IsWatermarkAchievedBy(result));

            // Assert
            exception.Should().BeNull();
            status.Should().BeTrue();
        }

        [Fact(DisplayName = "IsWatermarkAchievedBy reacts if offset is not reached.")]
        [Trait("Category", "Unit")]
        public void IsWatermarkAchievedByReatcsOnNotRightOffset()
        {

            // Arrange
            var topicName = new LoadingTopic("Test", true);
            var offsets = new WatermarkOffsets(new Offset(1), new Offset(2));
            var partition = new Partition(1);

            var result = new ConsumeResult<object, object>()
            {
                Offset = offsets.Low
            };

            var pw = new PartitionWatermark(topicName, offsets, partition);
            var status = false;

            // Act
            var exception = Record.Exception(() => status = pw.IsWatermarkAchievedBy(result));

            // Assert
            exception.Should().BeNull();
            status.Should().BeFalse();
        }

        [Fact(DisplayName = "AssingWithConsumer fails on null consumer.")]
        [Trait("Category", "Unit")]
        public void AssingWithConsumerFailsOnNullConsumer()
        {

            // Arrange
            var topicName = new LoadingTopic("Test", true);
            var offsets = new WatermarkOffsets(new Offset(1), new Offset(2));
            var partition = new Partition(1);
            var pw = new PartitionWatermark(topicName, offsets, partition);
            IConsumer<object, object> consumer = null!;

            // Act
            var exception = Record.Exception(() => pw.AssingWithConsumer(consumer));

            // Assert
            exception.Should().NotBeNull().And.BeOfType<ArgumentNullException>();
        }

        [Fact(DisplayName = "PartitionWatermark could be assing to consumer.")]
        [Trait("Category", "Unit")]
        public void PartitionWatermarkCouldBeAssingToConsumer()
        {

            // Arrange
            var topicName = new LoadingTopic("Test", true);
            var offsets = new WatermarkOffsets(new Offset(1), new Offset(2));
            var partition = new Partition(1);
            var pw = new PartitionWatermark(topicName, offsets, partition);

            var consumerMock = new Mock<IConsumer<object, object>>();
            var consumer = consumerMock.Object;

            // Act
            var exception = Record.Exception(() => pw.AssingWithConsumer(consumer));

            // Assert
            exception.Should().BeNull();
            consumerMock.Verify(x => x.Assign(It.Is<TopicPartition>(a => a.Topic == topicName.Value && a.Partition == partition)), Times.Once);
        }


        [Fact(DisplayName = "PartitionWatermark could be assing to consumer with date.")]
        [Trait("Category", "Unit")]
        public void PartitionWatermarkCouldBeAssingToConsumerWithDate()
        {

            // Arrange
            var date = DateTime.UtcNow;
            var timeout = TimeSpan.FromSeconds(10);
            var topicName = new LoadingTopic("Test", true, date);
            var offsets = new WatermarkOffsets(new Offset(1), new Offset(2));
            var partition = new Partition(1);
            var pw = new PartitionWatermark(topicName, offsets, partition);
            var consumerMock = new Mock<IConsumer<object, object>>();
            var topicWithOffset = new TopicPartitionOffset(new TopicPartition(topicName.Value, new Partition()), new Offset(1));
            consumerMock.Setup(x => x.OffsetsForTimes(It.IsAny<IEnumerable<TopicPartitionTimestamp>>(), timeout)).Returns(new List<TopicPartitionOffset>
            {
               topicWithOffset
            });
            var consumer = consumerMock.Object;
            bool result = false;
            // Act
            var exception = Record.Exception(() => result = pw.AssingWithConsumer(consumer, date, timeout));

            // Assert
            exception.Should().BeNull();
            result.Should().BeTrue();
            consumerMock.Verify(x => x.Assign(It.Is<TopicPartitionOffset>(a => a.Topic == topicName.Value && a.Partition == topicWithOffset.Partition)), Times.Once);
        }

        [Fact(DisplayName = "PartitionWatermark could be assing to consumer with date but too big.")]
        [Trait("Category", "Unit")]
        public void PartitionWatermarkCouldBeAssingToConsumerWithDateTooBig()
        {

            // Arrange
            var date = DateTime.UtcNow;
            var timeout = TimeSpan.FromSeconds(10);
            var topicName = new LoadingTopic("Test", true, date);
            var offsets = new WatermarkOffsets(new Offset(1), new Offset(2));
            var partition = new Partition(1);
            var pw = new PartitionWatermark(topicName, offsets, partition);
            var consumerMock = new Mock<IConsumer<object, object>>();
            var topicWithOffset = new TopicPartitionOffset(new TopicPartition(topicName.Value, new Partition()), new Offset(Offset.End));
            consumerMock.Setup(x => x.OffsetsForTimes(It.IsAny<IEnumerable<TopicPartitionTimestamp>>(), timeout)).Returns(new List<TopicPartitionOffset>
            {
               topicWithOffset
            });
            var consumer = consumerMock.Object;
            bool result = true;
            // Act
            var exception = Record.Exception(() => result = pw.AssingWithConsumer(consumer, date, timeout));

            // Assert
            exception.Should().BeNull();
            result.Should().BeFalse();
            consumerMock.Verify(x => x.Assign(It.Is<TopicPartitionOffset>(a => a.Topic == topicName.Value && a.Partition == topicWithOffset.Partition)), Times.Never);
        }
    }
}
