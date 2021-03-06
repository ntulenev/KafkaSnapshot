using Xunit;

using FluentAssertions;

using KafkaSnapshot.Models.Import;

namespace KafkaSnapshot.Models.Tests
{
    public class LoadingTopicTests
    {
        [Theory(DisplayName = "Topic name can't be null.")]
        [Trait("Category", "Unit")]
        [InlineData(true)]
        [InlineData(false)]
        public void CantCreateNullTopicName(bool compactingRule)
        {

            // Arrange
            string name = null!;

            // Act
            var exception = Record.Exception(() => new LoadingTopic(name, compactingRule, new DateFilterParams(null!, null!)));

            // Assert
            exception.Should().NotBeNull().And.BeOfType<ArgumentNullException>();
        }

        [Theory(DisplayName = "Topic name can't be null 2.")]
        [Trait("Category", "Unit")]
        [InlineData(true)]
        [InlineData(false)]
        public void CantCreateNullTopicName2(bool compactingRule)
        {

            // Arrange
            string name = null!;

            // Act
            var exception = Record.Exception(() => new LoadingTopic(name, compactingRule, new DateFilterParams(null!, null!)));

            // Assert
            exception.Should().NotBeNull().And.BeOfType<ArgumentNullException>();
        }

        [Theory(DisplayName = "Topic name can't be empty.")]
        [Trait("Category", "Unit")]
        [InlineData(true)]
        [InlineData(false)]
        public void CantCreateEmptyTopicName(bool compactingRule)
        {

            // Arrange
            var name = string.Empty;

            // Act
            var exception = Record.Exception(() => new LoadingTopic(name, compactingRule, new DateFilterParams(null!, null!)));

            // Assert
            exception.Should().NotBeNull().And.BeOfType<ArgumentException>();
        }

        [Theory(DisplayName = "Topic name can't be empty 2.")]
        [Trait("Category", "Unit")]
        [InlineData(true)]
        [InlineData(false)]
        public void CantCreateEmptyTopicName2(bool compactingRule)
        {

            // Arrange
            var name = string.Empty;

            // Act
            var exception = Record.Exception(() => new LoadingTopic(name, compactingRule, new DateFilterParams(null!, null!)));

            // Assert
            exception.Should().NotBeNull().And.BeOfType<ArgumentException>();
        }

        [Theory(DisplayName = "Topic name can't be whitespaces.")]
        [Trait("Category", "Unit")]
        [InlineData(true)]
        [InlineData(false)]
        public void CantCreateWhitespacesTopicName(bool compactingRule)
        {

            // Arrange
            var name = "     ";

            // Act
            var exception = Record.Exception(() => new LoadingTopic(name, compactingRule, new DateFilterParams(null!, null!)));

            // Assert
            exception.Should().NotBeNull().And.BeOfType<ArgumentException>();
        }

        [Theory(DisplayName = "Topic name can't be whitespaces 2.")]
        [Trait("Category", "Unit")]
        [InlineData(true)]
        [InlineData(false)]
        public void CantCreateWhitespacesTopicName2(bool compactingRule)
        {

            // Arrange
            var name = "     ";

            // Act
            var exception = Record.Exception(() => new LoadingTopic(name, compactingRule, new DateFilterParams(null!, null!)));

            // Assert
            exception.Should().NotBeNull().And.BeOfType<ArgumentException>();
        }

        [Theory(DisplayName = "Topic name can't have any whitespaces.")]
        [Trait("Category", "Unit")]
        [InlineData(true)]
        [InlineData(false)]
        public void CantCreateAnyWhitespacesTopicName(bool compactingRule)
        {

            // Arrange
            var name = "topic name";

            // Act
            var exception = Record.Exception(() => new LoadingTopic(name, compactingRule, new DateFilterParams(null!, null!)));

            // Assert
            exception.Should().NotBeNull().And.BeOfType<ArgumentException>();
        }

        [Theory(DisplayName = "Topic name can't have any whitespaces 2.")]
        [Trait("Category", "Unit")]
        [InlineData(true)]
        [InlineData(false)]
        public void CantCreateAnyWhitespacesTopicName2(bool compactingRule)
        {

            // Arrange
            var name = "topic name";

            // Act
            var exception = Record.Exception(() => new LoadingTopic(name, compactingRule, new DateFilterParams(null!, null!)));

            // Assert
            exception.Should().NotBeNull().And.BeOfType<ArgumentException>();
        }

        [Theory(DisplayName = "Topic name can't have long names.")]
        [Trait("Category", "Unit")]
        [InlineData(true)]
        [InlineData(false)]
        public void CantCreateLongTopicName(bool compactingRule)
        {

            // Arrange
            var name = new string('x', 250);

            // Act
            var exception = Record.Exception(() => new LoadingTopic(name, compactingRule, new DateFilterParams(null!, null!)));

            // Assert
            exception.Should().NotBeNull().And.BeOfType<ArgumentException>();
        }

        [Theory(DisplayName = "Topic name can't have long names 2.")]
        [Trait("Category", "Unit")]
        [InlineData(true)]
        [InlineData(false)]
        public void CantCreateLongTopicName2(bool compactingRule)
        {

            // Arrange
            var name = new string('x', 250);

            // Act
            var exception = Record.Exception(() => new LoadingTopic(name, compactingRule, new DateFilterParams(null!, null!)));

            // Assert
            exception.Should().NotBeNull().And.BeOfType<ArgumentException>();
        }

        [Theory(DisplayName = "Topic name can't have bad symbols names.")]
        [Trait("Category", "Unit")]
        [InlineData(true)]
        [InlineData(false)]
        public void CantCreateBadTopicName(bool compactingRule)
        {

            // Arrange
            var name = "ы?:%";

            // Act
            var exception = Record.Exception(() => new LoadingTopic(name, compactingRule, new DateFilterParams(null!, null!)));

            // Assert
            exception.Should().NotBeNull().And.BeOfType<ArgumentException>();
        }

        [Theory(DisplayName = "Topic name can't have bad symbols names 2.")]
        [Trait("Category", "Unit")]
        [InlineData(true)]
        [InlineData(false)]
        public void CantCreateBadTopicName2(bool compactingRule)
        {

            // Arrange
            var name = "ы?:%";

            // Act
            var exception = Record.Exception(() => new LoadingTopic(name, compactingRule, new DateFilterParams(null!, null!)));

            // Assert
            exception.Should().NotBeNull().And.BeOfType<ArgumentException>();
        }

        [Theory(DisplayName = "Topic with valid name can be created.")]
        [Trait("Category", "Unit")]
        [InlineData(true)]
        [InlineData(false)]
        public void CanCreateValidTopic(bool compactingRule)
        {

            // Arrange
            var name = "test";
            LoadingTopic item = null!;
            // Act
            var exception = Record.Exception(() => item = new LoadingTopic(name, compactingRule, new DateFilterParams(null!, null!)));

            // Assert
            exception.Should().BeNull();
            item.Should().NotBeNull();
            item.LoadWithCompacting.Should().Be(compactingRule);
            item.HasOffsetDate.Should().BeFalse();
            item.HasEndOffsetDate.Should().BeFalse();
        }

        [Theory(DisplayName = "Topic with valid name can be created 2.")]
        [Trait("Category", "Unit")]
        [InlineData(true)]
        [InlineData(false)]
        public void CanCreateValidTopic2(bool compactingRule)
        {

            // Arrange
            var name = "test";
            LoadingTopic item = null!;
            // Act
            var exception = Record.Exception(() => item = new LoadingTopic(name, compactingRule, new DateFilterParams(DateTime.UtcNow, DateTime.UtcNow)));

            // Assert
            exception.Should().BeNull();
            item.Should().NotBeNull();
            item.LoadWithCompacting.Should().Be(compactingRule);
            item.HasOffsetDate.Should().BeTrue();
            item.HasEndOffsetDate.Should().BeTrue();
        }

        [Fact(DisplayName = "Cant get topic offset date if not set.")]
        [Trait("Category", "Unit")]
        public void CanGetTopicOffsetDate()
        {

            // Arrange
            var name = "test";
            var topic = new LoadingTopic(name, true, new DateFilterParams(null!, DateTime.UtcNow));

            // Act
            var exception = Record.Exception(() => topic.OffsetDate);

            // Assert
            exception.Should().NotBeNull().And.BeOfType<InvalidOperationException>();
        }

        [Fact(DisplayName = "Can get topic offset date if date is set.")]
        [Trait("Category", "Unit")]
        public void CantGetTopicOffsetDate()
        {

            // Arrange
            var date = DateTime.UtcNow;
            var name = "test";
            var topic = new LoadingTopic(name, true, new DateFilterParams(date, null!));
            DateTime resultedDate = default;

            // Act
            var exception = Record.Exception(() => resultedDate = topic.OffsetDate);

            // Assert
            exception.Should().BeNull();
            resultedDate.Should().Be(date);
        }

        [Fact(DisplayName = "Cant get topic end offset date if not set.")]
        [Trait("Category", "Unit")]
        public void CantGetTopicEndOffsetDate()
        {

            // Arrange
            var name = "test";
            var topic = new LoadingTopic(name, true, new DateFilterParams(DateTime.UtcNow, null!));

            // Act
            var exception = Record.Exception(() => topic.EndOffsetDate);

            // Assert
            exception.Should().NotBeNull().And.BeOfType<InvalidOperationException>();
        }

        [Fact(DisplayName = "Can get topic end offset date if date is set.")]
        [Trait("Category", "Unit")]
        public void CanGetTopicEndOffsetDate()
        {

            // Arrange
            var date = DateTime.UtcNow;
            var name = "test";
            var topic = new LoadingTopic(name, true, new DateFilterParams(null!, date));
            DateTime resultedDate = default;

            // Act
            var exception = Record.Exception(() => resultedDate = topic.EndOffsetDate);

            // Assert
            exception.Should().BeNull();
            resultedDate.Should().Be(date);
        }

        [Fact(DisplayName = "Can't setup empty partition filter.")]
        [Trait("Category", "Unit")]
        public void CantCreateTopicWithEmptyPartitionFilter()
        {

            // Arrange
            var name = "test";

            // Act
            var exception = Record.Exception(() => new LoadingTopic(name, true, new DateFilterParams(DateTime.UtcNow, DateTime.UtcNow.AddDays(1)), new HashSet<int>()));

            // Assert
            exception.Should().NotBeNull().And.BeOfType<ArgumentException>();
        }

        [Fact(DisplayName = "Can setup partition filter.")]
        [Trait("Category", "Unit")]
        public void CanCreateTopicWithValidPartitionFilter()
        {

            // Arrange
            var name = "test";

            // Act
            var exception = Record.Exception(() => new LoadingTopic(name, true, new DateFilterParams(DateTime.UtcNow, DateTime.UtcNow.AddDays(1)), new HashSet<int>(new[] { 1, 2, 3 })));

            // Assert
            exception.Should().BeNull();
        }

        [Fact(DisplayName = "Can get topic partition filter.")]
        [Trait("Category", "Unit")]
        public void CanGetTopicPartitionFilter()
        {

            // Arrange
            var items = new[] { 1, 2, 3 };
            var date = DateTime.UtcNow;
            var name = "test";

            // Act
            var topic = new LoadingTopic(name, true, new DateFilterParams(date, date), new HashSet<int>(items));

            // Assert
            topic.HasPartitionFilter.Should().BeTrue();
            topic.PartitionFilter.Should().NotBeNull();
            topic.PartitionFilter.Should().Contain(items);

        }
    }
}
