using Xunit;

using Microsoft.Extensions.Options;


using FluentAssertions;

using KafkaSnapshot.Models.Configuration;
using KafkaSnapshot.Processing.Configuration;
using KafkaSnapshot.Processing.Configuration.Validation;

namespace KafkaSnapshot.Processing.Tests;

public class LoaderToolConfigurationValidatorTests
{
    [Fact(DisplayName = "LoaderToolConfigurationValidator can be created.")]
    [Trait("Category", "Unit")]
    public void LoaderToolConfigurationValidatorCanBeCreated()
    {

        // Arrange

        // Act
        var exception = Record.Exception(() => new LoaderToolConfigurationValidator());

        // Assert
        exception.Should().BeNull();
    }

    [Fact(DisplayName = "LoaderToolConfigurationValidator can be validated.")]
    [Trait("Category", "Unit")]
    public void LoaderToolConfigurationValidatorCanBeValidated()
    {

        // Arrange
        var validator = new LoaderToolConfigurationValidator();
        var name = "Test";
        ValidateOptionsResult result = null!;
        var options = new LoaderToolConfiguration
        {
            UseConcurrentLoad = true,
            Topics = new List<TopicConfiguration>
             {
                  new TopicConfiguration
                  {
                      Name = "test",
                      Compacting = CompactingMode.On,
                      ExportFileName = "export",
                      ExportRawMessage = true,
                      FilterKeyType = Models.Filters.FilterType.None,
                      KeyType = Models.Filters.KeyType.Json,
                  }
             }
        };

        // Act
        var exception = Record.Exception(() => result = validator.Validate(name, options));

        // Assert
        exception.Should().BeNull();
        result.Succeeded.Should().BeTrue();
    }

    [Fact(DisplayName = "LoaderToolConfigurationValidator can be validated with valid date range.")]
    [Trait("Category", "Unit")]
    public void LoaderToolConfigurationValidatorCanBeValidatedWithDates()
    {

        // Arrange
        var validator = new LoaderToolConfigurationValidator();
        var startDate = new DateTimeOffset(2022, 1, 1, 10, 0, 0, TimeSpan.Zero);
        var endDate = startDate.AddHours(1);
        ValidateOptionsResult result = null!;
        var options = new LoaderToolConfiguration
        {
            UseConcurrentLoad = false,
            Topics =
            [
                new TopicConfiguration
                {
                    Name = "test",
                    Compacting = CompactingMode.Off,
                    ExportFileName = "export",
                    FilterKeyType = Models.Filters.FilterType.None,
                    KeyType = Models.Filters.KeyType.String,
                    OffsetStartDate = startDate,
                    OffsetEndDate = endDate
                }
            ]
        };

        // Act
        var exception = Record.Exception(() => result = validator.Validate("Test", options));

        // Assert
        exception.Should().BeNull();
        result.Succeeded.Should().BeTrue();
    }


    [Fact(DisplayName = "LoaderToolConfigurationValidator can't be validated for same files.")]
    [Trait("Category", "Unit")]
    public void LoaderToolConfigurationValidatorCannotBeValidatedForSameFiles()
    {

        // Arrange
        var validator = new LoaderToolConfigurationValidator();
        var name = "Test";
        ValidateOptionsResult result = null!;
        var options = new LoaderToolConfiguration
        {
            UseConcurrentLoad = true,
            Topics = new List<TopicConfiguration>
             {
                  new TopicConfiguration
                  {
                      Name = "test",
                      Compacting = CompactingMode.On,
                      ExportFileName = "export",
                      ExportRawMessage = true,
                      FilterKeyType = Models.Filters.FilterType.None,
                      KeyType = Models.Filters.KeyType.Json,
                  },
                  new TopicConfiguration
                  {
                      Name = "test",
                      Compacting = CompactingMode.On,
                      ExportFileName = "Export",
                      ExportRawMessage = true,
                      FilterKeyType = Models.Filters.FilterType.None,
                      KeyType = Models.Filters.KeyType.Json,
                  },

             }
        };

        // Act
        var exception = Record.Exception(() => result = validator.Validate(name, options));

        // Assert
        exception.Should().BeNull();
        result.Succeeded.Should().BeFalse();
        result.Failures.Should().ContainSingle()
            .Which.Should().StartWith(
                ConfigurationValidationErrorCodes.ExportFileNameDuplicate);
    }

    [Fact(DisplayName = "LoaderToolConfigurationValidator can't be validated with null options.")]
    [Trait("Category", "Unit")]
    public void LoaderToolConfigurationValidatorCannotBeValidatedWithNullOptions()
    {

        // Arrange
        var validator = new LoaderToolConfigurationValidator();
        var name = "Test";
        var options = (LoaderToolConfiguration)null!;

        // Act
        var exception = Record.Exception(() => validator.Validate(name, options));

        // Assert
        exception.Should().NotBeNull().And.BeOfType<ArgumentNullException>();
    }

    [Fact(DisplayName = "LoaderToolConfigurationValidator can't be validated with null topics.")]
    [Trait("Category", "Unit")]
    public void LoaderToolConfigurationValidatorCannotBeValidatedWithNullTopics()
    {

        // Arrange
        var validator = new LoaderToolConfigurationValidator();
        var name = "Test";
        ValidateOptionsResult result = null!;
        var options = new LoaderToolConfiguration
        {
            UseConcurrentLoad = true,
            Topics = null!
        };

        // Act
        var exception = Record.Exception(() => result = validator.Validate(name, options));

        // Assert
        exception.Should().BeNull();
        result.Succeeded.Should().BeFalse();
    }

    [Fact(DisplayName = "LoaderToolConfigurationValidator can't be validated with empty topics.")]
    [Trait("Category", "Unit")]
    public void LoaderToolConfigurationValidatorCannotBeValidatedWithEmptyTopics()
    {

        // Arrange
        var validator = new LoaderToolConfigurationValidator();
        var name = "Test";
        ValidateOptionsResult result = null!;
        var options = new LoaderToolConfiguration
        {
            UseConcurrentLoad = true,
            Topics = new List<TopicConfiguration>()
        };

        // Act
        var exception = Record.Exception(() => result = validator.Validate(name, options));

        // Assert
        exception.Should().BeNull();
        result.Succeeded.Should().BeFalse();
    }

    [Fact(DisplayName = "LoaderToolConfigurationValidator can't be validated with null topic name.")]
    [Trait("Category", "Unit")]
    public void LoaderToolConfigurationValidatorCannotBeValidatedWithNullTopicName()
    {

        // Arrange
        var validator = new LoaderToolConfigurationValidator();
        var name = "Test";
        ValidateOptionsResult result = null!;
        var options = new LoaderToolConfiguration
        {
            UseConcurrentLoad = true,
            Topics = new List<TopicConfiguration>()
            {
                  new TopicConfiguration
                  {
                      Name = "test",
                      Compacting = CompactingMode.On,
                      ExportFileName = "export",
                      ExportRawMessage = true,
                      FilterKeyType = Models.Filters.FilterType.None,
                      KeyType = Models.Filters.KeyType.Json,
                  },
                  new TopicConfiguration
                  {
                      Name = null!,
                      Compacting = CompactingMode.On,
                      ExportFileName = "export",
                      ExportRawMessage = true,
                      FilterKeyType = Models.Filters.FilterType.None,
                      KeyType = Models.Filters.KeyType.Json,
                  }
            }
        };

        // Act
        var exception = Record.Exception(() => result = validator.Validate(name, options));

        // Assert
        exception.Should().BeNull();
        result.Succeeded.Should().BeFalse();
    }

    [Fact(DisplayName = "LoaderToolConfigurationValidator can't be validated with empty topic name.")]
    [Trait("Category", "Unit")]
    public void LoaderToolConfigurationValidatorCannotBeValidatedWithEmptyTopicName()
    {

        // Arrange
        var validator = new LoaderToolConfigurationValidator();
        var name = "Test";
        ValidateOptionsResult result = null!;
        var options = new LoaderToolConfiguration
        {
            UseConcurrentLoad = true,
            Topics = new List<TopicConfiguration>()
            {
                  new TopicConfiguration
                  {
                      Name = "test",
                      Compacting = CompactingMode.On,
                      ExportFileName = "export",
                      ExportRawMessage = true,
                      FilterKeyType = Models.Filters.FilterType.None,
                      KeyType = Models.Filters.KeyType.Json,
                  },
                  new TopicConfiguration
                  {
                      Name = string.Empty,
                      Compacting = CompactingMode.On,
                      ExportFileName = "export",
                      ExportRawMessage = true,
                      FilterKeyType = Models.Filters.FilterType.None,
                      KeyType = Models.Filters.KeyType.Json,
                  }
            }
        };

        // Act
        var exception = Record.Exception(() => result = validator.Validate(name, options));

        // Assert
        exception.Should().BeNull();
        result.Succeeded.Should().BeFalse();
    }

    [Fact(DisplayName = "LoaderToolConfigurationValidator can't be validated with space topic name.")]
    [Trait("Category", "Unit")]
    public void LoaderToolConfigurationValidatorCannotBeValidatedWithSpaceTopicName()
    {

        // Arrange
        var validator = new LoaderToolConfigurationValidator();
        var name = "Test";
        ValidateOptionsResult result = null!;
        var options = new LoaderToolConfiguration
        {
            UseConcurrentLoad = true,
            Topics = new List<TopicConfiguration>()
            {
                  new TopicConfiguration
                  {
                      Name = "test",
                      Compacting = CompactingMode.On,
                      ExportFileName = "export",
                      ExportRawMessage = true,
                      FilterKeyType = Models.Filters.FilterType.None,
                      KeyType = Models.Filters.KeyType.Json,
                  },
                  new TopicConfiguration
                  {
                      Name = "   ",
                      Compacting = CompactingMode.On,
                      ExportFileName = "export",
                      ExportRawMessage = true,
                      FilterKeyType = Models.Filters.FilterType.None,
                      KeyType = Models.Filters.KeyType.Json,
                  }
            }
        };

        // Act
        var exception = Record.Exception(() => result = validator.Validate(name, options));

        // Assert
        exception.Should().BeNull();
        result.Succeeded.Should().BeFalse();
    }

    [Theory(DisplayName = "LoaderToolConfigurationValidator can't be validated with any space topic name.")]
    [Trait("Category", "Unit")]
    [InlineData(" test")]
    [InlineData("test ")]
    [InlineData("te st")]
    public void LoaderToolConfigurationValidatorCannotBeValidatedWithAnySpaceTopicName(string topicName)
    {

        // Arrange
        var validator = new LoaderToolConfigurationValidator();
        var name = "Test";
        ValidateOptionsResult result = null!;
        var options = new LoaderToolConfiguration
        {
            UseConcurrentLoad = true,
            Topics = new List<TopicConfiguration>()
            {
                  new TopicConfiguration
                  {
                      Name = "test",
                      Compacting = CompactingMode.On,
                      ExportFileName = "export",
                      ExportRawMessage = true,
                      FilterKeyType = Models.Filters.FilterType.None,
                      KeyType = Models.Filters.KeyType.Json,
                  },
                  new TopicConfiguration
                  {
                      Name = topicName,
                      Compacting = CompactingMode.On,
                      ExportFileName = "export",
                      ExportRawMessage = true,
                      FilterKeyType = Models.Filters.FilterType.None,
                      KeyType = Models.Filters.KeyType.Json,
                  }
            }
        };

        // Act
        var exception = Record.Exception(() => result = validator.Validate(name, options));

        // Assert
        exception.Should().BeNull();
        result.Succeeded.Should().BeFalse();
    }

    [Fact(DisplayName = "LoaderToolConfigurationValidator can't be validated with long topic name.")]
    [Trait("Category", "Unit")]
    public void LoaderToolConfigurationValidatorCannotBeValidatedWithLongTopicName()
    {

        // Arrange
        var validator = new LoaderToolConfigurationValidator();
        var name = "Test";
        ValidateOptionsResult result = null!;
        var options = new LoaderToolConfiguration
        {
            UseConcurrentLoad = true,
            Topics = new List<TopicConfiguration>()
            {
                  new TopicConfiguration
                  {
                      Name = "test",
                      Compacting = CompactingMode.On,
                      ExportFileName = "export",
                      ExportRawMessage = true,
                      FilterKeyType = Models.Filters.FilterType.None,
                      KeyType = Models.Filters.KeyType.Json,
                  },
                  new TopicConfiguration
                  {
                      Name = new string('a',250),
                      Compacting = CompactingMode.On,
                      ExportFileName = "export",
                      ExportRawMessage = true,
                      FilterKeyType = Models.Filters.FilterType.None,
                      KeyType = Models.Filters.KeyType.Json,
                  }
            }
        };

        // Act
        var exception = Record.Exception(() => result = validator.Validate(name, options));

        // Assert
        exception.Should().BeNull();
        result.Succeeded.Should().BeFalse();
    }


    [Fact(DisplayName = "LoaderToolConfigurationValidator can't be validated with bad topic name.")]
    [Trait("Category", "Unit")]
    public void LoaderToolConfigurationValidatorCannotBeValidatedWithBadTopicName()
    {

        // Arrange
        var validator = new LoaderToolConfigurationValidator();
        var name = "Test";
        ValidateOptionsResult result = null!;
        var options = new LoaderToolConfiguration
        {
            UseConcurrentLoad = true,
            Topics = new List<TopicConfiguration>()
            {
                  new TopicConfiguration
                  {
                      Name = "test",
                      Compacting = CompactingMode.On,
                      ExportFileName = "export",
                      ExportRawMessage = true,
                      FilterKeyType = Models.Filters.FilterType.None,
                      KeyType = Models.Filters.KeyType.Json,
                  },
                  new TopicConfiguration
                  {
                      Name = "*()=%$#@!!",
                      Compacting = CompactingMode.On,
                      ExportFileName = "export",
                      ExportRawMessage = true,
                      FilterKeyType = Models.Filters.FilterType.None,
                      KeyType = Models.Filters.KeyType.Json,
                  }
            }
        };

        // Act
        var exception = Record.Exception(() => result = validator.Validate(name, options));

        // Assert
        exception.Should().BeNull();
        result.Succeeded.Should().BeFalse();
    }


    [Fact(DisplayName = "LoaderToolConfigurationValidator can't be validated with null export name.")]
    [Trait("Category", "Unit")]
    public void LoaderToolConfigurationValidatorCannotBeValidatedWithNullExportName()
    {

        // Arrange
        var validator = new LoaderToolConfigurationValidator();
        var name = "Test";
        ValidateOptionsResult result = null!;
        var options = new LoaderToolConfiguration
        {
            UseConcurrentLoad = true,
            Topics = new List<TopicConfiguration>()
            {
                  new TopicConfiguration
                  {
                      Name = "test",
                      Compacting = CompactingMode.On,
                      ExportFileName = "export",
                      ExportRawMessage = true,
                      FilterKeyType = Models.Filters.FilterType.None,
                      KeyType = Models.Filters.KeyType.Json,
                  },
                  new TopicConfiguration
                  {
                      Name = "test",
                      Compacting = CompactingMode.On,
                      ExportFileName = null!,
                      ExportRawMessage = true,
                      FilterKeyType = Models.Filters.FilterType.None,
                      KeyType = Models.Filters.KeyType.Json,
                  }
            }
        };

        // Act
        var exception = Record.Exception(() => result = validator.Validate(name, options));

        // Assert
        exception.Should().BeNull();
        result.Succeeded.Should().BeFalse();
    }

    [Fact(DisplayName = "LoaderToolConfigurationValidator can't be validated with empty export name.")]
    [Trait("Category", "Unit")]
    public void LoaderToolConfigurationValidatorCannotBeValidatedWithEmptyExportName()
    {

        // Arrange
        var validator = new LoaderToolConfigurationValidator();
        var name = "Test";
        ValidateOptionsResult result = null!;
        var options = new LoaderToolConfiguration
        {
            UseConcurrentLoad = true,
            Topics = new List<TopicConfiguration>()
            {
                  new TopicConfiguration
                  {
                      Name = "test",
                      Compacting = CompactingMode.On,
                      ExportFileName = "export",
                      ExportRawMessage = true,
                      FilterKeyType = Models.Filters.FilterType.None,
                      KeyType = Models.Filters.KeyType.Json,
                  },
                  new TopicConfiguration
                  {
                      Name = "test",
                      Compacting = CompactingMode.On,
                      ExportFileName = string.Empty,
                      ExportRawMessage = true,
                      FilterKeyType = Models.Filters.FilterType.None,
                      KeyType = Models.Filters.KeyType.Json,
                  }
            }
        };

        // Act
        var exception = Record.Exception(() => result = validator.Validate(name, options));

        // Assert
        exception.Should().BeNull();
        result.Succeeded.Should().BeFalse();
    }


    [Fact(DisplayName = "LoaderToolConfigurationValidator can't be validated with space export name.")]
    [Trait("Category", "Unit")]
    public void LoaderToolConfigurationValidatorCannotBeValidatedWithSpaceExportName()
    {

        // Arrange
        var validator = new LoaderToolConfigurationValidator();
        var name = "Test";
        ValidateOptionsResult result = null!;
        var options = new LoaderToolConfiguration
        {
            UseConcurrentLoad = true,
            Topics = new List<TopicConfiguration>()
            {
                  new TopicConfiguration
                  {
                      Name = "test",
                      Compacting = CompactingMode.On,
                      ExportFileName = "export",
                      ExportRawMessage = true,
                      FilterKeyType = Models.Filters.FilterType.None,
                      KeyType = Models.Filters.KeyType.Json,
                  },
                  new TopicConfiguration
                  {
                      Name = "test",
                      Compacting = CompactingMode.On,
                      ExportFileName = "  ",
                      ExportRawMessage = true,
                      FilterKeyType = Models.Filters.FilterType.None,
                      KeyType = Models.Filters.KeyType.Json,
                  }
            }
        };

        // Act
        var exception = Record.Exception(() => result = validator.Validate(name, options));

        // Assert
        exception.Should().BeNull();
        result.Succeeded.Should().BeFalse();
    }

    [Fact(DisplayName = "LoaderToolConfigurationValidator can be validated with filter.")]
    [Trait("Category", "Unit")]
    public void LoaderToolConfigurationValidatorCanBeValidatedWithFilter()
    {

        // Arrange
        var validator = new LoaderToolConfigurationValidator();
        var name = "Test";
        ValidateOptionsResult result = null!;
        var options = new LoaderToolConfiguration
        {
            UseConcurrentLoad = true,
            Topics = new List<TopicConfiguration>()
            {
                  new TopicConfiguration
                  {
                      Name = "test",
                      Compacting = CompactingMode.On,
                      ExportFileName = "export",
                      ExportRawMessage = true,
                      FilterKeyType = Models.Filters.FilterType.Equals,
                      FilterKeyValue = "{}",
                      KeyType = Models.Filters.KeyType.Json,
                  }
            }
        };

        // Act
        var exception = Record.Exception(() => result = validator.Validate(name, options));

        // Assert
        exception.Should().BeNull();
        result.Succeeded.Should().BeTrue();
    }

    [Fact(DisplayName = "LoaderToolConfigurationValidator can't be validated with null filter.")]
    [Trait("Category", "Unit")]
    public void LoaderToolConfigurationValidatorCannotBeValidatedWithNullFilter()
    {

        // Arrange
        var validator = new LoaderToolConfigurationValidator();
        var name = "Test";
        ValidateOptionsResult result = null!;
        var options = new LoaderToolConfiguration
        {
            UseConcurrentLoad = true,
            Topics = new List<TopicConfiguration>()
            {
                  new TopicConfiguration
                  {
                      Name = "test",
                      Compacting = CompactingMode.On,
                      ExportFileName = "export",
                      ExportRawMessage = true,
                      FilterKeyType = Models.Filters.FilterType.Equals,
                      FilterKeyValue = "{}",
                      KeyType = Models.Filters.KeyType.Json,
                  },
                  new TopicConfiguration
                  {
                      Name = "test",
                      Compacting = CompactingMode.On,
                      ExportFileName = "export",
                      ExportRawMessage = true,
                      FilterKeyType = Models.Filters.FilterType.Equals,
                      FilterKeyValue = null!,
                      KeyType = Models.Filters.KeyType.Json,
                  }
            }
        };

        // Act
        var exception = Record.Exception(() => result = validator.Validate(name, options));

        // Assert
        exception.Should().BeNull();
        result.Succeeded.Should().BeFalse();
    }

    [Fact(DisplayName = "LoaderToolConfigurationValidator can't be validated with compacting and key ignored.")]
    [Trait("Category", "Unit")]
    public void LoaderToolConfigurationValidatorCannotBeCompactedAndKeyIgnored()
    {

        // Arrange
        var validator = new LoaderToolConfigurationValidator();
        var name = "Test";
        ValidateOptionsResult result = null!;
        var options = new LoaderToolConfiguration
        {
            UseConcurrentLoad = true,
            Topics = new List<TopicConfiguration>()
            {
                  new TopicConfiguration
                  {
                      Name = "test",
                      Compacting = CompactingMode.On,
                      ExportFileName = "export",
                      ExportRawMessage = true,
                      FilterKeyType = Models.Filters.FilterType.Equals,
                      FilterKeyValue = "{}",
                      KeyType = Models.Filters.KeyType.Json,
                  },
                  new TopicConfiguration
                  {
                      Name = "test",
                      Compacting = CompactingMode.On,
                      ExportFileName = "export",
                      ExportRawMessage = true,
                      FilterKeyType = Models.Filters.FilterType.Equals,
                      FilterKeyValue = "{}",
                      KeyType = Models.Filters.KeyType.Ignored,
                  }
            }
        };

        // Act
        var exception = Record.Exception(() => result = validator.Validate(name, options));

        // Assert
        exception.Should().BeNull();
        result.Succeeded.Should().BeFalse();
    }

    [Fact(DisplayName = "LoaderToolConfigurationValidator can't be validated with start date greater than end date.")]
    [Trait("Category", "Unit")]
    public void LoaderToolConfigurationValidatorCannotBeValidatedWithIncorrectDates()
    {

        // Arrange
        var startDate = new DateTimeOffset(2022, 1, 1, 0, 0, 0, TimeSpan.Zero);
        var endDate = startDate.AddDays(-1);
        var validator = new LoaderToolConfigurationValidator();
        var name = "Test";
        ValidateOptionsResult result = null!;
        var options = new LoaderToolConfiguration
        {
            Topics = new List<TopicConfiguration>()
            {
                  new TopicConfiguration
                  {
                      Name = "test",
                      ExportFileName = "export",
                      KeyType = Models.Filters.KeyType.Json,
                      OffsetStartDate = startDate,
                      OffsetEndDate = endDate
                  }
            }
        };

        // Act
        var exception = Record.Exception(() => result = validator.Validate(name, options));

        // Assert
        exception.Should().BeNull();
        result.Succeeded.Should().BeFalse();
    }
}

