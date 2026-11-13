using FluentAssertions;
using KafkaSnapshot.Models.Names;
using Xunit;

namespace KafkaSnapshot.Models.Tests;

public class KafkaTopicNameRulesTests
{
    [Theory(DisplayName = "KafkaTopicNameRules accepts ASCII letters, digits, dots, underscores, and hyphens.")]
    [Trait("Category", "Unit")]
    [InlineData("AZaz09._-")]
    [InlineData(".")]
    [InlineData("_")]
    [InlineData("-")]
    public void AcceptsSupportedCharacters(string name)
        => KafkaTopicNameRules.IsValid(name).Should().BeTrue();

    [Theory(DisplayName = "KafkaTopicNameRules rejects missing names, whitespace, Unicode letters, and unsupported punctuation.")]
    [Trait("Category", "Unit")]
    [InlineData(null)]
    [InlineData("")]
    [InlineData(" ")]
    [InlineData("topic name")]
    [InlineData("topic/name")]
    [InlineData("caf\u00e9")]
    public void RejectsUnsupportedNames(string? name)
        => KafkaTopicNameRules.IsValid(name).Should().BeFalse();

    [Fact(DisplayName = "KafkaTopicNameRules accepts 249 characters and rejects 250 characters.")]
    [Trait("Category", "Unit")]
    public void EnforcesMaximumLengthBoundary()
    {
        KafkaTopicNameRules.IsValid(new string('a', 249)).Should().BeTrue();
        KafkaTopicNameRules.IsValid(new string('a', 250)).Should().BeFalse();
    }
}
