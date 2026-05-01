using System.Reflection;

using FluentAssertions;

using KafkaSnapshot.Models.Configuration;
using KafkaSnapshot.Models.Filters;
using KafkaSnapshot.Processing.Configuration;
using KafkaSnapshot.Processing.Configuration.Validation;
using KafkaSnapshot.Utility.DependencyInjection;

using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Options;

using Xunit;

namespace KafkaSnapshot.Utility.Tests;

public class TopicLoadersRegistrationTests
{
    [Fact(DisplayName = "TopicLoadersRegistrationHelper throws options validation for unsupported key type.")]
    [Trait("Category", "Unit")]
    public void TopicLoadersRegistrationHelperCreateTopicLoadersThrowsForUnsupportedKeyType()
    {
        // Arrange
        var configuration = UtilityTestConfiguration.BuildLoaderConfig(
            useConcurrentLoad: true,
            keyType: (KeyType)999);
        var services = new ServiceCollection();
        _ = services.AddOptions<LoaderToolConfiguration>()
            .Bind(configuration.GetSection(nameof(LoaderToolConfiguration)));
        _ = services.AddSingleton<IValidateOptions<LoaderToolConfiguration>>(
            new LoaderToolConfigurationValidator());
        using var provider = services.BuildServiceProvider();

        var method = GetStaticMethod(
            "KafkaSnapshot.Utility.Helpers.TopicLoadersRegistrationHelper",
            "CreateTopicLoaders",
            BindingFlags.NonPublic | BindingFlags.Static);

        // Act
        Action act = () => _ = method.Invoke(null, [provider]);

        // Assert
        var exception = act.Should().Throw<TargetInvocationException>().Which;
        var validationException = exception.InnerException.Should()
            .BeOfType<OptionsValidationException>()
            .Subject;
        validationException.Failures.Should().ContainSingle()
            .Which.Should().StartWith(
                ConfigurationValidationErrorCodes.KeyTypeUnsupported);
    }

    private static MethodInfo GetStaticMethod(
        string typeName,
        string methodName,
        BindingFlags flags = BindingFlags.Public | BindingFlags.Static)
    {
        var type = typeof(HostBuildHelper).Assembly.GetType(typeName);
        type.Should().NotBeNull();

        var method = type.GetMethod(methodName, flags);
        method.Should().NotBeNull();

        return method;
    }
}

