using FluentAssertions;

using KafkaSnapshot.Export.Serialization;
using KafkaSnapshot.Models.Message;

using Microsoft.Extensions.Logging.Abstractions;

using Xunit;

namespace KafkaSnapshot.Export.Tests;

[Trait("Category", "Unit")]
public class IgnoreKeySerializerSinglePassTests
{
    [Theory(DisplayName = "Ignored-key exports enumerate single-use input once in either output mode.")]
    [InlineData(false)]
    [InlineData(true)]
    public async Task SingleUseInputIsSupported(bool streaming)
    {
        var enumerations = 0;
        IEnumerable<KeyValuePair<string, KafkaMessage<string>>> Source()
        {
            enumerations++;
            if (enumerations > 1)
            {
                throw new InvalidOperationException("Input can only be enumerated once.");
            }

            yield return new("ignored", new("1", new(DateTimeOffset.UnixEpoch, 0, 0)));
            yield return new("also ignored", new("2", new(DateTimeOffset.UnixEpoch, 0, 1)));
        }

        var serializer = new IgnoreKeySerializer(NullLogger<IgnoreKeySerializer>.Instance);
        string json;
        if (streaming)
        {
            await using var stream = new MemoryStream();
            await serializer.SerializeAsync(Source(), false, stream, CancellationToken.None);
            json = System.Text.Encoding.UTF8.GetString(stream.ToArray());
        }
        else
        {
            json = serializer.Serialize(Source(), false);
        }

        enumerations.Should().Be(1);
        using var document = System.Text.Json.JsonDocument.Parse(json);
        document.RootElement.GetArrayLength().Should().Be(2);
    }
}
