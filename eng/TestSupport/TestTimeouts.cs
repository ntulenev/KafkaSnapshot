global using KafkaSnapshot.Tests;

namespace KafkaSnapshot.Tests;

internal static class TestTimeouts
{
    public static readonly TimeSpan Wait = TimeSpan.FromSeconds(30);
    public static readonly TimeSpan Scenario = TimeSpan.FromMinutes(2);
}
