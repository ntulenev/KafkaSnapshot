global using KafkaSnapshot.Tests;

using Xunit;

namespace KafkaSnapshot.Tests;

internal static class TestTimeouts
{
    public static CancellationTokenSource CreateScenarioCancellation()
    {
        var timeout = CancellationTokenSource.CreateLinkedTokenSource(TestContext.Current.CancellationToken);
        timeout.CancelAfter(Scenario);
        return timeout;
    }

    public static readonly TimeSpan Wait = TimeSpan.FromSeconds(30);
    public static readonly TimeSpan Scenario = TimeSpan.FromMinutes(2);
}
