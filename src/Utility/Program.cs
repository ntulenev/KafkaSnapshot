using KafkaSnapshot.Utility;

using Microsoft.Extensions.Hosting;

using var host = HostBuildHelper.CreateHost();
await host.RunAsync();
