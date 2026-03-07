using KafkaSnapshot.Utility;

using Microsoft.Extensions.Hosting;

using var host = HostBuildHelper.CreateHost(args);
await host.RunAsync();
