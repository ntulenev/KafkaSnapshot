using Microsoft.Extensions.DependencyInjection;

using KafkaSnapshot.Abstractions.Processing;
using KafkaSnapshot.Utility;

using var serviceScope = HostBuildHelper.CreateHost().Services.CreateScope();
var services = serviceScope.ServiceProvider;
var tool = services.GetRequiredService<ILoaderTool>();
await tool.ProcessAsync(CancellationToken.None).ConfigureAwait(false);
