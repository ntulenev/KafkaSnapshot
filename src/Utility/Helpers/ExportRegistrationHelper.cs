using KafkaSnapshot.Abstractions.Export;
using KafkaSnapshot.Export.File.Common;
using KafkaSnapshot.Export.File.Output;
using KafkaSnapshot.Export.Markers;
using KafkaSnapshot.Export.Serialization;

using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;

namespace KafkaSnapshot.Utility.Helpers;

/// <summary>
/// Registers export services.
/// </summary>
internal static class ExportRegistrationHelper
{
    internal static IServiceCollection Register(IServiceCollection services, IConfiguration configuration)
    {
        ArgumentNullException.ThrowIfNull(services);
        ArgumentNullException.ThrowIfNull(configuration);

        _ = services.ConfigureExport(configuration);
        _ = services.AddSingleton<ISerializer<string, string, IgnoreKeyMarker>, IgnoreKeySerializer>();
        _ = services.AddSingleton<ISerializer<string, string, JsonKeyMarker>, JsonKeySerializer>();
        _ = services.AddSingleton<ISerializer<string, string, OriginalKeyMarker>, OriginalKeySerializer<string>>();
        _ = services.AddSingleton<ISerializer<long, string, OriginalKeyMarker>, OriginalKeySerializer<long>>();
        _ = services.AddSingleton(typeof(IDataExporter<,,,>), typeof(JsonFileDataExporter<,,,>));
        _ = services.AddSingleton<IFileSaver, FileSaver>();
        _ = services.AddSingleton<IFileStreamProvider, FileStreamProvider>();

        return services;
    }
}
