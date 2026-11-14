# Dependency audit — 2026-11-13

The application and all test/tool projects target `net10.0`. The SDK stays on the
.NET 10 LTS line. `global.json` uses `10.0.100` as the minimum version with
`rollForward: latestFeature` and previews disabled, selecting the latest installed
stable 10.0 SDK. CI installs the latest stable SDK in that line via `10.0.x`.
The dependency audit below was originally validated with SDK `10.0.404` (runtime
`10.0.15`); that exact version is not required for development.

Stable package versions were checked against the NuGet v3 feed; pre-release
versions were excluded. GitHub Actions releases and the Kafka distribution were
also checked.

## Updated dependencies

| Dependency | Previous | Selected |
| --- | --- | --- |
| .NET SDK used for the audit | 10.0.301 | 10.0.404 |
| Microsoft.Extensions.Configuration.Binder / Json | 10.0.15 | 11.0.0 |
| Microsoft.Extensions.Hosting | 10.0.15 | 11.0.0 |
| Microsoft.Extensions.Logging / Abstractions | 10.0.15 | 11.0.0 |
| Microsoft.Extensions.Options | 10.0.15 | 11.0.0 |
| xUnit framework | xunit 2.9.3 | xunit.v3.mtp-off 4.0.3 |
| Microsoft.Bcl.AsyncInterfaces (transitive) | 6.0.0 | 11.0.0 |
| Microsoft.Extensions.DependencyModel (transitive) | 10.0.0 | 11.0.0 |
| Microsoft.NET.StringTools (transitive) | 17.11.4 | 18.9.6 |
| Serilog (transitive) | 4.3.0 | 4.4.0 |
| System.Diagnostics.EventLog (transitive) | 6.0.0 | 11.0.0 |

`Microsoft.Extensions` 11.0.0 provides `net10.0` assets and does not require changing
the application's target framework. StringTools 18.9.6 is the newest stable
release supporting .NET 10: 18.10.1, 18.11.0, and 18.12.2 require .NET 11. Its
compatibility warning is not suppressed. Consequently `dotnet list package
--outdated --include-transitive` still reports StringTools; that is an intentional
compatibility constraint, not an overlooked update.

Central transitive pinning is enabled in `src/Directory.Packages.props`.
`eng/Directory.Packages.props` imports the same versions for KafkaProbe and
Measurements, so tools use the same pinned dependencies. The root `NuGet.config`
limits both application and tool restores to nuget.org, independent of user feeds.

All seven test projects use executable xUnit v3 projects. The official `mtp-off`
package keeps VSTest, the Visual Studio adapter, Coverlet, TRX output, broker trait
filtering, and the existing hang diagnostics. Test scenarios link their deadlines
to xUnit's cancellation token; cleanup still joins workers after cancellation.
The custom Kafka fact forwards source locations to the xUnit v3 base attribute.

## Already current

Confluent.Kafka 2.16.0, MessagePack 3.1.12, Newtonsoft.Json 13.0.4, Moq 4.21.0,
FluentAssertions 8.11.0, Microsoft.NET.Test.Sdk 18.12.0, Coverlet 10.1.0,
xunit.runner.visualstudio 4.0.1, the Serilog integration/sink packages, and
ReportGenerator 5.6.0 were already on their current stable releases.

The existing GitHub Actions commit pins already resolve to checkout v7.0.1,
setup-dotnet v6.1.0, and upload-artifact v7.0.2. The digest-pinned Kafka 4.4.0 image
also remains current.

## Verification

Run `./eng/Test.ps1` and `./eng/Test-Kafka.ps1` with an installed stable .NET 10 SDK. Build
`eng/Measurements/Measurements.csproj` in Release as well. Audit each of
`src/KafkaSnapshot.slnx`, `eng/KafkaProbe/KafkaProbe.csproj`, and
`eng/Measurements/Measurements.csproj` with both:

```powershell
dotnet list <project-or-solution> package --outdated --include-transitive
dotnet list <project-or-solution> package --vulnerable --include-transitive
```

Verified with SDK 10.0.404: the solution and both tools build without warnings;
556 default test cases pass, with the six broker cases intentionally skipped in
that run; production coverage remains 1,549/1,549 lines, 371/371 branches, and
309/309 methods. All six broker cases also pass separately on Windows and Linux.
On 2026-11-14, the full coverage gate also passed with the globally installed SDK
10.0.301 after enabling flexible SDK selection.
The solution and both tool graphs have no reported vulnerable packages. Their
only outdated package identity is the documented StringTools constraint above.

## Sources

- [.NET 10 SDK and runtime releases](https://dotnet.microsoft.com/en-us/download/dotnet/10.0)
- [NuGet package feed](https://api.nuget.org/v3/index.json)
- [Microsoft.Extensions.Hosting 11.0.0 frameworks](https://www.nuget.org/packages/Microsoft.Extensions.Hosting/11.0.0)
- [StringTools 18.9.6](https://www.nuget.org/packages/Microsoft.NET.StringTools/18.9.6)
- [xUnit v3 migration](https://xunit.net/docs/getting-started/v3/migration)
- [xUnit testing platform variants](https://xunit.net/docs/getting-started/v3/microsoft-testing-platform)
- [Checkout v7.0.1](https://github.com/actions/checkout/releases/tag/v7.0.1)
- [Setup .NET v6.1.0](https://github.com/actions/setup-dotnet/releases/tag/v6.1.0)
- [Upload Artifact v7.0.2](https://github.com/actions/upload-artifact/releases/tag/v7.0.2)
- [Kafka releases](https://kafka.apache.org/downloads/)
