[CmdletBinding()]
param(
    [string]$BootstrapServers = '127.0.0.1:19092',
    [int[]]$MessageCounts = @(50000, 200000),
    [int]$Trials = 3
)
$ErrorActionPreference = 'Stop'
Set-StrictMode -Version Latest
$repoRoot = Split-Path -Parent $PSScriptRoot
$project = Join-Path $PSScriptRoot 'Measurements/Measurements.csproj'
dotnet build $project --configuration Release --verbosity quiet
if ($LASTEXITCODE -ne 0) { throw 'Building measurements failed.' }
$runner = Join-Path $PSScriptRoot 'Measurements/bin/Release/net10.0/Measurements.dll'
$outputDirectory = Join-Path $repoRoot "artifacts/measurements/$([guid]::NewGuid().ToString('N'))"
New-Item -ItemType Directory -Path $outputDirectory -Force | Out-Null
$measurements = @()
foreach ($count in $MessageCounts) {
    $topic = "snapshot-perf-$([guid]::NewGuid().ToString('N'))"
    try {
        dotnet $runner prepare $BootstrapServers $topic $count
        if ($LASTEXITCODE -ne 0) { throw 'Seeding benchmark data failed.' }
        for ($trial = 1; $trial -le $Trials; $trial++) {
            foreach ($mode in @('buffered', 'streaming')) {
                $output = Join-Path $outputDirectory "$count-$trial-$mode.json"
                $json = dotnet $runner $mode $BootstrapServers $topic $output
                if ($LASTEXITCODE -ne 0) { throw "Measurement failed: $mode" }
                $result = $json | ConvertFrom-Json
                $result | Add-Member -NotePropertyName messageCount -NotePropertyValue $count
                $result | Add-Member -NotePropertyName trial -NotePropertyValue $trial
                $result | Add-Member -NotePropertyName sha256 -NotePropertyValue (Get-FileHash -LiteralPath $output -Algorithm SHA256).Hash
                if ($result.exportedMessages -ne $count) { throw 'Exported message count differs from seeded data.' }
                $measurements += $result
                $result | ConvertTo-Json -Compress | Write-Host
                Remove-Item -LiteralPath $output
            }
        }
        $hashes = @($measurements | Where-Object messageCount -eq $count | Select-Object -ExpandProperty sha256 -Unique)
        if ($hashes.Count -ne 1) { throw 'Buffered and streaming exports differ.' }
    }
    finally {
        dotnet $runner delete $BootstrapServers $topic
    }
}
$report = Join-Path $outputDirectory 'results.json'
$measurements | ConvertTo-Json -Depth 5 | Set-Content -LiteralPath $report
Write-Host "Measurement results: $report"
