[CmdletBinding()]
param(
    [Parameter(Mandatory)]
    [string] $SummaryPath
)

$ErrorActionPreference = 'Stop'
Set-StrictMode -Version Latest

[xml] $report = Get-Content -LiteralPath $SummaryPath -Raw
$summary = $report.CoverageReport.Summary
$expectedAssemblies = @(
    'KafkaSnapshot.Export', 'KafkaSnapshot.Filters', 'KafkaSnapshot.Import',
    'KafkaSnapshot.Models', 'KafkaSnapshot.Processing', 'KafkaSnapshot.Sorting',
    'KafkaSnapshot.Utility'
)
$assemblies = @($report.CoverageReport.Coverage.Assembly | ForEach-Object { $_.name })
foreach ($assembly in $expectedAssemblies) {
    if ($assembly -notin $assemblies) {
        throw "Coverage report is missing production assembly $assembly."
    }
}

# Compare exact counts: rounded percentages could conceal uncovered code.
foreach ($metric in @(
    @{ Name = 'lines'; Covered = 'Coveredlines'; Total = 'Coverablelines' },
    @{ Name = 'branches'; Covered = 'Coveredbranches'; Total = 'Totalbranches' },
    @{ Name = 'methods'; Covered = 'Coveredmethods'; Total = 'Totalmethods' }
)) {
    $covered = [long] $summary.($metric.Covered)
    $total = [long] $summary.($metric.Total)
    if ($total -le 0 -or $covered -ne $total) {
        throw "Expected 100% coverage of $($metric.Name); covered $covered of $total. See $SummaryPath."
    }
    Write-Host "Coverage of $($metric.Name): $covered/$total (100%)."
}
