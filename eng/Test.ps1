[CmdletBinding()]
param()

$ErrorActionPreference = 'Stop'
Set-StrictMode -Version Latest
$repoRoot = Split-Path -Parent $PSScriptRoot
Push-Location $repoRoot
try {
    # Each invocation has its own directory, so old results cannot inflate coverage.
    $results = Join-Path $repoRoot "artifacts/test-results/$([guid]::NewGuid().ToString('N'))"
    $rawResults = Join-Path $results 'raw'
    $reportDirectory = Join-Path $results 'report'

    dotnet tool restore
    if ($LASTEXITCODE -ne 0) { throw 'Restoring local .NET tools failed.' }

    dotnet test src/KafkaSnapshot.slnx --configuration Release `
        --settings coverage.runsettings --collect 'XPlat Code Coverage' `
        --results-directory $rawResults --logger trx `
        --blame-hang-timeout 3m --blame-hang-dump-type mini
    if ($LASTEXITCODE -ne 0) { throw 'The test suite failed.' }

    $reports = @(Get-ChildItem -LiteralPath $rawResults -Filter coverage.opencover.xml -Recurse |
        Where-Object { $_.Directory.Parent.FullName -eq $rawResults })
    $testProjectCount = @(Get-ChildItem -Path src -Filter '*.Tests.csproj' -Recurse).Count
    if ($reports.Count -ne $testProjectCount) {
        throw "Expected coverage from $testProjectCount test projects; found $($reports.Count) reports."
    }

    # OpenCover keeps individual branch identities when merging multiple test projects.
    dotnet reportgenerator "-reports:$($reports.FullName -join ';')" `
        "-targetdir:$reportDirectory" '-reporttypes:Html;TextSummary;XmlSummary;MarkdownSummaryGithub'
    if ($LASTEXITCODE -ne 0) { throw 'Generating the coverage report failed.' }

    Get-Content -LiteralPath (Join-Path $reportDirectory 'Summary.txt')
    if ($env:GITHUB_STEP_SUMMARY) {
        Get-Content -LiteralPath (Join-Path $reportDirectory 'SummaryGithub.md') |
            Add-Content -LiteralPath $env:GITHUB_STEP_SUMMARY
    }
    & (Join-Path $PSScriptRoot 'Assert-Coverage.ps1') -SummaryPath (Join-Path $reportDirectory 'Summary.xml')
    Write-Host "HTML coverage report: $(Join-Path $reportDirectory 'index.html')"
}
finally {
    Pop-Location
}
