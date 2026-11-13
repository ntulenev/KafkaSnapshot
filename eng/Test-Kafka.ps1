[CmdletBinding()]
param(
    [string]$BootstrapServers,
    [int]$Port = 19092,
    [switch]$Measure,
    [ValidateRange(1, 600)]
    [int]$ReadinessTimeoutSeconds = 60
)
$ErrorActionPreference = 'Stop'
Set-StrictMode -Version Latest
$repoRoot = Split-Path -Parent $PSScriptRoot
$container = $null
$previousBootstrap = $env:KAFKA_SNAPSHOT_TEST_BOOTSTRAP
Push-Location $repoRoot
try {
    $results = Join-Path $repoRoot "artifacts/broker-results/$([guid]::NewGuid().ToString('N'))"
    New-Item -ItemType Directory -Path $results -Force | Out-Null
    Write-Host "Broker results and diagnostics: $results"
    dotnet build eng/KafkaProbe/KafkaProbe.csproj --configuration Release
    if ($LASTEXITCODE -ne 0) { throw 'Building the Kafka readiness probe failed.' }

    if ([string]::IsNullOrWhiteSpace($BootstrapServers)) {
        $container = "kafka-snapshot-tests-$([guid]::NewGuid().ToString('N'))"
        $BootstrapServers = "127.0.0.1:$Port"
        $dockerArgs = @(
            'run', '-d', '--name', $container, '-p', "127.0.0.1:${Port}:9092",
            '-e', 'KAFKA_NODE_ID=1', '-e', 'KAFKA_PROCESS_ROLES=broker,controller',
            '-e', 'KAFKA_LISTENERS=INTERNAL://:9094,EXTERNAL://:9092,CONTROLLER://:9093',
            '-e', "KAFKA_ADVERTISED_LISTENERS=INTERNAL://localhost:9094,EXTERNAL://127.0.0.1:$Port",
            '-e', 'KAFKA_INTER_BROKER_LISTENER_NAME=INTERNAL',
            '-e', 'KAFKA_CONTROLLER_LISTENER_NAMES=CONTROLLER',
            '-e', 'KAFKA_LISTENER_SECURITY_PROTOCOL_MAP=CONTROLLER:PLAINTEXT,INTERNAL:PLAINTEXT,EXTERNAL:PLAINTEXT',
            '-e', 'KAFKA_CONTROLLER_QUORUM_VOTERS=1@localhost:9093',
            '-e', 'KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR=1',
            '-e', 'KAFKA_TRANSACTION_STATE_LOG_REPLICATION_FACTOR=1',
            '-e', 'KAFKA_TRANSACTION_STATE_LOG_MIN_ISR=1',
            'apache/kafka:4.4.0@sha256:833b867a9585a141da455dc2b57431c65cbccb27f11c91f694444cca9f777e0f'
        )
        & docker @dockerArgs
        if ($LASTEXITCODE -ne 0) { throw 'Starting the Kafka test broker failed.' }
    }
    & dotnet eng/KafkaProbe/bin/Release/net10.0/KafkaProbe.dll $BootstrapServers $ReadinessTimeoutSeconds 2>&1 |
        Tee-Object -FilePath (Join-Path $results 'readiness.log')
    if ($LASTEXITCODE -ne 0) { throw 'Kafka readiness check failed. See readiness.log.' }

    $env:KAFKA_SNAPSHOT_TEST_BOOTSTRAP = $BootstrapServers
    dotnet test src/Import.Tests/Import.Tests.csproj --configuration Release `
        --filter 'Category=Broker' --logger 'trx;LogFileName=broker.trx' --results-directory $results `
        --blame-hang-timeout 3m --blame-hang-dump-type mini
    if ($LASTEXITCODE -ne 0) { throw 'Real Kafka integration tests failed.' }
    [xml]$trx = Get-Content -LiteralPath (Join-Path $results 'broker.trx') -Raw
    $counters = $trx.TestRun.ResultSummary.Counters
    if ([int]$counters.executed -le 0 -or [int]$counters.notExecuted -ne 0 -or
        [int]$counters.passed -ne [int]$counters.total) {
        throw 'The broker suite must execute at least one test and pass every test without skips.'
    }
    if ($Measure) {
        & (Join-Path $PSScriptRoot 'Measure.ps1') -BootstrapServers $BootstrapServers
    }
}
finally {
    $env:KAFKA_SNAPSHOT_TEST_BOOTSTRAP = $previousBootstrap
    if ($container) {
        # Save evidence before removing only the container created by this invocation.
        foreach ($diagnostic in @('logs', 'inspect')) {
            try {
                & docker $diagnostic $container 2>&1 |
                    Out-File -LiteralPath (Join-Path $results "docker-$diagnostic.log") -Encoding utf8
            }
            catch { Write-Warning "Could not collect docker ${diagnostic}: $_" }
        }
        try {
            docker rm -f $container | Out-Null
            if ($LASTEXITCODE -ne 0) { Write-Warning "Could not remove test container $container." }
        }
        catch { Write-Warning "Could not remove test container ${container}: $_" }
    }
    Pop-Location
}
