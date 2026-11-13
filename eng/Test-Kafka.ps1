[CmdletBinding()]
param(
    [string]$BootstrapServers,
    [int]$Port = 19092,
    [switch]$Measure
)
$ErrorActionPreference = 'Stop'
Set-StrictMode -Version Latest
$repoRoot = Split-Path -Parent $PSScriptRoot
$container = $null
$previousBootstrap = $env:KAFKA_SNAPSHOT_TEST_BOOTSTRAP
Push-Location $repoRoot
try {
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
        $deadline = [DateTime]::UtcNow.AddMinutes(1)
        while ($true) {
            $logs = docker logs $container 2>&1 | Out-String
            if ($logs -match 'Kafka Server started') { break }
            if ([DateTime]::UtcNow -ge $deadline) { throw "Kafka did not start: $logs" }
            Start-Sleep -Milliseconds 500
        }
    }
    $env:KAFKA_SNAPSHOT_TEST_BOOTSTRAP = $BootstrapServers
    $results = Join-Path $repoRoot "artifacts/broker-results/$([guid]::NewGuid().ToString('N'))"
    dotnet test src/Import.Tests/Import.Tests.csproj --configuration Release `
        --filter 'Category=Broker' --logger trx --results-directory $results
    if ($LASTEXITCODE -ne 0) { throw 'Real Kafka integration tests failed.' }
    if ($Measure) {
        & (Join-Path $PSScriptRoot 'Measure.ps1') -BootstrapServers $BootstrapServers
    }
}
finally {
    $env:KAFKA_SNAPSHOT_TEST_BOOTSTRAP = $previousBootstrap
    if ($container) {
        docker rm -f $container | Out-Null
    }
    Pop-Location
}
