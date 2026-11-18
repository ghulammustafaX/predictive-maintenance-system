#!/usr/bin/env pwsh
<#
.SYNOPSIS
  Guard: verifies that a proposed docker-compose project is isolated from
  the production PMS project before any containers are started.

.USAGE
  .\scripts\verify-compose-isolation.ps1 -TestProject "pms-m1-test"
#>

param(
    [Parameter(Mandatory=$true)]
    [string]$TestProject
)

$PRODUCTION_PROJECT  = "predictive-maintenance-system"
$PRODUCTION_VOLUMES  = @(
    "${PRODUCTION_PROJECT}_kafka-data",
    "${PRODUCTION_PROJECT}_zookeeper-data",
    "${PRODUCTION_PROJECT}_zookeeper-log",
    "${PRODUCTION_PROJECT}_influxdb-data",
    "${PRODUCTION_PROJECT}_postgres-data"
)
$PRODUCTION_NETWORKS = @("${PRODUCTION_PROJECT}_pms-network")
$PRODUCTION_CONTAINERS = @(
    "pms-kafka","pms-zookeeper","pms-influxdb","pms-postgres",
    "pms-simulator","pms-consumer","pms-preprocessing","pms-prediction","pms-backend"
)
$PRODUCTION_TOPICS   = @("sensor-telemetry","preprocessed-telemetry","rul-predictions","sensor-telemetry-dead-letter")

$errors = @()

Write-Host "=== Compose Isolation Guard ===" -ForegroundColor Cyan
Write-Host "Test project : $TestProject"
Write-Host "Production   : $PRODUCTION_PROJECT"
Write-Host ""

# 1. Verify test project name does not equal production
if ($TestProject -eq $PRODUCTION_PROJECT) {
    $errors += "FATAL: TestProject name equals production project name"
}

# 2. Verify no production volumes are referenced in the test compose file
$testComposeFiles = Get-ChildItem "D:\My Projects\MyFYP\predictive-maintenance-system" -Filter "docker-compose.*.yml" |
    Where-Object { $_.Name -match $TestProject.Replace("pms-","") }
foreach ($f in $testComposeFiles) {
    $content = Get-Content $f.FullName -Raw
    foreach ($vol in $PRODUCTION_VOLUMES) {
        if ($content -match [regex]::Escape($vol)) {
            $errors += "Volume '$vol' referenced in $($f.Name)"
        }
    }
}

# 3. Verify no running test containers share names with production
$runningNames = docker ps --format "{{.Names}}" 2>&1
foreach ($prod in $PRODUCTION_CONTAINERS) {
    if ($runningNames -contains $prod) {
        $errors += "Production container '$prod' is currently running — test must not attach to it"
    }
}

# 4. Verify test project uses different ports (check against known production ports)
$PRODUCTION_PORTS = @(9092, 2181, 8086, 5433, 8000, 8001)
foreach ($f in $testComposeFiles) {
    $content = Get-Content $f.FullName -Raw
    foreach ($port in $PRODUCTION_PORTS) {
        # Look for exact port mappings like "- "9092:9092""
        if ($content -match "(?m)^\s+-\s+`"?${port}:${port}`"?") {
            $errors += "Port $port in $($f.Name) conflicts with a production port"
        }
    }
}

# 5. Report
if ($errors.Count -eq 0) {
    Write-Host "ISOLATION CHECK: PASS — no production resource conflicts detected" -ForegroundColor Green
    exit 0
} else {
    Write-Host "ISOLATION CHECK: FAIL" -ForegroundColor Red
    $errors | ForEach-Object { Write-Host "  [!] $_" -ForegroundColor Yellow }
    exit 1
}
