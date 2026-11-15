[CmdletBinding()]
param(
    [Parameter(Mandatory = $true)]
    [string]$ComposeFile,

    [Parameter(Mandatory = $true)]
    [string]$ExpectedProject,

    [switch]$Testing,

    [string]$RenderedConfigPath,

    [switch]$SkipExistingResourceCheck
)

$ErrorActionPreference = "Stop"

$productionProject = "predictive-maintenance-system"
$productionContainers = @(
    "pms-zookeeper", "pms-kafka", "pms-kafka-topic-init", "pms-influxdb",
    "pms-postgres", "pms-backend", "pms-consumer", "pms-prediction",
    "pms-preprocessing", "pms-simulator"
)
$productionVolumes = @(
    "predictive-maintenance-system_influxdb-data",
    "predictive-maintenance-system_postgres-data"
)
$productionNetworks = @("predictive-maintenance-system_pms-network", "pms-network")
$productionPorts = @(2181, 9092, 8086, 5433, 8000, 8001)
$productionEndpoints = @("localhost:9092", "127.0.0.1:9092", "pms-kafka:9092")
$productionBuckets = @("sensor-data")
$productionGroups = @(
    "influxdb-writer-group",
    "preprocessing-service-group",
    "rul-prediction-service-group"
)

function Fail-Guard([string]$Message) {
    Write-Error "Compose isolation check failed: $Message"
    exit 1
}

function Test-ContainsIdentifier([string]$Text, [string]$Identifier) {
    return $Text -match "(?<![A-Za-z0-9_-])$([regex]::Escape($Identifier))(?![A-Za-z0-9_-])"
}

function Get-RenderedConfig {
    if ($RenderedConfigPath) {
        if (-not (Test-Path -LiteralPath $RenderedConfigPath -PathType Leaf)) {
            Fail-Guard "Rendered configuration file was not found: $RenderedConfigPath"
        }
        return (Get-Content -LiteralPath $RenderedConfigPath -Raw | ConvertFrom-Json)
    }

    if (-not (Test-Path -LiteralPath $ComposeFile -PathType Leaf)) {
        Fail-Guard "Compose file was not found: $ComposeFile"
    }

    $rendered = & docker compose -p $ExpectedProject -f $ComposeFile config --format json 2>&1
    if ($LASTEXITCODE -ne 0) {
        Fail-Guard "Docker Compose could not render the effective configuration."
    }
    try {
        return ($rendered -join [Environment]::NewLine | ConvertFrom-Json)
    }
    catch {
        Fail-Guard "Docker Compose returned invalid rendered configuration."
    }
}

if ($Testing -and $ExpectedProject -eq $productionProject) {
    Fail-Guard "testing configurations may not use the production project '$productionProject'."
}

$config = Get-RenderedConfig
$configText = $config | ConvertTo-Json -Depth 100 -Compress

if ($ExpectedProject -eq $productionProject -and $Testing) {
    Fail-Guard "testing configurations may not target the production project."
}

if ($config.name -and $config.name -ne $ExpectedProject) {
    Fail-Guard "rendered project identity '$($config.name)' does not match expected '$ExpectedProject'."
}

foreach ($container in $productionContainers) {
    if ($Testing -and (Test-ContainsIdentifier $configText $container)) {
        Fail-Guard "testing configuration references production container '$container'."
    }
}

foreach ($volume in $productionVolumes) {
    if ($Testing -and (Test-ContainsIdentifier $configText $volume)) {
        Fail-Guard "configuration references protected production volume '$volume'."
    }
}

foreach ($network in $productionNetworks) {
    if ($Testing -and (Test-ContainsIdentifier $configText $network)) {
        Fail-Guard "testing configuration references production network '$network'."
    }
}

foreach ($endpoint in $productionEndpoints) {
    if ($Testing -and (Test-ContainsIdentifier $configText $endpoint)) {
        Fail-Guard "testing configuration references production Kafka endpoint '$endpoint'."
    }
}

foreach ($bucket in $productionBuckets) {
    $bucketPattern = "(?i)(INFLUXDB_BUCKET|DOCKER_INFLUXDB_INIT_BUCKET|bucket)[^`"]*`"$([regex]::Escape($bucket))`""
    if ($Testing -and $configText -match $bucketPattern) {
        Fail-Guard "testing configuration references production InfluxDB bucket '$bucket'."
    }
}

foreach ($group in $productionGroups) {
    if ($Testing -and (Test-ContainsIdentifier $configText $group)) {
        Fail-Guard "testing configuration references production consumer group '$group'."
    }
}

$ports = @()
foreach ($service in $config.services.PSObject.Properties) {
    foreach ($port in @($service.Value.ports)) {
        if ($port.published) {
            $ports += [int]$port.published
        }
    }
}
if ($Testing) {
    foreach ($port in $productionPorts) {
        if ($ports -contains $port) {
            Fail-Guard "testing configuration reuses production host port $port."
        }
    }
}

if (-not $SkipExistingResourceCheck) {
    $existing = @(& docker ps -a --format "{{json .}}" 2>$null)
    if ($LASTEXITCODE -ne 0) {
        Fail-Guard "could not inspect existing Docker containers."
    }
    foreach ($line in $existing) {
        if (-not $line) { continue }
        try { $item = $line | ConvertFrom-Json } catch { continue }
        if ($Testing -and $item.Names -and (Test-ContainsIdentifier $configText $item.Names)) {
            $existingProject = (& docker inspect $item.Names --format "{{index .Config.Labels `"com.docker.compose.project`"}}" 2>$null).Trim()
            if ($existingProject -ne $ExpectedProject) {
                Fail-Guard "testing configuration collides with container '$($item.Names)' from project '$existingProject'."
            }
        }
    }

    foreach ($networkProperty in $config.networks.PSObject.Properties) {
        $networkName = if ($networkProperty.Value.name) { $networkProperty.Value.name } else {
            "$ExpectedProject`_$($networkProperty.Name)"
        }
        $networkJson = & docker network inspect $networkName --format "{{json .Labels}}" 2>$null
        if ($LASTEXITCODE -eq 0) {
            $labels = $networkJson | ConvertFrom-Json
            $resourceProject = $labels.'com.docker.compose.project'
            if ($Testing -and $resourceProject -ne $ExpectedProject) {
                Fail-Guard "testing configuration reuses network '$networkName' owned by project '$resourceProject'."
            }
            if ($Testing -and -not $resourceProject) {
                Fail-Guard "testing configuration reuses network '$networkName' without a Compose project label."
            }
        }
    }

    foreach ($volumeProperty in $config.volumes.PSObject.Properties) {
        $volumeName = if ($volumeProperty.Value.name) { $volumeProperty.Value.name } else {
            "$ExpectedProject`_$($volumeProperty.Name)"
        }
        $volumeJson = & docker volume inspect $volumeName --format "{{json .Labels}}" 2>$null
        if ($LASTEXITCODE -eq 0) {
            $labels = $volumeJson | ConvertFrom-Json
            $resourceProject = $labels.'com.docker.compose.project'
            if ($Testing -and $resourceProject -ne $ExpectedProject) {
                Fail-Guard "testing configuration reuses volume '$volumeName' owned by project '$resourceProject'."
            }
            if ($Testing -and -not $resourceProject) {
                Fail-Guard "testing configuration reuses volume '$volumeName' without a Compose project label."
            }
        }
    }
}

Write-Output "Compose isolation check passed: project '$ExpectedProject'."
Write-Output "Rendered configuration verified without starting services."
