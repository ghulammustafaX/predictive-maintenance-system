$ErrorActionPreference = "Stop"
$guard = Join-Path $PSScriptRoot "verify-compose-isolation.ps1"
$temp = Join-Path ([System.IO.Path]::GetTempPath()) "pms-compose-isolation-tests-$([guid]::NewGuid().ToString('N'))"
New-Item -ItemType Directory -Path $temp | Out-Null

function Write-Fixture([string]$Name, [string]$Text) {
    $path = Join-Path $temp "$Name.json"
    Set-Content -LiteralPath $path -Value $Text -Encoding utf8
    return $path
}

function Assert-Pass([string]$Name, [string]$Fixture, [string]$Project, [switch]$Testing) {
    & $guard -ComposeFile "unused.yml" -ExpectedProject $Project -RenderedConfigPath $Fixture -SkipExistingResourceCheck -Testing:$Testing | Out-Null
    if (-not $?) { throw "Expected pass: $Name" }
}

function Assert-Fail([string]$Name, [string]$Fixture, [string]$Project, [string]$Expected) {
    $result = Start-Process -FilePath "powershell.exe" -ArgumentList @(
        "-NoProfile", "-ExecutionPolicy", "Bypass", "-File", $guard,
        "-ComposeFile", "unused.yml", "-ExpectedProject", $Project,
        "-RenderedConfigPath", $Fixture, "-SkipExistingResourceCheck", "-Testing"
    ) -Wait -PassThru -WindowStyle Hidden
    if ($result.ExitCode -eq 0) { throw "Expected failure: $Name" }
}

$base = '{"name":"pms-isolated","services":{"app":{"container_name":"pms-isolated-app","ports":[{"published":19092,"target":9092}],"environment":{"KAFKA_BOOTSTRAP_SERVERS":"kafka:9092","INFLUXDB_BUCKET":"pms-isolated-bucket","GROUP_ID":"pms-isolated-group"}}},"networks":{"isolated":{"name":"pms-isolated-network"}},"volumes":{"data":{"name":"pms-isolated-data"}}}'
$pass = Write-Fixture "pass" $base
Assert-Pass "isolated project" $pass "pms-isolated" -Testing

$cases = @(
    @("production project", $base.Replace("pms-isolated", "predictive-maintenance-system"), "production project"),
    @("container collision", $base.Replace("pms-isolated-app", "pms-kafka"), "production container"),
    @("volume collision", $base.Replace("pms-isolated-data", "predictive-maintenance-system_influxdb-data"), "production volume"),
    @("network collision", $base.Replace("pms-isolated-network", "predictive-maintenance-system_pms-network"), "production network"),
    @("Kafka endpoint", $base.Replace("kafka:9092", "localhost:9092"), "production Kafka endpoint"),
    @("Influx bucket", $base.Replace("pms-isolated-bucket", "sensor-data"), "production InfluxDB bucket"),
    @("host port", $base.Replace("19092", "9092"), "production host port"),
    @("consumer group", $base.Replace("pms-isolated-group", "influxdb-writer-group"), "production consumer group")
)
foreach ($case in $cases) {
    $fixture = Write-Fixture ($case[2] -replace "\s", "-") $case[1]
    Assert-Fail $case[2] $fixture "pms-isolated" $case[2]
}

$contaminatedNetwork = docker network inspect pms-m6-test-network --format "{{json .Labels}}" 2>$null
if ($LASTEXITCODE -eq 0) {
    $fixture = Write-Fixture "existing-network-ownership" $base.Replace("pms-isolated-network", "pms-m6-test-network")
    Assert-Fail "existing network ownership" $fixture "pms-isolated" "existing network ownership"
}

Remove-Item -LiteralPath $temp -Recurse -Force
Write-Output "Compose isolation guard tests passed."
