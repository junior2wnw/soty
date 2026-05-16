param(
  [string]$Base = "https://xn--n1afe0b.online/agent",
  [string]$Revision = "",
  [string]$RelayId = "",
  [string]$DeviceId = "",
  [string]$DeviceNick = ""
)

$ErrorActionPreference = "Stop"
[Net.ServicePointManager]::SecurityProtocol = [Net.SecurityProtocolType]::Tls12

$BootstrapDir = Join-Path $env:TEMP "soty-agent-machine"
New-Item -ItemType Directory -Force -Path $BootstrapDir | Out-Null
$BootstrapLog = Join-Path $BootstrapDir "bootstrap.log"

function Write-BootstrapLog {
  param([string]$Message)
  $Message | Out-File -LiteralPath $BootstrapLog -Encoding ASCII -Append
  Write-Output $Message
}

function ConvertTo-PsSingleQuotedLiteral {
  param([string]$Value)
  if ($null -eq $Value) { $Value = "" }
  return "'" + $Value.Replace("'", "''") + "'"
}

function Test-SotyMachineHealth {
  param([string]$ExpectedVersion = "")
  try {
    $request = [System.Net.WebRequest]::Create("http://127.0.0.1:49424/health")
    $request.Method = "GET"
    $request.Timeout = 2500
    $request.ReadWriteTimeout = 2500
    $request.Headers.Add("Origin", "https://xn--n1afe0b.online")
    $response = $request.GetResponse()
    try {
      $reader = New-Object System.IO.StreamReader($response.GetResponseStream())
      $health = $reader.ReadToEnd() | ConvertFrom-Json
    } finally {
      try { $reader.Dispose() } catch {}
      try { $response.Dispose() } catch {}
    }
    if (-not (($health.scope -eq "Machine") -and ($health.system -eq $true))) {
      return $false
    }
    if ([string]::IsNullOrWhiteSpace($ExpectedVersion) -or $ExpectedVersion -eq "unknown") {
      return $true
    }
    $actualVersion = [string]$health.version
    try {
      return ([version]$actualVersion -ge [version]$ExpectedVersion)
    } catch {
      return ($actualVersion -eq $ExpectedVersion)
    }
  } catch {
    return $false
  }
}

function Wait-SotyMachineHealth {
  param(
    [int]$Seconds = 20,
    [string]$ExpectedVersion = ""
  )
  $deadline = (Get-Date).AddSeconds($Seconds)
  do {
    if (Test-SotyMachineHealth -ExpectedVersion $ExpectedVersion) { return $true }
    Start-Sleep -Milliseconds 700
  } while ((Get-Date) -lt $deadline)
  return $false
}

if ([string]::IsNullOrWhiteSpace($Revision)) {
  $Revision = "unknown"
}

Write-BootstrapLog ("soty-agent-machine:bootstrap-elevate:" + $Revision)

$baseLiteral = ConvertTo-PsSingleQuotedLiteral $Base
$revisionLiteral = ConvertTo-PsSingleQuotedLiteral $Revision
$relayLiteral = ConvertTo-PsSingleQuotedLiteral $RelayId
$deviceLiteral = ConvertTo-PsSingleQuotedLiteral $DeviceId
$nickLiteral = ConvertTo-PsSingleQuotedLiteral $DeviceNick

$elevatedLines = @(
  '$ErrorActionPreference = ''Stop''',
  '[Net.ServicePointManager]::SecurityProtocol = [Net.SecurityProtocolType]::Tls12',
  '$stage = Join-Path $env:ProgramData ''Soty\agent-install''',
  'New-Item -ItemType Directory -Force -Path $stage | Out-Null',
  '$script = Join-Path $stage ''install-windows.ps1''',
  '$log = Join-Path $stage ''bootstrap-elevated.log''',
  'function Add-Log([string]$Message) { $Message | Out-File -LiteralPath $log -Encoding ASCII -Append; Write-Output $Message }',
  '$base = ' + $baseLiteral,
  '$revision = ' + $revisionLiteral,
  '$relay = ' + $relayLiteral,
  '$device = ' + $deviceLiteral,
  '$nick = ' + $nickLiteral,
  'Add-Log (''soty-agent-machine:elevated-download:'' + $revision)',
  '$uri = $base.TrimEnd(''/'') + ''/install-windows.ps1''',
  'if (-not [string]::IsNullOrWhiteSpace($revision) -and $revision -ne ''unknown'') { $uri += ''?v='' + [uri]::EscapeDataString($revision) }',
  'Invoke-WebRequest -Uri $uri -UseBasicParsing -OutFile $script -TimeoutSec 60 -ErrorAction Stop',
  'Add-Log (''soty-agent-machine:elevated-run:'' + $revision)',
  '$installArgs = @(''-NoLogo'', ''-NoProfile'', ''-ExecutionPolicy'', ''Bypass'', ''-File'', $script, ''-Base'', $base, ''-Scope'', ''Machine'', ''-LaunchAppAtLogon'', ''-RelayId'', $relay)',
  'if (-not [string]::IsNullOrWhiteSpace($device)) { $installArgs += @(''-DeviceId'', $device); if (-not [string]::IsNullOrWhiteSpace($nick)) { $installArgs += @(''-DeviceNick'', $nick) } }',
  '& powershell.exe @installArgs',
  '$code = if ($null -eq $LASTEXITCODE) { 0 } else { [int]$LASTEXITCODE }',
  'Add-Log (''soty-agent-machine:elevated-exit:'' + $code)',
  'exit $code'
)

$elevatedScript = $elevatedLines -join "`r`n"
$encoded = [Convert]::ToBase64String([Text.Encoding]::Unicode.GetBytes($elevatedScript))
$process = Start-Process -FilePath "powershell.exe" -Verb RunAs -ArgumentList @("-NoLogo", "-NoProfile", "-ExecutionPolicy", "Bypass", "-EncodedCommand", $encoded) -Wait -PassThru
$exitCode = if ($null -eq $process.ExitCode) { 1 } else { [int]$process.ExitCode }
Write-BootstrapLog ("soty-agent-machine:bootstrap-elevated-exit:" + $exitCode)

if ($exitCode -eq 0) {
  exit 0
}

if (Wait-SotyMachineHealth -Seconds 25 -ExpectedVersion $Revision) {
  Write-BootstrapLog "soty-agent-machine:bootstrap-health-ok-after-nonzero"
  exit 0
}

exit $exitCode
