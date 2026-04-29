param(
  [string]$Base = "https://xn--n1afe0b.online/agent"
)

$ErrorActionPreference = "Stop"
$AgentDir = Join-Path $env:LOCALAPPDATA "soty-agent"
$AgentPath = Join-Path $AgentDir "soty-agent.mjs"
$RunnerPath = Join-Path $AgentDir "start-agent.ps1"
$ManifestUrl = "$Base/manifest.json"
$AgentUrl = "$Base/soty-agent.mjs"

New-Item -ItemType Directory -Force -Path $AgentDir | Out-Null

function Resolve-Node {
  $node = Get-Command node -ErrorAction SilentlyContinue
  if ($node) { return $node.Source }

  $winget = Get-Command winget -ErrorAction SilentlyContinue
  if ($winget) {
    & winget install -e --id OpenJS.NodeJS.LTS --silent --accept-package-agreements --accept-source-agreements
    $node = Get-Command node -ErrorAction SilentlyContinue
    if ($node) { return $node.Source }
  }

  Start-Process "https://nodejs.org/"
  throw "Node.js is required"
}

$NodePath = Resolve-Node
Invoke-WebRequest -Uri $ManifestUrl -UseBasicParsing -OutFile (Join-Path $AgentDir "manifest.json")
Invoke-WebRequest -Uri $AgentUrl -UseBasicParsing -OutFile $AgentPath

@"
`$env:SOTY_AGENT_MANAGED = "1"
`$env:SOTY_AGENT_UPDATE_URL = "$ManifestUrl"
while (`$true) {
  & "$NodePath" "$AgentPath"
  `$code = `$LASTEXITCODE
  if (`$code -eq 75) {
    Start-Sleep -Seconds 1
  } else {
    Start-Sleep -Seconds 3
  }
}
"@ | Set-Content -Path $RunnerPath -Encoding UTF8

$Action = New-ScheduledTaskAction -Execute "powershell.exe" -Argument "-NoProfile -ExecutionPolicy Bypass -WindowStyle Hidden -File `"$RunnerPath`""
$Trigger = New-ScheduledTaskTrigger -AtLogOn
$Settings = New-ScheduledTaskSettingsSet -AllowStartIfOnBatteries -ExecutionTimeLimit 0 -MultipleInstances IgnoreNew
Register-ScheduledTask -TaskName "soty-agent" -Action $Action -Trigger $Trigger -Settings $Settings -Description "soty.online local agent" -Force | Out-Null
Start-ScheduledTask -TaskName "soty-agent"

Write-Output "soty-agent:installed"
