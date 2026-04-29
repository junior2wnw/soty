param(
  [string]$Base = "https://xn--n1afe0b.online/agent"
)

$ErrorActionPreference = "Stop"
[Net.ServicePointManager]::SecurityProtocol = [Net.SecurityProtocolType]::Tls12

$AgentDir = Join-Path $env:LOCALAPPDATA "soty-agent"
$AgentPath = Join-Path $AgentDir "soty-agent.mjs"
$RunnerPath = Join-Path $AgentDir "start-agent.ps1"
$LogPath = Join-Path $AgentDir "install.log"
$ManifestUrl = "$Base/manifest.json"
$AgentUrl = "$Base/soty-agent.mjs"

New-Item -ItemType Directory -Force -Path $AgentDir | Out-Null
Start-Transcript -Path $LogPath -Append | Out-Null

try {
  function Resolve-Node {
    $node = Get-Command node -ErrorAction SilentlyContinue
    if ($node) { return $node.Source }

    $LocalNode = Join-Path $AgentDir "node\node.exe"
    if (Test-Path $LocalNode) { return $LocalNode }

    $arch = "win-x64"
    if ($env:PROCESSOR_ARCHITECTURE -match "ARM64") { $arch = "win-arm64" }

    try {
      $index = Invoke-RestMethod -Uri "https://nodejs.org/dist/index.json" -UseBasicParsing
      $release = $index | Where-Object { $_.lts -and ($_.files -contains $arch) } | Select-Object -First 1
      if (-not $release) { throw "No Node.js LTS release for $arch" }

      $zipName = "node-$($release.version)-$arch.zip"
      $zipUrl = "https://nodejs.org/dist/$($release.version)/$zipName"
      $zipPath = Join-Path $AgentDir $zipName
      $extractDir = Join-Path $AgentDir "node-download"
      $nodeDir = Join-Path $AgentDir "node"

      Remove-Item -LiteralPath $extractDir -Recurse -Force -ErrorAction SilentlyContinue
      Invoke-WebRequest -Uri $zipUrl -UseBasicParsing -OutFile $zipPath
      Expand-Archive -LiteralPath $zipPath -DestinationPath $extractDir -Force
      Remove-Item -LiteralPath $nodeDir -Recurse -Force -ErrorAction SilentlyContinue
      $inner = Get-ChildItem -LiteralPath $extractDir -Directory | Select-Object -First 1
      if (-not $inner) { throw "Node.js archive is empty" }
      Move-Item -LiteralPath $inner.FullName -Destination $nodeDir
      Remove-Item -LiteralPath $zipPath -Force -ErrorAction SilentlyContinue
      Remove-Item -LiteralPath $extractDir -Recurse -Force -ErrorAction SilentlyContinue
      return (Join-Path $nodeDir "node.exe")
    } catch {
      $winget = Get-Command winget -ErrorAction SilentlyContinue
      if ($winget) {
        & winget install -e --id OpenJS.NodeJS.LTS --silent --accept-package-agreements --accept-source-agreements
        $node = Get-Command node -ErrorAction SilentlyContinue
        if ($node) { return $node.Source }
      }
      Start-Process "https://nodejs.org/"
      throw
    }
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

  $Action = New-ScheduledTaskAction -Execute "powershell.exe" -Argument "-NoLogo -NoProfile -ExecutionPolicy Bypass -WindowStyle Hidden -File `"$RunnerPath`""
  $Trigger = New-ScheduledTaskTrigger -AtLogOn
  $Settings = New-ScheduledTaskSettingsSet -AllowStartIfOnBatteries -ExecutionTimeLimit 0 -MultipleInstances IgnoreNew
  Register-ScheduledTask -TaskName "soty-agent" -Action $Action -Trigger $Trigger -Settings $Settings -Description "soty.online local agent" -Force | Out-Null
  Start-ScheduledTask -TaskName "soty-agent"

  Start-Sleep -Milliseconds 700
  try {
    Invoke-RestMethod -Uri "http://127.0.0.1:49424/health" -Headers @{ Origin = "https://xn--n1afe0b.online" } | Out-Null
  } catch {
    Start-Process -WindowStyle Hidden -FilePath "powershell.exe" -ArgumentList "-NoLogo -NoProfile -ExecutionPolicy Bypass -File `"$RunnerPath`""
  }

  Write-Output "soty-agent:installed"
} finally {
  Stop-Transcript | Out-Null
}
