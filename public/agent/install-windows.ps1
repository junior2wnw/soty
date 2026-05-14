param(
  [string]$Base = "https://xn--n1afe0b.online/agent",
  [ValidateSet("CurrentUser", "Machine")]
  [string]$Scope = "CurrentUser",
  [string]$InstallDir = "",
  [switch]$LaunchAppAtLogon,
  [string]$AppUrl = "https://xn--n1afe0b.online/?pwa=1",
  [string]$RelayId = "",
  [string]$CodexProxyUrl = "",
  [switch]$InstallCodex
)

$ErrorActionPreference = "Stop"
$ProgressPreference = "SilentlyContinue"
[Net.ServicePointManager]::SecurityProtocol = [Net.SecurityProtocolType]::Tls12

function Resolve-AgentDir {
  if (-not [string]::IsNullOrWhiteSpace($InstallDir)) {
    return $InstallDir
  }
  if ($Scope -eq "Machine") {
    return (Join-Path $env:ProgramData "soty-agent")
  }
  $local = $env:LOCALAPPDATA
  if ([string]::IsNullOrWhiteSpace($local)) {
    $local = Join-Path $HOME "AppData\Local"
  }
  return (Join-Path $local "soty-agent")
}

$AgentDir = Resolve-AgentDir
$AgentPath = Join-Path $AgentDir "soty-agent.mjs"
$RunnerPath = Join-Path $AgentDir "start-agent.ps1"
$CtlPath = Join-Path $AgentDir "sotyctl.cmd"
$LogPath = Join-Path $AgentDir "install.log"
$ProxyEnvPath = Join-Path $AgentDir "proxy.env"
$ManifestUrl = "$Base/manifest.json"
$AgentUrl = "$Base/soty-agent.mjs"

New-Item -ItemType Directory -Force -Path $AgentDir | Out-Null
Start-Transcript -Path $LogPath -Append | Out-Null

try {
  function Test-NodeRuntime {
    param([string]$Path)
    if (-not $Path -or -not (Test-Path -LiteralPath $Path)) { return $false }
    try {
      & $Path -e "const v=process.versions.node.split('.').map(Number); process.exit(v[0] > 22 || (v[0] === 22 && v[1] >= 12) ? 0 : 1)"
      return ($LASTEXITCODE -eq 0)
    } catch {
      return $false
    }
  }

  function Get-NodeWindowsArch {
    $archText = "$env:PROCESSOR_ARCHITECTURE $env:PROCESSOR_ARCHITEW6432".ToUpperInvariant()
    if ($archText -match "ARM64") { return "win-arm64" }
    if ($archText -match "AMD64") { return "win-x64" }
    throw "Unsupported Windows architecture: $archText"
  }

  function Select-NodeRelease {
    param([string]$Arch)
    $fileKey = "$Arch-zip"
    $index = Invoke-RestMethod -Uri "https://nodejs.org/dist/index.json" -UseBasicParsing
    $release = $index | Where-Object { $_.lts -and ($_.files -contains $fileKey) } | Select-Object -First 1
    if (-not $release) {
      $release = $index | Where-Object { $_.files -contains $fileKey } | Select-Object -First 1
    }
    if (-not $release) { throw "No Node.js zip release for $fileKey" }
    return $release
  }

  function Save-PortableNode {
    param(
      [string]$Arch,
      [object]$Release
    )
    $zipName = "node-$($Release.version)-$Arch.zip"
    $zipUrl = "https://nodejs.org/dist/$($Release.version)/$zipName"
    $sumUrl = "https://nodejs.org/dist/$($Release.version)/SHASUMS256.txt"
    $zipPath = Join-Path $AgentDir $zipName
    $sumPath = Join-Path $AgentDir "SHASUMS256.txt"
    $extractDir = Join-Path $AgentDir "node-download"
    $nodeDir = Join-Path $AgentDir "node"

    Remove-Item -LiteralPath $extractDir -Recurse -Force -ErrorAction SilentlyContinue
    Invoke-WebRequest -Uri $zipUrl -UseBasicParsing -OutFile $zipPath
    Invoke-WebRequest -Uri $sumUrl -UseBasicParsing -OutFile $sumPath
    $expected = (Select-String -LiteralPath $sumPath -Pattern "  $([regex]::Escape($zipName))$").Line -replace "\s+.*$", ""
    if (-not $expected) { throw "Node.js checksum is missing for $zipName" }
    $actual = (Get-FileHash -Algorithm SHA256 -LiteralPath $zipPath).Hash.ToLowerInvariant()
    if ($actual -ne $expected.ToLowerInvariant()) { throw "Node.js checksum mismatch" }

    Expand-Archive -LiteralPath $zipPath -DestinationPath $extractDir -Force
    Remove-Item -LiteralPath $nodeDir -Recurse -Force -ErrorAction SilentlyContinue
    $inner = Get-ChildItem -LiteralPath $extractDir -Directory | Select-Object -First 1
    if (-not $inner) { throw "Node.js archive is empty" }
    Move-Item -LiteralPath $inner.FullName -Destination $nodeDir
    Remove-Item -LiteralPath $zipPath -Force -ErrorAction SilentlyContinue
    Remove-Item -LiteralPath $sumPath -Force -ErrorAction SilentlyContinue
    Remove-Item -LiteralPath $extractDir -Recurse -Force -ErrorAction SilentlyContinue
    return (Join-Path $nodeDir "node.exe")
  }

  function Resolve-Node {
    $node = Get-Command node -ErrorAction SilentlyContinue
    if ($node -and (Test-NodeRuntime $node.Source)) { return $node.Source }

    $LocalNode = Join-Path $AgentDir "node\node.exe"
    if (Test-NodeRuntime $LocalNode) { return $LocalNode }

    $arch = Get-NodeWindowsArch

    try {
      $release = Select-NodeRelease $arch
      $portableNode = Save-PortableNode -Arch $arch -Release $release
      if (Test-NodeRuntime $portableNode) { return $portableNode }
      throw "Portable Node.js failed to start"
    } catch {
      $winget = Get-Command winget -ErrorAction SilentlyContinue
      if ($winget) {
        & winget install -e --id OpenJS.NodeJS.LTS --silent --accept-package-agreements --accept-source-agreements
        $node = Get-Command node -ErrorAction SilentlyContinue
        if ($node -and (Test-NodeRuntime $node.Source)) { return $node.Source }
        $programNode = Join-Path $env:ProgramFiles "nodejs\node.exe"
        if (Test-NodeRuntime $programNode) { return $programNode }
      }
      throw
    }
  }

  function Test-CodexCli {
    param([string]$Path)
    if (-not $Path -or -not (Test-Path -LiteralPath $Path)) { return $false }
    if ($Path -notmatch '\.(cmd|exe|bat)$') { return $false }
    try {
      $version = & $Path --version 2>$null
      return (($LASTEXITCODE -eq 0) -and ([string]$version -match 'codex'))
    } catch {
      return $false
    }
  }

  function Resolve-Npm {
    param([string]$NodePath)
    $nodeDir = Split-Path -Parent $NodePath
    foreach ($candidate in @(
      (Join-Path $nodeDir "npm.cmd"),
      (Join-Path $nodeDir "npm")
    )) {
      if (Test-Path -LiteralPath $candidate) { return $candidate }
    }
    $npm = Get-Command npm.cmd -ErrorAction SilentlyContinue
    if ($npm) { return $npm.Source }
    $npm = Get-Command npm -ErrorAction SilentlyContinue
    if ($npm -and ($npm.Source -match '\.(cmd|exe|bat)$')) { return $npm.Source }
    throw "npm is required to install stock Codex CLI"
  }

  function Find-StockCodexCli {
    param([string]$NodePath)
    $nodeDir = Split-Path -Parent $NodePath
    $candidates = @(
      (Join-Path $nodeDir "codex.cmd"),
      (Join-Path $nodeDir "codex.exe"),
      (Join-Path $nodeDir "codex.bat")
    )
    if ($env:APPDATA) { $candidates += (Join-Path $env:APPDATA "npm\codex.cmd") }
    $pathCodex = Get-Command codex.cmd -ErrorAction SilentlyContinue
    if ($pathCodex) { $candidates += $pathCodex.Source }
    $pathCodexExe = Get-Command codex.exe -ErrorAction SilentlyContinue
    if ($pathCodexExe) { $candidates += $pathCodexExe.Source }
    foreach ($candidate in $candidates) {
      if (Test-CodexCli $candidate) { return $candidate }
    }
    return ""
  }

  function Install-StockCodexCli {
    param([string]$NodePath)
    try {
      Remove-Item -LiteralPath (Join-Path $AgentDir "codex-cli") -Recurse -Force -ErrorAction SilentlyContinue
      Remove-Item -LiteralPath (Join-Path $AgentDir "codex-home") -Recurse -Force -ErrorAction SilentlyContinue
      $existing = Find-StockCodexCli $NodePath
      if ($existing) {
        Write-Output "soty-codex-cli:available:$existing"
        return $existing
      }
      $npm = Resolve-Npm $NodePath
      $nodeDir = Split-Path -Parent $NodePath
      $npmUserBin = if ($env:APPDATA) { Join-Path $env:APPDATA "npm" } else { "" }
      $env:PATH = (@($nodeDir, $npmUserBin, $env:PATH) | Where-Object { $_ }) -join ";"
      $codexInstallLog = Join-Path $AgentDir "codex-install.log"
      $escapedNpm = $npm.Replace('"', '\"')
      $escapedLog = $codexInstallLog.Replace('"', '\"')
      $cmdLine = "`"$escapedNpm`" install -g `"@openai/codex@latest`" --no-audit --no-fund > `"$escapedLog`" 2>&1"
      & $env:ComSpec /d /s /c $cmdLine
      if ($LASTEXITCODE -ne 0) {
        Write-Output "soty-codex-cli:install-skipped:$LASTEXITCODE"
        return ""
      }
      $installed = Find-StockCodexCli $NodePath
      if ($installed) {
        Write-Output "soty-codex-cli:installed:$installed"
        return $installed
      }
      Write-Output "soty-codex-cli:install-skipped:not-found"
      return ""
    } catch {
      Write-Output "soty-codex-cli:install-skipped:$($_.Exception.Message)"
      return ""
    }
  }

  function Test-AgentHealth {
    try {
      $health = Invoke-RestMethod -Uri "http://127.0.0.1:49424/health" -Headers @{ Origin = "https://xn--n1afe0b.online" } -TimeoutSec 2
      if ($Scope -eq "Machine") {
        return (($health.scope -eq "Machine") -and ($health.system -eq $true))
      }
      return $true
    } catch {
      return $false
    }
  }

  function Stop-ExistingSotyMachineAgents {
    if ($Scope -ne "Machine") { return }
    try {
      $agentDirPattern = [regex]::Escape($AgentDir)
      $processes = Get-CimInstance Win32_Process -ErrorAction SilentlyContinue |
        Where-Object {
          $_.CommandLine -and
          ($_.CommandLine -match "soty-agent") -and
          ($_.CommandLine -match $agentDirPattern) -and
          (($_.CommandLine -match "soty-agent\.mjs") -or ($_.CommandLine -match "start-agent\.ps1"))
        }
      foreach ($process in @($processes)) {
        try {
          Stop-Process -Id $process.ProcessId -Force -ErrorAction SilentlyContinue
          Write-Output "soty-agent:stopped-machine-process:$($process.ProcessId)"
        } catch {}
      }
    } catch {
      Write-Output "soty-agent:stop-machine-agent-skipped"
    }
  }

  function Wait-AgentHealth {
    param([int]$Seconds = 18)
    $deadline = (Get-Date).AddSeconds($Seconds)
    do {
      if (Test-AgentHealth) { return $true }
      Start-Sleep -Milliseconds 700
    } while ((Get-Date) -lt $deadline)
    return $false
  }

  function Start-AgentNow {
    if (Test-AgentHealth) { return }
    if ($Scope -eq "Machine") {
      try { Start-ScheduledTask -TaskName "soty-agent-machine" -ErrorAction SilentlyContinue } catch {}
      if (Wait-AgentHealth) {
        Write-Output "soty-agent:health:machine"
        return
      }
      throw "Soty machine task did not report SYSTEM health on 127.0.0.1:49424"
    }
    Start-Process -WindowStyle Hidden -FilePath "powershell.exe" -ArgumentList "-NoLogo -NoProfile -ExecutionPolicy Bypass -File `"$RunnerPath`""
  }

  function Enable-AgentAutostart {
    $runCommand = "powershell.exe -NoLogo -NoProfile -ExecutionPolicy Bypass -WindowStyle Hidden -File `"$RunnerPath`""

    if ($Scope -eq "Machine") {
      Stop-ExistingSotyMachineAgents
      try { Stop-ScheduledTask -TaskName "soty-agent-machine" -ErrorAction SilentlyContinue } catch {}
      $Action = New-ScheduledTaskAction -Execute "powershell.exe" -Argument "-NoLogo -NoProfile -ExecutionPolicy Bypass -WindowStyle Hidden -File `"$RunnerPath`""
      $Trigger = New-ScheduledTaskTrigger -AtStartup
      $Settings = New-ScheduledTaskSettingsSet -AllowStartIfOnBatteries -DontStopIfGoingOnBatteries -ExecutionTimeLimit 0 -MultipleInstances IgnoreNew -StartWhenAvailable
      $Principal = New-ScheduledTaskPrincipal -UserId "SYSTEM" -LogonType ServiceAccount -RunLevel Highest
      Register-ScheduledTask -TaskName "soty-agent-machine" -Action $Action -Trigger $Trigger -Settings $Settings -Principal $Principal -Description "soty.online machine local agent" -Force | Out-Null
      Start-ScheduledTask -TaskName "soty-agent-machine"
      Write-Output "soty-agent:autostart:machine-task"
      return "machine-task"
    }

    try {
      $Action = New-ScheduledTaskAction -Execute "powershell.exe" -Argument "-NoLogo -NoProfile -ExecutionPolicy Bypass -WindowStyle Hidden -File `"$RunnerPath`""
      $Trigger = New-ScheduledTaskTrigger -AtLogOn
      $Settings = New-ScheduledTaskSettingsSet -AllowStartIfOnBatteries -ExecutionTimeLimit 0 -MultipleInstances IgnoreNew
      Register-ScheduledTask -TaskName "soty-agent" -Action $Action -Trigger $Trigger -Settings $Settings -Description "soty.online local agent" -Force | Out-Null
      Start-ScheduledTask -TaskName "soty-agent"
      Write-Output "soty-agent:autostart:task"
      return "task"
    } catch {
      Write-Output "soty-agent:autostart:task-denied"
    }

    try {
      $runKey = "HKCU:\Software\Microsoft\Windows\CurrentVersion\Run"
      New-Item -Path $runKey -Force | Out-Null
      New-ItemProperty -Path $runKey -Name "soty-agent" -Value $runCommand -PropertyType String -Force | Out-Null
      Write-Output "soty-agent:autostart:run"
      return "run"
    } catch {
      Write-Output "soty-agent:autostart:run-denied"
    }

    $startupDir = [Environment]::GetFolderPath("Startup")
    if (-not $startupDir) {
      throw "No user startup folder"
    }
    $startupPath = Join-Path $startupDir "soty-agent.vbs"
    $escapedCommand = $runCommand.Replace("""", """""")
@"
Set shell = CreateObject("WScript.Shell")
shell.Run "$escapedCommand", 0, False
"@ | Set-Content -Path $startupPath -Encoding ASCII
    Write-Output "soty-agent:autostart:startup"
    return "startup"
  }

  function Enable-AppLaunchAtLogon {
    if (-not $LaunchAppAtLogon) { return }
    $edge = Join-Path ${env:ProgramFiles(x86)} "Microsoft\Edge\Application\msedge.exe"
    if (-not (Test-Path -LiteralPath $edge)) {
      $edge = Join-Path $env:ProgramFiles "Microsoft\Edge\Application\msedge.exe"
    }
    $command = if (Test-Path -LiteralPath $edge) {
      "`"$edge`" --app=`"$AppUrl`""
    } else {
      "cmd.exe /c start `"`" `"$AppUrl`""
    }
    $runKey = if ($Scope -eq "Machine") {
      "HKLM:\Software\Microsoft\Windows\CurrentVersion\Run"
    } else {
      "HKCU:\Software\Microsoft\Windows\CurrentVersion\Run"
    }
    try {
      New-Item -Path $runKey -Force | Out-Null
      New-ItemProperty -Path $runKey -Name "soty-pwa" -Value $command -PropertyType String -Force | Out-Null
      Write-Output "soty-pwa:autostart:run"
    } catch {
      Write-Output "soty-pwa:autostart:run-denied"
    }
  }

  function Enable-BrowserLocalNetworkAccessPolicy {
    $origins = @(
      "https://xn--n1afe0b.online",
      "https://соты.online"
    )
    $browserPolicyRoots = @(
      "Software\Policies\Google\Chrome",
      "Software\Policies\Microsoft\Edge"
    )
    $registryHive = if ($Scope -eq "Machine") { "HKLM:" } else { "HKCU:" }

    foreach ($root in $browserPolicyRoots) {
      $path = Join-Path (Join-Path $registryHive $root) "LocalNetworkAccessAllowedForUrls"
      try {
        New-Item -Path $path -Force | Out-Null
        for ($index = 0; $index -lt $origins.Count; $index++) {
          New-ItemProperty -Path $path -Name ([string]($index + 1)) -Value $origins[$index] -PropertyType String -Force | Out-Null
        }
        Write-Output "soty-browser:local-network-policy:$path"
      } catch {
        Write-Output "soty-browser:local-network-policy-denied:$path"
      }
    }
  }

  function Normalize-AgentRelayId {
    param([string]$Value)
    $text = ([string]$Value).Trim()
    if ($text -match '^[A-Za-z0-9_-]{32,192}$') {
      return $text
    }
    return ""
  }

  function Normalize-CodexProxyUrl {
    param([string]$Value)
    $text = ([string]$Value).Trim()
    if ([string]::IsNullOrWhiteSpace($text)) { return "" }
    try {
      $uri = [Uri]$text
      $scheme = $uri.Scheme.ToLowerInvariant()
      if (@("http", "https", "socks5", "socks5h") -contains $scheme) {
        return $text
      }
    } catch {}
    return ""
  }

  function Get-ProxyScheme {
    param([string]$Value)
    try {
      return ([Uri]$Value).Scheme.ToLowerInvariant()
    } catch {
      return ""
    }
  }

  function Resolve-ExistingCodexProxyUrl {
    foreach ($candidate in @(
      $CodexProxyUrl,
      $env:SOTY_CODEX_PROXY_URL,
      $env:SOTY_AGENT_PROXY_URL
    )) {
      $proxy = Normalize-CodexProxyUrl $candidate
      if ($proxy) { return $proxy }
    }

    if (Test-Path -LiteralPath $ProxyEnvPath) {
      try {
        $line = Get-Content -LiteralPath $ProxyEnvPath -ErrorAction Stop |
          Where-Object { $_ -match '^\s*SOTY_CODEX_PROXY_URL\s*=' } |
          Select-Object -First 1
        if ($line -match '^\s*SOTY_CODEX_PROXY_URL\s*=\s*(.+?)\s*$') {
          $proxy = Normalize-CodexProxyUrl $Matches[1]
          if ($proxy) { return $proxy }
        }
      } catch {
        Write-Output "soty-codex-proxy:preserve-skip:proxy-env"
      }
    }

    if (Test-Path -LiteralPath $RunnerPath) {
      try {
        $text = Get-Content -LiteralPath $RunnerPath -Raw
        foreach ($name in @("SOTY_CODEX_PROXY_URL", "SOTY_AGENT_PROXY_URL")) {
          $match = [regex]::Match($text, "$name\s*=\s*`"([^`"]+)`"")
          if ($match.Success) {
            $proxy = Normalize-CodexProxyUrl $match.Groups[1].Value
            if ($proxy) { return $proxy }
          }
        }
      } catch {
        Write-Output "soty-codex-proxy:preserve-skip:runner"
      }
    }
    return ""
  }

  function Protect-AgentSecretFile {
    param([string]$Path)
    try {
      $currentSid = [System.Security.Principal.WindowsIdentity]::GetCurrent().User.Value
      $grants = @("*S-1-5-18:(F)", "*S-1-5-32-544:(F)")
      if ($Scope -ne "Machine" -and $currentSid) {
        $grants += "*${currentSid}:(F)"
      }
      & icacls.exe $Path /inheritance:r /grant:r $grants | Out-Null
    } catch {
      Write-Output "soty-codex-proxy:acl-skip"
    }
  }

  function Write-CodexProxySecret {
    param([string]$ProxyUrl)
    $proxy = Normalize-CodexProxyUrl $ProxyUrl
    if (-not $proxy) { return "" }
    Set-Content -LiteralPath $ProxyEnvPath -Encoding UTF8 -Value "SOTY_CODEX_PROXY_URL=$proxy"
    Protect-AgentSecretFile $ProxyEnvPath
    $scheme = Get-ProxyScheme $proxy
    if ($scheme) {
      Write-Output "soty-codex-proxy:configured:$scheme"
    } else {
      Write-Output "soty-codex-proxy:configured"
    }
    return $proxy
  }

  function Resolve-ExistingAgentRelayId {
    foreach ($path in @(
      (Join-Path $AgentDir "agent-config.json"),
      (Join-Path $AgentDir "start-agent.ps1")
    )) {
      if (-not (Test-Path -LiteralPath $path)) { continue }
      try {
        if ($path -like "*.json") {
          $config = Get-Content -LiteralPath $path -Raw | ConvertFrom-Json
          $candidate = Normalize-AgentRelayId ([string]$config.relayId)
          if ($candidate) { return $candidate }
        } else {
          $text = Get-Content -LiteralPath $path -Raw
          $match = [regex]::Match($text, 'SOTY_AGENT_RELAY_ID\s*=\s*"([^"]+)"')
          if ($match.Success) {
            $candidate = Normalize-AgentRelayId $match.Groups[1].Value
            if ($candidate) { return $candidate }
          }
        }
      } catch {
        Write-Output "soty-agent:relay-preserve-skip:$path"
      }
    }
    return ""
  }

  $NodePath = Resolve-Node
  $CodexPath = ""
  if ($InstallCodex) {
    $CodexPath = ([string](Install-StockCodexCli $NodePath | Select-Object -Last 1)).Trim()
  } else {
    Write-Output "soty-codex-cli:install-skipped:default-light-agent"
  }
  $NodeDir = Split-Path -Parent $NodePath
  $CodexDir = if ($CodexPath) { Split-Path -Parent $CodexPath } else { "" }
  $RunnerPathParts = @($NodeDir, $CodexDir) | Where-Object { $_ }
  $SafeRelayId = Normalize-AgentRelayId $RelayId
  if (-not $SafeRelayId) {
    $SafeRelayId = Resolve-ExistingAgentRelayId
  }
  $RelayEnv = if ($SafeRelayId) {
@"
`$env:SOTY_AGENT_RELAY_ID = "$SafeRelayId"
`$env:SOTY_AGENT_RELAY_URL = "https://xn--n1afe0b.online"
"@
  } else {
    ""
  }
  $ResolvedCodexProxyUrl = Resolve-ExistingCodexProxyUrl
  if ($ResolvedCodexProxyUrl) {
    [void](Write-CodexProxySecret $ResolvedCodexProxyUrl)
  }
  $ProxyEnv = @"
`$proxyEnvPath = Join-Path `$PSScriptRoot "proxy.env"
if (Test-Path -LiteralPath `$proxyEnvPath) {
  try {
    `$proxyLine = Get-Content -LiteralPath `$proxyEnvPath -ErrorAction Stop | Where-Object { `$_ -match '^\s*SOTY_CODEX_PROXY_URL\s*=' } | Select-Object -First 1
    if (`$proxyLine -match '^\s*SOTY_CODEX_PROXY_URL\s*=\s*(.+?)\s*$') {
      `$proxyValue = `$Matches[1].Trim()
      try {
        `$proxyUri = [Uri]`$proxyValue
        if (@("http", "https", "socks5", "socks5h") -contains `$proxyUri.Scheme.ToLowerInvariant()) {
          `$env:SOTY_CODEX_PROXY_URL = `$proxyValue
        }
      } catch {}
    }
  } catch {}
}
"@
  $CodexEnv = if ($RunnerPathParts.Count -gt 0) {
    $RunnerPathPrefix = ($RunnerPathParts -join ";")
@"
`$env:PATH = "$RunnerPathPrefix;`$env:PATH"
"@
  } else {
    ""
  }
  Invoke-WebRequest -Uri $ManifestUrl -UseBasicParsing -OutFile (Join-Path $AgentDir "manifest.json")
  Invoke-WebRequest -Uri $AgentUrl -UseBasicParsing -OutFile $AgentPath

@"
`$env:SOTY_AGENT_MANAGED = "1"
`$env:SOTY_AGENT_AUTO_UPDATE = "1"
`$env:SOTY_AGENT_SCOPE = "$Scope"
`$env:SOTY_AGENT_UPDATE_URL = "$ManifestUrl"
$RelayEnv
$ProxyEnv
$CodexEnv
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

@"
@echo off
"$NodePath" "$AgentPath" ctl %*
"@ | Set-Content -Path $CtlPath -Encoding ASCII

  $Autostart = Enable-AgentAutostart
  Enable-BrowserLocalNetworkAccessPolicy
  Enable-AppLaunchAtLogon
  Start-Sleep -Milliseconds 700
  Start-AgentNow

  Write-Output "soty-agent:installed:$Autostart"
} finally {
  Stop-Transcript | Out-Null
}
