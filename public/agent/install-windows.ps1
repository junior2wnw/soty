param(
  [string]$Base = "https://xn--n1afe0b.online/agent",
  [ValidateSet("CurrentUser", "Machine")]
  [string]$Scope = "Machine",
  [string]$InstallDir = "",
  [switch]$LaunchAppAtLogon,
  [string]$AppUrl = "https://xn--n1afe0b.online/?pwa=1",
  [string]$RelayId = "",
  [string]$DeviceId = "",
  [string]$DeviceNick = "",
  [switch]$SourceCompanion,
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
$RunnerStdoutPath = Join-Path $AgentDir "start-agent.out.log"
$RunnerStderrPath = Join-Path $AgentDir "start-agent.err.log"
$RunnerStatusPath = Join-Path $AgentDir "start-agent.status.log"
$ManifestUrl = "$Base/manifest.json"
$AgentUrl = "$Base/soty-agent.mjs"
$RelayBaseUrl = "https://xn--n1afe0b.online"
try {
  $BaseUri = [Uri]$Base
  if (@("http", "https") -contains $BaseUri.Scheme.ToLowerInvariant()) {
    $RelayBaseUrl = "$($BaseUri.Scheme)://$($BaseUri.Authority)"
  }
} catch {}

New-Item -ItemType Directory -Force -Path $AgentDir | Out-Null
Start-Transcript -Path $LogPath -Append | Out-Null

try {
  function Write-SotyLog {
    param([string]$Message)
    Write-Host $Message
  }

  function Write-SotyStep {
    param([string]$Message)
    Write-SotyLog ("soty-install:step:" + $Message)
  }

  function ConvertTo-SotyProcessArguments {
    param([string[]]$ArgumentList = @())
    $quoted = @()
    foreach ($argument in @($ArgumentList)) {
      $value = if ($null -eq $argument) { "" } else { [string]$argument }
      if (($value.Length -gt 0) -and ($value -notmatch '[\s"]')) {
        $quoted += $value
        continue
      }
      $builder = New-Object System.Text.StringBuilder
      [void]$builder.Append('"')
      $slashes = 0
      foreach ($char in $value.ToCharArray()) {
        if ($char -eq '\') {
          $slashes++
          continue
        }
        if ($char -eq '"') {
          if ($slashes -gt 0) { [void]$builder.Append(('\' * ($slashes * 2))) }
          [void]$builder.Append('\"')
          $slashes = 0
          continue
        }
        if ($slashes -gt 0) {
          [void]$builder.Append(('\' * $slashes))
          $slashes = 0
        }
        [void]$builder.Append($char)
      }
      if ($slashes -gt 0) { [void]$builder.Append(('\' * ($slashes * 2))) }
      [void]$builder.Append('"')
      $quoted += $builder.ToString()
    }
    return ($quoted -join " ")
  }

  function Invoke-SotyProcess {
    param(
      [string]$FilePath,
      [string[]]$ArgumentList = @(),
      [int]$TimeoutSec = 120,
      [string]$LogName = "process"
    )
    $safeName = ([string]$LogName) -replace '[^A-Za-z0-9_.-]', '-'
    $stdoutPath = Join-Path $AgentDir ($safeName + ".out.log")
    $stderrPath = Join-Path $AgentDir ($safeName + ".err.log")
    Remove-Item -LiteralPath $stdoutPath, $stderrPath -Force -ErrorAction SilentlyContinue
    $stdoutBuilder = New-Object System.Text.StringBuilder
    $stderrBuilder = New-Object System.Text.StringBuilder
    $processInfo = New-Object System.Diagnostics.ProcessStartInfo
    $processInfo.FileName = $FilePath
    $processInfo.Arguments = ConvertTo-SotyProcessArguments $ArgumentList
    $processInfo.UseShellExecute = $false
    $processInfo.CreateNoWindow = $true
    $processInfo.RedirectStandardOutput = $true
    $processInfo.RedirectStandardError = $true
    try { $processInfo.EnvironmentVariables["NODE_OPTIONS"] = "" } catch {}
    $process = New-Object System.Diagnostics.Process
    $process.StartInfo = $processInfo
    $outputHandler = [System.Diagnostics.DataReceivedEventHandler]{
      param($sender, $eventArgs)
      if ($null -ne $eventArgs.Data) { [void]$stdoutBuilder.AppendLine($eventArgs.Data) }
    }
    $errorHandler = [System.Diagnostics.DataReceivedEventHandler]{
      param($sender, $eventArgs)
      if ($null -ne $eventArgs.Data) { [void]$stderrBuilder.AppendLine($eventArgs.Data) }
    }
    $process.add_OutputDataReceived($outputHandler)
    $process.add_ErrorDataReceived($errorHandler)
    [void]$process.Start()
    $process.BeginOutputReadLine()
    $process.BeginErrorReadLine()
    if (-not $process.WaitForExit([math]::Max(1, $TimeoutSec) * 1000)) {
      try { Stop-Process -Id $process.Id -Force -ErrorAction SilentlyContinue } catch {}
      throw ("Process timed out after " + $TimeoutSec + "s: " + $FilePath)
    }
    $process.WaitForExit()
    $stdoutText = $stdoutBuilder.ToString()
    $stderrText = $stderrBuilder.ToString()
    if (-not [string]::IsNullOrWhiteSpace($stdoutText)) { $stdoutText | Set-Content -LiteralPath $stdoutPath -Encoding UTF8 }
    if (-not [string]::IsNullOrWhiteSpace($stderrText)) { $stderrText | Set-Content -LiteralPath $stderrPath -Encoding UTF8 }
    if ([int]$process.ExitCode -ne 0) {
      throw ("Process failed with exit code " + $process.ExitCode + ": " + $FilePath + ". See " + $stderrPath)
    }
    return [int]$process.ExitCode
  }

  function Invoke-SotyDownload {
    param(
      [string]$Uri,
      [string]$OutFile,
      [int]$TimeoutSec = 90,
      [int]$Retries = 3
    )
    $lastError = ""
    for ($attempt = 1; $attempt -le [math]::Max(1, $Retries); $attempt++) {
      $name = Split-Path -Leaf $OutFile
      Write-SotyStep ("download:" + $name + ":attempt:" + $attempt)
      Remove-Item -LiteralPath $OutFile -Force -ErrorAction SilentlyContinue
      try {
        Invoke-WebRequest -Uri $Uri -UseBasicParsing -OutFile $OutFile -TimeoutSec $TimeoutSec -ErrorAction Stop
        $item = Get-Item -LiteralPath $OutFile -ErrorAction Stop
        if ($item.Length -le 0) { throw "empty download" }
        return
      } catch {
        $lastError = $_.Exception.Message
        Write-SotyLog ("soty-download:invoke-webrequest-failed:attempt:" + $attempt + ":" + $lastError)
      }

      $curl = Get-Command curl.exe -ErrorAction SilentlyContinue
      if ($curl) {
        try {
          Invoke-SotyProcess -FilePath $curl.Source -ArgumentList @("-fL", "-sS", "--retry", "2", "--retry-delay", "2", "--connect-timeout", "20", "--max-time", ([string][math]::Max(30, $TimeoutSec)), "-o", $OutFile, $Uri) -TimeoutSec ([math]::Max(45, $TimeoutSec + 15)) -LogName ("download-curl-" + $name)
          $item = Get-Item -LiteralPath $OutFile -ErrorAction Stop
          if ($item.Length -le 0) { throw "empty curl download" }
          return
        } catch {
          $lastError = $_.Exception.Message
          Write-SotyLog ("soty-download:curl-failed:attempt:" + $attempt + ":" + $lastError)
        }
      }

      if ($attempt -lt $Retries) {
        Start-Sleep -Seconds ([math]::Min(8, 1 + $attempt * 2))
      }
    }
    throw ("Download failed: " + $Uri + " (" + $lastError + ")")
  }

  function Invoke-SotyJson {
    param(
      [string]$Uri,
      [string]$Name,
      [int]$TimeoutSec = 45
    )
    $path = Join-Path $AgentDir $Name
    Invoke-SotyDownload -Uri $Uri -OutFile $path -TimeoutSec $TimeoutSec -Retries 3
    return (Get-Content -LiteralPath $path -Raw | ConvertFrom-Json)
  }

  function Expand-SotyZip {
    param(
      [string]$ZipPath,
      [string]$DestinationPath
    )
    try {
      Expand-Archive -LiteralPath $ZipPath -DestinationPath $DestinationPath -Force
      return
    } catch {
      Write-SotyLog ("soty-zip:expand-archive-fallback:" + $_.Exception.Message)
    }
    Add-Type -AssemblyName System.IO.Compression.FileSystem
    if (Test-Path -LiteralPath $DestinationPath) {
      Remove-Item -LiteralPath $DestinationPath -Recurse -Force
    }
    New-Item -ItemType Directory -Force -Path $DestinationPath | Out-Null
    [System.IO.Compression.ZipFile]::ExtractToDirectory($ZipPath, $DestinationPath)
  }

  function Reset-BrokenNodeOptions {
    function Should-ClearNodeOptions([string]$Value) {
      if ([string]::IsNullOrWhiteSpace($Value)) { return $false }
      return ($Value -match "soty-node-require-shim|C:Users.*soty-node-require-shim|--require\s+[`"']?.*(?:\\|/)(?:Temp|AppData)(?:\\|/).*\.cjs")
    }

    $processValue = [string]$env:NODE_OPTIONS
    if (Should-ClearNodeOptions $processValue) {
      [Environment]::SetEnvironmentVariable("NODE_OPTIONS", $null, "Process")
      Remove-Item Env:NODE_OPTIONS -ErrorAction SilentlyContinue
      Write-SotyLog "soty-node-options:cleared:process"
    }
    foreach ($target in @("User", "Machine")) {
      try {
        $value = [Environment]::GetEnvironmentVariable("NODE_OPTIONS", $target)
        if ([string]::IsNullOrWhiteSpace([string]$value)) { continue }
        if (Should-ClearNodeOptions ([string]$value)) {
          [Environment]::SetEnvironmentVariable("NODE_OPTIONS", $null, $target)
          Write-SotyLog "soty-node-options:cleared:$($target.ToLowerInvariant())"
        }
      } catch {
        Write-SotyLog "soty-node-options:clear-skip:$($target.ToLowerInvariant())"
      }
    }
    foreach ($path in @(
      "HKCU:\Environment",
      "HKLM:\SYSTEM\CurrentControlSet\Control\Session Manager\Environment"
    )) {
      try {
        $property = Get-ItemProperty -LiteralPath $path -Name NODE_OPTIONS -ErrorAction SilentlyContinue
        if ($property -and (Should-ClearNodeOptions ([string]$property.NODE_OPTIONS))) {
          Remove-ItemProperty -LiteralPath $path -Name NODE_OPTIONS -Force -ErrorAction Stop
          Write-SotyLog "soty-node-options:cleared:registry"
        }
      } catch {
        Write-SotyLog "soty-node-options:registry-skip"
      }
    }
    try {
      if (Test-Path -LiteralPath "Registry::HKEY_USERS") {
        Get-ChildItem -LiteralPath "Registry::HKEY_USERS" -ErrorAction SilentlyContinue |
          Where-Object { $_.PSChildName -match '^S-1-5-21-' -and $_.PSChildName -notmatch '_Classes$' } |
          ForEach-Object {
            $userEnvPath = "Registry::HKEY_USERS\$($_.PSChildName)\Environment"
            try {
              $property = Get-ItemProperty -LiteralPath $userEnvPath -Name NODE_OPTIONS -ErrorAction SilentlyContinue
              if ($property -and (Should-ClearNodeOptions ([string]$property.NODE_OPTIONS))) {
                Remove-ItemProperty -LiteralPath $userEnvPath -Name NODE_OPTIONS -Force -ErrorAction Stop
                Write-SotyLog "soty-node-options:cleared:user-hive"
              }
            } catch {
              Write-SotyLog "soty-node-options:user-hive-skip"
            }
          }
      }
    } catch {
      Write-SotyLog "soty-node-options:hku-skip"
    }
  }

  Reset-BrokenNodeOptions

  function Test-PathUnder {
    param([string]$Path, [string]$Root)
    if ([string]::IsNullOrWhiteSpace($Path) -or [string]::IsNullOrWhiteSpace($Root)) { return $false }
    try {
      $fullPath = [IO.Path]::GetFullPath($Path).TrimEnd('\')
      $fullRoot = [IO.Path]::GetFullPath($Root).TrimEnd('\')
      return $fullPath.Equals($fullRoot, [StringComparison]::OrdinalIgnoreCase) -or
        $fullPath.StartsWith($fullRoot + "\", [StringComparison]::OrdinalIgnoreCase)
    } catch {
      return $false
    }
  }

  function Test-NodePathAllowedForScope {
    param([string]$Path)
    if ([string]::IsNullOrWhiteSpace($Path)) { return $false }
    if ($Scope -ne "Machine") { return $true }
    foreach ($root in @($env:LOCALAPPDATA, $env:APPDATA, $env:TEMP, $env:TMP)) {
      if (Test-PathUnder -Path $Path -Root $root) {
        return $false
      }
    }
    if ($env:USERPROFILE) {
      $userAppData = Join-Path $env:USERPROFILE "AppData"
      if (Test-PathUnder -Path $Path -Root $userAppData) {
        return $false
      }
    }
    return $true
  }

  function Get-SystemNodeCandidates {
    $candidates = @()
    foreach ($root in @($env:ProgramFiles, ${env:ProgramFiles(x86)}, $env:ProgramW6432)) {
      if ([string]::IsNullOrWhiteSpace($root)) { continue }
      $candidates += (Join-Path $root "nodejs\node.exe")
    }
    $pathNode = Get-Command node -ErrorAction SilentlyContinue
    if ($pathNode) { $candidates += $pathNode.Source }
    return @($candidates | Where-Object { -not [string]::IsNullOrWhiteSpace($_) } | Select-Object -Unique)
  }

  function Test-NodeRuntime {
    param([string]$Path)
    if (-not $Path -or -not (Test-Path -LiteralPath $Path)) { return $false }
    $oldNodeOptions = $env:NODE_OPTIONS
    try {
      Remove-Item Env:NODE_OPTIONS -ErrorAction SilentlyContinue
      $probeOut = Join-Path $AgentDir "node-probe.out.log"
      $probeErr = Join-Path $AgentDir "node-probe.err.log"
      $probeScript = Join-Path $AgentDir "node-probe.mjs"
      Remove-Item -LiteralPath $probeOut, $probeErr -Force -ErrorAction SilentlyContinue
      "const v = process.versions.node.split('.').map(Number); process.exit(v[0] > 22 || (v[0] === 22 && v[1] >= 12) ? 0 : 1);" | Set-Content -LiteralPath $probeScript -Encoding ASCII
      $processInfo = New-Object System.Diagnostics.ProcessStartInfo
      $processInfo.FileName = $Path
      $processInfo.Arguments = ConvertTo-SotyProcessArguments @($probeScript)
      $processInfo.UseShellExecute = $false
      $processInfo.CreateNoWindow = $true
      $processInfo.RedirectStandardOutput = $true
      $processInfo.RedirectStandardError = $true
      $process = New-Object System.Diagnostics.Process
      $process.StartInfo = $processInfo
      [void]$process.Start()
      if (-not $process.WaitForExit(10000)) {
        try { Stop-Process -Id $process.Id -Force -ErrorAction SilentlyContinue } catch {}
        Write-SotyLog "soty-node:probe-timeout"
        return $false
      }
      $probeStdout = $process.StandardOutput.ReadToEnd()
      $probeStderr = $process.StandardError.ReadToEnd()
      if (-not [string]::IsNullOrWhiteSpace($probeStdout)) { $probeStdout | Set-Content -LiteralPath $probeOut -Encoding UTF8 }
      if (-not [string]::IsNullOrWhiteSpace($probeStderr)) { $probeStderr | Set-Content -LiteralPath $probeErr -Encoding UTF8 }
      return ($process.ExitCode -eq 0)
    } catch {
      return $false
    } finally {
      if ([string]::IsNullOrWhiteSpace($oldNodeOptions)) {
        Remove-Item Env:NODE_OPTIONS -ErrorAction SilentlyContinue
      } else {
        $env:NODE_OPTIONS = $oldNodeOptions
      }
    }
  }

  function Get-NodeWindowsArch {
    $archText = "$env:PROCESSOR_ARCHITECTURE $env:PROCESSOR_ARCHITEW6432".ToUpperInvariant()
    if ($archText -match "ARM64") { return "win-arm64" }
    if ($archText -match "AMD64") { return "win-x64" }
    throw "Unsupported Windows architecture: $archText"
  }

  function Get-NodeReleaseCandidates {
    param([string]$Arch)
    $fileKey = "$Arch-zip"
    $releases = @()
    try {
      $index = Invoke-SotyJson -Uri "https://nodejs.org/dist/index.json" -Name "node-index.json" -TimeoutSec 45
      $release = $index | Where-Object { $_.lts -and ($_.files -contains $fileKey) } | Select-Object -First 1
      if (-not $release) {
        $release = $index | Where-Object { $_.files -contains $fileKey } | Select-Object -First 1
      }
      if ($release) {
        $releases += $release
      } else {
        Write-SotyLog ("soty-node:index-no-match:" + $fileKey)
      }
    } catch {
      Write-SotyLog ("soty-node:index-failed:" + $_.Exception.Message)
    }
    foreach ($version in @("v24.13.1", "v22.12.0")) {
      if ($releases | Where-Object { $_.version -eq $version } | Select-Object -First 1) { continue }
      $releases += [pscustomobject]@{
        version = $version
        files = @($fileKey)
      }
    }
    if (-not $releases.Count) { throw "No Node.js zip release for $fileKey" }
    return @($releases)
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
    Invoke-SotyDownload -Uri $zipUrl -OutFile $zipPath -TimeoutSec 240 -Retries 3
    Invoke-SotyDownload -Uri $sumUrl -OutFile $sumPath -TimeoutSec 45 -Retries 3
    $expected = (Select-String -LiteralPath $sumPath -Pattern "  $([regex]::Escape($zipName))$").Line -replace "\s+.*$", ""
    if (-not $expected) { throw "Node.js checksum is missing for $zipName" }
    $actual = (Get-FileHash -Algorithm SHA256 -LiteralPath $zipPath).Hash.ToLowerInvariant()
    if ($actual -ne $expected.ToLowerInvariant()) { throw "Node.js checksum mismatch" }

    Write-SotyStep "extract:portable-node"
    Expand-SotyZip -ZipPath $zipPath -DestinationPath $extractDir
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
    Write-SotyStep "node:probe"
    $LocalNode = Join-Path $AgentDir "node\node.exe"
    if (Test-NodeRuntime $LocalNode) {
      Write-SotyLog "soty-node:using:portable-cache"
      return $LocalNode
    }

    if ($Scope -eq "Machine") {
      foreach ($candidate in (Get-SystemNodeCandidates)) {
        if ((Test-NodePathAllowedForScope $candidate) -and (Test-NodeRuntime $candidate)) {
          Write-SotyLog ("soty-node:using:system-path:" + $candidate)
          return $candidate
        }
      }
    } else {
      $node = Get-Command node -ErrorAction SilentlyContinue
      if ($node -and (Test-NodeRuntime $node.Source)) {
        Write-SotyLog "soty-node:using:path"
        return $node.Source
      }
    }

    $arch = Get-NodeWindowsArch

    $lastPortableError = ""
    foreach ($release in (Get-NodeReleaseCandidates $arch)) {
      try {
        Write-SotyStep ("node:portable-download:" + $release.version + ":" + $arch)
        $portableNode = Save-PortableNode -Arch $arch -Release $release
        if (Test-NodeRuntime $portableNode) { return $portableNode }
        throw "Portable Node.js failed to start"
      } catch {
        $lastPortableError = $_.Exception.Message
        Write-SotyLog ("soty-node:portable-failed:" + $release.version + ":" + $lastPortableError)
      }
    }
    throw ("Portable Node.js setup failed: " + $lastPortableError)
  }

  function Test-AgentHealth {
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
      if ($Scope -eq "Machine") {
        return (($health.scope -eq "Machine") -and ($health.system -eq $true))
      }
      return $true
    } catch {
      return $false
    }
  }

  function Get-AgentHealthSummary {
    try {
      $request = [System.Net.WebRequest]::Create("http://127.0.0.1:49424/health")
      $request.Method = "GET"
      $request.Timeout = 2000
      $request.ReadWriteTimeout = 2000
      $request.Headers.Add("Origin", "https://xn--n1afe0b.online")
      $response = $request.GetResponse()
      try {
        $reader = New-Object System.IO.StreamReader($response.GetResponseStream())
        $health = $reader.ReadToEnd() | ConvertFrom-Json
      } finally {
        try { $reader.Dispose() } catch {}
        try { $response.Dispose() } catch {}
      }
      return ("health:scope=" + [string]$health.scope + ",system=" + [string]$health.system + ",version=" + [string]$health.version + ",user=" + [string]$health.windowsUser)
    } catch {
      return ("health:error=" + $_.Exception.Message)
    }
  }

  function Get-SotyPortOwnerDiagnostics {
    $lines = @()
    try {
      $connections = @(Get-NetTCPConnection -LocalPort 49424 -State Listen -ErrorAction SilentlyContinue)
      foreach ($connection in $connections) {
        $pidText = [string]$connection.OwningProcess
        $command = ""
        try {
          $process = Get-CimInstance Win32_Process -Filter ("ProcessId=" + [int]$connection.OwningProcess) -ErrorAction SilentlyContinue
          if ($process) { $command = ([string]$process.CommandLine) }
        } catch {}
        $lines += ("port:49424:pid=" + $pidText + ":cmd=" + ($command -replace '\s+', ' ').Trim())
      }
    } catch {
      $lines += ("port:49424:diagnostic-error=" + $_.Exception.Message)
    }
    if ($lines.Count -eq 0) { return "port:49424:no-listener" }
    return ($lines -join " | ")
  }

  function Stop-SotyPortOwnerIfStale {
    if ($Scope -ne "Machine") { return }
    if (Test-AgentHealth) { return }
    try {
      $connections = @(Get-NetTCPConnection -LocalPort 49424 -State Listen -ErrorAction SilentlyContinue)
      foreach ($connection in $connections) {
        $ownerPid = [int]$connection.OwningProcess
        if ($ownerPid -le 0 -or $ownerPid -eq $PID) { continue }
        $command = ""
        try {
          $process = Get-CimInstance Win32_Process -Filter ("ProcessId=" + $ownerPid) -ErrorAction SilentlyContinue
          if ($process) { $command = [string]$process.CommandLine }
        } catch {}
        if ($command -match "soty-agent|start-agent\.ps1|sotyctl\.cmd") {
          try {
            Stop-Process -Id $ownerPid -Force -ErrorAction SilentlyContinue
            Write-SotyLog "soty-agent:stopped-port-owner:$ownerPid"
          } catch {}
        }
      }
    } catch {
      Write-SotyLog "soty-agent:stop-port-owner-skipped"
    }
  }

  function Get-SotyTaskDiagnostics {
    if ($Scope -ne "Machine") { return "" }
    $parts = @()
    try {
      $task = Get-ScheduledTask -TaskName "soty-agent-machine" -ErrorAction SilentlyContinue
      if ($task) {
        $parts += ("task:state=" + [string]$task.State)
        try { $parts += ("task:principal=" + [string]$task.Principal.UserId) } catch {}
        try {
          $info = Get-ScheduledTaskInfo -TaskName "soty-agent-machine" -ErrorAction SilentlyContinue
          if ($info) {
            $parts += ("task:lastResult=" + [string]$info.LastTaskResult)
            $parts += ("task:lastRun=" + [string]$info.LastRunTime)
          }
        } catch {}
      } else {
        $parts += "task:missing"
      }
    } catch {
      $parts += ("task:error=" + $_.Exception.Message)
    }
    return ($parts -join ",")
  }

  function Get-LogTailText {
    param([string]$Path, [int]$Tail = 30)
    try {
      if (-not (Test-Path -LiteralPath $Path)) { return "" }
      return ((Get-Content -LiteralPath $Path -Tail $Tail -ErrorAction Stop) -join " | ")
    } catch {
      return ("log-tail-error:" + $_.Exception.Message)
    }
  }

  function Stop-ExistingSotyAgents {
    if ($Scope -ne "Machine") { return }
    try {
      $agentDirPattern = [regex]::Escape($AgentDir)
      $processes = Get-CimInstance Win32_Process -ErrorAction SilentlyContinue |
        Where-Object {
          $_.ProcessId -ne $PID -and
          $_.CommandLine -and
          ($_.CommandLine -match "soty-agent") -and
          (
            ($_.CommandLine -match $agentDirPattern) -or
            ($_.CommandLine -match "AppData\\Local\\soty-agent") -or
            ($_.CommandLine -match "ProgramData\\soty-agent")
          ) -and
          (($_.CommandLine -match "soty-agent\.mjs") -or ($_.CommandLine -match "start-agent\.ps1") -or ($_.CommandLine -match "sotyctl\.cmd"))
        }
      foreach ($process in @($processes)) {
        try {
          Stop-Process -Id $process.ProcessId -Force -ErrorAction SilentlyContinue
          Write-SotyLog "soty-agent:stopped-existing-process:$($process.ProcessId)"
        } catch {}
      }
    } catch {
      Write-SotyLog "soty-agent:stop-existing-skipped"
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
    Write-SotyStep "agent:start"
    if ((-not $SourceCompanion) -and (Test-AgentHealth)) { return }
    if ($Scope -eq "Machine") {
      Stop-ExistingSotyAgents
      Stop-SotyPortOwnerIfStale
      try { Start-ScheduledTask -TaskName "soty-agent-machine" -ErrorAction SilentlyContinue } catch {}
      if (Wait-AgentHealth 75) {
        Write-SotyLog "soty-agent:health:machine"
        return
      }
      $diagnostics = @(
        (Get-AgentHealthSummary),
        (Get-SotyPortOwnerDiagnostics),
        (Get-SotyTaskDiagnostics),
        ("runner-status=" + (Get-LogTailText -Path $RunnerStatusPath -Tail 20)),
        ("runner-stderr=" + (Get-LogTailText -Path $RunnerStderrPath -Tail 25)),
        ("runner-stdout=" + (Get-LogTailText -Path $RunnerStdoutPath -Tail 10))
      ) | Where-Object { -not [string]::IsNullOrWhiteSpace($_) }
      foreach ($line in $diagnostics) {
        Write-SotyLog ("soty-agent:start-diagnostic:" + $line)
      }
      throw ("Soty machine task did not report SYSTEM health on 127.0.0.1:49424; " + ($diagnostics -join "; "))
    }
    Start-Process -WindowStyle Hidden -FilePath "powershell.exe" -ArgumentList @("-NoLogo", "-NoProfile", "-ExecutionPolicy", "Bypass", "-File", $RunnerPath)
  }

  function Enable-AgentAutostart {
    $runCommand = "powershell.exe -NoLogo -NoProfile -ExecutionPolicy Bypass -WindowStyle Hidden -File `"$RunnerPath`""

    if ($Scope -eq "Machine") {
      Write-SotyStep "autostart:machine-task"
      Stop-ExistingSotyAgents
      try { Stop-ScheduledTask -TaskName "soty-agent-machine" -ErrorAction SilentlyContinue } catch {}
      $Action = New-ScheduledTaskAction -Execute "powershell.exe" -Argument "-NoLogo -NoProfile -ExecutionPolicy Bypass -WindowStyle Hidden -File `"$RunnerPath`""
      $Trigger = New-ScheduledTaskTrigger -AtStartup
      $Settings = New-ScheduledTaskSettingsSet -AllowStartIfOnBatteries -DontStopIfGoingOnBatteries -ExecutionTimeLimit 0 -MultipleInstances IgnoreNew -StartWhenAvailable
      $Principal = New-ScheduledTaskPrincipal -UserId "SYSTEM" -LogonType ServiceAccount -RunLevel Highest
      Register-ScheduledTask -TaskName "soty-agent-machine" -Action $Action -Trigger $Trigger -Settings $Settings -Principal $Principal -Description "soty.online machine local agent" -Force | Out-Null
      Start-ScheduledTask -TaskName "soty-agent-machine"
      Write-SotyLog "soty-agent:autostart:machine-task"
      return "machine-task"
    }

    try {
      $Action = New-ScheduledTaskAction -Execute "powershell.exe" -Argument "-NoLogo -NoProfile -ExecutionPolicy Bypass -WindowStyle Hidden -File `"$RunnerPath`""
      $Trigger = New-ScheduledTaskTrigger -AtLogOn
      $Settings = New-ScheduledTaskSettingsSet -AllowStartIfOnBatteries -ExecutionTimeLimit 0 -MultipleInstances IgnoreNew
      Register-ScheduledTask -TaskName "soty-agent" -Action $Action -Trigger $Trigger -Settings $Settings -Description "soty.online local agent" -Force | Out-Null
      Start-ScheduledTask -TaskName "soty-agent"
      Write-SotyLog "soty-agent:autostart:task"
      return "task"
    } catch {
      Write-SotyLog "soty-agent:autostart:task-denied"
    }

    try {
      $runKey = "HKCU:\Software\Microsoft\Windows\CurrentVersion\Run"
      New-Item -Path $runKey -Force | Out-Null
      New-ItemProperty -Path $runKey -Name "soty-agent" -Value $runCommand -PropertyType String -Force | Out-Null
      Write-SotyLog "soty-agent:autostart:run"
      return "run"
    } catch {
      Write-SotyLog "soty-agent:autostart:run-denied"
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
    Write-SotyLog "soty-agent:autostart:startup"
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
      Write-SotyLog "soty-pwa:autostart:run"
    } catch {
      Write-SotyLog "soty-pwa:autostart:run-denied"
    }
  }

  function Enable-BrowserLocalNetworkAccessPolicy {
    $origins = @(
      "https://xn--n1afe0b.online",
      "https://СЃРѕС‚С‹.online"
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
        Write-SotyLog "soty-browser:local-network-policy:$path"
      } catch {
        Write-SotyLog "soty-browser:local-network-policy-denied:$path"
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

  function Normalize-AgentDeviceId {
    param([string]$Value)
    $text = ([string]$Value).Trim()
    if ($text -match '^[A-Za-z0-9_-]{8,192}$') {
      return $text
    }
    return ""
  }

  function Escape-PowerShellDoubleQuoted {
    param([string]$Value)
    return ([string]$Value).Replace('`', '``').Replace('"', '`"').Replace('$', '`$')
  }

  function Write-AgentConfigSeed {
    $configPath = Join-Path $AgentDir "agent-config.json"
    $existing = $null
    try {
      if (Test-Path -LiteralPath $configPath) {
        $existing = Get-Content -LiteralPath $configPath -Raw | ConvertFrom-Json
      }
    } catch {
      $existing = $null
    }

    $relayId = $SafeRelayId
    if (-not $relayId -and $existing -and $existing.relayId) { $relayId = [string]$existing.relayId }
    $relayBaseUrl = $RelayBaseUrl
    if ($existing -and $existing.relayBaseUrl -and -not $relayBaseUrl) { $relayBaseUrl = [string]$existing.relayBaseUrl }
    $deviceId = $SafeDeviceId
    if (-not $deviceId -and $existing -and $existing.deviceId) { $deviceId = [string]$existing.deviceId }
    $deviceNick = $SafeDeviceNick
    if (-not $deviceNick -and $existing -and $existing.deviceNick) { $deviceNick = [string]$existing.deviceNick }
    $installId = ""
    if ($existing -and $existing.installId) { $installId = [string]$existing.installId }

    [ordered]@{
      relayId = $relayId
      relayBaseUrl = $relayBaseUrl
      deviceId = $deviceId
      deviceNick = $deviceNick
      installId = $installId
    } | ConvertTo-Json | Set-Content -Path $configPath -Encoding UTF8
    Write-SotyLog "soty-agent:config-seeded"
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
        Write-SotyLog "soty-agent:relay-preserve-skip:$path"
      }
    }
    return ""
  }

  $NodePath = Resolve-Node
  Write-SotyLog "soty-codex-cli:disabled:server-relay-only"
  $NodeDir = Split-Path -Parent $NodePath
  $RunnerPathParts = @($NodeDir)
  $SafeRelayId = Normalize-AgentRelayId $RelayId
  if (-not $SafeRelayId) {
    $SafeRelayId = Resolve-ExistingAgentRelayId
  }
  $SafeDeviceId = Normalize-AgentDeviceId $DeviceId
  $SafeDeviceNick = ([string]$DeviceNick).Trim()
  if ($SafeDeviceNick.Length -gt 80) {
    $SafeDeviceNick = $SafeDeviceNick.Substring(0, 80)
  }
  $RelayEnv = if ($SafeRelayId) {
@"
`$env:SOTY_AGENT_RELAY_ID = "$SafeRelayId"
`$env:SOTY_AGENT_RELAY_URL = "$RelayBaseUrl"
"@
  } else {
    ""
  }
  $CompanionEnv = if ($SourceCompanion) {
@"
`$env:SOTY_AGENT_COMPANION = "1"
`$env:SOTY_AGENT_PORT = "0"
"@
  } else {
    ""
  }
  $DeviceEnv = if ($SafeDeviceId) {
    $nickLine = if ($SafeDeviceNick) {
      "`$env:SOTY_AGENT_DEVICE_NICK = `"$(Escape-PowerShellDoubleQuoted $SafeDeviceNick)`""
    } else {
      ""
    }
@"
`$env:SOTY_AGENT_DEVICE_ID = "$SafeDeviceId"
$nickLine
"@
  } else {
    ""
  }
  $RunnerPathEnv = if ($RunnerPathParts.Count -gt 0) {
    $RunnerPathPrefix = ($RunnerPathParts -join ";")
@"
`$env:PATH = "$RunnerPathPrefix;`$env:PATH"
"@
  } else {
    ""
  }
  Write-SotyStep "agent:download"
  Invoke-SotyDownload -Uri $ManifestUrl -OutFile (Join-Path $AgentDir "manifest.json") -TimeoutSec 45 -Retries 3
  Invoke-SotyDownload -Uri $AgentUrl -OutFile $AgentPath -TimeoutSec 90 -Retries 3
  Write-AgentConfigSeed

@"
`$env:NODE_OPTIONS = ""
`$env:SOTY_AGENT_MANAGED = "1"
`$env:SOTY_AGENT_AUTO_UPDATE = "1"
`$env:SOTY_AGENT_SCOPE = "$Scope"
`$env:SOTY_AGENT_UPDATE_URL = "$ManifestUrl"
$RelayEnv
$CompanionEnv
$DeviceEnv
$RunnerPathEnv
`$stdoutPath = Join-Path `$PSScriptRoot "start-agent.out.log"
`$stderrPath = Join-Path `$PSScriptRoot "start-agent.err.log"
`$statusPath = Join-Path `$PSScriptRoot "start-agent.status.log"
while (`$true) {
  try {
    ("start " + (Get-Date).ToString("o") + " node=$NodePath agent=$AgentPath") | Out-File -LiteralPath `$statusPath -Encoding UTF8 -Append
    & "$NodePath" "$AgentPath" >> `$stdoutPath 2>> `$stderrPath
    `$code = if (`$null -eq `$LASTEXITCODE) { 1 } else { [int]`$LASTEXITCODE }
    ("exit " + (Get-Date).ToString("o") + " code=" + `$code) | Out-File -LiteralPath `$statusPath -Encoding UTF8 -Append
  } catch {
    `$code = 1
    ("error " + (Get-Date).ToString("o") + " " + `$_.Exception.Message) | Out-File -LiteralPath `$statusPath -Encoding UTF8 -Append
    `$_.Exception.Message | Out-File -LiteralPath `$stderrPath -Encoding UTF8 -Append
  }
  if (`$code -eq 75) { Start-Sleep -Seconds 1 } else { Start-Sleep -Seconds 3 }
}
"@ | Set-Content -Path $RunnerPath -Encoding UTF8

@"
@echo off
set NODE_OPTIONS=
"$NodePath" "$AgentPath" ctl %*
"@ | Set-Content -Path $CtlPath -Encoding ASCII

  $Autostart = Enable-AgentAutostart
  Enable-BrowserLocalNetworkAccessPolicy
  Enable-AppLaunchAtLogon
  Start-Sleep -Milliseconds 700
  Start-AgentNow

  Write-SotyLog "soty-agent:installed:$Autostart"
} finally {
  Stop-Transcript | Out-Null
}
