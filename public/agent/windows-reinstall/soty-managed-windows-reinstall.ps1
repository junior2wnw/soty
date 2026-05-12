param(
  [ValidateSet("preflight", "prepare", "status", "arm")]
  [string] $Action = "status",
  [string] $UsbDriveLetter = "D",
  [string] $ConfirmationPhrase = "",
  [switch] $UseExistingUsbInstallImage,
  [string] $ManifestUrl = "https://xn--n1afe0b.online/agent/manifest.json",
  [string] $PanelSiteUrl = "https://xn--n1afe0b.online",
  [string] $WorkspaceRoot = "C:\ProgramData\Soty\WindowsReinstall"
)

$ErrorActionPreference = "Stop"
$ProgressPreference = "SilentlyContinue"
try {
  [Console]::InputEncoding = [System.Text.Encoding]::UTF8
  [Console]::OutputEncoding = [System.Text.Encoding]::UTF8
  $OutputEncoding = [System.Text.Encoding]::UTF8
  chcp.com 65001 > $null
} catch {}

function Emit($Value, [int] $Code = 0) {
  $Value | ConvertTo-Json -Depth 14 -Compress
  exit $Code
}

function New-Dir([string] $Path) {
  if (-not [string]::IsNullOrWhiteSpace($Path)) {
    New-Item -ItemType Directory -Force -Path $Path | Out-Null
  }
}

function Read-JsonFile([string] $Path) {
  if (-not (Test-Path -LiteralPath $Path)) { return $null }
  try {
    return (Get-Content -LiteralPath $Path -Raw -ErrorAction Stop | ConvertFrom-Json -ErrorAction Stop)
  } catch {
    return $null
  }
}

function Tail-Text([string] $Path, [int] $Chars = 4000) {
  if (-not (Test-Path -LiteralPath $Path)) { return "" }
  $stream = $null
  try {
    $item = Get-Item -LiteralPath $Path -ErrorAction Stop
    $bytesToRead = [Math]::Min([int64] $item.Length, [int64] ([Math]::Max(1024, $Chars * 4)))
    $buffer = New-Object byte[] $bytesToRead
    $stream = [System.IO.File]::Open($Path, [System.IO.FileMode]::Open, [System.IO.FileAccess]::Read, [System.IO.FileShare]::ReadWrite)
    [void] $stream.Seek(-$bytesToRead, [System.IO.SeekOrigin]::End)
    $read = $stream.Read($buffer, 0, $buffer.Length)
    $text = [System.Text.Encoding]::UTF8.GetString($buffer, 0, $read)
    if ($text.Length -gt $Chars) { return $text.Substring($text.Length - $Chars) }
    return $text
  } catch {
    try {
      $text = (Get-Content -LiteralPath $Path -Tail 80 -ErrorAction Stop | Out-String)
      if ($text.Length -gt $Chars) { return $text.Substring($text.Length - $Chars) }
      return $text
    } catch {
      return ""
    }
  } finally {
    if ($stream) { $stream.Dispose() }
  }
}

function Normalize-UsbLetter([string] $Value) {
  $letter = ([string] $Value).Trim().TrimEnd([char[]]":\").ToUpperInvariant()
  if ($letter -notmatch "^[A-Z]$") { return "D" }
  return $letter
}

function Get-SotyUserName {
  return (-join ([char[]](0x0421, 0x043E, 0x0442, 0x044B)))
}

function Count-FilesSafe([string] $Path) {
  if (-not (Test-Path -LiteralPath $Path)) { return 0 }
  try { return @((Get-ChildItem -LiteralPath $Path -File -Recurse -ErrorAction SilentlyContinue)).Count } catch { return 0 }
}

function Get-PrepareJobs([string] $Root) {
  $roots = @(
    (Join-Path $env:ProgramData "soty-agent\ops\jobs"),
    (Join-Path $Root "jobs")
  ) | Where-Object { -not [string]::IsNullOrWhiteSpace($_) } | Select-Object -Unique
  $items = New-Object System.Collections.Generic.List[object]
  foreach ($jobsRoot in $roots) {
    if (-not (Test-Path -LiteralPath $jobsRoot)) { continue }
    Get-ChildItem -LiteralPath $jobsRoot -Directory -ErrorAction SilentlyContinue |
      Where-Object { $_.Name -match "prepare.*windows.*reinstall|prepare-" } |
      Sort-Object LastWriteTimeUtc -Descending |
      Select-Object -First 8 |
      ForEach-Object {
        $resultPath = Join-Path $_.FullName "result.json"
        $result = Read-JsonFile $resultPath
        $stdoutPath = Join-Path $_.FullName "stdout.txt"
        $stderrPath = Join-Path $_.FullName "stderr.txt"
        $items.Add([pscustomobject]@{
          id = $_.Name
          path = $_.FullName
          status = if ($result) { [string] $result.status } else { "running-or-started" }
          ok = if ($result) { [bool] $result.ok } else { $false }
          exitCode = if ($result -and $null -ne $result.exitCode) { [int] $result.exitCode } else { $null }
          caseId = if ($result) { [string] $result.caseId } else { "" }
          resultPath = if (Test-Path -LiteralPath $resultPath) { $resultPath } else { "" }
          stdoutTail = Tail-Text $stdoutPath 2500
          stderrTail = Tail-Text $stderrPath 1500
          updated = $_.LastWriteTimeUtc.ToString("o")
        })
      }
  }
  return @($items | Sort-Object updated -Descending | Select-Object -First 8)
}

function Get-InstallImageCandidate([string[]] $SourceRoots) {
  foreach ($root in $SourceRoots) {
    if ([string]::IsNullOrWhiteSpace($root) -or -not (Test-Path -LiteralPath $root)) { continue }
    foreach ($name in @("install.swm", "install.esd", "install.wim")) {
      $path = Join-Path $root $name
      if (Test-Path -LiteralPath $path) { return $path }
    }
  }
  return ""
}

function Get-MediaStatus([string] $Root, [string] $Letter) {
  $usbRoot = $Letter + ":\"
  $downloadProcesses = @()
  try {
    $downloadProcesses = @(Get-CimInstance Win32_Process -ErrorAction SilentlyContinue |
      Where-Object {
        ([string]$_.Name -match "^(curl|powershell|pwsh|bitsadmin)\.exe$") -and
        ([string]$_.CommandLine -match "WindowsReinstall|Windows11_|\.download|dl\.delivery|Soty Windows reinstall image")
      } |
      Select-Object -First 8 ProcessId, Name, CommandLine)
  } catch { $downloadProcesses = @() }
  $roots = @(
    (Join-Path $Root "media"),
    (Join-Path $usbRoot "sources"),
    (Join-Path (Join-Path $usbRoot "Soty-Reinstall") "sources")
  ) | Where-Object { -not [string]::IsNullOrWhiteSpace($_) } | Select-Object -Unique
  $items = New-Object System.Collections.Generic.List[object]
  foreach ($root in $roots) {
    if (-not (Test-Path -LiteralPath $root)) { continue }
    Get-ChildItem -LiteralPath $root -File -ErrorAction SilentlyContinue |
      Where-Object { $_.Name -match "\.(download|esd|wim|swm)$" } |
      ForEach-Object {
        $items.Add([pscustomobject]@{
          path = $_.FullName
          name = $_.Name
          bytes = [int64] $_.Length
          gb = [math]::Round(([double] $_.Length / 1GB), 2)
          downloading = $_.Name -match "\.download$"
          complete = $_.Name -notmatch "\.download$"
          updated = $_.LastWriteTimeUtc.ToString("o")
          updatedAgeSeconds = [math]::Round(((Get-Date).ToUniversalTime() - $_.LastWriteTimeUtc).TotalSeconds, 0)
        })
      }
    Get-ChildItem -LiteralPath $root -Directory -Filter "*.download.parts" -ErrorAction SilentlyContinue |
      ForEach-Object {
        $partDir = $_.FullName
        $downloadPath = $partDir.Substring(0, $partDir.Length - ".parts".Length)
        $prefixBytes = [int64]0
        $prefixUpdated = [datetime]::MinValue
        if (Test-Path -LiteralPath $downloadPath) {
          $prefix = Get-Item -LiteralPath $downloadPath -ErrorAction SilentlyContinue
          if ($prefix) {
            $prefixBytes = [int64] $prefix.Length
            $prefixUpdated = $prefix.LastWriteTimeUtc
          }
        }
        $partBytes = [int64]0
        $partUpdated = $prefixUpdated
        Get-ChildItem -LiteralPath $partDir -File -ErrorAction SilentlyContinue |
          Where-Object { $_.Name -match "\.(seg|tmp)$" } |
          ForEach-Object {
            $partBytes += [int64] $_.Length
            if ($_.LastWriteTimeUtc -gt $partUpdated) { $partUpdated = $_.LastWriteTimeUtc }
          }
        $updatedAge = if ($partUpdated -gt [datetime]::MinValue) { [math]::Round(((Get-Date).ToUniversalTime() - $partUpdated).TotalSeconds, 0) } else { $null }
        $items.Add([pscustomobject]@{
          path = $downloadPath
          name = $_.Name
          bytes = [int64] ($prefixBytes + $partBytes)
          gb = [math]::Round(([double] ($prefixBytes + $partBytes) / 1GB), 2)
          downloading = $true
          complete = $false
          updated = if ($partUpdated -gt [datetime]::MinValue) { $partUpdated.ToString("o") } else { "" }
          updatedAgeSeconds = $updatedAge
        })
      }
  }
  $largest = @($items | Sort-Object bytes -Descending | Select-Object -First 1)
  if (-not $largest) {
    return [pscustomobject]@{ found = $false; path = ""; bytes = 0; gb = 0; downloading = $false; complete = $false; active = $false; activeProcessCount = @($downloadProcesses).Count; updated = ""; updatedAgeSeconds = $null }
  }
  $age = if ($null -ne $largest.updatedAgeSeconds) { [double] $largest.updatedAgeSeconds } else { $null }
  $active = [bool]($largest.downloading -and ((@($downloadProcesses).Count -gt 0) -or ($null -ne $age -and $age -lt 900)))
  return [pscustomobject]@{
    found = $true
    path = [string] $largest.path
    name = [string] $largest.name
    bytes = [int64] $largest.bytes
    gb = [double] $largest.gb
    downloading = [bool] $largest.downloading
    complete = [bool] $largest.complete
    active = $active
    activeProcessCount = @($downloadProcesses).Count
    updated = [string] $largest.updated
    updatedAgeSeconds = $age
  }
}

function Get-ReinstallStatus([string] $Root, [string] $Letter) {
  $usbRoot = $Letter + ":\"
  $usbReinstall = Join-Path (Join-Path $usbRoot "Soty-Reinstall") "reinstall"
  $readyPath = Join-Path $Root "ready.json"
  $usbReadyPath = Join-Path $usbReinstall "ready.json"
  $ready = Read-JsonFile $readyPath
  if (-not $ready) { $ready = Read-JsonFile $usbReadyPath }
  $backupProof = if ($ready) { $ready.backupProof } else { $null }
  $installImage = Get-InstallImageCandidate @((Join-Path $usbRoot "sources"), (Join-Path (Join-Path $usbRoot "Soty-Reinstall") "sources"))
  $prepareJobs = Get-PrepareJobs $Root
  $latestPrepare = $prepareJobs | Select-Object -First 1
  return [pscustomobject]@{
    ok = $true
    action = "status"
    computerName = $env:COMPUTERNAME
    workspaceRoot = $Root
    usbRoot = $usbRoot
    ready = [bool] $ready
    readyPath = if (Test-Path -LiteralPath $readyPath) { $readyPath } elseif (Test-Path -LiteralPath $usbReadyPath) { $usbReadyPath } else { "" }
    caseId = if ($ready) { [string] $ready.caseId } else { "" }
    confirmationPhrase = if ($ready) { [string] $ready.confirmationPhrase } else { "" }
    managedUserName = if ($ready) { [string] $ready.managedUserName } else { "" }
    managedUserPasswordMode = if ($ready) { [string] $ready.managedUserPasswordMode } else { "" }
    backupRoot = if ($ready) { [string] $ready.backupRoot } else { "" }
    backupProofOk = if ($backupProof) { [bool] $backupProof.ok } else { $false }
    personalFileTotalCount = if ($backupProof -and $null -ne $backupProof.personalFileTotalCount) { [int] $backupProof.personalFileTotalCount } else { $null }
    desktopFileCount = if ($backupProof -and $null -ne $backupProof.desktopFileCount) { [int] $backupProof.desktopFileCount } else { $null }
    media = Get-MediaStatus $Root $Letter
    installImage = $installImage
    rootAutounattend = Test-Path -LiteralPath (Join-Path $usbRoot "Autounattend.xml")
    oemSetupComplete = if ($installImage) {
      $sourceRoot = Split-Path -Parent $installImage
      Test-Path -LiteralPath (Join-Path (Join-Path (Join-Path (Join-Path $sourceRoot '$OEM$') '$$') "Setup\Scripts") "SetupComplete.cmd")
    } else { $false }
    latestPrepare = $latestPrepare
    prepareJobs = @($prepareJobs)
  }
}

function Get-ManagedScript([string] $Name, [string] $Root) {
  if ([string]::IsNullOrWhiteSpace($ManifestUrl)) { throw "manifestUrl is empty" }
  $manifest = Invoke-RestMethod -Uri $ManifestUrl -UseBasicParsing -TimeoutSec 30 -ErrorAction Stop
  $scriptSpec = @($manifest.windowsReinstall.scripts | Where-Object { [string]$_.name -eq $Name } | Select-Object -First 1)
  if (-not $scriptSpec) { throw ("manifest missing windowsReinstall script " + $Name) }
  if ([string]::IsNullOrWhiteSpace([string]$scriptSpec.url) -or [string]::IsNullOrWhiteSpace([string]$scriptSpec.sha256)) {
    throw ("manifest script " + $Name + " is incomplete")
  }
  $downloadRoot = Join-Path $Root "downloads\manifest-scripts"
  New-Dir $downloadRoot
  $baseUri = New-Object System.Uri -ArgumentList $ManifestUrl
  $scriptUri = New-Object System.Uri -ArgumentList $baseUri, ([string] $scriptSpec.url)
  $fileName = Split-Path -Leaf ([string] $scriptSpec.url)
  if ([string]::IsNullOrWhiteSpace($fileName)) { $fileName = "soty-" + $Name + ".ps1" }
  $path = Join-Path $downloadRoot $fileName
  $expected = ([string] $scriptSpec.sha256).ToLowerInvariant()
  if (Test-Path -LiteralPath $path) {
    $cached = (Get-FileHash -Algorithm SHA256 -LiteralPath $path).Hash.ToLowerInvariant()
    if ($cached -eq $expected) {
      return [pscustomobject]@{ path = $path; url = $scriptUri.AbsoluteUri; sha256 = $cached; bytes = (Get-Item -LiteralPath $path).Length; cached = $true }
    }
  }
  Invoke-WebRequest -Uri $scriptUri.AbsoluteUri -UseBasicParsing -OutFile $path -TimeoutSec 120 -ErrorAction Stop
  $actual = (Get-FileHash -Algorithm SHA256 -LiteralPath $path).Hash.ToLowerInvariant()
  if ($actual -ne $expected) { throw ("SHA256 mismatch for " + $Name + ": expected=" + $expected + " actual=" + $actual) }
  return [pscustomobject]@{ path = $path; url = $scriptUri.AbsoluteUri; sha256 = $actual; bytes = (Get-Item -LiteralPath $path).Length; cached = $false }
}

function Invoke-ManagedPrepare([string] $Root, [string] $Letter) {
  $currentStatus = Get-ReinstallStatus $Root $Letter
  if ($currentStatus.ready -and $currentStatus.backupProofOk) {
    Emit ([pscustomobject]@{ ok = $true; action = "prepare"; alreadyReady = $true; status = $currentStatus }) 0
  }
  if ($currentStatus.latestPrepare -and [string] $currentStatus.latestPrepare.status -eq "running-or-started") {
    $updated = $null
    try { $updated = [DateTime]::Parse([string] $currentStatus.latestPrepare.updated).ToUniversalTime() } catch {}
    if ($updated -and (((Get-Date).ToUniversalTime() - $updated).TotalMinutes -lt 180)) {
      Emit ([pscustomobject]@{ ok = $true; action = "prepare"; alreadyRunning = $true; status = $currentStatus }) 0
    }
  }
  if ($currentStatus.media -and $currentStatus.media.downloading -and $currentStatus.media.active) {
    Emit ([pscustomobject]@{ ok = $true; action = "prepare"; alreadyRunning = $true; status = $currentStatus }) 0
  }
  $script = Get-ManagedScript "prepare" $Root
  $managedUserName = Get-SotyUserName
  $panel = ([string] $PanelSiteUrl).TrimEnd("/")
  if ([string]::IsNullOrWhiteSpace($panel)) { $panel = "https://xn--n1afe0b.online" }
  $psArgs = @(
    "-NoLogo", "-NoProfile", "-ExecutionPolicy", "Bypass",
    "-File", $script.path,
    "-WorkspaceRoot", $Root,
    "-UsbDriveLetter", $Letter,
    "-ManagedUserName", $managedUserName,
    "-PanelSiteUrl", $panel,
    "-NoTemporaryManagedPassword"
  )
  if ($UseExistingUsbInstallImage) { $psArgs += "-UseExistingUsbInstallImage" }
  if (-not [string]::IsNullOrWhiteSpace($ConfirmationPhrase)) { $psArgs += @("-ConfirmationPhrase", $ConfirmationPhrase) }
  $output = & powershell.exe @psArgs 2>&1
  $code = if ($null -ne $global:LASTEXITCODE) { [int] $global:LASTEXITCODE } else { 0 }
  $text = ($output | Out-String).Trim()
  $launched = $null
  try { if ($text) { $launched = $text | ConvertFrom-Json -ErrorAction Stop } } catch {}
  Emit ([pscustomobject]@{
    ok = ($code -eq 0)
    action = "prepare"
    script = $script
    launched = $launched
    text = if ($launched) { "" } else { $text }
    status = Get-ReinstallStatus $Root $Letter
  }) $code
}

function Invoke-ManagedArm([string] $Root, [string] $Letter) {
  $status = Get-ReinstallStatus $Root $Letter
  $managedUserName = Get-SotyUserName
  if (-not $status.ready) { throw "managed reinstall is not ready" }
  if ([string] $status.managedUserName -ne $managedUserName) { throw ("managed user must be " + $managedUserName + ", got " + [string] $status.managedUserName) }
  if ([string] $status.managedUserPasswordMode -ne "blank-no-password") { throw ("managed account must be passwordless, got " + [string] $status.managedUserPasswordMode) }
  if ($status.backupProofOk -ne $true) { throw "backup proof is incomplete" }
  if ([string]::IsNullOrWhiteSpace($ConfirmationPhrase)) { throw "confirmation phrase is empty" }
  $script = Get-ManagedScript "arm" $Root
  $output = & powershell.exe -NoLogo -NoProfile -ExecutionPolicy Bypass -File $script.path -WorkspaceRoot $Root -UsbDriveLetter $Letter -ConfirmationPhrase $ConfirmationPhrase -ExpectedManagedUserName $managedUserName 2>&1
  $code = if ($null -ne $global:LASTEXITCODE) { [int] $global:LASTEXITCODE } else { 0 }
  $text = ($output | Out-String).Trim()
  $result = $null
  try { if ($text) { $result = $text | ConvertFrom-Json -ErrorAction Stop } } catch {}
  Emit ([pscustomobject]@{
    ok = ($code -eq 0)
    action = "arm"
    script = $script
    result = $result
    text = if ($result) { "" } else { $text }
  }) $code
}

try {
  New-Dir $WorkspaceRoot
  $letter = Normalize-UsbLetter $UsbDriveLetter
  if ($Action -eq "status") {
    Emit (Get-ReinstallStatus $WorkspaceRoot $letter)
  }
  if ($Action -eq "preflight") {
    $identity = [Security.Principal.WindowsIdentity]::GetCurrent()
    $principal = New-Object Security.Principal.WindowsPrincipal($identity)
    $isAdmin = $principal.IsInRole([Security.Principal.WindowsBuiltInRole]::Administrator)
    $os = Get-CimInstance Win32_OperatingSystem -ErrorAction SilentlyContinue
    $usb = $null
    try {
      $volume = Get-Volume -DriveLetter $letter -ErrorAction Stop
      $usb = [pscustomobject]@{
        driveLetter = $letter
        driveType = [string] $volume.DriveType
        fileSystem = [string] $volume.FileSystem
        sizeGB = [math]::Round(([double] $volume.Size / 1GB), 2)
        freeGB = [math]::Round(([double] $volume.SizeRemaining / 1GB), 2)
        removable = ([string] $volume.DriveType -eq "Removable")
      }
    } catch {
      $usb = [pscustomobject]@{ driveLetter = $letter; error = $_.Exception.Message }
    }
    $bitlocker = $null
    try {
      $blv = Get-BitLockerVolume -MountPoint "C:" -ErrorAction Stop
      $bitlocker = [pscustomobject]@{ protectionStatus = [string] $blv.ProtectionStatus; volumeStatus = [string] $blv.VolumeStatus; encryptionPercentage = [int] $blv.EncryptionPercentage }
    } catch {}
    $status = Get-ReinstallStatus $WorkspaceRoot $letter
    $blockers = New-Object System.Collections.Generic.List[string]
    if (-not $isAdmin) { $blockers.Add("not-elevated") }
    if ($usb.error) { $blockers.Add("usb-not-found") }
    elseif ($usb.removable -ne $true) { $blockers.Add("usb-not-removable") }
    elseif ($usb.freeGB -lt 12) { $blockers.Add("usb-free-space-low") }
    Emit ([pscustomobject]@{
      ok = ($blockers.Count -eq 0)
      action = "preflight"
      computerName = $env:COMPUTERNAME
      osCaption = if ($os) { [string] $os.Caption } else { "" }
      osVersion = if ($os) { [string] $os.Version } else { "" }
      isAdmin = $isAdmin
      usb = $usb
      bitLockerC = $bitlocker
      status = $status
      blockers = @($blockers)
    }) $(if ($blockers.Count -eq 0) { 0 } else { 1 })
  }
  if ($Action -eq "prepare") {
    Invoke-ManagedPrepare $WorkspaceRoot $letter
  }
  if ($Action -eq "arm") {
    Invoke-ManagedArm $WorkspaceRoot $letter
  }
  throw ("unsupported reinstall action: " + $Action)
} catch {
  $letter = Normalize-UsbLetter $UsbDriveLetter
  $status = $null
  try { $status = Get-ReinstallStatus $WorkspaceRoot $letter } catch {}
  Emit ([pscustomobject]@{
    ok = $false
    action = $Action
    error = $_.Exception.Message
    status = $status
  }) 1
}
