param(
  [string] $WorkspaceRoot = "C:\ProgramData\Soty\WindowsReinstall",
  [string] $UsbDriveLetter = "D",
  [string] $ManagedUserName = (-join ([char[]](0x0421, 0x043E, 0x0442, 0x044B))),
  [string] $ManagedUserPassword = "",
  [string] $PanelSiteUrl = "https://xn--n1afe0b.online",
  [string] $WindowsImageUrl = "http://dl.delivery.mp.microsoft.com/filestreamingservice/files/071fc359-1d92-46c0-ad88-c7801d2f69be/26200.6584.250915-1905.25h2_ge_release_svc_refresh_CLIENTCONSUMER_RET_x64FRE_ru-ru.esd",
  [string] $WindowsImageSha256 = "cb2fbc4af7979cf7e5f740f03289d6eacb19dd75a4858d66bc6a50aa26c37005",
  [string] $ConfirmationPhrase = "",
  [switch] $UseExistingUsbInstallImage,
  [switch] $AllowTemporaryManagedPassword,
  [switch] $NoTemporaryManagedPassword,
  [switch] $Detached,
  [string] $JobRoot = ""
)

$ErrorActionPreference = "Stop"
$ProgressPreference = "SilentlyContinue"
[Console]::InputEncoding = [System.Text.Encoding]::UTF8
[Console]::OutputEncoding = [System.Text.Encoding]::UTF8
$OutputEncoding = [System.Text.Encoding]::UTF8
try { chcp.com 65001 > $null } catch {}

function Quote-Arg([string] $Value) {
  '"' + ($Value -replace '"', '\"') + '"'
}

function New-Directory([string] $Path) {
  if ([string]::IsNullOrWhiteSpace($Path)) { throw "Empty directory path." }
  New-Item -ItemType Directory -Force -Path $Path | Out-Null
}

function Normalize-UsbLetter([string] $Value) {
  $letter = ([string] $Value).Trim().TrimEnd([char[]]":\").ToUpperInvariant()
  if ($letter -notmatch "^[A-Z]$") { return "" }
  return $letter
}

function Get-UsbVolumeSafe([string] $Letter) {
  if ([string]::IsNullOrWhiteSpace($Letter)) { return $null }
  try { return Get-Volume -DriveLetter $Letter -ErrorAction Stop } catch { return $null }
}

function Get-UsbCandidateVolumes {
  $items = New-Object System.Collections.Generic.List[object]
  try {
    Get-Volume -ErrorAction SilentlyContinue |
      Where-Object { -not [string]::IsNullOrWhiteSpace([string] $_.DriveLetter) } |
      ForEach-Object {
        $letter = ([string] $_.DriveLetter).Trim().ToUpperInvariant()
        if ($letter -match "^[A-Z]$") {
          $root = $letter + ":\"
          $sourceRoots = @(
            (Join-Path $root "sources"),
            (Join-Path (Join-Path $root "Soty-Reinstall") "sources")
          )
          $hasReinstall = Test-Path -LiteralPath (Join-Path $root "Soty-Reinstall")
          $hasInstallImage = -not [string]::IsNullOrWhiteSpace((Get-InstallImageCandidate $sourceRoots).Path)
          $removable = ([string] $_.DriveType -eq "Removable")
          if ($removable -or $hasReinstall -or $hasInstallImage) {
            $items.Add([pscustomobject]@{
              driveLetter = $letter
              root = $root
              driveType = [string] $_.DriveType
              fileSystem = [string] $_.FileSystem
              sizeGB = [math]::Round(([double] $_.Size / 1GB), 2)
              freeGB = [math]::Round(([double] $_.SizeRemaining / 1GB), 2)
              removable = $removable
              hasSotyReinstall = $hasReinstall
              hasInstallImage = $hasInstallImage
              accepted = ($removable -or $hasReinstall -or $hasInstallImage)
            })
          }
        }
      }
  } catch {}
  return @($items | Sort-Object @{ Expression = "hasSotyReinstall"; Descending = $true }, @{ Expression = "hasInstallImage"; Descending = $true }, "driveLetter")
}

function New-UsbSelection([string] $Letter, $Volume, [bool] $AutoSelected = $false, [object[]] $Candidates = @()) {
  $root = $Letter + ":\"
  $sourceRoots = @(
    (Join-Path $root "sources"),
    (Join-Path (Join-Path $root "Soty-Reinstall") "sources")
  )
  $hasReinstall = Test-Path -LiteralPath (Join-Path $root "Soty-Reinstall")
  $hasInstallImage = -not [string]::IsNullOrWhiteSpace((Get-InstallImageCandidate $sourceRoots).Path)
  $removable = ([string] $Volume.DriveType -eq "Removable")
  return [pscustomobject]@{
    found = $true
    autoSelected = $AutoSelected
    driveLetter = $Letter
    root = $root
    driveType = [string] $Volume.DriveType
    fileSystem = [string] $Volume.FileSystem
    sizeGB = [math]::Round(([double] $Volume.Size / 1GB), 2)
    freeGB = [math]::Round(([double] $Volume.SizeRemaining / 1GB), 2)
    removable = $removable
    hasSotyReinstall = $hasReinstall
    hasInstallImage = $hasInstallImage
    accepted = ($removable -or $hasReinstall -or $hasInstallImage)
    candidates = @($Candidates)
  }
}

function Resolve-InstallUsbDrive([string] $RequestedLetter) {
  $letter = Normalize-UsbLetter $RequestedLetter
  $candidates = @(Get-UsbCandidateVolumes)
  $volume = Get-UsbVolumeSafe $letter
  if ($volume) {
    return New-UsbSelection $letter $volume $false $candidates
  }
  $usable = @($candidates | Where-Object { $_.accepted -eq $true })
  if (@($usable).Count -eq 1) {
    $selectedLetter = [string] $usable[0].driveLetter
    $selectedVolume = Get-UsbVolumeSafe $selectedLetter
    if ($selectedVolume) {
      return New-UsbSelection $selectedLetter $selectedVolume $true $candidates
    }
  }
  return [pscustomobject]@{
    found = $false
    autoSelected = $false
    requestedDriveLetter = $letter
    driveLetter = ""
    root = ""
    ambiguous = (@($usable).Count -gt 1)
    error = if (@($usable).Count -gt 1) { "Multiple possible reinstall USB drives found." } elseif ($letter) { "Drive " + $letter + ": was not found." } else { "No reinstall USB drive was found." }
    candidates = @($candidates)
  }
}

if (-not $Detached) {
  $jobsRoot = Join-Path $env:ProgramData "soty-agent\ops\jobs"
  New-Directory $jobsRoot
  $jobId = (Get-Date -Format "yyyyMMdd-HHmmss") + "-prepare-windows-reinstall"
  $jobRootValue = Join-Path $jobsRoot $jobId
  New-Directory $jobRootValue
  $jobScript = Join-Path $jobRootValue "soty-prepare-windows-reinstall.ps1"
  Copy-Item -LiteralPath $PSCommandPath -Destination $jobScript -Force

  $argParts = @(
    "-NoLogo",
    "-NoProfile",
    "-ExecutionPolicy",
    "Bypass",
    "-File",
    (Quote-Arg $jobScript),
    "-Detached",
    "-JobRoot",
    (Quote-Arg $jobRootValue),
    "-WorkspaceRoot",
    (Quote-Arg $WorkspaceRoot),
    "-UsbDriveLetter",
    (Quote-Arg $UsbDriveLetter),
    "-ManagedUserName",
    (Quote-Arg $ManagedUserName),
    "-ManagedUserPassword",
    (Quote-Arg $ManagedUserPassword),
    "-PanelSiteUrl",
    (Quote-Arg $PanelSiteUrl),
    "-WindowsImageUrl",
    (Quote-Arg $WindowsImageUrl),
    "-WindowsImageSha256",
    (Quote-Arg $WindowsImageSha256),
    "-ConfirmationPhrase",
    (Quote-Arg $ConfirmationPhrase)
  )
  if ($UseExistingUsbInstallImage) {
    $argParts += "-UseExistingUsbInstallImage"
  }
  if ($AllowTemporaryManagedPassword) {
    $argParts += "-AllowTemporaryManagedPassword"
  }
  if ($NoTemporaryManagedPassword) {
    $argParts += "-NoTemporaryManagedPassword"
  }
  $argList = $argParts -join " "

  Start-Process -FilePath "powershell.exe" -WindowStyle Hidden -ArgumentList $argList
  [pscustomobject]@{
    ok = $true
    detached = $true
    jobId = $jobId
    jobRoot = $jobRootValue
    resultPath = (Join-Path $jobRootValue "result.json")
    stdoutPath = (Join-Path $jobRootValue "stdout.txt")
    stderrPath = (Join-Path $jobRootValue "stderr.txt")
  } | ConvertTo-Json -Compress
  exit 0
}

if ([string]::IsNullOrWhiteSpace($JobRoot)) {
  $JobRoot = Join-Path $WorkspaceRoot ("jobs\prepare-" + (Get-Date -Format "yyyyMMdd-HHmmss"))
}

New-Directory $JobRoot
$stdoutPath = Join-Path $JobRoot "stdout.txt"
$stderrPath = Join-Path $JobRoot "stderr.txt"
$resultPath = Join-Path $JobRoot "result.json"
$startedAt = Get-Date
$script:caseId = "case-" + (Get-Date -Format "yyyyMMdd-HHmmss")

function Log([string] $Message) {
  $line = "[" + (Get-Date).ToString("o") + "] " + $Message
  Add-Content -LiteralPath $stdoutPath -Value $line -Encoding UTF8
  Write-Output $line
}

function Finish([string] $Status, [int] $ExitCode, [hashtable] $Extra = @{}) {
  $result = [ordered]@{
    ok = ($ExitCode -eq 0)
    status = $Status
    exitCode = $ExitCode
    startedAt = $startedAt.ToString("o")
    finishedAt = (Get-Date).ToString("o")
    caseId = $script:caseId
    workspaceRoot = $WorkspaceRoot
    usbRoot = $script:usbRoot
    reinstallRoot = $script:reinstallRoot
    backupRoot = $script:backupRoot
    internalBootRoot = $script:internalBootRoot
    confirmationPhrase = $script:confirmationPhrase
    stdoutPath = $stdoutPath
    stderrPath = $stderrPath
  }
  foreach ($key in $Extra.Keys) { $result[$key] = $Extra[$key] }
  Set-Content -LiteralPath $resultPath -Value ($result | ConvertTo-Json -Depth 12) -Encoding UTF8
  if ($ExitCode -ne 0) { exit $ExitCode }
}

function Invoke-LoggedCli([string] $FilePath, [string[]] $ArgumentList, [string] $LogName) {
  $log = Join-Path $JobRoot $LogName
  Log ("Running " + $FilePath + " " + ($ArgumentList -join " "))
  & $FilePath @ArgumentList *> $log
  $code = if ($null -ne $global:LASTEXITCODE) { [int] $global:LASTEXITCODE } else { 0 }
  if ($code -ne 0) {
    throw ($FilePath + " failed with exit code " + $code + ". See " + $log)
  }
}

function Invoke-LoggedCliWithTimeout([string] $FilePath, [string[]] $ArgumentList, [string] $LogName, [int] $TimeoutSec) {
  $log = Join-Path $JobRoot $LogName
  $err = $log + ".err"
  Remove-Item -LiteralPath $log, $err -Force -ErrorAction SilentlyContinue
  Log ("Running with timeout " + $TimeoutSec + "s: " + $FilePath + " " + ($ArgumentList -join " "))
  $process = Start-Process -FilePath $FilePath -ArgumentList $ArgumentList -WindowStyle Hidden -RedirectStandardOutput $log -RedirectStandardError $err -PassThru
  if (-not $process.WaitForExit($TimeoutSec * 1000)) {
    try { $process.Kill() } catch {}
    throw ($FilePath + " timed out after " + $TimeoutSec + "s. See " + $log)
  }
  try { $process.Refresh() } catch {}
  if (Test-Path -LiteralPath $err) {
    Add-Content -LiteralPath $log -Value (Get-Content -LiteralPath $err -Raw) -Encoding UTF8
  }
  if ($null -eq $process.ExitCode) {
    throw ($FilePath + " exited but exit code was unavailable. See " + $log)
  }
  if ($process.ExitCode -ne 0) {
    throw ($FilePath + " failed with exit code " + $process.ExitCode + ". See " + $log)
  }
}

function Get-FileLengthSafe([string] $Path) {
  try {
    if (Test-Path -LiteralPath $Path) { return [int64](Get-Item -LiteralPath $Path).Length }
  } catch {}
  return [int64]0
}

function Test-FileSha256([string] $Path, [string] $ExpectedSha256) {
  if ([string]::IsNullOrWhiteSpace($ExpectedSha256) -or -not (Test-Path -LiteralPath $Path)) { return $false }
  try {
    $actual = (Get-FileHash -Algorithm SHA256 -LiteralPath $Path).Hash.ToLowerInvariant()
    return ($actual -eq $ExpectedSha256.ToLowerInvariant())
  } catch {
    return $false
  }
}

function Join-BinaryFile([string] $Source, [string] $Destination) {
  $inputStream = $null
  $outputStream = $null
  try {
    $inputStream = [System.IO.File]::OpenRead($Source)
    $outputStream = [System.IO.File]::Open($Destination, [System.IO.FileMode]::Append, [System.IO.FileAccess]::Write, [System.IO.FileShare]::Read)
    $inputStream.CopyTo($outputStream)
  } finally {
    if ($inputStream) { $inputStream.Dispose() }
    if ($outputStream) { $outputStream.Dispose() }
  }
}

function Invoke-HttpRangeDownloadAttempt([string] $Uri, [string] $TempPath, [string] $LogName) {
  $before = Get-FileLengthSafe $TempPath
  $part = $TempPath + ".part"
  Remove-Item -LiteralPath $part -Force -ErrorAction SilentlyContinue
  $headers = @{}
  if ($before -gt 0) { $headers["Range"] = "bytes=$before-" }
  $response = Invoke-WebRequest -Uri $Uri -UseBasicParsing -Headers $headers -OutFile $part -TimeoutSec 1800 -ErrorAction Stop
  $statusCode = if ($response -and $null -ne $response.StatusCode) { [int]$response.StatusCode } else { 0 }
  Set-Content -LiteralPath (Join-Path $JobRoot $LogName) -Encoding UTF8 -Value ("Invoke-WebRequest status=" + $statusCode + " resumeFrom=" + $before)
  if (-not (Test-Path -LiteralPath $part)) { throw "HTTP download attempt did not create $part" }
  if ($before -gt 0 -and $statusCode -eq 206) {
    Join-BinaryFile -Source $part -Destination $TempPath
    Remove-Item -LiteralPath $part -Force -ErrorAction SilentlyContinue
  } else {
    Move-Item -LiteralPath $part -Destination $TempPath -Force
  }
}

function Get-HttpDownloadInfo([string] $Uri, [string] $LogName) {
  $result = [ordered]@{ length = [int64]0; acceptRanges = $false; source = "" }
  $curl = Get-Command curl.exe -ErrorAction SilentlyContinue
  if ($curl) {
    try {
      $head = & curl.exe -I -L --max-time 30 --retry 2 --retry-delay 2 $Uri 2>&1
      $text = ($head | Out-String)
      Set-Content -LiteralPath (Join-Path $JobRoot $LogName) -Encoding UTF8 -Value $text
      foreach ($line in ($text -split "`r?`n")) {
        if ($line -match '^\s*Content-Length:\s*(\d+)') { $result.length = [int64] $matches[1] }
        if ($line -match '^\s*Accept-Ranges:\s*bytes\b') { $result.acceptRanges = $true }
      }
      if ($result.length -gt 0) {
        $result.source = "curl-head"
        return [pscustomobject] $result
      }
    } catch {
      Log ("WARN download HEAD via curl failed: " + $_.Exception.Message)
    }
  }
  try {
    $request = [System.Net.HttpWebRequest]::Create($Uri)
    $request.Method = "HEAD"
    $request.AllowAutoRedirect = $true
    $request.Timeout = 30000
    $request.UserAgent = "Soty Windows reinstall media downloader"
    $response = $request.GetResponse()
    try {
      $result.length = [int64] $response.ContentLength
      $result.acceptRanges = ([string] $response.Headers["Accept-Ranges"]) -match '^bytes$'
      $result.source = "http-head"
    } finally {
      $response.Dispose()
    }
  } catch {
    Log ("WARN download HEAD via .NET failed: " + $_.Exception.Message)
  }
  return [pscustomobject] $result
}

function Get-ParallelDownloadCount([int64] $Bytes) {
  $envCount = 0
  try { $envCount = [int] $env:SOTY_WINDOWS_DOWNLOAD_PARALLELISM } catch { $envCount = 0 }
  if ($envCount -ge 1) { return [math]::Min(16, $envCount) }
  if ($Bytes -ge 4GB) { return 8 }
  if ($Bytes -ge 2GB) { return 6 }
  if ($Bytes -ge 512MB) { return 4 }
  return 2
}

function New-RangeSegments([int64] $Start, [int64] $End, [int] $Count, [string] $PartDir) {
  $remaining = [int64]($End - $Start + 1)
  if ($remaining -le 0) { return @() }
  $segmentCount = [math]::Max(1, [math]::Min($Count, [int][math]::Ceiling([double]$remaining / [double]64MB)))
  $chunk = [int64][math]::Ceiling([double]$remaining / [double]$segmentCount)
  $segments = @()
  for ($index = 0; $index -lt $segmentCount; $index += 1) {
    $segmentStart = [int64]($Start + ([int64]$index * $chunk))
    if ($segmentStart -gt $End) { break }
    $segmentEnd = [int64][math]::Min($End, $segmentStart + $chunk - 1)
    $fileName = ("{0:D3}-{1}-{2}.seg" -f $index, $segmentStart, $segmentEnd)
    $segments += [pscustomobject]@{
      index = $index
      start = $segmentStart
      end = $segmentEnd
      expected = [int64]($segmentEnd - $segmentStart + 1)
      path = (Join-Path $PartDir $fileName)
      tmp = (Join-Path $PartDir ($fileName + ".tmp"))
      attempts = 0
    }
  }
  return @($segments)
}

function Get-DownloadPartBytes([string] $PartDir) {
  if (-not (Test-Path -LiteralPath $PartDir)) { return [int64]0 }
  $sum = [int64]0
  Get-ChildItem -LiteralPath $PartDir -File -ErrorAction SilentlyContinue |
    Where-Object { $_.Name -match '\.(seg|tmp)$' } |
    ForEach-Object { $sum += [int64] $_.Length }
  return $sum
}

function Invoke-ParallelRangeDownloadAttempt([string] $Uri, [string] $TempPath, [string] $LogPrefix) {
  $curl = Get-Command curl.exe -ErrorAction SilentlyContinue
  if (-not $curl) { return $false }
  $info = Get-HttpDownloadInfo -Uri $Uri -LogName ($LogPrefix + "-head.txt")
  $script:lastDownloadContentLength = [int64] $info.length
  if (-not $info.acceptRanges -or $info.length -le 0) {
    Log "HTTP server did not advertise byte ranges; falling back to resumable single-stream download."
    return $false
  }

  $prefixBytes = Get-FileLengthSafe $TempPath
  if ($prefixBytes -gt $info.length) {
    Log "WARN partial download is larger than the official content length; restarting media download."
    Remove-Item -LiteralPath $TempPath -Force -ErrorAction SilentlyContinue
    $prefixBytes = 0
  }
  if ($prefixBytes -eq $info.length) { return $true }

  $partDir = $TempPath + ".parts"
  New-Directory $partDir
  $remaining = [int64]($info.length - $prefixBytes)
  $parallel = Get-ParallelDownloadCount $remaining
  $segments = @(New-RangeSegments -Start $prefixBytes -End ([int64]$info.length - 1) -Count $parallel -PartDir $partDir)
  if (@($segments).Count -eq 0) { return $true }
  Log ("Parallel Windows image download: " + @($segments).Count + " ranges, " + [math]::Round($remaining / 1MB, 1) + " MB remaining.")

  $pending = New-Object System.Collections.Queue
  foreach ($segment in $segments) { $pending.Enqueue($segment) }
  $running = New-Object System.Collections.ArrayList
  $lastProgressLog = Get-Date
  while ($pending.Count -gt 0 -or $running.Count -gt 0) {
    while ($running.Count -lt $parallel -and $pending.Count -gt 0) {
      $segment = $pending.Dequeue()
      if ((Get-FileLengthSafe $segment.path) -eq $segment.expected) { continue }
      Remove-Item -LiteralPath $segment.path, $segment.tmp -Force -ErrorAction SilentlyContinue
      $segment.attempts += 1
      if ($segment.attempts -gt 4) {
        throw ("Download range " + $segment.start + "-" + $segment.end + " failed too many times.")
      }
      $range = ([string]$segment.start) + "-" + ([string]$segment.end)
      $log = Join-Path $JobRoot ("{0}-segment-{1:D3}-attempt-{2}.txt" -f $LogPrefix, $segment.index, $segment.attempts)
      $err = $log + ".err"
      Remove-Item -LiteralPath $log, $err -Force -ErrorAction SilentlyContinue
      $args = @(
        "-L",
        "--fail",
        "--retry",
        "8",
        "--retry-delay",
        "2",
        "--retry-connrefused",
        "--connect-timeout",
        "20",
        "--speed-time",
        "180",
        "--speed-limit",
        "65536",
        "--range",
        $range,
        "--output",
        $segment.tmp,
        $Uri
      )
      $process = Start-Process -FilePath "curl.exe" -ArgumentList $args -WindowStyle Hidden -RedirectStandardOutput $log -RedirectStandardError $err -PassThru
      [void] $running.Add([pscustomobject]@{ segment = $segment; process = $process; log = $log; err = $err })
    }

    $completed = @()
    foreach ($item in @($running)) {
      if (-not $item.process.HasExited) { continue }
      try { $item.process.Refresh() } catch {}
      if (Test-Path -LiteralPath $item.err) {
        Add-Content -LiteralPath $item.log -Value (Get-Content -LiteralPath $item.err -Raw) -Encoding UTF8
      }
      $segment = $item.segment
      $bytes = Get-FileLengthSafe $segment.tmp
      if ($item.process.ExitCode -eq 0 -and $bytes -eq $segment.expected) {
        Move-Item -LiteralPath $segment.tmp -Destination $segment.path -Force
        Log ("Download range complete: " + $segment.start + "-" + $segment.end)
      } else {
        Log ("WARN download range failed: " + $segment.start + "-" + $segment.end + " exit=" + $item.process.ExitCode + " bytes=" + $bytes + "/" + $segment.expected)
        Remove-Item -LiteralPath $segment.tmp, $segment.path -Force -ErrorAction SilentlyContinue
        $pending.Enqueue($segment)
      }
      $completed += $item
    }
    foreach ($item in $completed) { [void] $running.Remove($item) }

    if (((Get-Date) - $lastProgressLog).TotalSeconds -ge 30) {
      $downloaded = (Get-FileLengthSafe $TempPath) + (Get-DownloadPartBytes $partDir)
      Log ("Parallel download progress: " + [math]::Round($downloaded / 1GB, 3) + " / " + [math]::Round(([int64]$info.length) / 1GB, 3) + " GB.")
      $lastProgressLog = Get-Date
    }
    if ($pending.Count -gt 0 -or $running.Count -gt 0) { Start-Sleep -Seconds 2 }
  }

  if ((Get-FileLengthSafe $TempPath) -ne $prefixBytes) {
    throw "Partial download prefix changed while range download was running."
  }
  foreach ($segment in @($segments | Sort-Object start)) {
    if ((Get-FileLengthSafe $segment.path) -ne $segment.expected) {
      throw ("Downloaded range is incomplete: " + $segment.path)
    }
    Join-BinaryFile -Source $segment.path -Destination $TempPath
  }
  Remove-Item -LiteralPath $partDir -Recurse -Force -ErrorAction SilentlyContinue
  return $true
}

function Invoke-ResumableDownload([string] $Uri, [string] $Destination, [string] $ExpectedSha256, [string] $LogPrefix, [int] $MaxTotalSeconds = 172800) {
  $expected = $ExpectedSha256.ToLowerInvariant()
  $tmp = $Destination + ".download"
  $parts = $tmp + ".parts"
  $script:lastDownloadContentLength = [int64]0
  if (Test-Path -LiteralPath $Destination) {
    if (Test-FileSha256 -Path $Destination -ExpectedSha256 $expected) {
      Log ("Verified cached download: " + $Destination)
      return
    }
    Log ("Removing cached file with wrong SHA256: " + $Destination)
    Remove-Item -LiteralPath $Destination -Force
  }
  if (Test-Path -LiteralPath $tmp) {
    if (Test-FileSha256 -Path $tmp -ExpectedSha256 $expected) {
      Log ("Completing previously downloaded image from " + $tmp)
      Move-Item -LiteralPath $tmp -Destination $Destination -Force
      return
    }
    $partialGb = [math]::Round((Get-FileLengthSafe $tmp) / 1GB, 2)
    Log ("Resuming Windows image download from " + $partialGb + " GB.")
  } else {
    Log "Downloading Windows image."
  }

  Get-BitsTransfer -AllUsers -ErrorAction SilentlyContinue |
    Where-Object { $_.DisplayName -eq "Soty Windows reinstall image" } |
    Remove-BitsTransfer -Confirm:$false -ErrorAction SilentlyContinue

  $initialInfo = Get-HttpDownloadInfo -Uri $Uri -LogName ($LogPrefix + "-initial-head.txt")
  $script:lastDownloadContentLength = [int64] $initialInfo.length
  if ($script:lastDownloadContentLength -lt 1GB) {
    throw "Windows image URL did not advertise a valid image size; refusing a zero-byte or expired media download."
  }

  $curl = Get-Command curl.exe -ErrorAction SilentlyContinue
  $started = Get-Date
  $attempt = 0
  $noGrowthAttempts = 0
  $maxNoGrowthAttempts = 3
  try {
    $envNoGrowth = [int] $env:SOTY_WINDOWS_DOWNLOAD_MAX_NO_GROWTH_ATTEMPTS
    if ($envNoGrowth -ge 1) { $maxNoGrowthAttempts = [math]::Min(20, $envNoGrowth) }
  } catch {}
  $lastBytes = Get-FileLengthSafe $tmp
  while (((Get-Date) - $started).TotalSeconds -lt $MaxTotalSeconds) {
    $attempt += 1
    $before = Get-FileLengthSafe $tmp
    $beforeGb = [math]::Round($before / 1GB, 3)
    Log ("Download attempt " + $attempt + " starting at " + $beforeGb + " GB.")
    try {
      $usedParallel = $false
      if ($curl) {
        $usedParallel = Invoke-ParallelRangeDownloadAttempt -Uri $Uri -TempPath $tmp -LogPrefix ($LogPrefix + "-attempt-" + $attempt)
      }
      if (-not $usedParallel -and $curl) {
        $curlArgs = @(
          "-L",
          "--fail",
          "--connect-timeout",
          "30",
          "--retry",
          "20",
          "--retry-delay",
          "5",
          "--retry-connrefused",
          "--speed-time",
          "300",
          "--speed-limit",
          "65536"
        )
        if ($before -gt 0) { $curlArgs += @("-C", "-") }
        $curlArgs += @("--output", $tmp, $Uri)
        Invoke-LoggedCliWithTimeout curl.exe $curlArgs ($LogPrefix + "-attempt-" + $attempt + ".txt") 7200
      } else {
        Invoke-HttpRangeDownloadAttempt -Uri $Uri -TempPath $tmp -LogName ($LogPrefix + "-attempt-" + $attempt + ".txt")
      }
    } catch {
      Log ("WARN download attempt " + $attempt + " did not finish: " + $_.Exception.Message)
    }

    if (-not (Test-Path -LiteralPath $tmp)) {
      $noGrowthAttempts += 1
      if ($noGrowthAttempts -ge $maxNoGrowthAttempts) {
        throw "Windows image download did not create a partial file after $noGrowthAttempts attempts; the media URL is probably expired or blocked."
      }
      Start-Sleep -Seconds ([math]::Min(300, 10 + ($attempt * 5)))
      continue
    }
    $after = Get-FileLengthSafe $tmp
    $afterGb = [math]::Round($after / 1GB, 3)
    if ($after -gt $before -or $after -gt $lastBytes) {
      $deltaMb = [math]::Round((($after - [math]::Max($before, $lastBytes)) / 1MB), 1)
      Log ("Download progress: " + $afterGb + " GB, +" + $deltaMb + " MB.")
      $noGrowthAttempts = 0
    } else {
      $noGrowthAttempts += 1
      Log ("WARN download made no visible progress; partial size is " + $afterGb + " GB.")
      if ($noGrowthAttempts -ge $maxNoGrowthAttempts) {
        throw "Windows image download made no progress after $noGrowthAttempts attempts; the media URL is probably expired or blocked."
      }
    }
    $lastBytes = $after
    if (Test-FileSha256 -Path $tmp -ExpectedSha256 $expected) {
      Move-Item -LiteralPath $tmp -Destination $Destination -Force
      Log ("Windows image download verified: " + $Destination)
      return
    }
    if ($script:lastDownloadContentLength -gt 0 -and (Get-FileLengthSafe $tmp) -ge $script:lastDownloadContentLength) {
      Log "WARN downloaded image reached the expected size but SHA256 did not match; restarting with a clean media download."
      Remove-Item -LiteralPath $tmp -Force -ErrorAction SilentlyContinue
      Remove-Item -LiteralPath $parts -Recurse -Force -ErrorAction SilentlyContinue
      $lastBytes = 0
      continue
    }
    $delay = if ($noGrowthAttempts -gt 0) { [math]::Min(900, 30 * $noGrowthAttempts) } else { [math]::Min(300, 10 + ($attempt * 5)) }
    Start-Sleep -Seconds $delay
  }
  $partial = if (Test-Path -LiteralPath $tmp) { [math]::Round((Get-FileLengthSafe $tmp) / 1GB, 3) } else { 0 }
  throw "Windows image download did not complete within the retry window; partial file is preserved at $tmp ($partial GB)."
}

function Copy-TreeIfExists([string] $Source, [string] $Destination, [int] $TimeoutSec = 45) {
  if (-not (Test-Path -LiteralPath $Source)) { return $false }
  New-Directory $Destination
  $logName = "robocopy-" + ((Split-Path -Leaf $Destination) -replace '[^A-Za-z0-9_.-]', '_') + ".txt"
  $log = Join-Path $JobRoot $logName
  $err = $log + ".err"
  Remove-Item -LiteralPath $log, $err -Force -ErrorAction SilentlyContinue
  $args = @(
    (Quote-Arg $Source),
    (Quote-Arg $Destination),
    "/E",
    "/R:1",
    "/W:1",
    "/XJ",
    "/NFL",
    "/NDL",
    "/NP"
  ) -join " "
  $process = Start-Process -FilePath "robocopy.exe" -ArgumentList $args -WindowStyle Hidden -RedirectStandardOutput $log -RedirectStandardError $err -PassThru
  if (-not $process.WaitForExit($TimeoutSec * 1000)) {
    try { $process.Kill() } catch {}
    Log "WARN robocopy timed out after ${TimeoutSec}s for $Source -> $Destination. Continuing without this optional backup folder."
    $global:LASTEXITCODE = 0
    return $false
  }
  try { $process.Refresh() } catch {}
  if (Test-Path -LiteralPath $err) {
    Add-Content -LiteralPath $log -Value (Get-Content -LiteralPath $err -Raw) -Encoding UTF8
  }
  $robocopyExitCode = if ($null -eq $process.ExitCode) { 16 } else { [int] $process.ExitCode }
  if ($robocopyExitCode -ge 8) {
    Log "WARN robocopy failed for $Source -> $Destination with exit code $robocopyExitCode"
    $global:LASTEXITCODE = 0
    return $false
  }
  $global:LASTEXITCODE = 0
  return $true
}

function Save-SotyOperatorExport([string] $Path) {
  try {
    New-Directory (Split-Path -Parent $Path)
    $response = Invoke-RestMethod -Uri "http://127.0.0.1:49424/operator/export" -Method Get -Headers @{ Origin = "https://xn--n1afe0b.online" } -TimeoutSec 70
    if (-not $response.ok -or [string]::IsNullOrWhiteSpace([string] $response.text)) {
      Log "WARN Soty operator export was unavailable. Continuing with browser-state backup only."
      return $false
    }
    [System.IO.File]::WriteAllText($Path, [string] $response.text, (New-Object System.Text.UTF8Encoding($false)))
    Log ("Captured Soty operator export: " + $Path)
    return $true
  } catch {
    Log ("WARN Soty operator export failed: " + $_.Exception.Message)
    return $false
  }
}

function Count-Files([string] $Path, [string] $Filter = "*", [switch] $Recurse) {
  if (-not (Test-Path -LiteralPath $Path)) { return 0 }
  $params = @{
    LiteralPath = $Path
    Filter = $Filter
    File = $true
    ErrorAction = "SilentlyContinue"
  }
  if ($Recurse) { $params.Recurse = $true }
  return @((Get-ChildItem @params)).Count
}

function Write-SetupFallbackArtifacts([string] $UsbRoot, [string] $UsbSources, [string] $UsbReinstall, [string] $UsbRestore) {
  $rootUnattend = Join-Path $UsbRoot "Autounattend.xml"
  Copy-Item -LiteralPath (Join-Path $UsbReinstall "unattend.xml") -Destination $rootUnattend -Force

  $oemRoot = Join-Path $UsbSources '$OEM$'
  $oemWindowsRoot = Join-Path $oemRoot '$$'
  $oemSystemDriveRoot = Join-Path $oemRoot '$1'
  $oemScripts = Join-Path $oemWindowsRoot "Setup\Scripts"
  $oemRestore = Join-Path $oemSystemDriveRoot "ProgramData\Soty\WindowsReinstall\restore"
  New-Directory $oemScripts
  New-Directory $oemRestore

  Get-ChildItem -LiteralPath $UsbRestore -Force -ErrorAction SilentlyContinue | ForEach-Object {
    Copy-Item -LiteralPath $_.FullName -Destination $oemRestore -Recurse -Force
  }

  Set-Content -LiteralPath (Join-Path $oemScripts "SetupComplete.cmd") -Encoding ASCII -Value @(
    "@echo off",
    "if not exist ""C:\ProgramData\Soty\WindowsReinstall\logs"" mkdir ""C:\ProgramData\Soty\WindowsReinstall\logs"" >nul 2>&1",
    "powershell.exe -NoLogo -NoProfile -ExecutionPolicy Bypass -File ""C:\ProgramData\Soty\WindowsReinstall\restore\postinstall.ps1"" >> ""C:\ProgramData\Soty\WindowsReinstall\logs\setupcomplete-wrapper.log"" 2>&1"
  )
}

function Get-ReinstallBackupProof {
  $sotyBrowserArtifactCount = Count-Files -Path $sotyStateRoot -Filter "*" -Recurse
  $folderNames = if ($null -ne $personalFolderNames -and @($personalFolderNames).Count -gt 0) { @($personalFolderNames) } else { @("Desktop") }
  $personalFolderCounts = [ordered]@{}
  $personalFileTotalCount = 0
  foreach ($folderName in $folderNames) {
    $count = Count-Files -Path (Join-Path $personalFilesRoot $folderName) -Filter "*" -Recurse
    $personalFolderCounts[$folderName] = $count
    $personalFileTotalCount += $count
  }
  $oemSourcesRoot = if (-not [string]::IsNullOrWhiteSpace($script:installMediaSources)) { $script:installMediaSources } else { $usbSources }
  $proof = [ordered]@{
    schema = "soty.windows-reinstall.backup-proof.v1"
    backupRoot = $script:backupRoot
    backupRootExists = (Test-Path -LiteralPath $script:backupRoot)
    wifiProfileCount = Count-Files -Path $wifiRoot -Filter "*.xml"
    driverInfCount = Count-Files -Path $driverRoot -Filter "*.inf" -Recurse
    sotyOperatorExportBackedUp = $sotyOperatorExportBackedUp
    sotyBrowserArtifactCount = $sotyBrowserArtifactCount
    personalFolderCounts = $personalFolderCounts
    personalFileTotalCount = $personalFileTotalCount
    personalFoldersBackedUp = $personalFilesBackedUp
    desktopFileCount = Count-Files -Path $desktopBackupRoot -Filter "*" -Recurse
    postinstallScript = (Test-Path -LiteralPath (Join-Path $usbRestore "postinstall.ps1"))
    rootAutounattend = (Test-Path -LiteralPath (Join-Path $script:usbRoot "Autounattend.xml"))
    oemSetupComplete = (Test-Path -LiteralPath (Join-Path (Join-Path (Join-Path (Join-Path $oemSourcesRoot '$OEM$') '$$') "Setup\Scripts") "SetupComplete.cmd"))
    createdAt = (Get-Date).ToString("o")
  }
  $proof["ok"] = [bool](
    $proof.backupRootExists -and
    $proof.postinstallScript -and
    $proof.rootAutounattend -and
    $proof.oemSetupComplete
  )
  return $proof
}

function Escape-Xml([string] $Value) {
  [System.Security.SecurityElement]::Escape($Value)
}

function Test-WindowsInstallImage([string] $ImageName, [string] $ImageDescription, [Int64] $ImageSize = 0) {
  $text = (($ImageName, $ImageDescription) -join " ")
  if ($text -match '(?i)setup|windows pe|setup media') { return $false }
  if ($ImageSize -gt 0 -and $ImageSize -lt 5GB) { return $false }
  return $true
}

function Get-WindowsEditionKind([string] $Text) {
  if ($Text -match '(?i)professional|windows\s+11\s+pro|windows\s+10\s+pro|pro\b|профессион') { return "pro" }
  if ($Text -match '(?i)home|core|домаш') { return "home" }
  return ""
}

function Test-WindowsEditionMatch([string] $ImageText, [string] $PreferredEditionHint) {
  $preferred = Get-WindowsEditionKind $PreferredEditionHint
  if ([string]::IsNullOrWhiteSpace($preferred)) { return $true }
  return ((Get-WindowsEditionKind $ImageText) -eq $preferred)
}

function Select-WindowsInstallImage($Images, [string] $PreferredEditionHint = "") {
  $installImages = @($Images | Where-Object {
    Test-WindowsInstallImage -ImageName ([string]$_.ImageName) -ImageDescription ([string]$_.ImageDescription) -ImageSize ([Int64]$_.ImageSize)
  })
  if ($installImages.Count -eq 0) {
    throw "No installable Windows OS image found in ESD."
  }
  $editionMatched = $installImages | Where-Object {
    Test-WindowsEditionMatch -ImageText ((([string]$_.ImageName), ([string]$_.ImageDescription)) -join " ") -PreferredEditionHint $PreferredEditionHint
  } | Select-Object -First 1
  if ($editionMatched) { return $editionMatched }
  $preferred = $installImages | Where-Object {
    (([string]$_.ImageName) -match '(?i)home|core|домашн') -or
    (([string]$_.ImageDescription) -match '(?i)home|core|домашн')
  } | Select-Object -First 1
  if ($preferred) { return $preferred }
  return ($installImages | Select-Object -First 1)
}

function Test-ExistingInstallWim([string] $Path, [string] $PreferredEditionHint = "") {
  if (-not (Test-Path -LiteralPath $Path)) { return $false }
  try {
    $images = @(Get-WindowsImage -ImagePath $Path -ErrorAction Stop)
    $first = $images | Select-Object -First 1
    if (-not $first) { return $false }
    if (-not (Test-WindowsInstallImage -ImageName ([string]$first.ImageName) -ImageDescription ([string]$first.ImageDescription) -ImageSize ([Int64]$first.ImageSize))) { return $false }
    return (Test-WindowsEditionMatch -ImageText ((([string]$first.ImageName), ([string]$first.ImageDescription)) -join " ") -PreferredEditionHint $PreferredEditionHint)
  } catch {
    return $false
  }
}

function Get-InstallImageCandidate([string[]] $SourceRoots) {
  foreach ($root in $SourceRoots) {
    if ([string]::IsNullOrWhiteSpace($root) -or -not (Test-Path -LiteralPath $root)) { continue }
    foreach ($name in @("install.swm", "install.esd", "install.wim")) {
      $path = Join-Path $root $name
      if (Test-Path -LiteralPath $path) {
        return [pscustomobject]@{
          Path = $path
          SourceRoot = $root
          Kind = ([IO.Path]::GetExtension($path).TrimStart(".").ToLowerInvariant())
        }
      }
    }
  }
  return $null
}

function Test-ExistingInstallImage([string] $Path, [string] $SourceRoot, [string] $PreferredEditionHint = "") {
  if (-not (Test-Path -LiteralPath $Path)) { return $false }
  if ([IO.Path]::GetExtension($Path).Equals(".swm", [StringComparison]::OrdinalIgnoreCase)) {
    $swmPattern = Join-Path $SourceRoot "install*.swm"
    $output = & dism.exe /English /Get-WimInfo "/WimFile:$Path" "/SWMFile:$swmPattern" 2>&1
    $text = $output | Out-String
    return ($LASTEXITCODE -eq 0 -and ($text -match "Index") -and (Test-WindowsEditionMatch -ImageText $text -PreferredEditionHint $PreferredEditionHint))
  }
  return (Test-ExistingInstallWim -Path $Path -PreferredEditionHint $PreferredEditionHint)
}

function Get-LoggedOnUserLeaf {
  try {
    $name = (Get-CimInstance Win32_ComputerSystem).UserName
    if (-not [string]::IsNullOrWhiteSpace($name)) {
      return (($name -split "\\")[-1])
    }
  } catch {}
  return $ManagedUserName
}

function Get-WinReImagePath {
  $candidates = @(
    "C:\Windows\System32\Recovery\Winre.wim",
    "C:\Recovery\WindowsRE\Winre.wim"
  )
  foreach ($candidate in $candidates) {
    if (Test-Path -LiteralPath $candidate) { return $candidate }
  }
  $info = (& reagentc.exe /info) 2>&1 | Out-String
  $match = [regex]::Match($info, "Windows RE location:\s+(.+)")
  if ($match.Success) {
    $location = $match.Groups[1].Value.Trim()
    if ($location.StartsWith("\\?\GLOBALROOT", [System.StringComparison]::OrdinalIgnoreCase)) {
      $path = Join-Path $location "Winre.wim"
      if (Test-Path -LiteralPath $path) { return $path }
    }
  }
  throw "WinRE image not found."
}

function Write-Unattend([string] $Path, [string] $ComputerName, [string] $UserPassword) {
  $user = Escape-Xml $ManagedUserName
  $computer = Escape-Xml $ComputerName
  $password = Escape-Xml $UserPassword
  $xml = @"
<?xml version="1.0" encoding="utf-8"?>
<unattend xmlns="urn:schemas-microsoft-com:unattend">
  <settings pass="specialize">
    <component name="Microsoft-Windows-Shell-Setup" processorArchitecture="amd64" publicKeyToken="31bf3856ad364e35" language="neutral" versionScope="nonSxS">
      <ComputerName>$computer</ComputerName>
      <TimeZone>Ekaterinburg Standard Time</TimeZone>
    </component>
  </settings>
  <settings pass="oobeSystem">
    <component name="Microsoft-Windows-International-Core" processorArchitecture="amd64" publicKeyToken="31bf3856ad364e35" language="neutral" versionScope="nonSxS">
      <InputLocale>ru-RU;en-US</InputLocale>
      <SystemLocale>ru-RU</SystemLocale>
      <UILanguage>ru-RU</UILanguage>
      <UserLocale>ru-RU</UserLocale>
    </component>
    <component name="Microsoft-Windows-Shell-Setup" processorArchitecture="amd64" publicKeyToken="31bf3856ad364e35" language="neutral" versionScope="nonSxS">
      <RegisteredOwner>$user</RegisteredOwner>
      <TimeZone>Ekaterinburg Standard Time</TimeZone>
      <AutoLogon>
        <Enabled>true</Enabled>
        <Username>$user</Username>
        <LogonCount>1</LogonCount>
        <Password>
          <Value>$password</Value>
          <PlainText>true</PlainText>
        </Password>
      </AutoLogon>
      <OOBE>
        <HideEULAPage>true</HideEULAPage>
        <HideLocalAccountScreen>true</HideLocalAccountScreen>
        <HideOEMRegistrationScreen>true</HideOEMRegistrationScreen>
        <HideOnlineAccountScreens>true</HideOnlineAccountScreens>
        <HideWirelessSetupInOOBE>true</HideWirelessSetupInOOBE>
        <NetworkLocation>Work</NetworkLocation>
        <ProtectYourPC>3</ProtectYourPC>
      </OOBE>
      <UserAccounts>
        <LocalAccounts>
          <LocalAccount xmlns:wcm="http://schemas.microsoft.com/WMIConfig/2002/State" wcm:action="add">
            <Name>$user</Name>
            <DisplayName>$user</DisplayName>
            <Group>Administrators</Group>
            <Password>
              <Value>$password</Value>
              <PlainText>true</PlainText>
            </Password>
          </LocalAccount>
        </LocalAccounts>
      </UserAccounts>
    </component>
  </settings>
</unattend>
"@
  [System.IO.File]::WriteAllText($Path, $xml, (New-Object System.Text.UTF8Encoding($false)))
}

function Write-PostInstall([string] $Path, [string] $CaseId, [string] $SourceProfileName, [bool] $ResetManagedPasswordToBlank) {
  $panel = $PanelSiteUrl
  $resetLiteral = if ($ResetManagedPasswordToBlank) { '$true' } else { '$false' }
  $post = @"
`$ErrorActionPreference = 'Continue'
`$ProgressPreference = 'SilentlyContinue'
`$logRoot = 'C:\ProgramData\Soty\WindowsReinstall\logs'
New-Item -ItemType Directory -Force -Path `$logRoot | Out-Null
`$log = Join-Path `$logRoot 'postinstall.log'
function Log([string]`$Message) { Add-Content -LiteralPath `$log -Value ('[' + (Get-Date).ToString('o') + '] ' + `$Message) -Encoding UTF8 }
Log 'postinstall started'
`$usbRoot = `$null
foreach (`$letter in 'D','E','F','G','H','I','J','K','L','M','N','O','P','Q','R','S','T','U','V','W','X','Y','Z') {
  `$candidate = `$letter + ':\Soty-Reinstall'
  if (Test-Path -LiteralPath `$candidate) { `$usbRoot = `$candidate; break }
}
if (-not `$usbRoot) { Log 'USB Soty-Reinstall not found'; exit 1 }
`$usbDrive = Split-Path -Qualifier `$usbRoot
`$backupRoot = Join-Path `$usbDrive 'Soty-Backups\$CaseId'
Log ('usbRoot=' + `$usbRoot + ' backupRoot=' + `$backupRoot)
`$wifiRoot = Join-Path `$backupRoot 'wifi-profiles'
if (Test-Path -LiteralPath `$wifiRoot) {
  Get-ChildItem -LiteralPath `$wifiRoot -Filter '*.xml' -File -ErrorAction SilentlyContinue | ForEach-Object {
    Log ('import wifi ' + `$_.Name)
    & netsh.exe wlan add profile filename="`$(`$_.FullName)" user=all | Out-Null
  }
}
`$driversRoot = Join-Path `$backupRoot 'drivers'
if (Test-Path -LiteralPath `$driversRoot) {
  Log 'adding exported drivers'
  & pnputil.exe /add-driver "`$driversRoot\*.inf" /subdirs /install | Out-File -LiteralPath (Join-Path `$logRoot 'pnputil-add-driver.log') -Encoding UTF8
}
try {
  [Net.ServicePointManager]::SecurityProtocol = [Net.SecurityProtocolType]::Tls12
  `$dir = Join-Path `$env:ProgramData 'Soty\agent-install'
  New-Item -ItemType Directory -Force -Path `$dir | Out-Null
  `$installer = Join-Path `$dir 'install-windows.ps1'
  Invoke-WebRequest -Uri '$panel/agent/install-windows.ps1' -UseBasicParsing -OutFile `$installer
  & powershell.exe -NoLogo -NoProfile -ExecutionPolicy Bypass -File `$installer -Scope Machine -LaunchAppAtLogon | Out-File -LiteralPath (Join-Path `$logRoot 'soty-agent-install.log') -Encoding UTF8
} catch { Log ('soty install failed: ' + `$_.Exception.ToString()) }
`$firstLogon = Join-Path `$logRoot 'first-logon-restore.ps1'
@'
`$ErrorActionPreference = "Continue"
`$backupRoot = "`$backupRoot"
`$log = "C:\ProgramData\Soty\WindowsReinstall\logs\first-logon-restore.log"
function Log([string]`$Message) { Add-Content -LiteralPath `$log -Value ("[" + (Get-Date).ToString("o") + "] " + `$Message) -Encoding UTF8 }
function CopyDir([string]`$Source, [string]`$Dest) {
  if (-not (Test-Path -LiteralPath `$Source)) { return }
  New-Item -ItemType Directory -Force -Path `$Dest | Out-Null
  robocopy.exe `$Source `$Dest /E /R:1 /W:1 /XJ /NFL /NDL /NP | Out-Null
  if (`$LASTEXITCODE -le 7) { `$global:LASTEXITCODE = 0 }
}
Log "first logon restore started"
`$resetManagedPasswordToBlank = $resetLiteral
try {
  Remove-ItemProperty -Path "HKLM:\SOFTWARE\Microsoft\Windows NT\CurrentVersion\Winlogon" -Name AutoAdminLogon,DefaultPassword,AutoLogonCount,DefaultUserName,DefaultDomainName -ErrorAction SilentlyContinue
} catch {}
if (`$resetManagedPasswordToBlank) {
  try {
    & net.exe user "$ManagedUserName" "" | Out-Null
    Log "managed user password reset to blank"
  } catch { Log ("managed password cleanup warning: " + `$_.Exception.Message) }
}
`$personalFolders = @("Desktop", "Documents", "Downloads", "Pictures", "Videos", "Music")
foreach (`$folderName in `$personalFolders) {
  `$folderBackup = Join-Path `$backupRoot "personal-files\$SourceProfileName\`$folderName"
  `$folderDest = if (`$folderName -eq "Desktop") { [Environment]::GetFolderPath("Desktop") } else { Join-Path `$env:USERPROFILE `$folderName }
  if ([string]::IsNullOrWhiteSpace(`$folderDest)) { `$folderDest = Join-Path `$env:USERPROFILE `$folderName }
  CopyDir `$folderBackup `$folderDest
}
`$desktopDest = [Environment]::GetFolderPath("Desktop")
if ([string]::IsNullOrWhiteSpace(`$desktopDest)) { `$desktopDest = Join-Path `$env:USERPROFILE "Desktop" }
`$edgeDefault = Join-Path `$env:USERPROFILE "AppData\Local\Microsoft\Edge\User Data\Default"
`$sourceState = Join-Path `$backupRoot "soty-state\$SourceProfileName"
CopyDir (Join-Path `$sourceState "Edge-IndexedDB") (Join-Path `$edgeDefault "IndexedDB")
CopyDir (Join-Path `$sourceState "Edge-LocalStorage") (Join-Path `$edgeDefault "Local Storage")
CopyDir (Join-Path `$sourceState "Edge-ServiceWorker") (Join-Path `$edgeDefault "Service Worker")
CopyDir (Join-Path `$sourceState "Edge-Sessions") (Join-Path `$edgeDefault "Sessions")
`$operatorExport = Join-Path `$sourceState "operator-export.json"
`$restoreUrl = "$panel/?pwa=1"
if (Test-Path -LiteralPath `$operatorExport) {
  `$restoreUrl = "$panel/?pwa=1&restore-local=1"
  try {
    Copy-Item -LiteralPath `$operatorExport -Destination (Join-Path `$desktopDest "soty-restore.json") -Force
    Log "operator export copied to desktop"
  } catch { Log ("operator export desktop copy warning: " + `$_.Exception.Message) }
}
try { Start-Process "msedge.exe" -ArgumentList "--app=`$restoreUrl" } catch { Start-Process `$restoreUrl }
if (Test-Path -LiteralPath `$operatorExport) {
  for (`$i = 1; `$i -le 45; `$i += 1) {
    try {
      `$text = [System.IO.File]::ReadAllText(`$operatorExport)
      `$body = @{ text = `$text } | ConvertTo-Json -Compress
      `$result = Invoke-RestMethod -Uri "http://127.0.0.1:49424/operator/import" -Method Post -Headers @{ Origin = "https://xn--n1afe0b.online" } -ContentType "application/json" -Body `$body -TimeoutSec 8
      if (`$result.ok) {
        Log ("operator import completed: " + `$result.text)
        break
      }
      Log ("operator import waiting: " + `$result.text)
    } catch {
      if (`$i -eq 45) { Log ("WARN operator import failed: " + `$_.Exception.Message) }
    }
    Start-Sleep -Seconds 2
  }
}
try { Unregister-ScheduledTask -TaskName "Soty-FirstLogon-Restore" -Confirm:`$false } catch {}
Log "first logon restore finished"
'@ | Set-Content -LiteralPath `$firstLogon -Encoding UTF8
`$action = New-ScheduledTaskAction -Execute 'powershell.exe' -Argument ('-NoLogo -NoProfile -ExecutionPolicy Bypass -File "' + `$firstLogon + '"')
`$trigger = New-ScheduledTaskTrigger -AtLogOn -User '$ManagedUserName'
Register-ScheduledTask -TaskName 'Soty-FirstLogon-Restore' -Action `$action -Trigger `$trigger -Description 'Restore Soty PWA state on first managed logon' -Force | Out-Null
Log 'postinstall finished'
"@
  [System.IO.File]::WriteAllText($Path, $post, (New-Object System.Text.UTF8Encoding($true)))
}

function Write-WinPeWorker([string] $Path) {
  $worker = @'
@echo off
setlocal enableextensions enabledelayedexpansion
for %%I in ("%~dp0..") do set "REINSTALLROOT=%%~fI"
for %%I in ("%REINSTALLROOT%\..") do set "MEDIAROOT=%%~fI"
set "LOGDIR=%REINSTALLROOT%\reinstall\logs"
if not exist "%LOGDIR%" mkdir "%LOGDIR%" >nul 2>nul
set "LOG=%LOGDIR%\winre-reinstall.log"
echo [%DATE% %TIME%] worker started>"%LOG%"
if not exist "%REINSTALLROOT%\reinstall\armed.flag" (
  echo [%DATE% %TIME%] armed.flag missing>>"%LOG%"
  wpeutil reboot
  exit /b 1
)
call "%REINSTALLROOT%\reinstall\config.cmd"
echo [%DATE% %TIME%] target disk %TARGET_DISK%, managed user %MANAGED_USER%>>"%LOG%"
set "DISKPART=%REINSTALLROOT%\reinstall\diskpart.txt"
(
  echo select disk %TARGET_DISK%
  echo clean
  echo convert gpt
  echo create partition efi size=260
  echo format quick fs=fat32 label="System"
  echo assign letter=S
  echo create partition msr size=16
  echo create partition primary
  echo shrink minimum=1024
  echo format quick fs=ntfs label="Windows"
  echo assign letter=W
  echo create partition primary
  echo format quick fs=ntfs label="Recovery"
  echo assign letter=R
  echo set id=de94bba4-06d1-4d40-a16a-bfd50179d6ac
  echo gpt attributes=0x8000000000000001
  echo exit
) > "%DISKPART%"
echo [%DATE% %TIME%] diskpart clean/apply layout>>"%LOG%"
diskpart /s "%DISKPART%" >>"%LOG%" 2>>&1
if errorlevel 1 goto fail
if /I "%INSTALL_SOURCE_ROOT%"=="REINSTALL" goto select_reinstall_image
if exist "%MEDIAROOT%\sources\install.swm" (
  set "IMAGEARG=/ImageFile:%MEDIAROOT%\sources\install.swm"
  set "SWMARG=/SWMFile:%MEDIAROOT%\sources\install*.swm"
) else if exist "%MEDIAROOT%\sources\install.esd" (
  set "IMAGEARG=/ImageFile:%MEDIAROOT%\sources\install.esd"
  set "SWMARG="
) else if exist "%MEDIAROOT%\sources\install.wim" (
  set "IMAGEARG=/ImageFile:%MEDIAROOT%\sources\install.wim"
  set "SWMARG="
) else (
  goto select_reinstall_image
)
goto apply_image
:select_reinstall_image
if exist "%REINSTALLROOT%\sources\install.swm" (
  set "IMAGEARG=/ImageFile:%REINSTALLROOT%\sources\install.swm"
  set "SWMARG=/SWMFile:%REINSTALLROOT%\sources\install*.swm"
) else if exist "%REINSTALLROOT%\sources\install.esd" (
  set "IMAGEARG=/ImageFile:%REINSTALLROOT%\sources\install.esd"
  set "SWMARG="
) else (
  set "IMAGEARG=/ImageFile:%REINSTALLROOT%\sources\install.wim"
  set "SWMARG="
)
:apply_image
echo [%DATE% %TIME%] applying Windows image>>"%LOG%"
dism /Apply-Image %IMAGEARG% %SWMARG% /Index:1 /ApplyDir:W:\ >>"%LOG%" 2>>&1
if errorlevel 1 goto fail
mkdir W:\Windows\Panther >nul 2>nul
copy /y "%REINSTALLROOT%\reinstall\unattend.xml" W:\Windows\Panther\Unattend.xml >>"%LOG%" 2>>&1
dism /Image:W:\ /Apply-Unattend:"%REINSTALLROOT%\reinstall\unattend.xml" >>"%LOG%" 2>>&1
mkdir W:\ProgramData\Soty\WindowsReinstall\restore >nul 2>nul
mkdir W:\ProgramData\Soty\WindowsReinstall\logs >nul 2>nul
xcopy "%REINSTALLROOT%\restore" W:\ProgramData\Soty\WindowsReinstall\restore /E /I /H /Y >>"%LOG%" 2>>&1
mkdir W:\Windows\Setup\Scripts >nul 2>nul
(
  echo @echo off
  echo if not exist "C:\ProgramData\Soty\WindowsReinstall\logs" mkdir "C:\ProgramData\Soty\WindowsReinstall\logs" ^>nul 2^>^&1
  echo powershell.exe -NoLogo -NoProfile -ExecutionPolicy Bypass -File "C:\ProgramData\Soty\WindowsReinstall\restore\postinstall.ps1" ^>^> "C:\ProgramData\Soty\WindowsReinstall\logs\setupcomplete-wrapper.log" 2^>^&1
) > W:\Windows\Setup\Scripts\SetupComplete.cmd
echo [%DATE% %TIME%] creating boot files>>"%LOG%"
bcdboot W:\Windows /s S: /f UEFI >>"%LOG%" 2>>&1
if errorlevel 1 goto fail
echo [%DATE% %TIME%] worker completed, rebooting>>"%LOG%"
wpeutil reboot
exit /b 0
:fail
echo [%DATE% %TIME%] worker failed with errorlevel %ERRORLEVEL%>>"%LOG%"
cmd /c exit /b %ERRORLEVEL%
'@
  [System.IO.File]::WriteAllText($Path, $worker, [System.Text.Encoding]::ASCII)
}

function Write-StartNet([string] $Path) {
  $startnet = @'
@echo off
setlocal enableextensions enabledelayedexpansion
if not exist X:\Soty mkdir X:\Soty >nul 2>nul
set LOG_PATH=X:\Soty\soty-startnet.log
if exist C:\Soty-Boot\logs (
  set LOG_PATH=C:\Soty-Boot\logs\startnet.log
) else (
  mkdir C:\Soty-Boot\logs >nul 2>nul
  if exist C:\Soty-Boot\logs set LOG_PATH=C:\Soty-Boot\logs\startnet.log
)
>>"%LOG_PATH%" echo [%DATE% %TIME%] startnet entered
wpeinit >>"%LOG_PATH%" 2>&1
>>"%LOG_PATH%" echo [%DATE% %TIME%] wpeinit finished
set USBROOT=
for /L %%i in (1,1,180) do (
  set "USBROOT="
  for %%d in (C D E F G H I J K L M N O P Q R S T U V W X Y Z) do (
    if exist "%%d:\Soty-Reinstall\reinstall\winre-reinstall.cmd" if exist "%%d:\Soty-Reinstall\reinstall\config.cmd" set "USBROOT=%%d:\Soty-Reinstall"
  )
  if defined USBROOT goto usb_found
  >>"%LOG_PATH%" echo [%DATE% %TIME%] probe %%i: usb root not found
  timeout /t 2 /nobreak >nul
)
>>"%LOG_PATH%" echo [%DATE% %TIME%] usb root was not found
wpeutil reboot
exit /b 1
:usb_found
>>"%LOG_PATH%" echo [%DATE% %TIME%] usb root found at %USBROOT%
if not exist "%USBROOT%\reinstall\logs" mkdir "%USBROOT%\reinstall\logs" >nul 2>nul
type "%LOG_PATH%" >> "%USBROOT%\reinstall\logs\startnet.log" 2>nul
set LOG_PATH=%USBROOT%\reinstall\logs\startnet.log
if exist "%USBROOT%\reinstall\armed.flag" (
  call "%USBROOT%\reinstall\winre-reinstall.cmd"
  set WORKER_EXIT=!ERRORLEVEL!
  >>"%LOG_PATH%" echo [%DATE% %TIME%] worker exit code=!WORKER_EXIT!
  exit /b !WORKER_EXIT!
)
>>"%LOG_PATH%" echo [%DATE% %TIME%] armed.flag missing, rebooting without wipe
wpeutil reboot
exit /b 1
'@
  [System.IO.File]::WriteAllText($Path, $startnet, [System.Text.Encoding]::ASCII)
}

function Patch-WinPeBoot([string] $SourceWinRe, [string] $InternalBootRoot) {
  New-Directory $InternalBootRoot
  New-Directory (Join-Path $InternalBootRoot "logs")
  $mountDir = Join-Path $WorkspaceRoot "mount\winpe"
  New-Directory $mountDir
  $bootWim = Join-Path $InternalBootRoot "boot.wim"
  $stagedBoot = Join-Path $WorkspaceRoot "media-source\boot.wim"
  New-Directory (Split-Path -Parent $stagedBoot)
  try { Dismount-WindowsImage -Path $mountDir -Discard -ErrorAction SilentlyContinue | Out-Null } catch {}
  Copy-Item -LiteralPath $SourceWinRe -Destination $stagedBoot -Force
  Invoke-LoggedCli dism.exe @("/Mount-Image", "/ImageFile:$stagedBoot", "/Index:1", "/MountDir:$mountDir") "dism-mount-bootwim.txt"
  try {
    Write-StartNet (Join-Path $mountDir "Windows\System32\startnet.cmd")
    Set-Content -LiteralPath (Join-Path $mountDir "Windows\System32\soty-launch.cmd") -Value "@echo off`r`ncall %SYSTEMROOT%\System32\startnet.cmd`r`n" -Encoding ASCII
    Set-Content -LiteralPath (Join-Path $mountDir "Windows\System32\winpeshl.ini") -Value "[LaunchApps]`r`n%SYSTEMROOT%\System32\cmd.exe, /c %SYSTEMROOT%\System32\soty-launch.cmd`r`n" -Encoding ASCII
  } finally {
    Invoke-LoggedCli dism.exe @("/Unmount-Image", "/MountDir:$mountDir", "/Commit") "dism-commit-bootwim.txt"
  }
  Copy-Item -LiteralPath $stagedBoot -Destination $bootWim -Force
  $bootSdiCandidates = @(
    "C:\Windows\Boot\DVD\PCAT\boot.sdi",
    "C:\Windows\Boot\DVD\EFI\boot.sdi",
    "C:\Windows\System32\Recovery\boot.sdi"
  )
  $bootSdi = $bootSdiCandidates | Where-Object { Test-Path -LiteralPath $_ } | Select-Object -First 1
  if (-not $bootSdi) { throw "boot.sdi not found." }
  Copy-Item -LiteralPath $bootSdi -Destination (Join-Path $InternalBootRoot "boot.sdi") -Force
}

try {
  Log "Preparing Soty Windows reinstall assets."
  $identity = [Security.Principal.WindowsIdentity]::GetCurrent()
  $principal = New-Object Security.Principal.WindowsPrincipal($identity)
  if (-not $principal.IsInRole([Security.Principal.WindowsBuiltInRole]::Administrator)) {
    throw "This script must run elevated or as SYSTEM."
  }

  & powercfg.exe /change standby-timeout-ac 0 | Out-Null
  & powercfg.exe /change hibernate-timeout-ac 0 | Out-Null
  & powercfg.exe /change monitor-timeout-ac 0 | Out-Null
  & powercfg.exe /change standby-timeout-dc 0 | Out-Null
  & powercfg.exe /change hibernate-timeout-dc 0 | Out-Null

  $usbSelection = Resolve-InstallUsbDrive $UsbDriveLetter
  if ($usbSelection.found -ne $true) {
    Finish "blocked" 2 @{
      blockers = @($(if ($usbSelection.ambiguous) { "usb-ambiguous" } else { "usb-not-found" }))
      usb = $usbSelection
    }
  }
  if ($usbSelection.accepted -ne $true) {
    Finish "blocked" 2 @{
      blockers = @("usb-not-removable")
      usb = $usbSelection
    }
  }
  if ($usbSelection.freeGB -lt 12) {
    Finish "blocked" 2 @{
      blockers = @("usb-free-space-low")
      usb = $usbSelection
    }
  }

  $UsbDriveLetter = [string] $usbSelection.driveLetter
  $script:usbRoot = [string] $usbSelection.root
  $usbVolume = Get-UsbVolumeSafe $UsbDriveLetter
  if (-not $usbVolume) { throw "Drive $UsbDriveLetter was not available after USB detection." }

  $script:reinstallRoot = Join-Path $script:usbRoot "Soty-Reinstall"
  $script:backupRoot = Join-Path (Join-Path $script:usbRoot "Soty-Backups") $script:caseId
  $script:internalBootRoot = "C:\Soty-Boot"
  $script:confirmationPhrase = if ([string]::IsNullOrWhiteSpace($ConfirmationPhrase)) {
    "ERASE INTERNAL DISK " + $env:COMPUTERNAME
  } else {
    $ConfirmationPhrase
  }
  $stageRoot = Join-Path $WorkspaceRoot ("stage-bundles\" + $script:caseId)
  $restoreRoot = Join-Path $stageRoot "restore"
  $mediaRoot = Join-Path $WorkspaceRoot "media"
  $sourceRoot = Join-Path $WorkspaceRoot "media-source"
  $usbMediaSources = Join-Path $script:usbRoot "sources"
  $usbSources = Join-Path $script:reinstallRoot "sources"
  $usbReinstall = Join-Path $script:reinstallRoot "reinstall"
  $usbRestore = Join-Path $script:reinstallRoot "restore"
  foreach ($path in @($WorkspaceRoot, $stageRoot, $restoreRoot, $mediaRoot, $sourceRoot, $script:reinstallRoot, $script:backupRoot, $usbSources, $usbReinstall, $usbRestore)) {
    New-Directory $path
  }

  Log ("caseId=" + $script:caseId)
  Log ("usbRoot=" + $script:usbRoot)

  $preferredEditionHint = ""
  try {
    $currentOs = Get-CimInstance Win32_OperatingSystem -ErrorAction Stop
    $preferredEditionHint = (([string]$currentOs.Caption), ([string]$currentOs.OperatingSystemSKU)) -join " "
    Log ("Preferred Windows edition from current OS: " + $preferredEditionHint)
  } catch {
    Log ("WARN could not read current Windows edition: " + $_.Exception.Message)
  }

  $sourceProfileName = Get-LoggedOnUserLeaf
  $sourceProfile = Join-Path "C:\Users" $sourceProfileName
  $wifiRoot = Join-Path $script:backupRoot "wifi-profiles"
  $driverRoot = Join-Path $script:backupRoot "drivers"
  $sotyStateRoot = Join-Path (Join-Path $script:backupRoot "soty-state") $sourceProfileName
  $personalFilesRoot = Join-Path (Join-Path $script:backupRoot "personal-files") $sourceProfileName
  $personalFolderNames = @("Desktop", "Documents", "Downloads", "Pictures", "Videos", "Music")
  $desktopBackupRoot = Join-Path $personalFilesRoot "Desktop"
  $operatorExportPath = Join-Path $sotyStateRoot "operator-export.json"
  $personalFilesBackedUp = $false
  $sotyOperatorExportBackedUp = $false
  $backupScope = @(
    "wifi-profiles",
    "exported-drivers",
    "soty-operator-export",
    "soty-browser-return-state",
    "personal-folders-Desktop-Documents-Downloads-Pictures-Videos-Music"
  )
  New-Directory $wifiRoot
  New-Directory $driverRoot
  New-Directory $sotyStateRoot
  New-Directory $personalFilesRoot
  Log "Backup scope: Wi-Fi profiles, exported drivers, Soty operator export, Soty browser return state, and personal folders: Desktop, Documents, Downloads, Pictures, Videos, Music."
  try { & netsh.exe wlan export profile key=clear folder="$wifiRoot" | Out-File -LiteralPath (Join-Path $JobRoot "netsh-wifi-export.txt") -Encoding UTF8 } catch { Log ("WARN wifi export failed: " + $_.Exception.Message) }
  try { Invoke-LoggedCliWithTimeout dism.exe @("/online", "/export-driver", "/destination:$driverRoot") "dism-export-drivers.txt" 1800 } catch { Log ("WARN driver export failed: " + $_.Exception.Message) }
  if (Save-SotyOperatorExport $operatorExportPath) { $sotyOperatorExportBackedUp = $true }
  if (Test-Path -LiteralPath $sourceProfile) {
    foreach ($folderName in $personalFolderNames) {
      $folderSource = Join-Path $sourceProfile $folderName
      $folderDestination = Join-Path $personalFilesRoot $folderName
      if (Copy-TreeIfExists $folderSource $folderDestination 600) { $personalFilesBackedUp = $true }
    }
    $edgeDefault = Join-Path $sourceProfile "AppData\Local\Microsoft\Edge\User Data\Default"
    Copy-TreeIfExists (Join-Path $edgeDefault "IndexedDB") (Join-Path $sotyStateRoot "Edge-IndexedDB") 90 | Out-Null
    Copy-TreeIfExists (Join-Path $edgeDefault "Local Storage") (Join-Path $sotyStateRoot "Edge-LocalStorage") 90 | Out-Null
    Copy-TreeIfExists (Join-Path $edgeDefault "Service Worker") (Join-Path $sotyStateRoot "Edge-ServiceWorker") 90 | Out-Null
    Copy-TreeIfExists (Join-Path $edgeDefault "Sessions") (Join-Path $sotyStateRoot "Edge-Sessions") 90 | Out-Null
  }
  # The machine worker is reinstalled by postinstall. Copying its live ops/jobs
  # tree can hold locks or copy an active job forever, so keep it out of the
  # reinstall backup.

  $existingUsbImage = Get-InstallImageCandidate @($usbMediaSources, $usbSources)
  if ($existingUsbImage -and (Test-ExistingInstallImage -Path $existingUsbImage.Path -SourceRoot $existingUsbImage.SourceRoot -PreferredEditionHint $preferredEditionHint)) {
    $script:installMediaSources = $existingUsbImage.SourceRoot
    Log ("Using existing USB install image: " + $existingUsbImage.Path)
  } elseif ($UseExistingUsbInstallImage) {
    throw "UseExistingUsbInstallImage was set, but no valid install.swm/esd/wim exists under $usbMediaSources or $usbSources."
  } else {
    $script:installMediaSources = $usbSources
    $esdPath = Join-Path $mediaRoot "Windows11_25H2_CLIENTCONSUMER_RET_x64FRE_ru-ru.esd"
    if (Test-Path -LiteralPath $esdPath) {
      $existingHash = (Get-FileHash -Algorithm SHA256 -LiteralPath $esdPath).Hash.ToLowerInvariant()
      if ($existingHash -ne $WindowsImageSha256.ToLowerInvariant()) {
        Remove-Item -LiteralPath $esdPath -Force
      }
    }
    if (-not (Test-Path -LiteralPath $esdPath)) {
      Invoke-ResumableDownload -Uri $WindowsImageUrl -Destination $esdPath -ExpectedSha256 $WindowsImageSha256 -LogPrefix "windows-image-download"
    }

    $installWim = Join-Path $sourceRoot "install.wim"
    if ((Test-Path -LiteralPath $installWim) -and -not (Test-ExistingInstallWim -Path $installWim -PreferredEditionHint $preferredEditionHint)) {
      Log "Removing invalid cached install.wim."
      Remove-Item -LiteralPath $installWim -Force
    }
    if (-not (Test-Path -LiteralPath $installWim)) {
      Log "Exporting Windows edition from ESD."
      $images = @(Get-WindowsImage -ImagePath $esdPath -ErrorAction Stop)
      $image = Select-WindowsInstallImage -Images $images -PreferredEditionHint $preferredEditionHint
      Log ("Selected image index " + [int]$image.ImageIndex + ": " + [string]$image.ImageName)
      Invoke-LoggedCli dism.exe @("/Export-Image", "/SourceImageFile:$esdPath", "/SourceIndex:$([int]$image.ImageIndex)", "/DestinationImageFile:$installWim", "/Compress:max", "/CheckIntegrity") "dism-export-installwim.txt"
    }
    Remove-Item -LiteralPath (Join-Path $script:installMediaSources "install.swm") -Force -ErrorAction SilentlyContinue
    Remove-Item -LiteralPath (Join-Path $script:installMediaSources "install.wim") -Force -ErrorAction SilentlyContinue
    Remove-Item -LiteralPath (Join-Path $script:installMediaSources "install.esd") -Force -ErrorAction SilentlyContinue
    Get-ChildItem -LiteralPath $script:installMediaSources -Filter "install*.swm" -File -ErrorAction SilentlyContinue | Remove-Item -Force
    if ([string]$usbVolume.FileSystem -eq "NTFS") {
      Log "Copying one-index install.wim onto NTFS USB."
      Copy-Item -LiteralPath $installWim -Destination (Join-Path $script:installMediaSources "install.wim") -Force
    } else {
      Log "Splitting install image onto USB."
      Invoke-LoggedCli dism.exe @("/Split-Image", "/ImageFile:$installWim", "/SWMFile:$(Join-Path $script:installMediaSources 'install.swm')", "/FileSize:3800", "/CheckIntegrity") "dism-split-install.txt"
    }
  }

  $computerName = $env:COMPUTERNAME
  $effectiveManagedUserPassword = $ManagedUserPassword
  $managedUserPasswordGenerated = $false
  if ($AllowTemporaryManagedPassword -and $NoTemporaryManagedPassword) {
    throw "Choose either -AllowTemporaryManagedPassword or -NoTemporaryManagedPassword, not both."
  }
  if ([string]::IsNullOrEmpty($effectiveManagedUserPassword)) {
    if ($AllowTemporaryManagedPassword -and -not $NoTemporaryManagedPassword) {
      $effectiveManagedUserPassword = "Soty-" + ([Guid]::NewGuid().ToString("N"))
      $managedUserPasswordGenerated = $true
    } else {
      $effectiveManagedUserPassword = ""
    }
  }
  $managedUserPasswordMode = if ($managedUserPasswordGenerated) {
    "generated-temporary-reset-on-first-logon"
  } elseif ([string]::IsNullOrEmpty($ManagedUserPassword)) {
    "blank-no-password"
  } else {
    "operator-provided"
  }
  $installSourceMode = if ([string]::Equals($script:installMediaSources, $usbMediaSources, [StringComparison]::OrdinalIgnoreCase)) { "MEDIA" } else { "REINSTALL" }
  Write-Unattend -Path (Join-Path $usbReinstall "unattend.xml") -ComputerName $computerName -UserPassword $effectiveManagedUserPassword
  Set-Content -LiteralPath (Join-Path $usbReinstall "config.cmd") -Value @(
    "@echo off",
    "set TARGET_DISK=0",
    "set MANAGED_USER=$ManagedUserName",
    "set CASE_ID=$($script:caseId)",
    "set INSTALL_SOURCE_ROOT=$installSourceMode"
  ) -Encoding ASCII
  Write-WinPeWorker -Path (Join-Path $usbReinstall "winre-reinstall.cmd")
  Write-PostInstall -Path (Join-Path $usbRestore "postinstall.ps1") -CaseId $script:caseId -SourceProfileName $sourceProfileName -ResetManagedPasswordToBlank $managedUserPasswordGenerated
  Copy-Item -LiteralPath (Join-Path $usbRestore "postinstall.ps1") -Destination (Join-Path $restoreRoot "postinstall.ps1") -Force
  $restoreConfig = @{
    schema = "soty.windows-reinstall.restore.v1"
    caseId = $script:caseId
    backupRoot = $script:backupRoot
    panelSiteUrl = $PanelSiteUrl
    managedUserName = $ManagedUserName
    managedUserPasswordMode = $managedUserPasswordMode
    backupScope = $backupScope
    personalFolderNames = $personalFolderNames
    personalFilesBackedUp = $personalFilesBackedUp
    sotyOperatorExportBackedUp = $sotyOperatorExportBackedUp
    sourceProfileName = $sourceProfileName
    createdAt = (Get-Date).ToString("o")
  }
  $restoreConfig | ConvertTo-Json -Depth 8 | Set-Content -LiteralPath (Join-Path $usbRestore "restore-config.json") -Encoding UTF8
  Write-SetupFallbackArtifacts -UsbRoot $script:usbRoot -UsbSources $script:installMediaSources -UsbReinstall $usbReinstall -UsbRestore $usbRestore
  $backupProof = Get-ReinstallBackupProof
  $backupProof | ConvertTo-Json -Depth 8 | Set-Content -LiteralPath (Join-Path $script:backupRoot "backup-proof.json") -Encoding UTF8
  $backupProof | ConvertTo-Json -Depth 8 | Set-Content -LiteralPath (Join-Path $usbReinstall "backup-proof.json") -Encoding UTF8

  $winre = Get-WinReImagePath
  Log ("Patching WinPE boot image from " + $winre)
  Patch-WinPeBoot -SourceWinRe $winre -InternalBootRoot $script:internalBootRoot

  $ready = [ordered]@{
    schema = "soty.windows-reinstall.ready.v1"
    caseId = $script:caseId
    computerName = $computerName
    managedUserName = $ManagedUserName
    usbRoot = $script:usbRoot
    reinstallRoot = $script:reinstallRoot
    backupRoot = $script:backupRoot
    backupScope = $backupScope
    personalFolderNames = $personalFolderNames
    installImageSourceRoot = $script:installMediaSources
    preferredEditionHint = $preferredEditionHint
    personalFilesBackedUp = $personalFilesBackedUp
    sotyOperatorExportBackedUp = $sotyOperatorExportBackedUp
    internalBootRoot = $script:internalBootRoot
    confirmationPhrase = $script:confirmationPhrase
    managedUserPasswordMode = $managedUserPasswordMode
    backupProof = $backupProof
    createdAt = (Get-Date).ToString("o")
  }
  $ready | ConvertTo-Json -Depth 8 | Set-Content -LiteralPath (Join-Path $WorkspaceRoot "ready.json") -Encoding UTF8
  $ready | ConvertTo-Json -Depth 8 | Set-Content -LiteralPath (Join-Path $usbReinstall "ready.json") -Encoding UTF8
  Finish "ready" 0 @{ ready = $ready }
} catch {
  $_.Exception.ToString() | Set-Content -LiteralPath $stderrPath -Encoding UTF8
  Finish "failed" 1 @{ error = $_.Exception.Message }
}
