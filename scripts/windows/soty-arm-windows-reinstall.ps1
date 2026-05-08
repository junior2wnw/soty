param(
  [string] $WorkspaceRoot = "C:\ProgramData\Soty\WindowsReinstall",
  [string] $UsbDriveLetter = "D",
  [string] $ConfirmationPhrase = "",
  [string] $ExpectedConfirmationPhrase = ""
)

$ErrorActionPreference = "Stop"
$ProgressPreference = "SilentlyContinue"
[Console]::InputEncoding = [System.Text.Encoding]::UTF8
[Console]::OutputEncoding = [System.Text.Encoding]::UTF8
$OutputEncoding = [System.Text.Encoding]::UTF8
try { chcp.com 65001 > $null } catch {}

function New-Directory([string] $Path) {
  New-Item -ItemType Directory -Force -Path $Path | Out-Null
}

function Invoke-Bcd([string[]] $ArgumentList) {
  $output = & bcdedit.exe @ArgumentList 2>&1
  $code = if ($null -ne $global:LASTEXITCODE) { [int] $global:LASTEXITCODE } else { 0 }
  if ($code -ne 0) {
    throw ("bcdedit " + ($ArgumentList -join " ") + " failed with exit code " + $code + ": " + ($output -join "`n"))
  }
  return ($output -join "`n")
}

function Ensure-RamdiskOptions {
  $existing = ""
  try { $existing = Invoke-Bcd @("/enum", "{ramdiskoptions}") } catch {}
  if ($existing -match "no matching objects|store is empty|не.*найд|пуст") {
    Invoke-Bcd @("/create", "{ramdiskoptions}", "/d", "Soty Ramdisk Options") | Out-Null
  }
}

function New-BcdOsLoader([string] $Description) {
  $createOutput = Invoke-Bcd @("/create", "/d", $Description, "/application", "osloader")
  $matches = [regex]::Matches($createOutput, "\{[0-9a-fA-F-]{36}\}")
  if ($matches.Count -ne 1) {
    throw "Could not parse a single BCD entry id from: $createOutput"
  }
  return $matches[0].Value
}

function Assert-SotyBcdEntry([string] $EntryId) {
  $entryText = Invoke-Bcd @("/enum", $EntryId, "/v")
  if ($entryText -notmatch "Soty Reinstall WinPE") {
    throw "BCD entry $EntryId does not have the expected Soty description."
  }
  if ($entryText -notmatch [regex]::Escape("\Soty-Boot\boot.wim")) {
    throw "BCD entry $EntryId does not point to C:\Soty-Boot\boot.wim."
  }
  if ($entryText -match "winresume\.efi") {
    throw "BCD entry $EntryId resolved to a resume object, refusing to bootsequence it."
  }
}

$identity = [Security.Principal.WindowsIdentity]::GetCurrent()
$principal = New-Object Security.Principal.WindowsPrincipal($identity)
if (-not $principal.IsInRole([Security.Principal.WindowsBuiltInRole]::Administrator)) {
  throw "This script must run elevated or as SYSTEM."
}

$UsbDriveLetter = $UsbDriveLetter.TrimEnd([char[]]":\")
$usbRoot = $UsbDriveLetter + ":\"
$reinstallRoot = Join-Path $usbRoot "Soty-Reinstall"
$usbReinstall = Join-Path $reinstallRoot "reinstall"
$usbSources = Join-Path $reinstallRoot "sources"
$internalBootRoot = "C:\Soty-Boot"
$bootWim = Join-Path $internalBootRoot "boot.wim"
$bootSdi = Join-Path $internalBootRoot "boot.sdi"
$logRoot = Join-Path $WorkspaceRoot "logs"
New-Directory $logRoot
$log = Join-Path $logRoot ("arm-" + (Get-Date -Format "yyyyMMdd-HHmmss") + ".log")
function Log([string] $Message) {
  Add-Content -LiteralPath $log -Value ("[" + (Get-Date).ToString("o") + "] " + $Message) -Encoding UTF8
}

Log "arm requested"
$required = @(
  (Join-Path $usbReinstall "winre-reinstall.cmd"),
  (Join-Path $usbReinstall "config.cmd"),
  (Join-Path $usbReinstall "unattend.xml"),
  (Join-Path $usbReinstall "ready.json"),
  (Join-Path $reinstallRoot "restore\postinstall.ps1"),
  $bootWim,
  $bootSdi
)
foreach ($path in $required) {
  if (-not (Test-Path -LiteralPath $path)) { throw "Required reinstall artifact missing: $path" }
}
$installImage = @(
  (Join-Path $usbSources "install.swm"),
  (Join-Path $usbSources "install.esd"),
  (Join-Path $usbSources "install.wim")
) | Where-Object { Test-Path -LiteralPath $_ } | Select-Object -First 1
if (-not $installImage) {
  throw "Required install image missing under $usbSources."
}

$ready = Get-Content -LiteralPath (Join-Path $usbReinstall "ready.json") -Raw | ConvertFrom-Json
if ([string]::IsNullOrWhiteSpace($ExpectedConfirmationPhrase)) {
  $ExpectedConfirmationPhrase = [string]$ready.confirmationPhrase
}
if ([string]::IsNullOrWhiteSpace($ExpectedConfirmationPhrase)) {
  throw "Expected confirmation phrase is empty. Refusing to arm reinstall."
}
if ($ConfirmationPhrase -ne $ExpectedConfirmationPhrase) {
  throw "Confirmation phrase mismatch. Refusing to arm reinstall."
}
$marker = [ordered]@{
  schema = "soty.windows-reinstall.arm.v1"
  caseId = $ready.caseId
  computerName = $env:COMPUTERNAME
  armedAt = (Get-Date).ToString("o")
}
$marker | ConvertTo-Json -Depth 8 | Set-Content -LiteralPath "C:\Soty-Reinstall-Target.marker" -Encoding UTF8
$marker | ConvertTo-Json -Depth 8 | Set-Content -LiteralPath (Join-Path $usbReinstall "armed.flag") -Encoding UTF8
Remove-Item -LiteralPath (Join-Path $usbReinstall "armed.started") -Force -ErrorAction SilentlyContinue

Ensure-RamdiskOptions
Invoke-Bcd @("/set", "{ramdiskoptions}", "ramdisksdidevice", "partition=C:") | Out-Null
Invoke-Bcd @("/set", "{ramdiskoptions}", "ramdisksdipath", "\Soty-Boot\boot.sdi") | Out-Null
$entryId = New-BcdOsLoader -Description ("Soty Reinstall WinPE " + $ready.caseId)
Invoke-Bcd @("/set", $entryId, "device", "ramdisk=[C:]\Soty-Boot\boot.wim,{ramdiskoptions}") | Out-Null
Invoke-Bcd @("/set", $entryId, "osdevice", "ramdisk=[C:]\Soty-Boot\boot.wim,{ramdiskoptions}") | Out-Null
Invoke-Bcd @("/set", $entryId, "path", "\Windows\System32\winload.efi") | Out-Null
Invoke-Bcd @("/set", $entryId, "systemroot", "\Windows") | Out-Null
Invoke-Bcd @("/set", $entryId, "winpe", "Yes") | Out-Null
Invoke-Bcd @("/set", $entryId, "detecthal", "Yes") | Out-Null
Assert-SotyBcdEntry -EntryId $entryId
Invoke-Bcd @("/bootsequence", $entryId) | Out-Null
Log ("bootsequence set to " + $entryId)

[pscustomobject]@{
  ok = $true
  status = "armed"
  caseId = $ready.caseId
  bcdEntry = $entryId
  log = $log
  rebooting = $true
} | ConvertTo-Json -Compress

shutdown.exe /r /t 5 /c "Soty Windows reinstall armed"
