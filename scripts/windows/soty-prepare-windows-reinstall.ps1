param(
  [string] $WorkspaceRoot = "C:\ProgramData\Soty\WindowsReinstall",
  [string] $UsbDriveLetter = "D",
  [string] $ManagedUserName = "Soty",
  [string] $ManagedUserPassword = "",
  [string] $PanelSiteUrl = "https://xn--n1afe0b.online",
  [string] $WindowsImageUrl = "http://dl.delivery.mp.microsoft.com/filestreamingservice/files/071fc359-1d92-46c0-ad88-c7801d2f69be/26200.6584.250915-1905.25h2_ge_release_svc_refresh_CLIENTCONSUMER_RET_x64FRE_ru-ru.esd",
  [string] $WindowsImageSha256 = "cb2fbc4af7979cf7e5f740f03289d6eacb19dd75a4858d66bc6a50aa26c37005",
  [string] $ConfirmationPhrase = "",
  [switch] $UseExistingUsbInstallImage,
  [switch] $AllowTemporaryManagedPassword,
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
    Log "WARN robocopy timed out after ${TimeoutSec}s for $Source -> $Destination. Continuing without this optional browser-state folder."
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

function Escape-Xml([string] $Value) {
  [System.Security.SecurityElement]::Escape($Value)
}

function Test-WindowsInstallImage([string] $ImageName, [string] $ImageDescription, [Int64] $ImageSize = 0) {
  $text = (($ImageName, $ImageDescription) -join " ")
  if ($text -match '(?i)setup|windows pe|setup media') { return $false }
  if ($ImageSize -gt 0 -and $ImageSize -lt 5GB) { return $false }
  return $true
}

function Select-WindowsInstallImage($Images) {
  $installImages = @($Images | Where-Object {
    Test-WindowsInstallImage -ImageName ([string]$_.ImageName) -ImageDescription ([string]$_.ImageDescription) -ImageSize ([Int64]$_.ImageSize)
  })
  if ($installImages.Count -eq 0) {
    throw "No installable Windows OS image found in ESD."
  }
  $preferred = $installImages | Where-Object {
    (([string]$_.ImageName) -match '(?i)home|core|домашн') -or
    (([string]$_.ImageDescription) -match '(?i)home|core|домашн')
  } | Select-Object -First 1
  if ($preferred) { return $preferred }
  return ($installImages | Select-Object -First 1)
}

function Test-ExistingInstallWim([string] $Path) {
  if (-not (Test-Path -LiteralPath $Path)) { return $false }
  try {
    $images = @(Get-WindowsImage -ImagePath $Path -ErrorAction Stop)
    $first = $images | Select-Object -First 1
    if (-not $first) { return $false }
    return (Test-WindowsInstallImage -ImageName ([string]$first.ImageName) -ImageDescription ([string]$first.ImageDescription) -ImageSize ([Int64]$first.ImageSize))
  } catch {
    return $false
  }
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
`$desktopBackup = Join-Path `$backupRoot "personal-files\$SourceProfileName\Desktop"
`$desktopDest = [Environment]::GetFolderPath("Desktop")
if ([string]::IsNullOrWhiteSpace(`$desktopDest)) { `$desktopDest = Join-Path `$env:USERPROFILE "Desktop" }
CopyDir `$desktopBackup `$desktopDest
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
  [System.IO.File]::WriteAllText($Path, $post, (New-Object System.Text.UTF8Encoding($false)))
}

function Write-WinPeWorker([string] $Path) {
  $worker = @'
@echo off
setlocal enableextensions enabledelayedexpansion
for %%I in ("%~dp0..") do set "USBROOT=%%~fI"
set "LOGDIR=%USBROOT%\reinstall\logs"
if not exist "%LOGDIR%" mkdir "%LOGDIR%" >nul 2>nul
set "LOG=%LOGDIR%\winre-reinstall.log"
echo [%DATE% %TIME%] worker started>"%LOG%"
if not exist "%USBROOT%\reinstall\armed.flag" (
  echo [%DATE% %TIME%] armed.flag missing>>"%LOG%"
  wpeutil reboot
  exit /b 1
)
call "%USBROOT%\reinstall\config.cmd"
echo [%DATE% %TIME%] target disk %TARGET_DISK%, managed user %MANAGED_USER%>>"%LOG%"
set "DISKPART=%USBROOT%\reinstall\diskpart.txt"
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
if exist "%USBROOT%\sources\install.swm" (
  set "IMAGEARG=/ImageFile:%USBROOT%\sources\install.swm"
  set "SWMARG=/SWMFile:%USBROOT%\sources\install*.swm"
) else if exist "%USBROOT%\sources\install.esd" (
  set "IMAGEARG=/ImageFile:%USBROOT%\sources\install.esd"
  set "SWMARG="
) else (
  set "IMAGEARG=/ImageFile:%USBROOT%\sources\install.wim"
  set "SWMARG="
)
echo [%DATE% %TIME%] applying Windows image>>"%LOG%"
dism /Apply-Image %IMAGEARG% %SWMARG% /Index:1 /ApplyDir:W:\ >>"%LOG%" 2>>&1
if errorlevel 1 goto fail
mkdir W:\Windows\Panther >nul 2>nul
copy /y "%USBROOT%\reinstall\unattend.xml" W:\Windows\Panther\Unattend.xml >>"%LOG%" 2>>&1
dism /Image:W:\ /Apply-Unattend:"%USBROOT%\reinstall\unattend.xml" >>"%LOG%" 2>>&1
mkdir W:\ProgramData\Soty\WindowsReinstall\restore >nul 2>nul
mkdir W:\ProgramData\Soty\WindowsReinstall\logs >nul 2>nul
xcopy "%USBROOT%\restore" W:\ProgramData\Soty\WindowsReinstall\restore /E /I /H /Y >>"%LOG%" 2>>&1
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

  $UsbDriveLetter = $UsbDriveLetter.TrimEnd([char[]]":\")
  $script:usbRoot = $UsbDriveLetter + ":\"
  $usbVolume = Get-Volume -DriveLetter $UsbDriveLetter -ErrorAction Stop
  if ($usbVolume.DriveType -ne "Removable") { throw "Drive $UsbDriveLetter is not removable." }
  if ($usbVolume.SizeRemaining -lt 12GB) { throw "Drive $UsbDriveLetter needs at least 12 GB free." }

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
  $usbSources = Join-Path $script:reinstallRoot "sources"
  $usbReinstall = Join-Path $script:reinstallRoot "reinstall"
  $usbRestore = Join-Path $script:reinstallRoot "restore"
  foreach ($path in @($WorkspaceRoot, $stageRoot, $restoreRoot, $mediaRoot, $sourceRoot, $script:reinstallRoot, $script:backupRoot, $usbSources, $usbReinstall, $usbRestore)) {
    New-Directory $path
  }

  Log ("caseId=" + $script:caseId)
  Log ("usbRoot=" + $script:usbRoot)

  $sourceProfileName = Get-LoggedOnUserLeaf
  $sourceProfile = Join-Path "C:\Users" $sourceProfileName
  $wifiRoot = Join-Path $script:backupRoot "wifi-profiles"
  $driverRoot = Join-Path $script:backupRoot "drivers"
  $sotyStateRoot = Join-Path (Join-Path $script:backupRoot "soty-state") $sourceProfileName
  $personalFilesRoot = Join-Path (Join-Path $script:backupRoot "personal-files") $sourceProfileName
  $desktopBackupRoot = Join-Path $personalFilesRoot "Desktop"
  $operatorExportPath = Join-Path $sotyStateRoot "operator-export.json"
  $personalFilesBackedUp = $false
  $sotyOperatorExportBackedUp = $false
  $backupScope = @(
    "wifi-profiles",
    "exported-drivers",
    "soty-operator-export",
    "soty-browser-return-state",
    "desktop-personal-files"
  )
  New-Directory $wifiRoot
  New-Directory $driverRoot
  New-Directory $sotyStateRoot
  New-Directory $personalFilesRoot
  Log "Backup scope: Wi-Fi profiles, exported drivers, Soty operator export, Soty browser return state, and Desktop personal files."
  try { & netsh.exe wlan export profile key=clear folder="$wifiRoot" | Out-File -LiteralPath (Join-Path $JobRoot "netsh-wifi-export.txt") -Encoding UTF8 } catch { Log ("WARN wifi export failed: " + $_.Exception.Message) }
  try { Invoke-LoggedCliWithTimeout dism.exe @("/online", "/export-driver", "/destination:$driverRoot") "dism-export-drivers.txt" 120 } catch { Log ("WARN driver export failed: " + $_.Exception.Message) }
  if (Save-SotyOperatorExport $operatorExportPath) { $sotyOperatorExportBackedUp = $true }
  if (Test-Path -LiteralPath $sourceProfile) {
    $desktopSource = Join-Path $sourceProfile "Desktop"
    if (Copy-TreeIfExists $desktopSource $desktopBackupRoot) { $personalFilesBackedUp = $true }
    $edgeDefault = Join-Path $sourceProfile "AppData\Local\Microsoft\Edge\User Data\Default"
    Copy-TreeIfExists (Join-Path $edgeDefault "IndexedDB") (Join-Path $sotyStateRoot "Edge-IndexedDB") | Out-Null
    Copy-TreeIfExists (Join-Path $edgeDefault "Local Storage") (Join-Path $sotyStateRoot "Edge-LocalStorage") | Out-Null
    Copy-TreeIfExists (Join-Path $edgeDefault "Service Worker") (Join-Path $sotyStateRoot "Edge-ServiceWorker") | Out-Null
    Copy-TreeIfExists (Join-Path $edgeDefault "Sessions") (Join-Path $sotyStateRoot "Edge-Sessions") | Out-Null
  }
  # The machine worker is reinstalled by postinstall. Copying its live ops/jobs
  # tree can hold locks or copy an active job forever, so keep it out of the
  # reinstall backup.

  if ($UseExistingUsbInstallImage) {
    $existingUsbImage = @(
      (Join-Path $usbSources "install.swm"),
      (Join-Path $usbSources "install.esd"),
      (Join-Path $usbSources "install.wim")
    ) | Where-Object { Test-Path -LiteralPath $_ } | Select-Object -First 1
    if (-not $existingUsbImage) {
      throw "UseExistingUsbInstallImage was set, but no install.swm/esd/wim exists under $usbSources."
    }
    Log ("Using existing USB install image: " + $existingUsbImage)
  } else {
    $esdPath = Join-Path $mediaRoot "Windows11_25H2_CLIENTCONSUMER_RET_x64FRE_ru-ru.esd"
    if (Test-Path -LiteralPath $esdPath) {
      $existingHash = (Get-FileHash -Algorithm SHA256 -LiteralPath $esdPath).Hash.ToLowerInvariant()
      if ($existingHash -ne $WindowsImageSha256.ToLowerInvariant()) {
        Remove-Item -LiteralPath $esdPath -Force
      }
    }
    if (-not (Test-Path -LiteralPath $esdPath)) {
      $tmp = $esdPath + ".download"
      Remove-Item -LiteralPath $tmp -Force -ErrorAction SilentlyContinue
      Log "Downloading Windows image."
      Get-BitsTransfer -AllUsers -ErrorAction SilentlyContinue |
        Where-Object { $_.DisplayName -eq "Soty Windows reinstall image" } |
        Remove-BitsTransfer -Confirm:$false -ErrorAction SilentlyContinue
      $curl = Get-Command curl.exe -ErrorAction SilentlyContinue
      if ($curl) {
        try {
          Invoke-LoggedCliWithTimeout curl.exe @("-L", "--fail", "--retry", "5", "--retry-delay", "5", "--connect-timeout", "30", "--output", $tmp, $WindowsImageUrl) "curl-download-windows-image.txt" 7200
        } catch {
          if (-not (Test-Path -LiteralPath $tmp)) {
            throw
          }
          $tmpHash = (Get-FileHash -Algorithm SHA256 -LiteralPath $tmp).Hash.ToLowerInvariant()
          if ($tmpHash -ne $WindowsImageSha256.ToLowerInvariant()) {
            throw
          }
          Log ("WARN curl reported an error after producing a valid image: " + $_.Exception.Message)
        }
      } else {
        Invoke-WebRequest -Uri $WindowsImageUrl -UseBasicParsing -OutFile $tmp
      }
      if (-not (Test-Path -LiteralPath $tmp)) {
        throw "Windows image download did not create $tmp"
      }
      $actualHash = (Get-FileHash -Algorithm SHA256 -LiteralPath $tmp).Hash.ToLowerInvariant()
      if ($actualHash -ne $WindowsImageSha256.ToLowerInvariant()) {
        throw "Windows image SHA256 mismatch. expected=$WindowsImageSha256 actual=$actualHash"
      }
      Move-Item -LiteralPath $tmp -Destination $esdPath -Force
    }

    $installWim = Join-Path $sourceRoot "install.wim"
    if ((Test-Path -LiteralPath $installWim) -and -not (Test-ExistingInstallWim -Path $installWim)) {
      Log "Removing invalid cached install.wim."
      Remove-Item -LiteralPath $installWim -Force
    }
    if (-not (Test-Path -LiteralPath $installWim)) {
      Log "Exporting Windows Home/Core from ESD."
      $images = @(Get-WindowsImage -ImagePath $esdPath -ErrorAction Stop)
      $image = Select-WindowsInstallImage -Images $images
      Log ("Selected image index " + [int]$image.ImageIndex + ": " + [string]$image.ImageName)
      Invoke-LoggedCli dism.exe @("/Export-Image", "/SourceImageFile:$esdPath", "/SourceIndex:$([int]$image.ImageIndex)", "/DestinationImageFile:$installWim", "/Compress:max", "/CheckIntegrity") "dism-export-installwim.txt"
    }
    Remove-Item -LiteralPath (Join-Path $usbSources "install.swm") -Force -ErrorAction SilentlyContinue
    Remove-Item -LiteralPath (Join-Path $usbSources "install.wim") -Force -ErrorAction SilentlyContinue
    Remove-Item -LiteralPath (Join-Path $usbSources "install.esd") -Force -ErrorAction SilentlyContinue
    Get-ChildItem -LiteralPath $usbSources -Filter "install*.swm" -File -ErrorAction SilentlyContinue | Remove-Item -Force
    if ([string]$usbVolume.FileSystem -eq "NTFS") {
      Log "Copying one-index install.wim onto NTFS USB."
      Copy-Item -LiteralPath $installWim -Destination (Join-Path $usbSources "install.wim") -Force
    } else {
      Log "Splitting install image onto USB."
      Invoke-LoggedCli dism.exe @("/Split-Image", "/ImageFile:$installWim", "/SWMFile:$(Join-Path $usbSources 'install.swm')", "/FileSize:3800", "/CheckIntegrity") "dism-split-install.txt"
    }
  }

  $computerName = $env:COMPUTERNAME
  $effectiveManagedUserPassword = $ManagedUserPassword
  $managedUserPasswordGenerated = $false
  if ([string]::IsNullOrEmpty($effectiveManagedUserPassword)) {
    if ($AllowTemporaryManagedPassword) {
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
  Write-Unattend -Path (Join-Path $usbReinstall "unattend.xml") -ComputerName $computerName -UserPassword $effectiveManagedUserPassword
  Set-Content -LiteralPath (Join-Path $usbReinstall "config.cmd") -Value @(
    "@echo off",
    "set TARGET_DISK=0",
    "set MANAGED_USER=$ManagedUserName",
    "set CASE_ID=$($script:caseId)"
  ) -Encoding ASCII
  Write-WinPeWorker -Path (Join-Path $usbReinstall "winre-reinstall.cmd")
  Write-PostInstall -Path (Join-Path $usbRestore "postinstall.ps1") -CaseId $script:caseId -SourceProfileName $sourceProfileName -ResetManagedPasswordToBlank $managedUserPasswordGenerated
  Copy-Item -LiteralPath (Join-Path $usbRestore "postinstall.ps1") -Destination (Join-Path $restoreRoot "postinstall.ps1") -Force
  @{
    schema = "soty.windows-reinstall.restore.v1"
    caseId = $script:caseId
    backupRoot = $script:backupRoot
    panelSiteUrl = $PanelSiteUrl
    managedUserName = $ManagedUserName
    managedUserPasswordMode = $managedUserPasswordMode
    backupScope = $backupScope
    personalFilesBackedUp = $personalFilesBackedUp
    sotyOperatorExportBackedUp = $sotyOperatorExportBackedUp
    sourceProfileName = $sourceProfileName
    createdAt = (Get-Date).ToString("o")
  } | ConvertTo-Json -Depth 8 | Set-Content -LiteralPath (Join-Path $usbRestore "restore-config.json") -Encoding UTF8

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
    personalFilesBackedUp = $personalFilesBackedUp
    sotyOperatorExportBackedUp = $sotyOperatorExportBackedUp
    internalBootRoot = $script:internalBootRoot
    confirmationPhrase = $script:confirmationPhrase
    managedUserPasswordMode = $managedUserPasswordMode
    createdAt = (Get-Date).ToString("o")
  }
  $ready | ConvertTo-Json -Depth 8 | Set-Content -LiteralPath (Join-Path $WorkspaceRoot "ready.json") -Encoding UTF8
  $ready | ConvertTo-Json -Depth 8 | Set-Content -LiteralPath (Join-Path $usbReinstall "ready.json") -Encoding UTF8
  Finish "ready" 0 @{ ready = $ready }
} catch {
  $_.Exception.ToString() | Set-Content -LiteralPath $stderrPath -Encoding UTF8
  Finish "failed" 1 @{ error = $_.Exception.Message }
}
