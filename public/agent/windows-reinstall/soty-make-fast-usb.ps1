param(
  [Parameter(Mandatory = $true)]
  [string] $UsbDriveLetter,
  [string] $SourceMediaRoot = (Join-Path $env:ProgramData "Soty\WindowsReinstall\media"),
  [string] $ManagedUserName = "Soty",
  [string] $ManagedUserPassword = "",
  [string] $PanelSiteUrl = "https://xn--n1afe0b.online",
  [switch] $AllowTemporaryManagedPassword
)

$ErrorActionPreference = "Stop"
$ProgressPreference = "SilentlyContinue"
[Console]::InputEncoding = [System.Text.Encoding]::UTF8
[Console]::OutputEncoding = [System.Text.Encoding]::UTF8
$OutputEncoding = [System.Text.Encoding]::UTF8
try { chcp.com 65001 > $null } catch {}

function New-Dir([string] $Path) {
  New-Item -ItemType Directory -Force -Path $Path | Out-Null
}

function Copy-Tree([string] $Source, [string] $Destination) {
  if (-not (Test-Path -LiteralPath $Source)) { throw "Missing source: $Source" }
  New-Dir $Destination
  & robocopy.exe $Source $Destination /E /R:1 /W:1 /XJ /NFL /NDL /NP
  if ($LASTEXITCODE -ge 8) { throw "robocopy failed: $Source -> $Destination, exit $LASTEXITCODE" }
  $global:LASTEXITCODE = 0
}

function Escape-Xml([string] $Value) {
  [System.Security.SecurityElement]::Escape($Value)
}

function Write-Unattend([string] $Path, [string] $ComputerName, [string] $Password) {
  $user = Escape-Xml $ManagedUserName
  $computer = Escape-Xml $ComputerName
  $passwordValue = Escape-Xml $Password
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
        <Password><Value>$passwordValue</Value><PlainText>true</PlainText></Password>
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
            <Password><Value>$passwordValue</Value><PlainText>true</PlainText></Password>
          </LocalAccount>
        </LocalAccounts>
      </UserAccounts>
      <FirstLogonCommands>
        <SynchronousCommand xmlns:wcm="http://schemas.microsoft.com/WMIConfig/2002/State" wcm:action="add">
          <Order>1</Order>
          <Description>Soty first logon restore</Description>
          <CommandLine>cmd.exe /c C:\ProgramData\Soty\WindowsReinstall\restore\soty-firstlogon.cmd</CommandLine>
        </SynchronousCommand>
      </FirstLogonCommands>
    </component>
  </settings>
</unattend>
"@
  [IO.File]::WriteAllText($Path, $xml, (New-Object Text.UTF8Encoding($false)))
}

function Write-Worker([string] $Path) {
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
bcdboot W:\Windows /s S: /f UEFI >>"%LOG%" 2>>&1
if errorlevel 1 goto fail
wpeutil reboot
exit /b 0
:fail
echo [%DATE% %TIME%] worker failed %ERRORLEVEL%>>"%LOG%"
cmd /c exit /b %ERRORLEVEL%
'@
  [IO.File]::WriteAllText($Path, $worker, [Text.Encoding]::ASCII)
}

function Write-Postinstall([string] $Path, [bool] $ResetManagedPasswordToBlank) {
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
if (`$usbRoot) {
  `$backupRoot = Join-Path (Split-Path -Qualifier `$usbRoot) 'Soty-Backups\fast'
  `$wifiRoot = Join-Path `$backupRoot 'wifi-profiles'
  if (Test-Path -LiteralPath `$wifiRoot) {
    Get-ChildItem -LiteralPath `$wifiRoot -Filter '*.xml' -File -ErrorAction SilentlyContinue | ForEach-Object {
      & netsh.exe wlan add profile filename="`$(`$_.FullName)" user=all | Out-Null
    }
  }
  `$driversRoot = Join-Path `$backupRoot 'drivers'
  if (Test-Path -LiteralPath `$driversRoot) {
    & pnputil.exe /add-driver "`$driversRoot\*.inf" /subdirs /install | Out-File -LiteralPath (Join-Path `$logRoot 'pnputil-add-driver.log') -Encoding UTF8
  }
}
try {
  [Net.ServicePointManager]::SecurityProtocol = [Net.SecurityProtocolType]::Tls12
  `$dir = Join-Path `$env:ProgramData 'Soty\agent-install'
  New-Item -ItemType Directory -Force -Path `$dir | Out-Null
  `$installer = Join-Path `$dir 'install-windows.ps1'
  Invoke-WebRequest -Uri '$PanelSiteUrl/agent/install-windows.ps1' -UseBasicParsing -OutFile `$installer
  & powershell.exe -NoLogo -NoProfile -ExecutionPolicy Bypass -File `$installer -Scope Machine -LaunchAppAtLogon | Out-File -LiteralPath (Join-Path `$logRoot 'soty-agent-install.log') -Encoding UTF8
} catch { Log ('soty install failed: ' + `$_.Exception.ToString()) }
try { Start-Process 'msedge.exe' -ArgumentList '--app=$PanelSiteUrl/?pwa=1' } catch { Start-Process '$PanelSiteUrl/?pwa=1' }
try {
  `$restoreRoot = Split-Path -Parent `$MyInvocation.MyCommand.Path
  `$firstLogonPs1 = Join-Path `$restoreRoot 'soty-firstlogon.ps1'
  @'
`$ErrorActionPreference = "Continue"
`$logRoot = "C:\ProgramData\Soty\WindowsReinstall\logs"
New-Item -ItemType Directory -Force -Path `$logRoot | Out-Null
`$log = Join-Path `$logRoot "firstlogon.log"
function Log([string]`$Message) { Add-Content -LiteralPath `$log -Value ("[" + (Get-Date).ToString("o") + "] " + `$Message) -Encoding UTF8 }
Log "first logon started"
if ($resetLiteral) {
  try {
    & net.exe user "$ManagedUserName" "" | Out-Null
    Log "managed user password reset to blank"
  } catch { Log ("managed password cleanup warning: " + `$_.Exception.Message) }
}
try {
  Remove-ItemProperty -Path "HKLM:\SOFTWARE\Microsoft\Windows NT\CurrentVersion\Winlogon" -Name AutoAdminLogon,DefaultPassword,AutoLogonCount,DefaultUserName,DefaultDomainName -ErrorAction SilentlyContinue
} catch {}
try { Start-Process "msedge.exe" -ArgumentList "--app=$PanelSiteUrl/?pwa=1" } catch { try { Start-Process "$PanelSiteUrl/?pwa=1" } catch {} }
Log "first logon finished"
'@ | Set-Content -LiteralPath `$firstLogonPs1 -Encoding UTF8
  Set-Content -LiteralPath (Join-Path `$restoreRoot 'soty-firstlogon.cmd') -Encoding ASCII -Value ('@echo off' + "`r`n" + 'powershell.exe -NoLogo -NoProfile -ExecutionPolicy Bypass -File "C:\ProgramData\Soty\WindowsReinstall\restore\soty-firstlogon.ps1"')
} catch { Log ('first logon script staging failed: ' + `$_.Exception.ToString()) }
Log 'postinstall finished'
"@
  [IO.File]::WriteAllText($Path, $post, (New-Object Text.UTF8Encoding($false)))
}

$UsbDriveLetter = $UsbDriveLetter.TrimEnd([char[]]":\")
$usbRoot = $UsbDriveLetter + ":\"
$volume = Get-Volume -DriveLetter $UsbDriveLetter -ErrorAction Stop
if ($volume.DriveType -ne "Removable") { throw "$UsbDriveLetter is not a removable drive." }
if (-not (Test-Path -LiteralPath (Join-Path $SourceMediaRoot "sources\install.esd"))) {
  throw "Source media must contain sources\install.esd: $SourceMediaRoot"
}

Set-Volume -DriveLetter $UsbDriveLetter -NewFileSystemLabel "SOTYWIN" -ErrorAction SilentlyContinue
Copy-Tree $SourceMediaRoot $usbRoot
$reinstallRoot = Join-Path $usbRoot "Soty-Reinstall"
$reinstall = Join-Path $reinstallRoot "reinstall"
$restore = Join-Path $reinstallRoot "restore"
New-Dir $reinstall
New-Dir $restore
$installImage = Join-Path $usbRoot "sources\install.esd"
$installSourceMode = "MEDIA"
if (-not (Test-Path -LiteralPath $installImage)) {
  New-Dir (Join-Path $reinstallRoot "sources")
  $installImage = Join-Path $reinstallRoot "sources\install.esd"
  $installSourceMode = "REINSTALL"
  Copy-Item -LiteralPath (Join-Path $SourceMediaRoot "sources\install.esd") -Destination $installImage -Force
}
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
Set-Content -LiteralPath (Join-Path $reinstall "config.cmd") -Encoding ASCII -Value @(
  "@echo off",
  "set TARGET_DISK=0",
  "set MANAGED_USER=$ManagedUserName",
  "set CASE_ID=fast",
  "set INSTALL_SOURCE_ROOT=$installSourceMode"
)
Write-Unattend -Path (Join-Path $reinstall "unattend.xml") -ComputerName "*" -Password $effectiveManagedUserPassword
Write-Worker -Path (Join-Path $reinstall "winre-reinstall.cmd")
Write-Postinstall -Path (Join-Path $restore "postinstall.ps1") -ResetManagedPasswordToBlank $managedUserPasswordGenerated
@{
  schema = "soty.fast-usb.v1"
  createdAt = (Get-Date).ToString("o")
  sourceMediaRoot = $SourceMediaRoot
  managedUserName = $ManagedUserName
  managedUserPasswordMode = $managedUserPasswordMode
  confirmationPhrase = "ERASE INTERNAL DISK HHHD"
} | ConvertTo-Json -Depth 8 | Set-Content -LiteralPath (Join-Path $reinstall "ready.json") -Encoding UTF8

[pscustomobject]@{
  ok = $true
  usbRoot = $usbRoot
  reinstallRoot = $reinstallRoot
  installImage = $installImage
  confirmationPhrase = "ERASE INTERNAL DISK HHHD"
} | ConvertTo-Json -Compress
