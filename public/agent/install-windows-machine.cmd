@echo off
rem soty-agent-machine-bootstrap:1.2.8
setlocal
set "BASE=https://xn--n1afe0b.online/agent"
set "INSTALLER_REVISION=1.2.8"
if not defined SOTY_CONNECTOR_LINK_ID set "SOTY_CONNECTOR_LINK_ID="
if not defined SOTY_CONNECTOR_DEVICE_ID set "SOTY_CONNECTOR_DEVICE_ID="
if not defined SOTY_CONNECTOR_DEVICE_NICK set "SOTY_CONNECTOR_DEVICE_NICK="
set "SOTY_CONNECTOR_INSTALL_BASE=%BASE%"
set "SOTY_CONNECTOR_INSTALLER_REVISION=%INSTALLER_REVISION%"

echo Downloading Soty Connector installer %INSTALLER_REVISION%...
powershell.exe -NoLogo -NoProfile -ExecutionPolicy Bypass -Command "$ErrorActionPreference='Stop'; [Net.ServicePointManager]::SecurityProtocol = [Net.SecurityProtocolType]::Tls12; $dir = Join-Path $env:TEMP 'soty-agent-machine'; New-Item -ItemType Directory -Force -Path $dir | Out-Null; $bootstrap = Join-Path $dir 'install-windows-machine-bootstrap.ps1'; $log = Join-Path $dir 'bootstrap.log'; 'soty-connector-machine:bootstrap-download:' + $env:SOTY_CONNECTOR_INSTALLER_REVISION | Out-File -LiteralPath $log -Encoding ASCII; $uri = $env:SOTY_CONNECTOR_INSTALL_BASE.TrimEnd('/') + '/install-windows-machine-bootstrap.ps1'; if (-not [string]::IsNullOrWhiteSpace($env:SOTY_CONNECTOR_INSTALLER_REVISION)) { $uri += '?v=' + [uri]::EscapeDataString($env:SOTY_CONNECTOR_INSTALLER_REVISION) }; Invoke-WebRequest -Uri $uri -UseBasicParsing -OutFile $bootstrap -TimeoutSec 45 -ErrorAction Stop; $installArgs = @('-NoLogo','-NoProfile','-ExecutionPolicy','Bypass','-File',$bootstrap,'-Base',$env:SOTY_CONNECTOR_INSTALL_BASE); if (-not [string]::IsNullOrWhiteSpace($env:SOTY_CONNECTOR_INSTALLER_REVISION)) { $installArgs += @('-Revision',$env:SOTY_CONNECTOR_INSTALLER_REVISION) }; if (-not [string]::IsNullOrWhiteSpace($env:SOTY_CONNECTOR_LINK_ID)) { $installArgs += @('-RelayId',$env:SOTY_CONNECTOR_LINK_ID) }; if (-not [string]::IsNullOrWhiteSpace($env:SOTY_CONNECTOR_DEVICE_ID)) { $installArgs += @('-DeviceId',$env:SOTY_CONNECTOR_DEVICE_ID); if (-not [string]::IsNullOrWhiteSpace($env:SOTY_CONNECTOR_DEVICE_NICK)) { $installArgs += @('-DeviceNick',$env:SOTY_CONNECTOR_DEVICE_NICK) } }; & powershell.exe @installArgs; exit $LASTEXITCODE"
if errorlevel 1 goto fail

exit /b 0

:fail
echo.
echo Soty Connector machine install failed
echo %ProgramData%\soty-agent\install.log
echo %TEMP%\soty-agent-machine\bootstrap.log
echo %ProgramData%\Soty\agent-install\bootstrap-elevated.log
echo.
if exist "%TEMP%\soty-agent-machine\bootstrap.log" (
  echo --- bootstrap.log ---
  type "%TEMP%\soty-agent-machine\bootstrap.log"
)
if exist "%ProgramData%\Soty\agent-install\bootstrap-elevated.log" (
  echo.
  echo --- bootstrap-elevated.log ---
  type "%ProgramData%\Soty\agent-install\bootstrap-elevated.log"
)
if exist "%ProgramData%\soty-agent\install.log" (
  echo.
  echo --- install.log tail ---
  powershell.exe -NoLogo -NoProfile -ExecutionPolicy Bypass -Command "Get-Content -LiteralPath '%ProgramData%\soty-agent\install.log' -Tail 80"
)
if exist "%ProgramData%\soty-agent\node-probe.err.log" (
  echo.
  echo --- node-probe.err.log ---
  type "%ProgramData%\soty-agent\node-probe.err.log"
)
if exist "%ProgramData%\soty-agent\start-agent.status.log" (
  echo.
  echo --- start-agent.status.log ---
  powershell.exe -NoLogo -NoProfile -ExecutionPolicy Bypass -Command "Get-Content -LiteralPath '%ProgramData%\soty-agent\start-agent.status.log' -Tail 40"
)
if exist "%ProgramData%\soty-agent\start-agent.err.log" (
  echo.
  echo --- start-agent.err.log ---
  powershell.exe -NoLogo -NoProfile -ExecutionPolicy Bypass -Command "Get-Content -LiteralPath '%ProgramData%\soty-agent\start-agent.err.log' -Tail 80"
)
pause
exit /b 1
