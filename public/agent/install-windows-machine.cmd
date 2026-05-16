@echo off
rem soty-agent-machine-bootstrap:0.4.50
setlocal
set "BASE=https://xn--n1afe0b.online/agent"
set "INSTALLER_REVISION=0.4.50"
if not defined SOTY_AGENT_RELAY_ID set "SOTY_AGENT_RELAY_ID="
if not defined SOTY_AGENT_DEVICE_ID set "SOTY_AGENT_DEVICE_ID="
if not defined SOTY_AGENT_DEVICE_NICK set "SOTY_AGENT_DEVICE_NICK="

echo Downloading Soty Agent installer %INSTALLER_REVISION%...
powershell.exe -NoLogo -NoProfile -ExecutionPolicy Bypass -Command "$ErrorActionPreference='Stop'; [Net.ServicePointManager]::SecurityProtocol = [Net.SecurityProtocolType]::Tls12; $dir = Join-Path $env:TEMP 'soty-agent-machine'; New-Item -ItemType Directory -Force -Path $dir | Out-Null; $bootstrap = Join-Path $dir 'install-windows-machine-bootstrap.ps1'; $log = Join-Path $dir 'bootstrap.log'; 'soty-agent-machine:bootstrap-download:%INSTALLER_REVISION%' | Out-File -LiteralPath $log -Encoding ASCII; Invoke-WebRequest -Uri '%BASE%/install-windows-machine-bootstrap.ps1?v=%INSTALLER_REVISION%' -UseBasicParsing -OutFile $bootstrap -TimeoutSec 45 -ErrorAction Stop; & powershell.exe -NoLogo -NoProfile -ExecutionPolicy Bypass -File $bootstrap -Base '%BASE%' -Revision '%INSTALLER_REVISION%' -RelayId '%SOTY_AGENT_RELAY_ID%' -DeviceId '%SOTY_AGENT_DEVICE_ID%' -DeviceNick '%SOTY_AGENT_DEVICE_NICK%'; exit $LASTEXITCODE"
if errorlevel 1 goto fail

exit /b 0

:fail
echo.
echo soty-agent machine install failed
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
