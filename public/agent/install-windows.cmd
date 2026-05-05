@echo off
setlocal
set "BASE=https://xn--n1afe0b.online/agent"
if not defined SOTY_AGENT_RELAY_ID set "SOTY_AGENT_RELAY_ID="

powershell.exe -NoLogo -NoProfile -ExecutionPolicy Bypass -Command "$ErrorActionPreference='Stop'; [Net.ServicePointManager]::SecurityProtocol = [Net.SecurityProtocolType]::Tls12; $local = [Environment]::GetFolderPath('LocalApplicationData'); if ([string]::IsNullOrWhiteSpace($local)) { $local = Join-Path $HOME 'AppData\Local' }; $dir = Join-Path $local 'soty-agent'; New-Item -ItemType Directory -Force -Path $dir | Out-Null; $script = Join-Path $dir 'install-windows.ps1'; Invoke-WebRequest -Uri '%BASE%/install-windows.ps1' -UseBasicParsing -OutFile $script; & $script -Base '%BASE%' -RelayId '%SOTY_AGENT_RELAY_ID%'"
if errorlevel 1 goto fail

exit /b 0

:fail
echo.
echo soty-agent install failed
echo %LOCALAPPDATA%\soty-agent\install.log
pause
exit /b 1
