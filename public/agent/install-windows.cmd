@echo off
setlocal
set "BASE=https://xn--n1afe0b.online/agent"

powershell.exe -NoLogo -NoProfile -ExecutionPolicy Bypass -Command "$ErrorActionPreference='Stop'; [Net.ServicePointManager]::SecurityProtocol = [Net.SecurityProtocolType]::Tls12; $script = Join-Path $env:TEMP ('install-soty-agent-' + [guid]::NewGuid().ToString('N') + '.ps1'); try { Invoke-WebRequest -Uri '%BASE%/install-windows.ps1' -UseBasicParsing -OutFile $script; & $script -Base '%BASE%' } finally { Remove-Item -LiteralPath $script -Force -ErrorAction SilentlyContinue }"
if errorlevel 1 goto fail

exit /b 0

:fail
echo.
echo soty-agent install failed
echo %LOCALAPPDATA%\soty-agent\install.log
pause
exit /b 1
