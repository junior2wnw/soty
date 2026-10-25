$ErrorActionPreference = 'Stop'
$ProgressPreference = 'SilentlyContinue'
[Net.ServicePointManager]::SecurityProtocol = [Net.SecurityProtocolType]::Tls12
$utf8 = New-Object Text.UTF8Encoding($false)
$setupStage = 'Windows'
function Read-Jsonc([string]$text) {
    $clean = [regex]::Replace($text, '("(?:\\.|[^"\\])*")|//[^\r\n]*|/\*[\s\S]*?\*/', { param($m) if ($m.Groups[1].Success) { $m.Value } else { '' } })
    $clean = [regex]::Replace($clean, '("(?:\\.|[^"\\])*")|,\s*(?=[}\]])', { param($m) if ($m.Groups[1].Success) { $m.Value } else { '' } })
    $obj = ConvertFrom-Json -InputObject $clean
    if ($null -eq $obj -or $obj -isnot [pscustomobject]) { throw 'INVALID_CONFIG' }
    return $obj
}
function Set-Field($obj, [string]$name, $value) { $obj | Add-Member -NotePropertyName $name -NotePropertyValue $value -Force }
function Write-Atomic([string]$path, [string]$value) {
    $tempPath = $path + '.' + [guid]::NewGuid().ToString('N') + '.tmp'
    try {
        [IO.File]::WriteAllText($tempPath, $value, $utf8)
        if (Test-Path -LiteralPath $path) { [IO.File]::Replace($tempPath, $path, [NullString]::Value) }
        else { [IO.File]::Move($tempPath, $path) }
    } finally { if (Test-Path -LiteralPath $tempPath) { Remove-Item -LiteralPath $tempPath -Force } }
}
function Save-Backup([string]$path) {
    if (Test-Path -LiteralPath $path) {
        $sealed = [Security.Cryptography.ProtectedData]::Protect([IO.File]::ReadAllBytes($path), $null, 'CurrentUser')
        [IO.File]::WriteAllBytes((Join-Path $backupDir ([guid]::NewGuid().ToString('N') + '.dpapi')), $sealed)
    }
}
function Find-Desktop {
    $paths = @((Join-Path $env:LOCALAPPDATA 'Programs\OpenCode\OpenCode.exe'), (Join-Path $env:LOCALAPPDATA 'OpenCode\OpenCode.exe'))
    foreach ($regRoot in @('HKCU:\Software\Microsoft\Windows\CurrentVersion\Uninstall', 'HKLM:\Software\Microsoft\Windows\CurrentVersion\Uninstall')) {
        foreach ($entry in @(Get-ItemProperty "$regRoot\*" -ErrorAction SilentlyContinue | Where-Object DisplayName -eq 'OpenCode')) {
            if ($entry.InstallLocation) { $paths += Join-Path $entry.InstallLocation 'OpenCode.exe' }
        }
    }
    foreach ($path in $paths) { if (Test-Path -LiteralPath $path -PathType Leaf) { return $path } }
    return $null
}
if ($env:SOTY_SETUP_FUNCTIONS_ONLY -eq '1') { return }
try {
    Write-Host 'OpenCode + Соты' -ForegroundColor Yellow
    if ($PSVersionTable.PSVersion -lt [version]'5.1' -or [Environment]::OSVersion.Version.Major -lt 10) { throw 'WINDOWS_10_OR_11_REQUIRED' }
    Add-Type -AssemblyName System.Security
    $payload = ConvertFrom-Json ([Text.Encoding]::UTF8.GetString([Convert]::FromBase64String('__PAYLOAD__')))
    $privateDir = Join-Path $env:LOCALAPPDATA 'SotyOpenCode'
    [IO.Directory]::CreateDirectory($privateDir) | Out-Null
    $acl = New-Object Security.AccessControl.DirectorySecurity
    $acl.SetAccessRuleProtection($true, $false)
    foreach ($sid in @([Security.Principal.WindowsIdentity]::GetCurrent().User, (New-Object Security.Principal.SecurityIdentifier('S-1-5-18')))) {
        $acl.AddAccessRule((New-Object Security.AccessControl.FileSystemAccessRule($sid, 'FullControl', 'ContainerInherit,ObjectInherit', 'None', 'Allow')))
    }
    Set-Acl -LiteralPath $privateDir -AclObject $acl
    $backupDir = Join-Path $privateDir 'backups'
    [IO.Directory]::CreateDirectory($backupDir) | Out-Null
    $configRoot = if ($env:XDG_CONFIG_HOME) { $env:XDG_CONFIG_HOME } else { Join-Path ([Environment]::GetFolderPath('UserProfile')) '.config' }
    $configDir = Join-Path $configRoot 'opencode'
    [IO.Directory]::CreateDirectory($configDir) | Out-Null
    $files = @((Join-Path $configDir 'opencode.json'), (Join-Path $configDir 'opencode.jsonc')) | Where-Object { Test-Path -LiteralPath $_ }
    if (@($files).Count -eq 0) { $files = @((Join-Path $configDir 'opencode.json')) }
    $configs = @()
    foreach ($file in $files) {
        $cfg = if (Test-Path -LiteralPath $file) { Read-Jsonc ([IO.File]::ReadAllText($file)) } else { [pscustomobject]@{} }
        if ($cfg.providers) { throw 'OPENCODE_V2_USE_MANUAL_PROFILE' }
        if (-not $cfg.provider) { Set-Field $cfg 'provider' ([pscustomobject]@{}) }
        if ($cfg.provider -isnot [pscustomobject]) { throw 'INVALID_CONFIG' }
        $configs += @{path=$file;config=$cfg}
    }
    $desktopExe = Find-Desktop
    if ($desktopExe) {
        $versionText = [Diagnostics.FileVersionInfo]::GetVersionInfo($desktopExe).ProductVersion
        if ($versionText -match '^[2-9]\.') { throw 'OPENCODE_V2_USE_MANUAL_PROFILE' }
    } else {
        $setupStage = 'Загрузка OpenCode 1.18.31'
        Write-Host $setupStage
        $arch = if ($env:PROCESSOR_ARCHITEW6432) { $env:PROCESSOR_ARCHITEW6432 } else { $env:PROCESSOR_ARCHITECTURE }
        switch ($arch) {
            'AMD64' { $asset='opencode-desktop-win-x64.exe'; $sha='7e6e4bca1b243609172fa580520e72ed240627fc9c36a80763a6f29bd64b698f' }
            'ARM64' { $asset='opencode-desktop-win-arm64.exe'; $sha='e8c3ccdc92fe2346ef185d9f73b37773fe32a6edefe634a75fb9bf1e4791eb22' }
            default { throw '64_BIT_WINDOWS_REQUIRED' }
        }
        $installer = Join-Path $privateDir $asset
        if (-not (Test-Path -LiteralPath $installer) -or (Get-FileHash -LiteralPath $installer -Algorithm SHA256).Hash -ne $sha) {
            Invoke-WebRequest -UseBasicParsing -Uri "https://github.com/anomalyco/opencode/releases/download/v1.18.31/$asset" -OutFile $installer -TimeoutSec 600
        }
        if ((Get-FileHash -LiteralPath $installer -Algorithm SHA256).Hash -ne $sha) { throw 'CHECKSUM_MISMATCH' }
        $proc=Start-Process -FilePath $installer -ArgumentList '/S','/currentuser' -WindowStyle Hidden -Wait -PassThru
        if ($proc.ExitCode -notin @(0,3010)) { throw 'INSTALL_FAILED' }
        $desktopExe=Find-Desktop
        if (-not $desktopExe) { throw 'DESKTOP_NOT_FOUND' }
    }
    $setupStage = 'Получение личного ключа'
    Write-Host $setupStage
    # A receipt is scoped to this one-time claim; secrets never go into config backups in plaintext.
    $digest = [Security.Cryptography.SHA256]::Create().ComputeHash([Text.Encoding]::UTF8.GetBytes($payload.claim))
    $claimId = ([BitConverter]::ToString($digest)).Replace('-','').Substring(0,24)
    $receiptPath = Join-Path $privateDir ($claimId + '.dpapi')
    if (Test-Path -LiteralPath $receiptPath) {
        $receipt = ConvertFrom-Json ([Text.Encoding]::UTF8.GetString([Security.Cryptography.ProtectedData]::Unprotect([IO.File]::ReadAllBytes($receiptPath),$null,'CurrentUser')))
    } else {
        $noncePath = Join-Path $privateDir ($claimId + '.nonce')
        if (-not (Test-Path -LiteralPath $noncePath)) { Write-Atomic $noncePath ([guid]::NewGuid().ToString('N')) }
        $nonce = [IO.File]::ReadAllText($noncePath)
        $receipt = Invoke-RestMethod -Method Post -Uri ($payload.baseUrl + '/setup/redeem') -ContentType 'application/json' -Body (@{claim=$payload.claim;installationNonce=$nonce}|ConvertTo-Json) -TimeoutSec 30
        if (-not $receipt.secret) { throw 'GET_NEW_INSTALLER_FROM_CABINET' }
        $sealed = [Security.Cryptography.ProtectedData]::Protect([Text.Encoding]::UTF8.GetBytes(($receipt|ConvertTo-Json -Depth 20)),$null,'CurrentUser')
        [IO.File]::WriteAllBytes($receiptPath,$sealed)
    }
    $keyPath=Join-Path $privateDir ($claimId + '.key.txt')
    $modelMap=[ordered]@{}
    foreach ($id in @($receipt.models)) { $modelMap[$id]=@{name=$id.Split('/')[-1]} }
    if ($modelMap.Count -eq 0) { $modelMap[$payload.model]=@{name=$payload.model.Split('/')[-1]} }
    $provider=@{npm='@ai-sdk/openai-compatible';name='Соты';options=@{baseURL=$receipt.apiBaseUrl;apiKey=('{file:'+$keyPath.Replace('\','/')+'}');timeout=60000};models=$modelMap}
    $providerId = 'soty'
    $setupStage = 'Сохранение настроек'
    Save-Backup $keyPath
    Write-Atomic $keyPath $receipt.secret
    foreach ($item in $configs) {
        Save-Backup $item.path
        Set-Field $item.config.provider $providerId $provider
        Set-Field $item.config 'model' ($providerId+'/'+$payload.model)
        if ($item.config.enabled_providers -and $providerId -notin @($item.config.enabled_providers)) { Set-Field $item.config 'enabled_providers' @(@($item.config.enabled_providers)+$providerId) }
        if ($item.config.disabled_providers) { Set-Field $item.config 'disabled_providers' @($item.config.disabled_providers|Where-Object {$_ -ne $providerId}) }
        Write-Atomic $item.path ($item.config|ConvertTo-Json -Depth 100)
    }
    $setupStage = 'Активация ключа'
    Invoke-RestMethod -Method Post -Uri ($payload.baseUrl+'/setup/activate') -Headers @{Authorization=('Bearer '+$receipt.secret)} -ContentType 'application/json' -Body (@{keyId=$receipt.keyId}|ConvertTo-Json) -TimeoutSec 30 | Out-Null
    Invoke-RestMethod -Uri ($receipt.apiBaseUrl+'/models') -Headers @{Authorization=('Bearer '+$receipt.secret)} -TimeoutSec 25 | Out-Null
    $shortcut=(New-Object -ComObject WScript.Shell).CreateShortcut((Join-Path ([Environment]::GetFolderPath('Desktop')) 'OpenCode - Soty.lnk'))
    $shortcut.TargetPath=$desktopExe
    $shortcut.WorkingDirectory=[Environment]::GetFolderPath('UserProfile')
    $shortcut.Save()
    if (@(Get-Process -Name OpenCode -ErrorAction SilentlyContinue).Count -eq 0) { Start-Process -FilePath $desktopExe -WorkingDirectory ([Environment]::GetFolderPath('UserProfile')) | Out-Null }
    Write-Host 'Готово. Личный ключ активирован; расход будет в кабинете Сот.' -ForegroundColor Green
    Write-Host 'Если OpenCode уже открыт, перезапустите его. Баланс пополняется в кабинете.'
    $receipt=$null
    exit 0
} catch {
    Write-Host ('Не завершён этап: '+$setupStage) -ForegroundColor Red
    $code=[string]$_.Exception.Message
    if ($code -match '^[A-Z0-9_]+$') { Write-Host $code }
    else { Write-Host ('Тип ошибки: '+$_.Exception.GetType().Name) }
    Write-Host 'Повторите запуск. Если срок установки истёк — скачайте новый файл в кабинете.'
    exit 1
}
