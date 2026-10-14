// Shared by the existing Machine companion and its pinned runtime-only updater.
// Seed a new user once; subsequent Machine starts cannot replace that user's
// runtime, identity or approved folders with the Machine installation's copy.
export function windowsUserCompanion({ nodePath, machineScript, machineConfig, openCodePath = '' }) {
  const quote = value => `'${String(value).replaceAll("'", "''")}'`;
  return [
    "param([switch]$PrepareOnly)",
    "$ErrorActionPreference = 'Stop'",
    "$sid = [System.Security.Principal.WindowsIdentity]::GetCurrent().User.Value",
    "if ($sid -eq 'S-1-5-18') { exit 0 }",
    "$mutex = $null",
    "if (-not $PrepareOnly) { $mutex = New-Object System.Threading.Mutex($false, ('Global\\SotyConnectorUser-' + $sid)); if (-not $mutex.WaitOne(0)) { exit 0 } }",
    "try {",
    "$userDir = Join-Path $env:LOCALAPPDATA 'soty-connector'",
    "New-Item -ItemType Directory -Force -Path $userDir | Out-Null",
    "$userScript = Join-Path $userDir 'soty-connector.mjs'",
    "$userConfig = Join-Path $userDir 'connector-config.json'",
    `if (-not (Test-Path -LiteralPath $userScript)) { Copy-Item -LiteralPath ${quote(machineScript)} -Destination $userScript }`,
    `if (-not (Test-Path -LiteralPath $userConfig)) {`,
    `  $sourceData = Get-Content -LiteralPath ${quote(machineConfig)} -Raw -Encoding UTF8 | ConvertFrom-Json`,
    "  $random = [Security.Cryptography.RandomNumberGenerator]::Create()",
    "  try { $token = New-Object byte[] 32; $link = New-Object byte[] 32; $random.GetBytes($token); $random.GetBytes($link) } finally { $random.Dispose() }",
    // Existing setup already uses per-installation secrets. A different OS user
    // must get its own unclaimed installation, not the Machine's credentials.
    "  $userData = [pscustomobject]@{schema=$sourceData.schema;serverUrl=$sourceData.serverUrl;gonkaModel=$sourceData.gonkaModel;deviceId=('device-' + [guid]::NewGuid().ToString());installId=('install-' + [guid]::NewGuid().ToString());connectorToken=([Convert]::ToBase64String($token).TrimEnd('=').Replace('+','-').Replace('/','_'));linkId=([Convert]::ToBase64String($link).TrimEnd('=').Replace('+','-').Replace('/','_'))}",
    "  $userData | ConvertTo-Json -Depth 20 | Set-Content -LiteralPath $userConfig -Encoding UTF8",
    "}",
    "if ($PrepareOnly) { exit 0 }",
    "$env:SOTY_CONNECTOR_MANAGED = '1'",
    "$env:SOTY_CONNECTOR_AUTO_UPDATE = '1'",
    "$env:SOTY_CONNECTOR_SCOPE = 'CurrentUser'",
    "$env:SOTY_CONNECTOR_COMPANION = '1'",
    "$env:SOTY_CONNECTOR_PORT = '0'",
    "$env:SOTY_CONNECTOR_DATA_DIR = $userDir",
    // Each user's persisted config remains the source of its identity.
    "foreach ($name in @('SOTY_CONNECTOR_SERVER_URL','SOTY_CONNECTOR_LINK_ID','SOTY_CONNECTOR_DEVICE_ID','SOTY_CONNECTOR_DEVICE_NICK','SOTY_AGENT_RELAY_URL','SOTY_AGENT_RELAY_ID','SOTY_AGENT_DEVICE_ID','SOTY_AGENT_DEVICE_NICK')) { Remove-Item -LiteralPath ('Env:' + $name) -ErrorAction SilentlyContinue }",
    ...(openCodePath ? [`$env:SOTY_OPENCODE_PATH = ${quote(openCodePath)}`] : []),
    "$env:NODE_OPTIONS = ''",
    "$pendingPath = $userScript + '.update-pending.json'",
    "$previousPath = $userScript + '.previous'",
    "while ($true) {",
    `  & ${quote(nodePath)} $userScript`,
    "  $code = if ($null -eq $LASTEXITCODE) { 1 } else { [int]$LASTEXITCODE }",
    "  if ($code -ne 75 -and (Test-Path -LiteralPath $pendingPath) -and (Test-Path -LiteralPath $previousPath)) {",
    "    Copy-Item -LiteralPath $userScript -Destination ($userScript + '.failed') -Force",
    "    Move-Item -LiteralPath $previousPath -Destination $userScript -Force",
    "    Remove-Item -LiteralPath $pendingPath -Force",
    "  }",
    "  if ($code -eq 75) { Start-Sleep -Seconds 1 } else { Start-Sleep -Seconds 3 }",
    "}",
    "} finally {",
    "  if ($null -ne $mutex) { try { $mutex.ReleaseMutex() | Out-Null } catch {}; $mutex.Dispose() }",
    "}"
  ].join('\r\n');
}
