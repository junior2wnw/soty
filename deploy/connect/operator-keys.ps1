param(
  [ValidateSet('init', 'publish', 'verify-backup')][string]$Action = 'init',
  [string]$KeyStore = (Join-Path $env:APPDATA 'SotyConnect\operator-keys.dpapi'),
  [string]$PublicDirectory = (Join-Path $PSScriptRoot 'trust'),
  [string]$PublishConfig,
  [string]$BackupFile
)
$ErrorActionPreference = 'Stop'
Add-Type -AssemblyName System.Security
$entropy = [Text.Encoding]::UTF8.GetBytes('soty-connect-operator-v1')
function Write-NewAtomicFile([string]$Destination, [byte[]]$Bytes) {
  $temporary = $Destination + '.' + [Guid]::NewGuid().ToString('N') + '.tmp'
  $stream = $null
  try {
    $stream = [IO.FileStream]::new($temporary, [IO.FileMode]::CreateNew, [IO.FileAccess]::Write, [IO.FileShare]::None)
    $stream.Write($Bytes, 0, $Bytes.Length)
    $stream.Flush($true)
    $stream.Dispose(); $stream=$null
    # The two-argument Move never replaces an existing destination.
    [IO.File]::Move($temporary, $Destination)
  } finally {
    if ($stream) { $stream.Dispose() }
    if ([IO.File]::Exists($temporary)) { [IO.File]::Delete($temporary) }
  }
}
if ($Action -eq 'init') {
  $parent = Split-Path -Parent $KeyStore
  [IO.Directory]::CreateDirectory($parent) | Out-Null
  if (-not [IO.File]::Exists($KeyStore)) {
    $acl = [Security.AccessControl.DirectorySecurity]::new()
    $acl.SetAccessRuleProtection($true, $false)
    $sid = [Security.Principal.WindowsIdentity]::GetCurrent().User
    $acl.SetOwner($sid)
    $acl.AddAccessRule([Security.AccessControl.FileSystemAccessRule]::new($sid, 'FullControl', 'ContainerInherit,ObjectInherit', 'None', 'Allow'))
    Set-Acl -LiteralPath $parent -AclObject $acl
  }
  $lockPath = $KeyStore + '.init.lock'
  $lock = [IO.FileStream]::new($lockPath, [IO.FileMode]::CreateNew, [IO.FileAccess]::Write, [IO.FileShare]::None)
  $keyBytes=$null; $clear=$null; $keyJson=$null; $keys=$null
  try {
    if (-not [IO.File]::Exists($KeyStore)) {
      $generationScript = 'import{generateKeyPairSync}from"node:crypto";const options={privateKeyEncoding:{type:"pkcs8",format:"pem"},publicKeyEncoding:{type:"spki",format:"pem"}};const sign=generateKeyPairSync("ed25519",options),backup=generateKeyPairSync("rsa",{...options,modulusLength:4096});process.stdout.write(JSON.stringify({sign,backup}));'
      $keyJson = $generationScript | & node --input-type=module
      if ($LASTEXITCODE -ne 0) { throw 'Key generation failed.' }
      $keyBytes = [Text.Encoding]::UTF8.GetBytes($keyJson)
      $protected = [Security.Cryptography.ProtectedData]::Protect($keyBytes, $entropy, [Security.Cryptography.DataProtectionScope]::CurrentUser)
      Write-NewAtomicFile $KeyStore $protected
    }
    # Always derive public trust from the durable store, including after a
    # previous initialization stopped between private and public commits.
    $clear = [Security.Cryptography.ProtectedData]::Unprotect([IO.File]::ReadAllBytes($KeyStore), $entropy, [Security.Cryptography.DataProtectionScope]::CurrentUser)
    $keys = [Text.Encoding]::UTF8.GetString($clear) | ConvertFrom-Json
    if (-not $keys.sign.publicKey -or -not $keys.backup.publicKey) { throw 'Stored public trust is invalid; private keys preserved.' }
    [IO.Directory]::CreateDirectory($PublicDirectory) | Out-Null
    $trustPath = Join-Path $PublicDirectory 'root.json'
    $backupPath = Join-Path $PublicDirectory 'backup-public.pem'
    if ([IO.File]::Exists($trustPath)) {
      $existing = [IO.File]::ReadAllText($trustPath) | ConvertFrom-Json
      if ($existing.threshold -ne 1 -or $existing.keys.'soty-connect-2026-09' -cne $keys.sign.publicKey) {
        throw 'Public signing trust differs from the stored key; no keys or pins were replaced.'
      }
    }
    if ([IO.File]::Exists($backupPath) -and [IO.File]::ReadAllText($backupPath) -cne $keys.backup.publicKey) {
      throw 'Public backup trust differs from the stored key; no keys or pins were replaced.'
    }
    if (-not [IO.File]::Exists($trustPath)) {
      $trust = [ordered]@{ threshold=1; keys=[ordered]@{ 'soty-connect-2026-09'=$keys.sign.publicKey } } | ConvertTo-Json -Depth 4
      Write-NewAtomicFile $trustPath ([Text.Encoding]::UTF8.GetBytes($trust + "`n"))
    }
    if (-not [IO.File]::Exists($backupPath)) { Write-NewAtomicFile $backupPath ([Text.Encoding]::UTF8.GetBytes($keys.backup.publicKey)) }
  } finally {
    if ($keyBytes) { [Array]::Clear($keyBytes, 0, $keyBytes.Length) }
    if ($clear) { [Array]::Clear($clear, 0, $clear.Length) }
    $keyJson=$null; $keys=$null
    $lock.Dispose()
    [IO.File]::Delete($lockPath)
  }
  Write-Output 'Public trust verified or restored; durable private keys preserved with current-user Windows DPAPI.'
  exit
}
$clear = [Security.Cryptography.ProtectedData]::Unprotect([IO.File]::ReadAllBytes($KeyStore), $entropy, [Security.Cryptography.DataProtectionScope]::CurrentUser)
try {
  $keys = [Text.Encoding]::UTF8.GetString($clear) | ConvertFrom-Json
  if ($Action -eq 'publish') {
    if (-not $PublishConfig) { throw 'PublishConfig required.' }
    $config = Get-Content -LiteralPath $PublishConfig -Raw | ConvertFrom-Json
    $inputJson = @{ config=$config; privateKeyPem=$keys.sign.privateKey } | ConvertTo-Json -Depth 12 -Compress
    $inputJson | & node (Join-Path $PSScriptRoot 'publish.mjs')
    if ($LASTEXITCODE -ne 0) { throw 'Signed publication failed.' }
  } else {
    if (-not $BackupFile) { throw 'BackupFile required.' }
    @{ file=$BackupFile; privateKeyPem=$keys.backup.privateKey } | ConvertTo-Json -Compress | & node (Join-Path $PSScriptRoot 'verify-backup.mjs')
    if ($LASTEXITCODE -ne 0) { throw 'Encrypted backup verification failed.' }
  }
} finally {
  [Array]::Clear($clear, 0, $clear.Length)
  $keys=$null; $inputJson=$null
}
