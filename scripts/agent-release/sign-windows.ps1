param(
  [Parameter(Mandatory = $true)]
  [string]$CertificatePath,

  [string]$CertificatePassword = $env:SOTY_SIGN_CERT_PASSWORD,

  [string]$TimestampServer = "http://timestamp.digicert.com"
)

$ErrorActionPreference = "Stop"
$repo = Resolve-Path (Join-Path $PSScriptRoot "..\..")
$targets = @(
  "public\agent\install-windows.ps1",
  "public\agent\install-windows-machine-bootstrap.ps1",
  "public\agent\windows-reinstall\soty-arm-windows-reinstall.ps1",
  "public\agent\windows-reinstall\soty-make-fast-usb.ps1",
  "public\agent\windows-reinstall\soty-managed-windows-reinstall.ps1",
  "public\agent\windows-reinstall\soty-prepare-windows-reinstall.ps1"
) | ForEach-Object { Join-Path $repo $_ }

$missing = $targets | Where-Object { -not (Test-Path -LiteralPath $_) }
if ($missing) {
  throw "Missing signing target(s): $($missing -join ', ')"
}

$securePassword = $null
if ($CertificatePassword) {
  $securePassword = ConvertTo-SecureString $CertificatePassword -AsPlainText -Force
}

$cert = Get-PfxCertificate -FilePath $CertificatePath -Password $securePassword
foreach ($target in $targets) {
  $result = Set-AuthenticodeSignature -FilePath $target -Certificate $cert -TimestampServer $TimestampServer -HashAlgorithm SHA256
  if ($result.Status -ne "Valid") {
    throw "Signing failed for $target: $($result.StatusMessage)"
  }
  Write-Output "signed $target"
}

Write-Output "Windows PowerShell installers signed. Rebuild the agent manifest after signing."
