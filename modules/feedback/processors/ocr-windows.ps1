param([Parameter(Mandatory=$true)][string]$InputPath,
      [Parameter(Mandatory=$true)][string]$OutputPath,
      [ValidateSet('ru','en')][string]$Language='ru')
$ErrorActionPreference='Stop'
try {
  $imageItem=Get-Item -LiteralPath $InputPath
  if ($imageItem.PSIsContainer -or $imageItem.Length -gt 1048576 -or (Test-Path -LiteralPath $OutputPath)) { throw 'processor_input_invalid' }
  Add-Type -AssemblyName System.Runtime.WindowsRuntime
  [Windows.Storage.StorageFile,Windows.Storage,ContentType=WindowsRuntime] | Out-Null
  [Windows.Graphics.Imaging.BitmapDecoder,Windows.Foundation,ContentType=WindowsRuntime] | Out-Null
  [Windows.Media.Ocr.OcrEngine,Windows.Foundation,ContentType=WindowsRuntime] | Out-Null
  [Windows.Globalization.Language,Windows.Globalization,ContentType=WindowsRuntime] | Out-Null
  $taskBridge=[System.WindowsRuntimeSystemExtensions].GetMethods() | Where-Object {
    $_.Name -eq 'AsTask' -and $_.IsGenericMethodDefinition -and $_.GetGenericArguments().Length -eq 1 -and
    $_.GetParameters().Length -eq 1 -and $_.GetParameters()[0].ParameterType.Name -eq 'IAsyncOperation`1'
  } | Select-Object -First 1
  function Read-WinRt($Operation,[type]$ResultType) {
    $task=$taskBridge.MakeGenericMethod($ResultType).Invoke($null,@($Operation)); return $task.GetAwaiter().GetResult()
  }
  $file=Read-WinRt ([Windows.Storage.StorageFile]::GetFileFromPathAsync($imageItem.FullName)) ([Windows.Storage.StorageFile])
  $stream=Read-WinRt ($file.OpenReadAsync()) ([Windows.Storage.Streams.IRandomAccessStreamWithContentType])
  try {
    $decoder=Read-WinRt ([Windows.Graphics.Imaging.BitmapDecoder]::CreateAsync($stream)) ([Windows.Graphics.Imaging.BitmapDecoder])
    if ($decoder.PixelWidth -gt 8192 -or $decoder.PixelHeight -gt 8192 -or
      ([long]$decoder.PixelWidth * [long]$decoder.PixelHeight) -gt 16777216) { throw 'processor_image_dimensions' }
    $bitmap=Read-WinRt ($decoder.GetSoftwareBitmapAsync()) ([Windows.Graphics.Imaging.SoftwareBitmap])
    try {
      if ($bitmap.PixelWidth -gt [Windows.Media.Ocr.OcrEngine]::MaxImageDimension -or
        $bitmap.PixelHeight -gt [Windows.Media.Ocr.OcrEngine]::MaxImageDimension) { throw 'processor_ocr_dimensions' }
      $recognizerLanguage=if ($Language -eq 'en') {'en-US'} else {'ru'}
      $engine=[Windows.Media.Ocr.OcrEngine]::TryCreateFromLanguage([Windows.Globalization.Language]::new($recognizerLanguage))
      if ($null -eq $engine) { throw 'processor_language_unavailable' }
      $result=Read-WinRt ($engine.RecognizeAsync($bitmap)) ([Windows.Media.Ocr.OcrResult])
      if ([System.Text.Encoding]::UTF8.GetByteCount($result.Text) -gt 32768) { throw 'processor_output_limit' }
      $value=[ordered]@{schema='soty.feedback.derived-text.v1';engine='windows-ocr-local';language=$Language;text=$result.Text;
        width=[int]$bitmap.PixelWidth;height=[int]$bitmap.PixelHeight}
      $json=$value | ConvertTo-Json -Compress
      $output=New-Object System.IO.FileStream($OutputPath,[System.IO.FileMode]::CreateNew,[System.IO.FileAccess]::Write,[System.IO.FileShare]::None)
      try { $bytes=[System.Text.Encoding]::UTF8.GetBytes($json); $output.Write($bytes,0,$bytes.Length); $output.Flush($true) } finally { $output.Dispose() }
    } finally { $bitmap.Dispose() }
  } finally { $stream.Dispose() }
  Write-Output 'local-feedback-ocr-complete'
} catch { Write-Output 'local-feedback-ocr-failed'; exit 1 }
