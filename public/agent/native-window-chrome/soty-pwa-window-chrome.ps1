param(
  [ValidateSet("status", "apply", "restore", "watch", "install", "uninstall")]
  [string] $Action = "status",
  [string] $TitlePattern = "\u0441\u043e\u0442\u044b\.online|soty\.online|xn--n1afe0b\.online",
  [int] $IntervalMs = 1500,
  [int] $DurationSeconds = 0
)

$ErrorActionPreference = "Stop"
$ProgressPreference = "SilentlyContinue"

$StateRoot = Join-Path $env:APPDATA "Soty\window-chrome"
$HelperPath = Join-Path $StateRoot "soty-pwa-window-chrome.ps1"
$PidPath = Join-Path $StateRoot "watcher.pid"
$TaskName = "Soty PWA Window Chrome"
$RunKey = "HKCU:\Software\Microsoft\Windows\CurrentVersion\Run"
$RunValueName = "SotyPwaWindowChrome"
$WS_CAPTION = [int64] 0x00C00000
$GWL_STYLE = -16
$SWP_NOSIZE = 0x0001
$SWP_NOMOVE = 0x0002
$SWP_NOZORDER = 0x0004
$SWP_FRAMECHANGED = 0x0020

function Emit($Value) {
  $Value | ConvertTo-Json -Depth 7 -Compress
}

function Ensure-StateRoot {
  New-Item -ItemType Directory -Force -Path $StateRoot | Out-Null
}

function Quote-CommandLineArg([string] $Value) {
  '"' + $Value.Replace('"', '\"') + '"'
}

function New-WatcherArgument([string] $Path = $HelperPath) {
  "-NoLogo -NoProfile -ExecutionPolicy Bypass -WindowStyle Hidden -File $(Quote-CommandLineArg $Path) -Action watch -TitlePattern $(Quote-CommandLineArg $TitlePattern) -IntervalMs $IntervalMs"
}

function New-WatcherRunCommand([string] $Path = $HelperPath) {
  "powershell.exe $(New-WatcherArgument $Path)"
}

function Test-ScheduledWatcher {
  [bool] (Get-ScheduledTask -TaskName $TaskName -ErrorAction SilentlyContinue)
}

function Get-RunWatcherCommand {
  if (-not (Test-Path -LiteralPath $RunKey)) {
    return $null
  }
  try {
    $item = Get-ItemProperty -LiteralPath $RunKey -Name $RunValueName -ErrorAction Stop
    [string] $item.$RunValueName
  } catch {
    $null
  }
}

function Get-WatcherPersistence {
  $items = @()
  if (Test-ScheduledWatcher) {
    $items += "scheduled-task"
  }
  if (Get-RunWatcherCommand) {
    $items += "hkcu-run"
  }
  @($items)
}

function Ensure-Win32 {
  if ("SotyWindowChrome" -as [type]) {
    return
  }
  Add-Type @"
using System;
using System.Runtime.InteropServices;

public static class SotyWindowChrome {
  [DllImport("user32.dll", EntryPoint="GetWindowLong", SetLastError=true)]
  private static extern IntPtr GetWindowLong32(IntPtr hWnd, int nIndex);

  [DllImport("user32.dll", EntryPoint="GetWindowLongPtr", SetLastError=true)]
  private static extern IntPtr GetWindowLongPtr64(IntPtr hWnd, int nIndex);

  [DllImport("user32.dll", EntryPoint="SetWindowLong", SetLastError=true)]
  private static extern IntPtr SetWindowLong32(IntPtr hWnd, int nIndex, IntPtr dwNewLong);

  [DllImport("user32.dll", EntryPoint="SetWindowLongPtr", SetLastError=true)]
  private static extern IntPtr SetWindowLongPtr64(IntPtr hWnd, int nIndex, IntPtr dwNewLong);

  [DllImport("user32.dll", SetLastError=true)]
  public static extern bool SetWindowPos(IntPtr hWnd, IntPtr hWndInsertAfter, int X, int Y, int cx, int cy, UInt32 uFlags);

  public static IntPtr GetWindowLongPtr(IntPtr hWnd, int nIndex) {
    return IntPtr.Size == 8 ? GetWindowLongPtr64(hWnd, nIndex) : GetWindowLong32(hWnd, nIndex);
  }

  public static IntPtr SetWindowLongPtr(IntPtr hWnd, int nIndex, IntPtr dwNewLong) {
    return IntPtr.Size == 8 ? SetWindowLongPtr64(hWnd, nIndex, dwNewLong) : SetWindowLong32(hWnd, nIndex, dwNewLong);
  }
}
"@
}

function Get-TargetWindows {
  $regex = [regex]::new($TitlePattern, [System.Text.RegularExpressions.RegexOptions]::IgnoreCase -bor [System.Text.RegularExpressions.RegexOptions]::CultureInvariant)
  @(Get-Process chrome, msedge -ErrorAction SilentlyContinue | Where-Object {
    $_.MainWindowHandle -ne [IntPtr]::Zero -and $_.MainWindowTitle -and $regex.IsMatch([string] $_.MainWindowTitle)
  } | Sort-Object ProcessName, Id)
}

function Get-WindowStyle([IntPtr] $Handle) {
  Ensure-Win32
  [SotyWindowChrome]::GetWindowLongPtr($Handle, $GWL_STYLE).ToInt64()
}

function Set-WindowStyle([IntPtr] $Handle, [int64] $Style) {
  Ensure-Win32
  [SotyWindowChrome]::SetWindowLongPtr($Handle, $GWL_STYLE, [IntPtr] $Style) | Out-Null
  [SotyWindowChrome]::SetWindowPos(
    $Handle,
    [IntPtr]::Zero,
    0,
    0,
    0,
    0,
    [uint32] ($SWP_NOMOVE -bor $SWP_NOSIZE -bor $SWP_NOZORDER -bor $SWP_FRAMECHANGED)
  ) | Out-Null
}

function Convert-WindowRecord($Process, [int64] $Style, [bool] $Changed) {
  [pscustomobject]@{
    process = $Process.ProcessName
    pid = [int] $Process.Id
    hwnd = ("0x{0:X}" -f $Process.MainWindowHandle.ToInt64())
    title = [string] $Process.MainWindowTitle
    caption = [bool] (($Style -band $WS_CAPTION) -ne 0)
    frameless = [bool] (($Style -band $WS_CAPTION) -eq 0)
    changed = $Changed
  }
}

function Apply-WindowChrome([bool] $Restore = $false) {
  $results = @()
  foreach ($process in Get-TargetWindows) {
    $handle = [IntPtr] $process.MainWindowHandle
    $style = Get-WindowStyle $handle
    $next = if ($Restore) {
      $style -bor $WS_CAPTION
    } else {
      $style -band (-bnot $WS_CAPTION)
    }
    $changed = $next -ne $style
    if ($changed) {
      Set-WindowStyle $handle $next
      Start-Sleep -Milliseconds 80
    }
    $finalStyle = Get-WindowStyle $handle
    $results += Convert-WindowRecord $process $finalStyle $changed
  }
  [pscustomobject]@{
    ok = $true
    action = if ($Restore) { "restore" } else { "apply" }
    titlePattern = $TitlePattern
    count = [int] $results.Count
    windows = @($results)
  }
}

function Get-Status {
  $items = @()
  foreach ($process in Get-TargetWindows) {
    $items += Convert-WindowRecord $process (Get-WindowStyle ([IntPtr] $process.MainWindowHandle)) $false
  }
  [pscustomobject]@{
    ok = $true
    action = "status"
    titlePattern = $TitlePattern
    installed = [bool] (@(Get-WatcherPersistence).Count -gt 0)
    persistence = @(Get-WatcherPersistence)
    runCommand = Get-RunWatcherCommand
    helperPath = $HelperPath
    count = [int] $items.Count
    windows = @($items)
  }
}

function Install-Watcher {
  Ensure-StateRoot
  Copy-Item -LiteralPath $PSCommandPath -Destination $HelperPath -Force
  $argument = New-WatcherArgument $HelperPath
  $runCommand = New-WatcherRunCommand $HelperPath
  $taskAction = New-ScheduledTaskAction -Execute "powershell.exe" -Argument $argument
  $trigger = New-ScheduledTaskTrigger -AtLogOn
  $userName = [System.Security.Principal.WindowsIdentity]::GetCurrent().Name
  $principal = New-ScheduledTaskPrincipal -UserId $userName -LogonType Interactive -RunLevel Limited
  $persistence = "scheduled-task"
  $scheduledTaskError = $null
  try {
    Register-ScheduledTask -TaskName $TaskName -Action $taskAction -Trigger $trigger -Principal $principal -Description "Keeps the Soty installed PWA window frameless for the current user." -Force | Out-Null
    if (Test-Path -LiteralPath $RunKey) {
      Remove-ItemProperty -LiteralPath $RunKey -Name $RunValueName -ErrorAction SilentlyContinue
    }
  } catch {
    $scheduledTaskError = $_.Exception.Message
    New-Item -Force -Path $RunKey | Out-Null
    New-ItemProperty -LiteralPath $RunKey -Name $RunValueName -Value $runCommand -PropertyType String -Force | Out-Null
    $persistence = "hkcu-run"
  }
  Start-Process -FilePath "powershell.exe" -WindowStyle Hidden -ArgumentList $argument | Out-Null
  [pscustomobject]@{
    ok = $true
    action = "install"
    taskName = $TaskName
    persistence = $persistence
    scheduledTaskError = $scheduledTaskError
    runCommand = Get-RunWatcherCommand
    helperPath = $HelperPath
    applied = (Apply-WindowChrome $false)
  }
}

function Uninstall-Watcher {
  Unregister-ScheduledTask -TaskName $TaskName -Confirm:$false -ErrorAction SilentlyContinue
  if (Test-Path -LiteralPath $RunKey) {
    Remove-ItemProperty -LiteralPath $RunKey -Name $RunValueName -ErrorAction SilentlyContinue
  }
  if (Test-Path -LiteralPath $PidPath) {
    $pidText = (Get-Content -LiteralPath $PidPath -ErrorAction SilentlyContinue | Select-Object -First 1)
    $watcherPid = 0
    if ([int]::TryParse([string] $pidText, [ref] $watcherPid)) {
      Stop-Process -Id $watcherPid -Force -ErrorAction SilentlyContinue
    }
  }
  [pscustomobject]@{
    ok = $true
    action = "uninstall"
    taskName = $TaskName
    persistence = @(Get-WatcherPersistence)
    restored = (Apply-WindowChrome $true)
  }
}

function Watch-WindowChrome {
  Ensure-StateRoot
  Set-Content -LiteralPath $PidPath -Value ([string] $PID) -Encoding ASCII
  $mutex = New-Object System.Threading.Mutex($false, "Local\SotyPwaWindowChrome")
  if (-not $mutex.WaitOne(0)) {
    Emit ([pscustomobject]@{ ok = $true; action = "watch"; alreadyRunning = $true })
    return
  }
  try {
    $started = Get-Date
    while ($true) {
      Apply-WindowChrome $false | Out-Null
      if ($DurationSeconds -gt 0 -and ((Get-Date) - $started).TotalSeconds -ge $DurationSeconds) {
        break
      }
      Start-Sleep -Milliseconds ([Math]::Max(500, $IntervalMs))
    }
  } finally {
    try { $mutex.ReleaseMutex() | Out-Null } catch {}
    try { $mutex.Dispose() } catch {}
  }
  [pscustomobject]@{ ok = $true; action = "watch"; completed = $true }
}

if ($env:OS -ne "Windows_NT") {
  throw "soty-pwa-window-chrome.ps1 supports Windows only"
}

switch ($Action) {
  "status" { Emit (Get-Status) }
  "apply" { Emit (Apply-WindowChrome $false) }
  "restore" { Emit (Apply-WindowChrome $true) }
  "install" { Emit (Install-Watcher) }
  "uninstall" { Emit (Uninstall-Watcher) }
  "watch" { Emit (Watch-WindowChrome) }
}
