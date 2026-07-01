export function createMcpSourceSystemAdapters() {
  function sourceProcessScript(args = {}) {
    const payload = Buffer.from(JSON.stringify({
      action: String(args.action || "list").slice(0, 40),
      pid: Number.isSafeInteger(args.pid) ? args.pid : Number.parseInt(String(args.pid || ""), 10),
      processName: String(args.processName || args.name || args.pattern || "").slice(0, 240),
      pattern: String(args.pattern || args.processName || args.name || "").slice(0, 240),
      file: String(args.file || args.path || "").slice(0, 2000),
      command: String(args.command || "").slice(0, 4000),
      arguments: Array.isArray(args.arguments)
        ? args.arguments.map((part) => String(part)).slice(0, 64)
        : String(args.arguments || args.args || "").slice(0, 4000),
      force: args.force === true,
      maxResults: Number.isSafeInteger(args.maxResults) ? Math.max(1, Math.min(args.maxResults, 200)) : 60
    }), "utf8").toString("base64");
    return `
const { spawn, spawnSync } = await import("node:child_process");
const os = await import("node:os");
const req = JSON.parse(Buffer.from("${payload}", "base64").toString("utf8"));
const emit = (value, code = 0) => {
  console.log(JSON.stringify(value));
  process.exit(code);
};
const action = String(req.action || "list").toLowerCase().replace(/_/g, "-");
const pid = Number.isSafeInteger(req.pid) ? req.pid : -1;
const pattern = String(req.pattern || req.processName || "").trim();
const maxResults = Math.max(1, Math.min(Number(req.maxResults) || 60, 200));
function run(file, args, input = "") {
  const result = spawnSync(file, args, { input, encoding: "utf8", windowsHide: true, maxBuffer: 4 * 1024 * 1024 });
  if (result.error) throw result.error;
  if (result.status !== 0) throw new Error((result.stderr || result.stdout || file + " failed").trim());
  return String(result.stdout || "").trim();
}
function psJson(script) {
  return JSON.parse(run("powershell.exe", ["-NoLogo", "-NoProfile", "-ExecutionPolicy", "Bypass", "-Command", script]) || "{}");
}
if (process.platform === "win32") {
  const psPayload = Buffer.from(JSON.stringify(req), "utf8").toString("base64");
  const ps = \`
$ErrorActionPreference = "Stop"
[Console]::OutputEncoding = [System.Text.UTF8Encoding]::new($false)
$req = [Text.Encoding]::UTF8.GetString([Convert]::FromBase64String('\${psPayload}')) | ConvertFrom-Json
$action = ([string]$req.action).ToLowerInvariant().Replace("_", "-")
$max = [Math]::Max(1, [Math]::Min([int]$req.maxResults, 200))
function Process-Info($p) {
  $path = ""
  try { $path = [string]$p.Path } catch {}
  [pscustomobject]@{
    pid = [int]$p.Id
    name = [string]$p.ProcessName
    title = [string]$p.MainWindowTitle
    path = $path
    responding = if ($null -ne $p.Responding) { [bool]$p.Responding } else { $null }
  }
}
if ($action -eq "list" -or $action -eq "status") {
  $items = Get-Process
  if ([int]$req.pid -gt 0) { $items = @($items | Where-Object { $_.Id -eq [int]$req.pid }) }
  $needle = ([string]$req.pattern).Trim()
  if (-not $needle) { $needle = ([string]$req.processName).Trim() }
  if ($needle) { $items = @($items | Where-Object { $_.ProcessName -like "*$needle*" -or $_.MainWindowTitle -like "*$needle*" }) }
  $out = @($items | Select-Object -First $max | ForEach-Object { Process-Info $_ })
  [pscustomobject]@{ ok = $true; action = $action; platform = "win32"; count = @($out).Count; processes = $out } | ConvertTo-Json -Depth 6 -Compress
  exit 0
}
if ($action -eq "start" -or $action -eq "launch" -or $action -eq "open") {
  $file = ([string]$req.file).Trim()
  if (-not $file) { $file = ([string]$req.command).Trim() }
  if (-not $file) { throw "file or command required" }
  $argList = $req.arguments
  if ($argList -is [array]) { $argList = @($argList | ForEach-Object { [string]$_ }) } else { $argList = [string]$argList }
  $p = if ($argList) { Start-Process -FilePath $file -ArgumentList $argList -PassThru } else { Start-Process -FilePath $file -PassThru }
  [pscustomobject]@{ ok = $true; action = "start"; platform = "win32"; pid = [int]$p.Id; name = [string]$p.ProcessName } | ConvertTo-Json -Depth 4 -Compress
  exit 0
}
if ($action -eq "stop" -or $action -eq "kill" -or $action -eq "close") {
  $items = @()
  if ([int]$req.pid -gt 0) { $items = @(Get-Process -Id ([int]$req.pid) -ErrorAction Stop) }
  else {
    $name = ([string]$req.processName).Trim()
    if (-not $name) { $name = ([string]$req.pattern).Trim() }
    if (-not $name) { throw "pid or processName required" }
    $items = @(Get-Process -Name $name -ErrorAction Stop)
  }
  $ids = @($items | Select-Object -ExpandProperty Id)
  if ([bool]$req.force) { $items | Stop-Process -Force -ErrorAction Stop }
  else { $items | Stop-Process -ErrorAction Stop }
  [pscustomobject]@{ ok = $true; action = "stop"; platform = "win32"; stopped = $ids } | ConvertTo-Json -Depth 4 -Compress
  exit 0
}
throw "unsupported process action: $action"
\`;
  emit(psJson(ps));
}
if (action === "list" || action === "status") {
  const stdout = run("ps", ["-axo", "pid=,comm=,args="]);
  const rows = stdout.split(/\\r?\\n/u).map((line) => {
    const match = line.trim().match(/^(\\d+)\\s+(\\S+)\\s*(.*)$/u);
    return match ? { pid: Number(match[1]), name: match[2], command: match[3] || "" } : null;
  }).filter(Boolean).filter((item) => {
    if (pid > 0 && item.pid !== pid) return false;
    if (pattern && !(item.name.includes(pattern) || item.command.includes(pattern))) return false;
    return true;
  }).slice(0, maxResults);
  emit({ ok: true, action, platform: process.platform, count: rows.length, processes: rows });
}
if (action === "start" || action === "launch" || action === "open") {
  const command = String(req.command || req.file || "").trim();
  if (!command) emit({ ok: false, action: "start", error: "file or command required" }, 2);
  const child = spawn(command, Array.isArray(req.arguments) ? req.arguments.map(String) : [], { detached: true, stdio: "ignore", shell: !Array.isArray(req.arguments) });
  child.unref();
  emit({ ok: true, action: "start", platform: process.platform, pid: child.pid, command });
}
if (action === "stop" || action === "kill" || action === "close") {
  if (pid <= 0) emit({ ok: false, action: "stop", error: "pid required on this platform" }, 2);
  process.kill(pid, req.force ? "SIGKILL" : "SIGTERM");
  emit({ ok: true, action: "stop", platform: process.platform, stopped: [pid] });
}
emit({ ok: false, action, error: "unsupported process action" }, 2);
`.trim();
  }

  function sourceClipboardScript(args = {}) {
    const payload = Buffer.from(JSON.stringify({
      action: String(args.action || "read").slice(0, 40),
      text: String(args.text ?? args.content ?? args.value ?? "").slice(0, 300_000),
      maxChars: Number.isSafeInteger(args.maxChars) ? Math.max(100, Math.min(args.maxChars, 12000)) : 4000
    }), "utf8").toString("base64");
    return `
const { spawnSync } = await import("node:child_process");
const req = JSON.parse(Buffer.from("${payload}", "base64").toString("utf8"));
const action = String(req.action || "read").toLowerCase().replace(/_/g, "-");
const maxChars = Math.max(100, Math.min(Number(req.maxChars) || 4000, 12000));
const emit = (value, code = 0) => {
  console.log(JSON.stringify(value));
  process.exit(code);
};
function run(file, args, input = "") {
  const result = spawnSync(file, args, { input, encoding: "utf8", windowsHide: true, maxBuffer: 2 * 1024 * 1024 });
  if (result.error) throw result.error;
  if (result.status !== 0) throw new Error((result.stderr || result.stdout || file + " failed").trim());
  return String(result.stdout || "");
}
function tryRun(commands, input = "") {
  const errors = [];
  for (const command of commands) {
    try {
      return run(command.file, command.args, input);
    } catch (error) {
      errors.push(error instanceof Error ? error.message : String(error));
    }
  }
  throw new Error(errors.join("; ") || "clipboard command unavailable");
}
if (action === "read" || action === "get" || action === "paste") {
  const text = process.platform === "win32"
    ? run("powershell.exe", ["-NoLogo", "-NoProfile", "-ExecutionPolicy", "Bypass", "-Command", "[Console]::OutputEncoding=[Text.UTF8Encoding]::new($false); Get-Clipboard -Raw"])
    : process.platform === "darwin"
      ? run("pbpaste", [])
      : tryRun([{ file: "wl-paste", args: ["--no-newline"] }, { file: "xclip", args: ["-selection", "clipboard", "-out"] }, { file: "xsel", args: ["--clipboard", "--output"] }]);
  emit({ ok: true, action: "read", platform: process.platform, length: text.length, text: text.slice(0, maxChars), truncated: text.length > maxChars });
}
if (action === "write" || action === "set" || action === "copy") {
  const text = String(req.text || "");
  if (process.platform === "win32") {
    run("powershell.exe", ["-NoLogo", "-NoProfile", "-ExecutionPolicy", "Bypass", "-Command", "[Console]::InputEncoding=[Text.UTF8Encoding]::new($false); [Console]::OutputEncoding=[Text.UTF8Encoding]::new($false); Set-Clipboard -Value ([Console]::In.ReadToEnd())"], text);
  } else if (process.platform === "darwin") {
    run("pbcopy", [], text);
  } else {
    tryRun([{ file: "wl-copy", args: [] }, { file: "xclip", args: ["-selection", "clipboard", "-in"] }, { file: "xsel", args: ["--clipboard", "--input"] }], text);
  }
  emit({ ok: true, action: "write", platform: process.platform, length: text.length });
}
emit({ ok: false, action, error: "unsupported clipboard action" }, 2);
`.trim();
  }

  function sourceNetworkScript(args = {}) {
    const payload = Buffer.from(JSON.stringify({
      action: String(args.action || "status").slice(0, 40),
      url: String(args.url || "").slice(0, 4000),
      host: String(args.host || args.hostname || "").slice(0, 255),
      port: Number.isSafeInteger(args.port) ? args.port : Number.parseInt(String(args.port || ""), 10),
      timeoutMs: Number.isSafeInteger(args.timeoutMs) ? Math.max(1000, Math.min(args.timeoutMs, 120000)) : 15000
    }), "utf8").toString("base64");
    return `
const os = await import("node:os");
const dns = await import("node:dns/promises");
const net = await import("node:net");
const req = JSON.parse(Buffer.from("${payload}", "base64").toString("utf8"));
const action = String(req.action || "status").toLowerCase().replace(/_/g, "-");
const timeoutMs = Math.max(1000, Math.min(Number(req.timeoutMs) || 15000, 120000));
const emit = (value, code = 0) => {
  console.log(JSON.stringify(value));
  process.exit(code);
};
function interfaces() {
  return Object.entries(os.networkInterfaces()).flatMap(([name, items]) => (items || [])
    .filter((item) => !item.internal)
    .map((item) => ({ name, family: item.family, address: item.address, mac: item.mac, cidr: item.cidr })));
}
function connect(host, port) {
  return new Promise((resolve) => {
    const started = Date.now();
    const socket = net.createConnection({ host, port, timeout: timeoutMs });
    socket.once("connect", () => {
      const latencyMs = Date.now() - started;
      socket.destroy();
      resolve({ ok: true, host, port, latencyMs });
    });
    socket.once("timeout", () => {
      socket.destroy();
      resolve({ ok: false, host, port, error: "timeout" });
    });
    socket.once("error", (error) => resolve({ ok: false, host, port, error: error.message }));
  });
}
if (action === "status" || action === "interfaces") {
  emit({ ok: true, action: "status", platform: process.platform, hostname: os.hostname(), interfaces: interfaces() });
}
if (action === "probe" || action === "connect" || action === "ping") {
  const url = String(req.url || "").trim();
  if (url) {
    const started = Date.now();
    const response = await fetch(url, { method: "GET", signal: AbortSignal.timeout(timeoutMs), cache: "no-store" });
    emit({ ok: response.ok, action: "probe", kind: "http", url, status: response.status, statusText: response.statusText, latencyMs: Date.now() - started });
  }
  const host = String(req.host || "").trim();
  if (!host) emit({ ok: false, action: "probe", error: "host or url required" }, 2);
  const port = Number.isSafeInteger(req.port) && req.port > 0 ? req.port : 443;
  const records = await dns.lookup(host, { all: true }).catch((error) => ({ error: error.message }));
  const connection = await connect(host, port);
  emit({ ...connection, action: "probe", kind: "tcp", addresses: Array.isArray(records) ? records : [], dnsError: records.error || "" }, connection.ok ? 0 : 1);
}
emit({ ok: false, action, error: "unsupported network action" }, 2);
`.trim();
  }

  return Object.freeze({
    sourceProcessScript,
    sourceClipboardScript,
    sourceNetworkScript
  });
}
