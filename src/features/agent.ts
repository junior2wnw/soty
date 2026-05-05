export interface LocalAgentStatus {
  readonly ok: boolean;
  readonly managed?: boolean;
  readonly scope?: string;
  readonly platform?: string;
  readonly shell?: string;
  readonly version?: string;
  readonly windowsUser?: string;
  readonly system?: boolean;
  readonly maintenance?: boolean;
  readonly relay?: boolean;
}

export interface LocalAgentReply {
  readonly ok: boolean;
  readonly text: string;
  readonly exitCode?: number;
}

const relayStorageKey = "soty:agent:relay-id";
const relayParamNames = ["agent", "agentRelay", "agentRelayId"];
const localAgentBlockedText = "Сообщение отправлено, но браузер пока не смог достучаться до локального агента. Я включил серверный мост, но агент еще не подключился к нему. Нажми установку агента один раз и потом обнови проверку.";

export function adoptAgentRelayFromUrl(): boolean {
  const url = new URL(window.location.href);
  const relayId = relayParamNames
    .map((name) => sanitizeRelayId(url.searchParams.get(name) || ""))
    .find(Boolean) || "";
  if (!relayId) {
    return false;
  }
  localStorage.setItem(relayStorageKey, relayId);
  for (const name of relayParamNames) {
    url.searchParams.delete(name);
  }
  window.history.replaceState({}, "", `${url.pathname}${url.search}${url.hash}`);
  return true;
}

export async function checkLocalAgent(timeoutMs = 850): Promise<LocalAgentStatus> {
  if (readAgentRelayId()) {
    const relay = await checkAgentRelay(timeoutMs);
    if (relay.ok) {
      return relay;
    }
  }
  const direct = await checkLocalAgentHttp(timeoutMs);
  if (direct.ok) {
    return direct;
  }
  const relay = await checkAgentRelay(timeoutMs);
  return relay.ok ? relay : direct;
}

export async function askLocalAgentReply(
  text: string,
  context: string,
  timeoutMs = 130_000
): Promise<LocalAgentReply> {
  if (readAgentRelayId()) {
    const relayStatus = await checkAgentRelay(1200);
    if (relayStatus.ok) {
      return await askAgentRelayReply(text, context, timeoutMs) || {
        ok: false,
        text: "Серверный мост агента подключен, но не вернул ответ.",
        exitCode: 124
      };
    }
  }
  const direct = await askLocalAgentReplyHttp(text, context, timeoutMs);
  if (direct) {
    return direct;
  }
  const relay = await askAgentRelayReply(text, context, timeoutMs);
  return relay || {
    ok: false,
    text: localAgentBlockedText,
    exitCode: 127
  };
}

export function hasAgentRelayId(): boolean {
  return Boolean(readAgentRelayId());
}

export function ensureAgentRelayId(): string {
  const existing = readAgentRelayId();
  if (existing) {
    return existing;
  }
  const relayId = createRelayId();
  localStorage.setItem(relayStorageKey, relayId);
  return relayId;
}

export function agentRelayInviteUrl(): string {
  const relayId = ensureAgentRelayId();
  const url = new URL(window.location.href);
  url.searchParams.set("agent", relayId);
  return url.toString();
}

export async function bindLocalAgentRelay(timeoutMs = 1200): Promise<boolean> {
  const relayId = ensureAgentRelayId();
  const controller = new AbortController();
  const timer = window.setTimeout(() => controller.abort(), timeoutMs);
  try {
    const response = await fetch("http://127.0.0.1:49424/agent/relay", {
      method: "POST",
      cache: "no-store",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({ relayId, relayBaseUrl: window.location.origin }),
      signal: controller.signal,
      targetAddressSpace: "loopback"
    } as RequestInit & { readonly targetAddressSpace: "loopback" });
    return response.ok;
  } catch {
    return false;
  } finally {
    window.clearTimeout(timer);
  }
}

async function askLocalAgentReplyHttp(
  text: string,
  context: string,
  timeoutMs: number
): Promise<LocalAgentReply | null> {
  const controller = new AbortController();
  const timer = window.setTimeout(() => controller.abort(), timeoutMs);
  try {
    const response = await fetch("http://127.0.0.1:49424/agent/reply", {
      method: "POST",
      cache: "no-store",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({ text, context }),
      signal: controller.signal,
      targetAddressSpace: "loopback"
    } as RequestInit & { readonly targetAddressSpace: "loopback" });
    const payload = await response.json() as {
      readonly ok?: boolean;
      readonly text?: string;
      readonly exitCode?: number;
    };
    return {
      ok: Boolean(payload.ok && response.ok),
      text: typeof payload.text === "string" ? payload.text : "",
      ...(typeof payload.exitCode === "number" ? { exitCode: payload.exitCode } : {})
    };
  } catch {
    return null;
  } finally {
    window.clearTimeout(timer);
  }
}

async function askAgentRelayReply(
  text: string,
  context: string,
  timeoutMs: number
): Promise<LocalAgentReply | null> {
  const relayId = readAgentRelayId();
  if (!relayId) {
    return null;
  }
  try {
    const request = await fetch("/api/agent/relay/request", {
      method: "POST",
      cache: "no-store",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({ relayId, text, context })
    });
    const created = await request.json() as { readonly ok?: boolean; readonly id?: string };
    if (!request.ok || !created.ok || typeof created.id !== "string") {
      return relayFailure("Серверный мост агента не принял сообщение.", 502);
    }
    const reply = await waitForAgentRelayReply(relayId, created.id, timeoutMs);
    return reply || relayFailure("Сообщение дошло до серверного моста, но локальный агент пока не забрал его.", 124);
  } catch {
    return relayFailure("Браузер не смог связаться с серверным мостом агента.", 127);
  }
}

async function waitForAgentRelayReply(
  relayId: string,
  id: string,
  timeoutMs: number
): Promise<LocalAgentReply | null> {
  const deadline = Date.now() + timeoutMs;
  while (Date.now() < deadline) {
    const remaining = deadline - Date.now();
    const controller = new AbortController();
    const timer = window.setTimeout(() => controller.abort(), Math.min(35_000, Math.max(1000, remaining)));
    try {
      const url = `/api/agent/relay/reply?relayId=${encodeURIComponent(relayId)}&id=${encodeURIComponent(id)}&wait=1`;
      const response = await fetch(url, { cache: "no-store", signal: controller.signal });
      if (!response.ok) {
        return relayFailure("Серверный мост агента потерял сообщение.", response.status);
      }
      const payload = await response.json() as {
        readonly reply?: {
          readonly ok?: boolean;
          readonly text?: string;
          readonly exitCode?: number;
        } | null;
      };
      if (payload.reply) {
        return {
          ok: Boolean(payload.reply.ok),
          text: typeof payload.reply.text === "string" ? payload.reply.text : "",
          ...(typeof payload.reply.exitCode === "number" ? { exitCode: payload.reply.exitCode } : {})
        };
      }
    } catch {
      // Keep polling until the outer timeout. A single long-poll can be interrupted by network switches.
    } finally {
      window.clearTimeout(timer);
    }
  }
  return null;
}

function relayFailure(text: string, exitCode: number): LocalAgentReply {
  return { ok: false, text, exitCode };
}

async function checkLocalAgentHttp(timeoutMs: number): Promise<LocalAgentStatus> {
  const controller = new AbortController();
  const timer = window.setTimeout(() => controller.abort(), timeoutMs);
  try {
    const response = await fetch("http://127.0.0.1:49424/health", {
      cache: "no-store",
      signal: controller.signal,
      targetAddressSpace: "loopback"
    } as RequestInit & { readonly targetAddressSpace: "loopback" });
    if (!response.ok) {
      return { ok: false };
    }
    const message = await response.json() as {
      readonly managed?: boolean;
      readonly scope?: string;
      readonly platform?: string;
      readonly shell?: string;
      readonly version?: string;
      readonly windowsUser?: string;
      readonly system?: boolean;
      readonly maintenance?: boolean;
      readonly relay?: boolean;
    };
    return {
      ok: true,
      ...(typeof message.managed === "boolean" ? { managed: message.managed } : {}),
      ...(typeof message.scope === "string" ? { scope: message.scope } : {}),
      ...(typeof message.platform === "string" ? { platform: message.platform } : {}),
      ...(typeof message.shell === "string" ? { shell: message.shell } : {}),
      ...(typeof message.version === "string" ? { version: message.version } : {}),
      ...(typeof message.windowsUser === "string" ? { windowsUser: message.windowsUser } : {}),
      ...(typeof message.system === "boolean" ? { system: message.system } : {}),
      ...(typeof message.maintenance === "boolean" ? { maintenance: message.maintenance } : {}),
      ...(typeof message.relay === "boolean" ? { relay: message.relay } : {})
    };
  } catch {
    return { ok: false };
  } finally {
    window.clearTimeout(timer);
  }
}

async function checkAgentRelay(timeoutMs: number): Promise<LocalAgentStatus> {
  const relayId = readAgentRelayId();
  if (!relayId) {
    return { ok: false };
  }
  const controller = new AbortController();
  const timer = window.setTimeout(() => controller.abort(), timeoutMs);
  try {
    const response = await fetch(`/api/agent/relay/status?relayId=${encodeURIComponent(relayId)}`, {
      cache: "no-store",
      signal: controller.signal
    });
    if (!response.ok) {
      return { ok: false };
    }
    const payload = await response.json() as {
      readonly connected?: boolean;
      readonly version?: string;
    };
    return {
      ok: Boolean(payload.connected),
      relay: true,
      managed: true,
      scope: "Relay",
      ...(typeof payload.version === "string" ? { version: payload.version } : {})
    };
  } catch {
    return { ok: false };
  } finally {
    window.clearTimeout(timer);
  }
}

export function isWindowsPlatform(): boolean {
  const nav = navigator as Navigator & { readonly userAgentData?: { readonly platform?: string } };
  const platform = `${nav.userAgentData?.platform || navigator.platform || navigator.userAgent}`.toLowerCase();
  return platform.includes("win");
}

export function agentInstallUrl(scope: "user" | "machine" = "user"): string {
  return isWindowsPlatform()
    ? (scope === "machine" ? "/agent/install-windows-machine.cmd" : "/agent/install-windows.cmd")
    : "/agent/install-macos-linux.sh";
}

export function downloadAgentInstaller(scope: "user" | "machine" = "user"): void {
  const relayId = ensureAgentRelayId();
  const base = `${window.location.origin}/agent`;
  if (isWindowsPlatform()) {
    downloadText(
      scope === "machine" ? "install-soty-agent-machine.cmd" : "install-soty-agent.cmd",
      buildWindowsInstaller(scope, base, relayId),
      "application/bat"
    );
    return;
  }
  downloadText("install-soty-agent.sh", buildUnixInstaller(base, relayId), "text/x-shellscript");
}

function buildWindowsInstaller(scope: "user" | "machine", base: string, relayId: string): string {
  if (scope === "machine") {
    return [
      "@echo off",
      "setlocal",
      `set "BASE=${base}"`,
      `set "RELAY=${relayId}"`,
      "",
      "powershell.exe -NoLogo -NoProfile -ExecutionPolicy Bypass -Command \"$ErrorActionPreference='Stop'; [Net.ServicePointManager]::SecurityProtocol = [Net.SecurityProtocolType]::Tls12; $dir = Join-Path $env:TEMP 'soty-agent-machine'; New-Item -ItemType Directory -Force -Path $dir | Out-Null; $script = Join-Path $dir 'install-windows.ps1'; Invoke-WebRequest -Uri '%BASE%/install-windows.ps1' -UseBasicParsing -OutFile $script; $arg = '-NoLogo -NoProfile -ExecutionPolicy Bypass -File \"' + $script + '\" -Base \"%BASE%\" -Scope Machine -LaunchAppAtLogon -RelayId \"%RELAY%\"'; $p = Start-Process -FilePath 'powershell.exe' -Verb RunAs -ArgumentList $arg -Wait -PassThru; exit $p.ExitCode\"",
      "if errorlevel 1 goto fail",
      "exit /b 0",
      "",
      ":fail",
      "echo.",
      "echo soty-agent machine install failed",
      "echo %ProgramData%\\soty-agent\\install.log",
      "pause",
      "exit /b 1",
      ""
    ].join("\r\n");
  }
  return [
    "@echo off",
    "setlocal",
    `set "BASE=${base}"`,
    `set "RELAY=${relayId}"`,
    "",
    "powershell.exe -NoLogo -NoProfile -ExecutionPolicy Bypass -Command \"$ErrorActionPreference='Stop'; [Net.ServicePointManager]::SecurityProtocol = [Net.SecurityProtocolType]::Tls12; $local = [Environment]::GetFolderPath('LocalApplicationData'); if ([string]::IsNullOrWhiteSpace($local)) { $local = Join-Path $HOME 'AppData\\Local' }; $dir = Join-Path $local 'soty-agent'; New-Item -ItemType Directory -Force -Path $dir | Out-Null; $script = Join-Path $dir 'install-windows.ps1'; Invoke-WebRequest -Uri '%BASE%/install-windows.ps1' -UseBasicParsing -OutFile $script; & $script -Base '%BASE%' -RelayId '%RELAY%'\"",
    "if errorlevel 1 goto fail",
    "exit /b 0",
    "",
    ":fail",
    "echo.",
    "echo soty-agent install failed",
    "echo %LOCALAPPDATA%\\soty-agent\\install.log",
    "pause",
    "exit /b 1",
    ""
  ].join("\r\n");
}

function buildUnixInstaller(base: string, relayId: string): string {
  return [
    "#!/usr/bin/env sh",
    "set -eu",
    `BASE="${shellEscape(base)}"`,
    `RELAY="${shellEscape(relayId)}"`,
    "DIR=\"${TMPDIR:-/tmp}/soty-agent-install\"",
    "mkdir -p \"$DIR\"",
    "SCRIPT=\"$DIR/install-macos-linux.sh\"",
    "if command -v curl >/dev/null 2>&1; then",
    "  curl -fsSL \"$BASE/install-macos-linux.sh\" -o \"$SCRIPT\"",
    "else",
    "  wget -qO \"$SCRIPT\" \"$BASE/install-macos-linux.sh\"",
    "fi",
    "sh \"$SCRIPT\" \"$BASE\" \"$RELAY\"",
    ""
  ].join("\n");
}

function downloadText(filename: string, content: string, type: string): void {
  const url = URL.createObjectURL(new Blob([content], { type }));
  const link = document.createElement("a");
  link.href = url;
  link.download = filename;
  document.body.append(link);
  link.click();
  link.remove();
  window.setTimeout(() => URL.revokeObjectURL(url), 1000);
}

function readAgentRelayId(): string {
  try {
    const value = localStorage.getItem(relayStorageKey) || "";
    return sanitizeRelayId(value);
  } catch {
    return "";
  }
}

function sanitizeRelayId(value: string): string {
  const text = String(value || "").trim();
  return /^[A-Za-z0-9_-]{32,192}$/u.test(text) ? text : "";
}

function createRelayId(): string {
  const bytes = new Uint8Array(32);
  crypto.getRandomValues(bytes);
  let binary = "";
  for (const byte of bytes) {
    binary += String.fromCharCode(byte);
  }
  return btoa(binary).replace(/\+/gu, "-").replace(/\//gu, "_").replace(/=+$/u, "");
}

function shellEscape(value: string): string {
  return value.replace(/["\\$`]/gu, "\\$&");
}
