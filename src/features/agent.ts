export interface LocalAgentStatus {
  readonly ok: boolean;
  readonly managed?: boolean;
  readonly scope?: string;
  readonly autoUpdate?: boolean;
  readonly platform?: string;
  readonly shell?: string;
  readonly version?: string;
  readonly executionPlane?: string;
  readonly interactiveTaskBridge?: boolean;
  readonly companion?: boolean;
  readonly sourceWorker?: boolean;
  readonly windowsUser?: string;
  readonly system?: boolean;
  readonly maintenance?: boolean;
  readonly relay?: boolean;
  readonly codex?: boolean;
  readonly codexBinary?: boolean;
  readonly codexAuth?: boolean;
  readonly relayId?: string;
  readonly lastSeenAt?: string;
  readonly deviceId?: string;
  readonly deviceNick?: string;
}

export interface AgentSourceClientState {
  readonly localAgent?: LocalAgentStatus;
}

export interface LocalAgentReply {
  readonly ok: boolean;
  readonly text: string;
  readonly messages?: readonly string[];
  readonly terminal?: readonly string[];
  readonly exitCode?: number;
}

export type LocalAgentMessageHandler = (message: string) => void;

export interface LocalAgentOperatorTarget {
  readonly id: string;
  readonly label: string;
  readonly deviceIds?: readonly string[];
  readonly hostDeviceId?: string;
  readonly access?: boolean;
  readonly host?: boolean;
  readonly selected?: boolean;
  readonly rank?: number;
  readonly lastActionAt?: string;
}

export interface LocalAgentRequestSource {
  readonly tunnelId?: string;
  readonly tunnelLabel?: string;
  readonly deviceId?: string;
  readonly deviceNick?: string;
  readonly sourceRelayId?: string;
  readonly appOrigin?: string;
  readonly preferredTargetId?: string;
  readonly preferredTargetLabel?: string;
  readonly localAgentDirect?: boolean;
  readonly operatorTargets?: readonly LocalAgentOperatorTarget[];
}

const relayStorageKey = "soty:agent:relay-id";
const relayParamNames = ["agent", "agentRelay", "agentRelayId"];
const maxCodexDialogMessages = 64;
const localAgentBlockedText = "! agent-relay: agent bridge is not connected";

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
  const direct = await checkLocalAgentHttp(timeoutMs);
  if (direct.ok && direct.codex !== false) {
    return direct;
  }

  const relayId = readAgentRelayId() || ensureAgentRelayId();
  if (relayId) {
    const relay = await checkAgentRelay(timeoutMs);
    if (relay.ok && relay.codex !== false) {
      return relay;
    }
    return direct.ok ? direct : relay;
  }

  return direct;
}

export async function checkLocalCompanionAgent(timeoutMs = 850): Promise<LocalAgentStatus> {
  return await checkLocalAgentHttp(timeoutMs);
}

export async function askLocalAgentReply(
  text: string,
  context: string,
  source: LocalAgentRequestSource = {},
  timeoutMs = 2 * 60 * 60_000,
  onMessage?: LocalAgentMessageHandler,
  onTerminal?: LocalAgentMessageHandler
): Promise<LocalAgentReply> {
  const relayId = readAgentRelayId() || ensureAgentRelayId();
  if (relayId) {
    const relay = await askAgentRelayReply(text, context, source, timeoutMs, onMessage, onTerminal, true);
    if (relay && !shouldRetryAgentRelayReply(relay)) {
      return relay;
    }
  }

  const direct = await askLocalAgentReplyHttp(
    text,
    context,
    { ...source, localAgentDirect: true },
    timeoutMs
  );
  const directNeedsServerFallback = direct ? shouldUseServerAgentReplyFallback(direct) : false;
  if (direct && !directNeedsServerFallback) {
    for (const message of direct.messages ?? []) {
      onMessage?.(message);
    }
    for (const message of direct.terminal ?? []) {
      onTerminal?.(message);
    }
    return direct;
  }

  if (relayId) {
    const relay = await askAgentRelayReply(text, context, source, timeoutMs, onMessage, onTerminal, directNeedsServerFallback);
    if (relay && !shouldRetryAgentRelayReply(relay)) {
      return relay;
    }
    if (relay && !direct) {
      return relay;
    }
  }

  if (direct) {
    return direct;
  }

  return {
    ok: false,
    text: localAgentBlockedText,
    exitCode: 127
  };
}

function shouldRetryAgentRelayReply(reply: LocalAgentReply): boolean {
  return !reply.ok && reply.text.trim().startsWith("! agent-relay:");
}

function shouldUseServerAgentReplyFallback(reply: LocalAgentReply): boolean {
  if (reply.ok) {
    return false;
  }
  return /! codex-cli:\s*(?:not found|missing auth|OpenAI\/ChatGPT transport rejected|local Codex did not start in time)/iu.test(reply.text);
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

export async function bindLocalAgentRelay(device?: { readonly id?: string; readonly nick?: string }, timeoutMs = 1200): Promise<boolean> {
  const relayId = ensureAgentRelayId();
  const controller = new AbortController();
  const timer = window.setTimeout(() => controller.abort(), timeoutMs);
  try {
    const response = await fetch("http://127.0.0.1:49424/agent/relay", {
      method: "POST",
      cache: "no-store",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({
        relayId,
        relayBaseUrl: window.location.origin,
        deviceId: typeof device?.id === "string" ? device.id : "",
        deviceNick: typeof device?.nick === "string" ? device.nick : ""
      }),
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

export async function grantAgentSourceAccess(deviceId: string, deviceNick: string, enabled: boolean, state: AgentSourceClientState = {}, timeoutMs = 1500): Promise<boolean> {
  const relayId = ensureAgentRelayId();
  if (!relayId || !deviceId) {
    return false;
  }
  const controller = new AbortController();
  const timer = window.setTimeout(() => controller.abort(), timeoutMs);
  try {
    const response = await fetch("/api/agent/source/grant", {
      method: "POST",
      cache: "no-store",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({
        relayId,
        deviceId,
        deviceNick,
        enabled,
        ...agentSourceClientPayload(state)
      }),
      signal: controller.signal
    });
    const payload = await response.json() as { readonly ok?: boolean };
    return response.ok && payload.ok === true;
  } catch {
    return false;
  } finally {
    window.clearTimeout(timer);
  }
}

export async function checkAgentSourceWorker(deviceId: string, timeoutMs = 1500): Promise<LocalAgentStatus> {
  const relayId = readAgentRelayId();
  if (!relayId || !deviceId) {
    return { ok: false };
  }
  const controller = new AbortController();
  const timer = window.setTimeout(() => controller.abort(), timeoutMs);
  try {
    const response = await fetch(`/api/agent/source/targets?relayId=${encodeURIComponent(relayId)}`, {
      cache: "no-store",
      signal: controller.signal
    });
    if (!response.ok) {
      return { ok: false };
    }
    const payload = await response.json() as {
      readonly targets?: readonly {
        readonly hostDeviceId?: string;
        readonly deviceIds?: readonly string[];
        readonly localAgent?: unknown;
        readonly workers?: { readonly user?: { readonly localAgent?: unknown } };
      }[];
    };
    const target = (payload.targets || []).find((item) => item.hostDeviceId === deviceId || item.deviceIds?.includes(deviceId));
    const worker = readLocalAgentStatus(target?.workers?.user?.localAgent) || readLocalAgentStatus(target?.localAgent);
    return worker ? { ...worker, relay: true } : { ok: false };
  } catch {
    return { ok: false };
  } finally {
    window.clearTimeout(timer);
  }
}

function agentSourceClientPayload(state: AgentSourceClientState): Record<string, unknown> {
  return {
    clientProtocol: "soty-source-client.v2",
    clientCapabilities: ["runas", "local-agent-health"],
    localAgent: publicLocalAgentHealth(state.localAgent)
  };
}

function publicLocalAgentHealth(status: LocalAgentStatus | undefined): Record<string, string | boolean> {
  return {
    ok: status?.ok === true,
    version: String(status?.version || "").slice(0, 40),
    scope: String(status?.scope || "").slice(0, 40),
    executionPlane: String(status?.executionPlane || "").slice(0, 80),
    interactiveTaskBridge: status?.interactiveTaskBridge === true,
    companion: status?.companion === true,
    sourceWorker: status?.sourceWorker === true,
    autoUpdate: status?.autoUpdate === true,
    system: status?.system === true
  };
}

function readLocalAgentStatus(value: unknown): LocalAgentStatus | null {
  if (!value || typeof value !== "object") {
    return null;
  }
  const source = value as Record<string, unknown>;
  const ok = source.ok === true;
  return {
    ok,
    ...(typeof source.version === "string" ? { version: source.version.slice(0, 40) } : {}),
    ...(typeof source.scope === "string" ? { scope: source.scope.slice(0, 40) } : {}),
    ...(typeof source.executionPlane === "string" ? { executionPlane: source.executionPlane.slice(0, 80) } : {}),
    ...(typeof source.interactiveTaskBridge === "boolean" ? { interactiveTaskBridge: source.interactiveTaskBridge } : {}),
    ...(typeof source.companion === "boolean" ? { companion: source.companion } : {}),
    ...(typeof source.sourceWorker === "boolean" ? { sourceWorker: source.sourceWorker } : {}),
    ...(typeof source.autoUpdate === "boolean" ? { autoUpdate: source.autoUpdate } : {}),
    ...(typeof source.system === "boolean" ? { system: source.system } : {})
  };
}

async function askLocalAgentReplyHttp(
  text: string,
  context: string,
  source: LocalAgentRequestSource,
  timeoutMs: number
): Promise<LocalAgentReply | null> {
  const controller = new AbortController();
  const timer = window.setTimeout(() => controller.abort(), timeoutMs);
  try {
    const response = await fetch("http://127.0.0.1:49424/agent/reply", {
      method: "POST",
      cache: "no-store",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({ text, context, source }),
      signal: controller.signal,
      targetAddressSpace: "loopback"
    } as RequestInit & { readonly targetAddressSpace: "loopback" });
    const payload = await response.json() as {
      readonly ok?: boolean;
      readonly text?: string;
      readonly messages?: readonly unknown[];
      readonly terminal?: readonly unknown[];
      readonly exitCode?: number;
    };
    const messages = cleanReplyMessages(payload.messages);
    const terminal = cleanReplyMessages(payload.terminal);
    return {
      ok: Boolean(payload.ok && response.ok),
      text: typeof payload.text === "string" ? payload.text : "",
      ...(messages.length > 0 ? { messages } : {}),
      ...(terminal.length > 0 ? { terminal } : {}),
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
  source: LocalAgentRequestSource,
  timeoutMs: number,
  onMessage?: LocalAgentMessageHandler,
  onTerminal?: LocalAgentMessageHandler,
  preferServer = false
): Promise<LocalAgentReply | null> {
  const relayId = readAgentRelayId() || ensureAgentRelayId();
  if (!relayId) {
    return null;
  }
  try {
    const request = await fetch("/api/agent/relay/request", {
      method: "POST",
      cache: "no-store",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({ relayId, text, context, source, ...(preferServer ? { preferServer: true } : {}) })
    });
    const created = await request.json() as { readonly ok?: boolean; readonly id?: string };
    if (!request.ok || !created.ok || typeof created.id !== "string") {
      const error = typeof (created as { readonly error?: unknown }).error === "string"
        ? (created as { readonly error: string }).error
        : "";
      return relayFailure(
        error === "relay-not-connected" ? localAgentBlockedText : "! agent-relay: request rejected",
        request.status || 502
      );
    }
    let stopEvents = false;
    const eventStream = onMessage || onTerminal
      ? watchAgentRelayEvents(relayId, created.id, timeoutMs, onMessage, onTerminal, () => stopEvents)
      : Promise.resolve();
    const reply = await waitForAgentRelayReply(relayId, created.id, timeoutMs);
    stopEvents = true;
    void eventStream.catch(() => undefined);
    return reply || relayFailure("! agent-relay: local Codex bridge did not pick up the request", 124);
  } catch {
    return relayFailure("! agent-relay: browser could not reach relay", 127);
  }
}

async function watchAgentRelayEvents(
  relayId: string,
  id: string,
  timeoutMs: number,
  onMessage: LocalAgentMessageHandler | undefined,
  onTerminal: LocalAgentMessageHandler | undefined,
  stopped: () => boolean
): Promise<void> {
  const deadline = Date.now() + timeoutMs;
  let after = 0;
  while (!stopped() && Date.now() < deadline) {
    const remaining = deadline - Date.now();
    const controller = new AbortController();
    const timer = window.setTimeout(() => controller.abort(), Math.min(35_000, Math.max(1000, remaining)));
    try {
      const url = `/api/agent/relay/events?relayId=${encodeURIComponent(relayId)}&id=${encodeURIComponent(id)}&after=${after}&wait=1`;
      const response = await fetch(url, { cache: "no-store", signal: controller.signal });
      if (!response.ok) {
        return;
      }
      if (stopped()) {
        return;
      }
      const payload = await response.json() as {
        readonly done?: boolean;
        readonly events?: readonly {
          readonly seq?: number;
          readonly type?: string;
          readonly text?: string;
        }[];
      };
      for (const event of payload.events ?? []) {
        const seq = typeof event.seq === "number" ? event.seq : 0;
        if (seq <= after) {
          continue;
        }
        after = seq;
        const type = event.type || "agent_message";
        if (type === "agent_terminal" && typeof event.text === "string" && event.text.trim() && onTerminal) {
          onTerminal(event.text);
        } else if (type === "agent_message" && typeof event.text === "string" && event.text.trim() && onMessage) {
          onMessage(event.text);
        }
      }
      if (payload.done) {
        return;
      }
    } catch {
      // Keep the final reply poll authoritative; streaming is best-effort.
    } finally {
      window.clearTimeout(timer);
    }
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
        return relayFailure("! agent-relay: reply was lost", response.status);
      }
      const payload = await response.json() as {
        readonly reply?: {
          readonly ok?: boolean;
          readonly text?: string;
          readonly messages?: readonly unknown[];
          readonly terminal?: readonly unknown[];
          readonly exitCode?: number;
        } | null;
      };
      if (payload.reply) {
        const messages = cleanReplyMessages(payload.reply.messages);
        const terminal = cleanReplyMessages(payload.reply.terminal);
        return {
          ok: Boolean(payload.reply.ok),
          text: typeof payload.reply.text === "string" ? payload.reply.text : "",
          ...(messages.length > 0 ? { messages } : {}),
          ...(terminal.length > 0 ? { terminal } : {}),
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

function cleanReplyMessages(value: readonly unknown[] | undefined): string[] {
  if (!Array.isArray(value)) {
    return [];
  }
  return value
    .filter((item): item is string => typeof item === "string")
    .map((item) => item.replace(/\r\n?/gu, "\n").trim().slice(0, 12_000))
    .filter(Boolean)
    .slice(0, maxCodexDialogMessages);
}

function isCodexMissingReply(reply: LocalAgentReply): boolean {
  return !reply.ok && (reply.exitCode === 126 || /codex-cli:\s*not found/iu.test(reply.text));
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
      readonly autoUpdate?: boolean;
      readonly executionPlane?: string;
      readonly interactiveTaskBridge?: boolean;
      readonly companion?: boolean;
      readonly windowsUser?: string;
      readonly system?: boolean;
      readonly maintenance?: boolean;
      readonly relay?: boolean;
      readonly sourceWorker?: boolean;
      readonly codex?: boolean;
      readonly codexBinary?: boolean;
      readonly codexAuth?: boolean;
    };
    return {
      ok: true,
      ...(typeof message.managed === "boolean" ? { managed: message.managed } : {}),
      ...(typeof message.scope === "string" ? { scope: message.scope } : {}),
      ...(typeof message.platform === "string" ? { platform: message.platform } : {}),
      ...(typeof message.shell === "string" ? { shell: message.shell } : {}),
      ...(typeof message.version === "string" ? { version: message.version } : {}),
      ...(typeof message.autoUpdate === "boolean" ? { autoUpdate: message.autoUpdate } : {}),
      ...(typeof message.executionPlane === "string" ? { executionPlane: message.executionPlane } : {}),
      ...(typeof message.interactiveTaskBridge === "boolean" ? { interactiveTaskBridge: message.interactiveTaskBridge } : {}),
      ...(typeof message.companion === "boolean" ? { companion: message.companion } : {}),
      ...(typeof message.windowsUser === "string" ? { windowsUser: message.windowsUser } : {}),
      ...(typeof message.system === "boolean" ? { system: message.system } : {}),
      ...(typeof message.maintenance === "boolean" ? { maintenance: message.maintenance } : {}),
      ...(typeof message.relay === "boolean" ? { relay: message.relay } : {}),
      ...(typeof message.sourceWorker === "boolean" ? { sourceWorker: message.sourceWorker } : {}),
      ...(typeof message.codex === "boolean" ? { codex: message.codex } : {}),
      ...(typeof message.codexBinary === "boolean" ? { codexBinary: message.codexBinary } : {}),
      ...(typeof message.codexAuth === "boolean" ? { codexAuth: message.codexAuth } : {})
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
      readonly codex?: boolean;
      readonly codexBinary?: boolean;
      readonly codexAuth?: boolean;
    };
    return {
      ok: Boolean(payload.connected),
      relay: true,
      managed: true,
      scope: "Relay",
      ...(typeof payload.version === "string" ? { version: payload.version } : {}),
      ...(typeof payload.codex === "boolean" ? { codex: payload.codex } : {}),
      ...(typeof payload.codexBinary === "boolean" ? { codexBinary: payload.codexBinary } : {}),
      ...(typeof payload.codexAuth === "boolean" ? { codexAuth: payload.codexAuth } : {})
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

export function canInstallMachineAgent(): boolean {
  const nav = navigator as Navigator & { readonly userAgentData?: { readonly platform?: string } };
  const platform = `${nav.userAgentData?.platform || navigator.platform || navigator.userAgent}`.toLowerCase();
  return !/(iphone|ipad|ipod|android|mobile)/u.test(platform);
}

export function agentInstallUrl(_scope: "user" | "machine" = "machine"): string {
  return isWindowsPlatform() ? "/agent/install-windows-machine.cmd" : "/agent/install-macos-linux.sh";
}

export function downloadAgentInstaller(scope: "user" | "machine" = "machine"): void {
  return downloadAgentInstallerForDevice(scope);
}

export function downloadAgentInstallerForDevice(
  scope: "user" | "machine" = "machine",
  device: { readonly id?: string; readonly nick?: string } = {}
): void {
  const relayId = ensureAgentRelayId();
  const base = `${window.location.origin}/agent`;
  if (isWindowsPlatform()) {
    downloadText(
      "install-soty-agent-machine.cmd",
      buildWindowsInstaller(base, relayId, device),
      "application/bat"
    );
    return;
  }
  downloadText("install-soty-agent.sh", buildUnixInstaller(scope, base, relayId), "text/x-shellscript");
}

function buildWindowsInstaller(
  base: string,
  relayId: string,
  device: { readonly id?: string; readonly nick?: string } = {}
): string {
  const deviceId = sanitizeWindowsCmdValue(device.id || "");
  const deviceNick = sanitizeWindowsCmdValue(device.nick || "");
  return [
    "@echo off",
    "setlocal",
    `set "BASE=${base}"`,
    `set "RELAY=${relayId}"`,
    `set "DEVICE=${deviceId}"`,
    `set "NICK=${deviceNick}"`,
    "",
    "powershell.exe -NoLogo -NoProfile -ExecutionPolicy Bypass -Command \"$ErrorActionPreference='Stop'; [Net.ServicePointManager]::SecurityProtocol = [Net.SecurityProtocolType]::Tls12; $dir = Join-Path $env:TEMP 'soty-agent-machine'; New-Item -ItemType Directory -Force -Path $dir | Out-Null; $script = Join-Path $dir 'install-windows.ps1'; Invoke-WebRequest -Uri '%BASE%/install-windows.ps1' -UseBasicParsing -OutFile $script; $extra = ''; if (-not [string]::IsNullOrWhiteSpace('%DEVICE%')) { $extra = ' -DeviceId \"%DEVICE%\"'; if (-not [string]::IsNullOrWhiteSpace('%NICK%')) { $extra += ' -DeviceNick \"%NICK%\"' } }; $arg = '-NoLogo -NoProfile -ExecutionPolicy Bypass -File \"' + $script + '\" -Base \"%BASE%\" -Scope Machine -LaunchAppAtLogon -RelayId \"%RELAY%\"' + $extra; $p = Start-Process -FilePath 'powershell.exe' -Verb RunAs -ArgumentList $arg -Wait -PassThru; exit $p.ExitCode\"",
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

function sanitizeWindowsCmdValue(value: string): string {
  return value.replace(/[\r\n"%']/gu, "").slice(0, 160);
}

function buildUnixInstaller(scope: "user" | "machine", base: string, relayId: string): string {
  return [
    "#!/usr/bin/env sh",
    "set -eu",
    `BASE="${shellEscape(base)}"`,
    `RELAY="${shellEscape(relayId)}"`,
    "DIR=\"${TMPDIR:-/tmp}/soty-agent-install\"",
    "mkdir -p \"$DIR\"",
    "SCRIPT=\"$DIR/install-macos-linux.sh\"",
    "if command -v curl >/dev/null 2>&1; then",
    "  curl -fsSL --retry 3 --retry-delay 2 \"$BASE/install-macos-linux.sh\" -o \"$SCRIPT\"",
    "else",
    "  wget -qO \"$SCRIPT\" \"$BASE/install-macos-linux.sh\"",
    "fi",
    scope === "machine"
      ? "sh \"$SCRIPT\" --scope machine --base \"$BASE\" --relay-id \"$RELAY\""
      : "sh \"$SCRIPT\" --scope user --base \"$BASE\" --relay-id \"$RELAY\"",
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
