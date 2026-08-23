export interface SotyAgentStatus {
  readonly id: "opencode";
  readonly name: "OpenCode";
  readonly provider: "gonka";
  readonly model?: string;
  readonly available: boolean;
  readonly version?: string;
  readonly reason?: string;
  readonly capabilities?: readonly string[];
}

export interface LocalAgentStatus {
  readonly ok: boolean;
  readonly connector?: boolean;
  readonly agentRuntime?: boolean;
  readonly managed?: boolean;
  readonly scope?: string;
  readonly scopes?: readonly string[];
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
  readonly relayId?: string;
  readonly lastSeenAt?: string;
  readonly deviceId?: string;
  readonly deviceNick?: string;
  readonly agent?: SotyAgentStatus;
}

export interface LocalAgentReply {
  readonly ok: boolean;
  readonly text: string;
  readonly messages?: readonly string[];
  readonly terminal?: readonly string[];
  readonly exitCode?: number;
  readonly sessionId?: string;
}

export type LocalAgentMessageHandler = (message: string) => void;

export interface SpreadExPairResult {
  readonly ok: boolean;
  readonly error?: string;
  readonly status?: {
    readonly paired?: boolean;
    readonly deviceId?: string;
    readonly baseOrigin?: string;
  };
}

export interface LocalAgentPendingRelayReply {
  readonly relayId: string;
  readonly id: string;
  readonly tunnelId: string;
  readonly text: string;
  readonly createdAt: number;
  readonly timeoutAt: number;
  readonly after: number;
  readonly messages: readonly string[];
  readonly terminal: readonly string[];
}

export interface LocalAgentRequestSource {
  readonly tunnelId?: string;
  readonly tunnelLabel?: string;
  readonly deviceId?: string;
  readonly deviceNick?: string;
  readonly localAgent?: LocalAgentStatus;
  readonly sourceRelayId?: string;
  readonly appOrigin?: string;
  readonly sessionId?: string;
  readonly cwd?: string;
}

export interface ConnectorJobInput {
  readonly kind: "agent" | "command" | "script";
  readonly text?: string;
  readonly context?: string;
  readonly cwd?: string;
  readonly sessionId?: string;
  readonly name?: string;
  readonly shell?: string;
  readonly script?: string;
  readonly runAs?: "user" | "system";
  readonly timeoutMs?: number;
}

export interface ConnectorJobEvent {
  readonly seq: number;
  readonly type: string;
  readonly text: string;
  readonly at?: string;
  readonly data?: Readonly<Record<string, unknown>>;
}

export interface RunConnectorJobOptions {
  readonly deviceId?: string;
  readonly threadId?: string;
  readonly input: ConnectorJobInput;
  readonly timeoutMs?: number;
  readonly signal?: AbortSignal;
  readonly onEvent?: (event: ConnectorJobEvent) => void;
}

const linkStorageKey = "soty:connector:link-id";
const previousLinkStorageKey = "soty:agent:relay-id";
const pendingJobsStorageKey = "soty:connector:pending-jobs:v1";
const spreadExPairSessionKey = "soty:spreadex:pair-code:v1";
const linkParamNames = ["connector", "link", "agent", "agentRelay", "agentRelayId"];
const pendingJobTtlMs = 2 * 60 * 60_000 + 30 * 60_000;
const maxMessages = 64;

export function adoptAgentRelayFromUrl(): boolean {
  const url = new URL(window.location.href);
  const linkId = linkParamNames.map((name) => sanitizeLinkId(url.searchParams.get(name) || "")).find(Boolean) || "";
  if (!linkId) return false;
  writeLinkId(linkId);
  for (const name of linkParamNames) url.searchParams.delete(name);
  window.history.replaceState({}, "", `${url.pathname}${url.search}${url.hash}`);
  return true;
}

export function spreadExPairCodeFromUrl(): string {
  const url = new URL(window.location.href);
  if (!/^\/install\/spreadex\/?$/u.test(url.pathname)) return "";
  const code = String(url.searchParams.get("pair") || "").trim();
  if (/^[A-Za-z0-9_-]{8,192}$/u.test(code)) {
    try { sessionStorage.setItem(spreadExPairSessionKey, code); } catch { /* The current URL remains the fallback. */ }
    url.searchParams.delete("pair");
    url.searchParams.set("pairing", "1");
    window.history.replaceState({}, "", `${url.pathname}${url.search}${url.hash}`);
    return code;
  }
  try {
    const saved = String(sessionStorage.getItem(spreadExPairSessionKey) || "").trim();
    return /^[A-Za-z0-9_-]{8,192}$/u.test(saved) ? saved : "";
  } catch { return ""; }
}

export function clearSpreadExPairCodeFromUrl(): void {
  const url = new URL(window.location.href);
  try { sessionStorage.removeItem(spreadExPairSessionKey); } catch { /* Session storage is optional. */ }
  url.searchParams.delete("pair");
  url.searchParams.delete("pairing");
  url.searchParams.set("paired", "1");
  window.history.replaceState({}, "", `${url.pathname}${url.search}${url.hash}`);
}

export async function pairLocalSpreadEx(pairCode: string, timeoutMs = 4_000): Promise<SpreadExPairResult> {
  const code = String(pairCode || "").trim();
  if (!/^[A-Za-z0-9_-]{8,192}$/u.test(code)) return { ok: false, error: "invalid-pair-code" };
  const result = await requestJson("http://127.0.0.1:49424/integrations/spreadex/v1/pair", {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify({ code }),
    targetAddressSpace: "loopback"
  } as RequestInit & { readonly targetAddressSpace: "loopback" }, timeoutMs);
  const paired = result.ok === true
    && result.schema === "soty.spreadex-ml.v1"
    && result.status?.paired === true
    && typeof result.status?.deviceId === "string"
    && Boolean(result.status.deviceId);
  return {
    ok: paired,
    ...(typeof result.error === "string" ? { error: result.error } : !paired ? { error: "agent-incompatible" } : {}),
    ...(result.status && typeof result.status === "object" ? { status: result.status } : {})
  };
}

export async function checkLocalAgent(timeoutMs = 3_000): Promise<LocalAgentStatus> {
  const linkId = readLinkId() || ensureAgentRelayId();
  const remote = await checkConnectorStatus(linkId, "", timeoutMs);
  if (remote.ok) return remote;
  return await checkLocalAgentHttp(timeoutMs);
}

export async function checkLocalCompanionAgent(timeoutMs = 3_000): Promise<LocalAgentStatus> {
  return await checkLocalAgentHttp(timeoutMs);
}

export async function askLocalAgentReply(
  text: string,
  context: string,
  source: LocalAgentRequestSource = {},
  timeoutMs = 2 * 60 * 60_000,
  onMessage?: LocalAgentMessageHandler,
  onTerminal?: LocalAgentMessageHandler,
  signal?: AbortSignal
): Promise<LocalAgentReply> {
  return await runConnectorJob({
    ...(source.deviceId ? { deviceId: source.deviceId } : {}),
    ...(source.tunnelId ? { threadId: source.tunnelId } : {}),
    input: {
      kind: "agent",
      text,
      context,
      ...(source.cwd ? { cwd: source.cwd } : {}),
      ...(source.sessionId ? { sessionId: source.sessionId } : {}),
      timeoutMs
    },
    timeoutMs,
    ...(signal ? { signal } : {}),
    onEvent: (event) => {
      if (event.type === "message") onMessage?.(event.text);
      else if (event.text && !["queued", "leased", "started", "heartbeat"].includes(event.type)) onTerminal?.(event.text);
    }
  });
}

export async function runConnectorJob(options: RunConnectorJobOptions): Promise<LocalAgentReply> {
  if (options.signal?.aborted) return cancelledReply();
  const linkId = readLinkId() || ensureAgentRelayId();
  const created = await requestJson("/api/connectors/jobs", {
    method: "POST",
    headers: { "Content-Type": "application/json", "X-Soty-Link-Id": linkId },
    body: JSON.stringify({
      linkId,
      deviceId: options.deviceId || "",
      threadId: cleanId(options.threadId || ""),
      kind: options.input.kind,
      input: options.input,
      permissions: { sandbox: "workspace-write", approval: "never" }
    })
  }, 30_000, options.signal);
  if (!created.ok || typeof created.job?.id !== "string") {
    return failureReply(created.error === "invalid-job" ? "Задание некорректно" : "Soty Agent не принял задание", 502);
  }
  const id = created.job.id;
  const timeoutMs = Math.max(1_000, options.timeoutMs || options.input.timeoutMs || 2 * 60 * 60_000);
  rememberPending({
    relayId: linkId,
    id,
    tunnelId: options.threadId || id,
    text: options.input.text || options.input.name || "",
    createdAt: Date.now(),
    timeoutAt: Date.now() + timeoutMs,
    after: 0,
    messages: [],
    terminal: []
  });
  const abort = () => void cancelConnectorJob(id, linkId);
  options.signal?.addEventListener("abort", abort, { once: true });
  try {
    return await waitForJob(linkId, id, timeoutMs, options.onEvent, options.signal);
  } finally {
    options.signal?.removeEventListener("abort", abort);
  }
}

export async function cancelConnectorJob(id: string, explicitLinkId = ""): Promise<boolean> {
  const linkId = sanitizeLinkId(explicitLinkId) || readLinkId();
  if (!linkId || !cleanId(id)) return false;
  const result = await requestJson(`/api/connectors/jobs/${encodeURIComponent(id)}/cancel`, {
    method: "POST",
    headers: { "Content-Type": "application/json", "X-Soty-Link-Id": linkId },
    body: JSON.stringify({ linkId })
  }, 5_000);
  return result.ok;
}

export function hasAgentRelayId(): boolean {
  return Boolean(readLinkId());
}

export function hasAvailableSotyAgent(status: LocalAgentStatus): boolean {
  return status.agent?.id === "opencode"
    && status.agent.provider === "gonka"
    && status.agent.available === true;
}

export function ensureAgentRelayId(): string {
  const existing = readLinkId();
  if (existing) return existing;
  const bytes = new Uint8Array(32);
  crypto.getRandomValues(bytes);
  let binary = "";
  for (const byte of bytes) binary += String.fromCharCode(byte);
  const linkId = btoa(binary).replace(/\+/gu, "-").replace(/\//gu, "_").replace(/=+$/u, "");
  writeLinkId(linkId);
  return linkId;
}

export function agentRelayInviteUrl(): string {
  const url = new URL(window.location.href);
  url.searchParams.set("connector", ensureAgentRelayId());
  return url.toString();
}

export async function bindLocalAgentRelay(device?: { readonly id?: string; readonly nick?: string }, timeoutMs = 1800): Promise<boolean> {
  const linkId = ensureAgentRelayId();
  const result = await requestJson("http://127.0.0.1:49424/connector/bind", {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify({
      linkId,
      serverUrl: window.location.origin,
      deviceId: device?.id || "",
      deviceNick: device?.nick || ""
    }),
    targetAddressSpace: "loopback"
  } as RequestInit & { readonly targetAddressSpace: "loopback" }, timeoutMs);
  return result.ok
    && result.schema === "soty.agent-runtime.v1"
    && (!device?.id || result.deviceId === device.id || result.currentDeviceId === device.id);
}

export async function checkAgentSourceWorker(deviceId: string, timeoutMs = 1500): Promise<LocalAgentStatus> {
  const status = await checkConnectorStatus(readLinkId(), deviceId, timeoutMs);
  if (!status.ok) return status;
  const hasUser = status.scopes?.some((item) => item === "CurrentUser" || item === "Dev")
    && hasAvailableSotyAgent(status);
  return hasUser ? status : { ok: false, deviceId, ...(status.deviceNick ? { deviceNick: status.deviceNick } : {}) };
}

export async function checkAgentSourceMachineAgent(deviceId: string, timeoutMs = 1500): Promise<LocalAgentStatus> {
  const status = await checkConnectorStatus(readLinkId(), deviceId, timeoutMs);
  if (!status.ok) return status;
  const machine = status.scopes?.includes("Machine");
  return machine ? { ...status, system: true, maintenance: true, scope: "Machine" } : { ok: false, deviceId, ...(status.deviceNick ? { deviceNick: status.deviceNick } : {}) };
}

export async function resumeAgentRelayReply(
  pending: LocalAgentPendingRelayReply,
  onMessage?: LocalAgentMessageHandler,
  onTerminal?: LocalAgentMessageHandler,
  signal?: AbortSignal
): Promise<LocalAgentReply> {
  const linkId = sanitizeLinkId(pending.relayId);
  const id = cleanId(pending.id);
  if (!linkId || !id) return failureReply("Сохранённое задание повреждено", 400);
  return await waitForJob(linkId, id, Math.max(1_000, pending.timeoutAt - Date.now()), (event) => {
    if (event.type === "message") onMessage?.(event.text);
    else if (event.text && !["queued", "leased", "started", "heartbeat"].includes(event.type)) onTerminal?.(event.text);
  }, signal, pending.after);
}

async function waitForJob(
  linkId: string,
  id: string,
  timeoutMs: number,
  onEvent?: (event: ConnectorJobEvent) => void,
  signal?: AbortSignal,
  initialAfter = 0
): Promise<LocalAgentReply> {
  const deadline = Date.now() + timeoutMs;
  let after = Math.max(0, initialAfter);
  const messages: string[] = [];
  const terminal: string[] = [];
  while (!signal?.aborted && Date.now() < deadline) {
    const result = await requestJson(`/api/connectors/jobs/${encodeURIComponent(id)}/events?after=${after}&wait=1`, {
      headers: { "X-Soty-Link-Id": linkId }
    }, Math.min(35_000, Math.max(1_000, deadline - Date.now())), signal);
    if (!result.ok) {
      if (signal?.aborted) break;
      if (result.error === "job-not-found") return failureReply("Задание больше не существует", 404);
      await delay(400);
      continue;
    }
    for (const value of Array.isArray(result.events) ? result.events : []) {
      const event = readEvent(value);
      if (!event || event.seq <= after) continue;
      after = event.seq;
      onEvent?.(event);
      if (event.type === "message" && event.text) messages.push(event.text);
      else if (event.text && ["terminal", "stdout", "stderr", "error"].includes(event.type)) terminal.push(event.text);
      recordPendingEvent(id, after, event);
    }
    if (result.done === true) {
      clearPendingAgentRelayReply(id);
      const reply = result.job?.result || {};
      const exitCode = typeof reply.exitCode === "number" ? reply.exitCode : result.job?.status === "succeeded" ? 0 : 1;
      return {
        ok: result.job?.status === "succeeded" && exitCode === 0,
        text: String(reply.text || messages.at(-1) || terminal.at(-1) || "").slice(0, 1_000_000),
        exitCode,
        messages: uniqueTail(messages),
        terminal: uniqueTail(terminal),
        ...(typeof reply.sessionId === "string" && reply.sessionId ? { sessionId: reply.sessionId } : {})
      };
    }
  }
  if (signal?.aborted) {
    await cancelConnectorJob(id, linkId).catch(() => false);
    clearPendingAgentRelayReply(id);
    return cancelledReply();
  }
  return failureReply("Soty Agent не завершил задание вовремя", 124);
}

async function checkConnectorStatus(linkId: string, deviceId: string, timeoutMs: number): Promise<LocalAgentStatus> {
  if (!sanitizeLinkId(linkId)) return { ok: false };
  const result = await requestJson(`/api/connectors/status?deviceId=${encodeURIComponent(deviceId)}`, {
    headers: { "X-Soty-Link-Id": linkId }
  }, timeoutMs);
  const device = (Array.isArray(result.devices) ? result.devices : []).find((item: any) => item.connected === true);
  if (!result.ok || !device) return { ok: false };
  const agent = readAgent(device.agent);
  const scopes = Array.isArray(device.scopes) ? device.scopes.filter((item: unknown): item is string => typeof item === "string") : [];
  return {
    ok: true,
    connector: true,
    relay: true,
    managed: true,
    scope: scopes.includes("Machine") ? "Machine" : scopes[0] || "Connector",
    scopes,
    version: typeof device.version === "string" ? device.version : "",
    platform: typeof device.platform === "string" ? device.platform : "",
    deviceId: typeof device.deviceId === "string" ? device.deviceId : "",
    deviceNick: typeof device.deviceNick === "string" ? device.deviceNick : "",
    lastSeenAt: typeof device.lastSeenAt === "string" ? device.lastSeenAt : "",
    system: scopes.includes("Machine"),
    maintenance: scopes.includes("Machine"),
    sourceWorker: true,
    ...(agent ? { agent } : {})
  };
}

async function checkLocalAgentHttp(timeoutMs: number): Promise<LocalAgentStatus> {
  const result = await requestJson("http://127.0.0.1:49424/health?update=1", {
    targetAddressSpace: "loopback"
  } as RequestInit & { readonly targetAddressSpace: "loopback" }, timeoutMs);
  if (!result.ok || result.schema !== "soty.agent-runtime.v1" || result.connector !== true || result.agentRuntime !== true) return { ok: false };
  const agent = readAgent(result.agent);
  return {
    ok: true,
    connector: result.connector === true,
    agentRuntime: result.agentRuntime === true,
    managed: result.managed === true,
    scope: typeof result.scope === "string" ? result.scope : "",
    autoUpdate: result.autoUpdate === true,
    platform: typeof result.platform === "string" ? result.platform : "",
    shell: typeof result.shell === "string" ? result.shell : "",
    version: typeof result.version === "string" ? result.version : "",
    executionPlane: typeof result.executionPlane === "string" ? result.executionPlane : "",
    interactiveTaskBridge: result.interactiveTaskBridge === true,
    companion: result.companion === true,
    sourceWorker: result.sourceWorker === true,
    system: result.system === true || result.scope === "Machine",
    maintenance: result.maintenance === true,
    relay: result.relay === true,
    deviceId: typeof result.deviceId === "string" ? result.deviceId : "",
    deviceNick: typeof result.deviceNick === "string" ? result.deviceNick : "",
    ...(agent ? { agent } : {})
  };
}

export function loadPendingAgentRelayReplies(): LocalAgentPendingRelayReply[] {
  const now = Date.now();
  const value = readPending().filter((item) => item.timeoutAt > now && now - item.createdAt < pendingJobTtlMs);
  writePending(value);
  return value;
}

export function clearPendingAgentRelayReply(id: string): void {
  const clean = cleanId(id);
  if (clean) writePending(readPending().filter((item) => item.id !== clean));
}

export function clearPendingAgentRelayRepliesForTunnel(tunnelId: string): void {
  const clean = String(tunnelId || "").trim();
  if (clean) writePending(readPending().filter((item) => item.tunnelId !== clean));
}

function rememberPending(value: LocalAgentPendingRelayReply): void {
  const item = sanitizePending(value);
  if (!item) return;
  writePending([item, ...readPending().filter((current) => current.id !== item.id)].slice(0, 16));
}

function recordPendingEvent(id: string, after: number, event: ConnectorJobEvent): void {
  const values = readPending();
  const index = values.findIndex((item) => item.id === id);
  const current = values[index];
  if (!current) return;
  const message = event.text.trim().slice(0, 12_000);
  const isMessage = event.type === "message";
  values[index] = {
    ...current,
    after,
    ...(message ? isMessage
      ? { messages: uniqueTail([...current.messages, message]) }
      : { terminal: uniqueTail([...current.terminal, message]) }
      : {})
  };
  writePending(values);
}

function readPending(): LocalAgentPendingRelayReply[] {
  try {
    const value = JSON.parse(localStorage.getItem(pendingJobsStorageKey) || "[]") as unknown;
    return Array.isArray(value) ? value.map(sanitizePending).filter((item): item is LocalAgentPendingRelayReply => Boolean(item)) : [];
  } catch { return []; }
}

function writePending(value: readonly LocalAgentPendingRelayReply[]): void {
  try { localStorage.setItem(pendingJobsStorageKey, JSON.stringify(value.slice(0, 16))); } catch { /* Resume remains optional. */ }
}

function sanitizePending(value: unknown): LocalAgentPendingRelayReply | null {
  const item = value && typeof value === "object" ? value as Partial<LocalAgentPendingRelayReply> : {};
  const relayId = sanitizeLinkId(item.relayId || "");
  const id = cleanId(item.id || "");
  const tunnelId = String(item.tunnelId || "").trim().slice(0, 180);
  if (!relayId || !id || !tunnelId) return null;
  const createdAt = Number.isFinite(item.createdAt) ? Number(item.createdAt) : Date.now();
  return {
    relayId,
    id,
    tunnelId,
    text: String(item.text || "").slice(0, 1_000),
    createdAt,
    timeoutAt: Number.isFinite(item.timeoutAt) ? Number(item.timeoutAt) : createdAt + pendingJobTtlMs,
    after: Math.max(0, Number(item.after) || 0),
    messages: cleanMessages(item.messages),
    terminal: cleanMessages(item.terminal)
  };
}

export function isWindowsPlatform(): boolean {
  const nav = navigator as Navigator & { readonly userAgentData?: { readonly platform?: string } };
  return `${nav.userAgentData?.platform || navigator.platform || navigator.userAgent}`.toLowerCase().includes("win");
}

export function canInstallMachineAgent(): boolean {
  const nav = navigator as Navigator & { readonly userAgentData?: { readonly platform?: string } };
  return !/(iphone|ipad|ipod|android|mobile)/u.test(`${nav.userAgentData?.platform || navigator.platform || navigator.userAgent}`.toLowerCase());
}

export function agentInstallUrl(_scope: "user" | "machine" = "machine"): string {
  return isWindowsPlatform() ? "/agent/install-windows-machine.cmd" : "/agent/install-macos-linux.sh";
}

export function downloadAgentInstaller(scope: "user" | "machine" = "machine"): void {
  downloadAgentInstallerForDevice(scope);
}

export function downloadAgentInstallerForDevice(
  scope: "user" | "machine" = "machine",
  device: { readonly id?: string; readonly nick?: string } = {},
  releaseVersion = ""
): void {
  const linkId = ensureAgentRelayId();
  const base = `${window.location.origin}/agent`;
  if (isWindowsPlatform()) {
    const revision = sanitizeRevision(releaseVersion);
    downloadText(revision ? `install-soty-agent-${revision}.cmd` : "install-soty-agent.cmd", buildWindowsInstaller(base, linkId, device, revision), "application/bat");
    return;
  }
  downloadText("install-soty-agent.sh", buildUnixInstaller(scope, base, linkId), "text/x-shellscript");
}

function buildWindowsInstaller(base: string, linkId: string, device: { readonly id?: string; readonly nick?: string }, revision: string): string {
  const clean = (value: string) => value.replace(/[\r\n"%']/gu, "").slice(0, 160);
  return [
    "@echo off",
    `rem soty-agent-machine-bootstrap:${revision || "current"}`,
    "setlocal",
    `set "SOTY_CONNECTOR_INSTALL_BASE=${base}"`,
    `set "SOTY_CONNECTOR_LINK_ID=${linkId}"`,
    `set "SOTY_CONNECTOR_DEVICE_ID=${clean(device.id || "")}"`,
    `set "SOTY_CONNECTOR_DEVICE_NICK=${clean(device.nick || "")}"`,
    `set "SOTY_CONNECTOR_INSTALLER_REVISION=${revision}"`,
    "powershell.exe -NoLogo -NoProfile -ExecutionPolicy Bypass -Command \"$ErrorActionPreference='Stop'; $dir=Join-Path $env:TEMP 'soty-connector-install'; New-Item -ItemType Directory -Force -Path $dir|Out-Null; $script=Join-Path $dir 'install-windows-machine-bootstrap.ps1'; $uri=$env:SOTY_CONNECTOR_INSTALL_BASE.TrimEnd('/')+'/install-windows-machine-bootstrap.ps1'; Invoke-WebRequest -Uri $uri -UseBasicParsing -OutFile $script -TimeoutSec 45; & powershell.exe -NoLogo -NoProfile -ExecutionPolicy Bypass -File $script -Base $env:SOTY_CONNECTOR_INSTALL_BASE -Revision $env:SOTY_CONNECTOR_INSTALLER_REVISION -RelayId $env:SOTY_CONNECTOR_LINK_ID -DeviceId $env:SOTY_CONNECTOR_DEVICE_ID -DeviceNick $env:SOTY_CONNECTOR_DEVICE_NICK; exit $LASTEXITCODE\"",
    "if errorlevel 1 goto fail",
    "exit /b 0",
    ":fail",
    "echo Soty Agent installation failed",
    "echo %ProgramData%\\soty-agent\\install.log",
    "pause",
    "exit /b 1",
    ""
  ].join("\r\n");
}

function buildUnixInstaller(scope: "user" | "machine", base: string, linkId: string): string {
  return [
    "#!/usr/bin/env sh",
    "set -eu",
    `BASE="${shellEscape(base)}"`,
    `LINK="${shellEscape(linkId)}"`,
    "DIR=\"${TMPDIR:-/tmp}/soty-connector-install\"",
    "mkdir -p \"$DIR\"",
    "if command -v curl >/dev/null 2>&1; then curl -fsSL --retry 3 \"$BASE/install-macos-linux.sh\" -o \"$DIR/install.sh\"; else wget -qO \"$DIR/install.sh\" \"$BASE/install-macos-linux.sh\"; fi",
    `sh "$DIR/install.sh" --scope ${scope} --base "$BASE" --relay-id "$LINK"`,
    ""
  ].join("\n");
}

async function requestJson(path: string, init: RequestInit, timeoutMs: number, externalSignal?: AbortSignal): Promise<any> {
  const controller = new AbortController();
  const abort = () => controller.abort();
  externalSignal?.addEventListener("abort", abort, { once: true });
  const timer = window.setTimeout(abort, Math.max(200, timeoutMs));
  try {
    const response = await fetch(path, { cache: "no-store", ...init, signal: controller.signal });
    const value = await response.json().catch(() => ({}));
    return { ...value, ok: response.ok && value.ok !== false, ...(response.ok ? {} : { error: value.error || `http-${response.status}` }) };
  } catch (error) {
    return { ok: false, error: externalSignal?.aborted ? "cancelled" : error instanceof DOMException && error.name === "AbortError" ? "timeout" : "network" };
  } finally {
    window.clearTimeout(timer);
    externalSignal?.removeEventListener("abort", abort);
  }
}

function readAgent(value: any): SotyAgentStatus | undefined {
  if (value?.id !== "opencode" || value?.provider !== "gonka") return undefined;
  return {
    id: "opencode",
    name: "OpenCode",
    provider: "gonka",
    model: typeof value.model === "string" ? value.model : "",
    available: value.available === true,
    version: typeof value.version === "string" ? value.version : "",
    reason: typeof value.reason === "string" ? value.reason : "",
    capabilities: Array.isArray(value.capabilities) ? value.capabilities.filter((part: unknown): part is string => typeof part === "string") : []
  };
}

function readEvent(value: any): ConnectorJobEvent | null {
  if (!Number.isSafeInteger(value?.seq) || typeof value?.type !== "string") return null;
  return { seq: value.seq, type: value.type, text: typeof value.text === "string" ? value.text : "", at: typeof value.at === "string" ? value.at : "", ...(value.data && typeof value.data === "object" ? { data: value.data } : {}) };
}

function failureReply(text: string, exitCode: number): LocalAgentReply {
  return { ok: false, text: `! agent: ${text}`, exitCode };
}

function cancelledReply(): LocalAgentReply {
  return { ok: false, text: "! cancelled", exitCode: 130 };
}

function readLinkId(): string {
  try {
    const current = sanitizeLinkId(localStorage.getItem(linkStorageKey) || "");
    if (current) return current;
    const previous = sanitizeLinkId(localStorage.getItem(previousLinkStorageKey) || "");
    if (previous) writeLinkId(previous);
    return previous;
  } catch { return ""; }
}

function writeLinkId(value: string): void {
  localStorage.setItem(linkStorageKey, value);
  localStorage.removeItem(previousLinkStorageKey);
}

function sanitizeLinkId(value: string): string {
  const text = String(value || "").trim();
  return /^[A-Za-z0-9_-]{32,192}$/u.test(text) ? text : "";
}

function cleanId(value: string): string {
  const text = String(value || "").trim().slice(0, 180);
  return /^[A-Za-z0-9][A-Za-z0-9_.:-]*$/u.test(text) ? text : "";
}

function cleanMessages(value: readonly unknown[] | undefined): string[] {
  return uniqueTail((Array.isArray(value) ? value : []).filter((item): item is string => typeof item === "string").map((item) => item.slice(0, 12_000)));
}

function uniqueTail(value: readonly string[]): string[] {
  return [...new Set(value.filter(Boolean))].slice(-maxMessages);
}

function sanitizeRevision(value: string): string {
  return value.replace(/[^A-Za-z0-9._-]/gu, "").slice(0, 40);
}

function shellEscape(value: string): string {
  return value.replace(/["\\$`]/gu, "\\$&");
}

function downloadText(filename: string, content: string, type: string): void {
  const url = URL.createObjectURL(new Blob([content], { type }));
  const link = document.createElement("a");
  link.href = url;
  link.download = filename;
  document.body.append(link);
  link.click();
  link.remove();
  window.setTimeout(() => URL.revokeObjectURL(url), 1_000);
}

function delay(ms: number): Promise<void> {
  return new Promise((resolve) => window.setTimeout(resolve, ms));
}
