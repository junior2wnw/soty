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

export interface LocalAgentDeviceNetwork {
  readonly protocol: "soty-device-network.v1";
  readonly controllerDeviceId: string;
  readonly controllerDeviceNick: string;
  readonly activeTunnelId: string;
  readonly activeTunnelLabel: string;
  readonly activeTunnelKind: "agent" | "peer";
  readonly selectedTargetId: string;
  readonly selectedTargetLabel: string;
  readonly selectedTargetDeviceId: string;
  readonly selectedTargetAccess: boolean;
  readonly selectedTargetLink: boolean;
  readonly capabilities: readonly string[];
  readonly targets: readonly LocalAgentOperatorTarget[];
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
  readonly operatorTargets?: readonly LocalAgentOperatorTarget[];
  readonly deviceNetwork?: LocalAgentDeviceNetwork;
}

const relayStorageKey = "soty:agent:relay-id";
const pendingRelayRepliesStorageKey = "soty:agent:pending-relay-replies:v1";
const relayParamNames = ["agent", "agentRelay", "agentRelayId"];
const maxCodexDialogMessages = 64;
const localAgentBlockedText = "! agent-relay: agent bridge is not connected";
const pendingRelayReplyTtlMs = 2 * 60 * 60_000 + 30 * 60_000;

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
  onTerminal?: LocalAgentMessageHandler,
  signal?: AbortSignal
): Promise<LocalAgentReply> {
  if (signal?.aborted) {
    return cancelledAgentReply();
  }
  const relayId = readAgentRelayId() || ensureAgentRelayId();
  if (relayId) {
    const relay = await askAgentRelayReply(text, context, source, timeoutMs, onMessage, onTerminal, true, signal);
    if (relay) {
      return relay;
    }
  }

  return {
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
        readonly workers?: {
          readonly user?: { readonly localAgent?: unknown };
          readonly system?: { readonly localAgent?: unknown };
        };
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

export async function checkAgentSourceMachineAgent(deviceId: string, timeoutMs = 1500): Promise<LocalAgentStatus> {
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
        readonly workers?: {
          readonly system?: { readonly localAgent?: unknown };
          readonly user?: { readonly localAgent?: unknown };
        };
      }[];
    };
    const target = (payload.targets || []).find((item) => item.hostDeviceId === deviceId || item.deviceIds?.includes(deviceId));
    const systemWorker = readLocalAgentStatus(target?.workers?.system?.localAgent);
    const targetAgent = readLocalAgentStatus(target?.localAgent);
    const machine = systemWorker || (targetAgent?.system === true ? targetAgent : null);
    return machine ? { ...machine, relay: true } : { ok: false };
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

async function askAgentRelayReply(
  text: string,
  context: string,
  source: LocalAgentRequestSource,
  timeoutMs: number,
  onMessage?: LocalAgentMessageHandler,
  onTerminal?: LocalAgentMessageHandler,
  preferServer = false,
  signal?: AbortSignal
): Promise<LocalAgentReply | null> {
  const relayId = readAgentRelayId() || ensureAgentRelayId();
  if (!relayId) {
    return null;
  }
  if (signal?.aborted) {
    return cancelledAgentReply();
  }
  try {
    const requestTimeout = timeoutAbortSignal(30_000, signal);
    let request: Response;
    try {
      request = await fetch("/api/agent/relay/request", {
        method: "POST",
        cache: "no-store",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({ relayId, text, context, source, ...(preferServer ? { preferServer: true } : {}) }),
        signal: requestTimeout.signal
      });
    } finally {
      requestTimeout.cleanup();
    }
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
    const createdId = created.id;
    rememberPendingAgentRelayReply({
      relayId,
      id: createdId,
      tunnelId: source.tunnelId || "",
      text,
      createdAt: Date.now(),
      timeoutAt: Date.now() + timeoutMs,
      after: 0,
      messages: [],
      terminal: []
    });
    let stopEvents = false;
    let cancelSent = false;
    const cancelRelay = () => {
      stopEvents = true;
      cancelSent = true;
      void cancelAgentRelayReply(relayId, createdId).catch(() => undefined);
    };
    if (signal?.aborted) {
      await cancelAgentRelayReply(relayId, createdId).catch(() => undefined);
      return cancelledAgentReply();
    }
    signal?.addEventListener("abort", cancelRelay, { once: true });
    const eventStream = onMessage || onTerminal
      ? watchAgentRelayEvents(relayId, createdId, timeoutMs, onMessage, onTerminal, () => stopEvents, signal)
      : Promise.resolve();
    try {
      const reply = await waitForAgentRelayReply(relayId, createdId, timeoutMs, signal);
      stopEvents = true;
      void eventStream.catch(() => undefined);
      if (signal?.aborted) {
        if (!cancelSent) {
          await cancelAgentRelayReply(relayId, createdId).catch(() => undefined);
        }
        return cancelledAgentReply();
      }
      clearPendingAgentRelayReply(createdId);
      return reply || relayFailure("! agent-relay: server Codex executor did not pick up the request", 124);
    } finally {
      stopEvents = true;
      signal?.removeEventListener("abort", cancelRelay);
    }
  } catch {
    if (signal?.aborted) {
      return cancelledAgentReply();
    }
    return relayFailure("! agent-relay: browser could not reach relay", 127);
  }
}

export async function resumeAgentRelayReply(
  pending: LocalAgentPendingRelayReply,
  onMessage?: LocalAgentMessageHandler,
  onTerminal?: LocalAgentMessageHandler,
  signal?: AbortSignal
): Promise<LocalAgentReply> {
  const relayId = sanitizeRelayId(pending.relayId);
  const id = cleanPendingRelayReplyId(pending.id);
  if (!relayId || !id) {
    return relayFailure("! agent-relay: pending reply is invalid", 400);
  }
  if (signal?.aborted) {
    return cancelledAgentReply();
  }
  const timeoutMs = Math.max(1000, Math.min(pendingRelayReplyTtlMs, pending.timeoutAt - Date.now()));
  let stopEvents = false;
  let cancelSent = false;
  const cancelRelay = () => {
    stopEvents = true;
    cancelSent = true;
    clearPendingAgentRelayReply(id);
    void cancelAgentRelayReply(relayId, id).catch(() => undefined);
  };
  signal?.addEventListener("abort", cancelRelay, { once: true });
  const eventStream = onMessage || onTerminal
    ? watchAgentRelayEvents(relayId, id, timeoutMs, onMessage, onTerminal, () => stopEvents, signal, pending.after || 0)
    : Promise.resolve();
  try {
    const reply = await waitForAgentRelayReply(relayId, id, timeoutMs, signal);
    stopEvents = true;
    void eventStream.catch(() => undefined);
    if (signal?.aborted) {
      if (!cancelSent) {
        clearPendingAgentRelayReply(id);
        await cancelAgentRelayReply(relayId, id).catch(() => undefined);
      }
      return cancelledAgentReply();
    }
    clearPendingAgentRelayReply(id);
    return reply || relayFailure("! agent-relay: running reply did not finish", 124);
  } finally {
    stopEvents = true;
    signal?.removeEventListener("abort", cancelRelay);
  }
}

async function watchAgentRelayEvents(
  relayId: string,
  id: string,
  timeoutMs: number,
  onMessage: LocalAgentMessageHandler | undefined,
  onTerminal: LocalAgentMessageHandler | undefined,
  stopped: () => boolean,
  signal?: AbortSignal,
  initialAfter = 0
): Promise<void> {
  const deadline = Date.now() + timeoutMs;
  let after = Math.max(0, initialAfter);
  while (!stopped() && !signal?.aborted && Date.now() < deadline) {
    const remaining = deadline - Date.now();
    const timeout = timeoutAbortSignal(Math.min(35_000, Math.max(1000, remaining)), signal);
    try {
      const url = `/api/agent/relay/events?relayId=${encodeURIComponent(relayId)}&id=${encodeURIComponent(id)}&after=${after}&wait=1`;
      const response = await fetch(url, { cache: "no-store", signal: timeout.signal });
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
          recordPendingAgentRelayEvent(id, seq, "agent_terminal", event.text);
        } else if (type === "agent_message" && typeof event.text === "string" && event.text.trim() && onMessage) {
          onMessage(event.text);
          recordPendingAgentRelayEvent(id, seq, "agent_message", event.text);
        } else {
          updatePendingAgentRelayReply(id, { after: seq });
        }
      }
      if (payload.done) {
        return;
      }
    } catch {
      // Keep the final reply poll authoritative; streaming is best-effort.
    } finally {
      timeout.cleanup();
    }
  }
}

async function waitForAgentRelayReply(
  relayId: string,
  id: string,
  timeoutMs: number,
  signal?: AbortSignal
): Promise<LocalAgentReply | null> {
  const deadline = Date.now() + timeoutMs;
  while (!signal?.aborted && Date.now() < deadline) {
    const remaining = deadline - Date.now();
    const timeout = timeoutAbortSignal(Math.min(35_000, Math.max(1000, remaining)), signal);
    try {
      const url = `/api/agent/relay/reply?relayId=${encodeURIComponent(relayId)}&id=${encodeURIComponent(id)}&wait=1`;
      const response = await fetch(url, { cache: "no-store", signal: timeout.signal });
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
      timeout.cleanup();
    }
  }
  if (signal?.aborted) {
    return cancelledAgentReply();
  }
  return null;
}

function relayFailure(text: string, exitCode: number): LocalAgentReply {
  return { ok: false, text, exitCode };
}

function cancelledAgentReply(): LocalAgentReply {
  return { ok: false, text: "! cancelled", exitCode: 130 };
}

function timeoutAbortSignal(timeoutMs: number, externalSignal?: AbortSignal): { readonly signal: AbortSignal; readonly cleanup: () => void } {
  const controller = new AbortController();
  const abort = () => controller.abort();
  if (externalSignal?.aborted) {
    controller.abort();
  } else {
    externalSignal?.addEventListener("abort", abort, { once: true });
  }
  const timer = window.setTimeout(() => controller.abort(), Math.max(1000, timeoutMs));
  return {
    signal: controller.signal,
    cleanup: () => {
      window.clearTimeout(timer);
      externalSignal?.removeEventListener("abort", abort);
    }
  };
}

async function cancelAgentRelayReply(relayId: string, id: string): Promise<void> {
  const timeout = timeoutAbortSignal(2500);
  try {
    await fetch("/api/agent/relay/cancel", {
      method: "POST",
      cache: "no-store",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({ relayId, id }),
      signal: timeout.signal
    });
  } finally {
    timeout.cleanup();
  }
}

export function loadPendingAgentRelayReplies(): LocalAgentPendingRelayReply[] {
  const now = Date.now();
  const replies = readPendingAgentRelayReplies()
    .filter((reply) => reply.id && reply.relayId && reply.tunnelId && reply.timeoutAt > now)
    .filter((reply) => now - reply.createdAt < pendingRelayReplyTtlMs)
    .sort((left, right) => right.createdAt - left.createdAt);
  writePendingAgentRelayReplies(replies);
  return replies;
}

export function clearPendingAgentRelayReply(id: string): void {
  const cleanId = cleanPendingRelayReplyId(id);
  if (!cleanId) {
    return;
  }
  writePendingAgentRelayReplies(readPendingAgentRelayReplies().filter((reply) => reply.id !== cleanId));
}

export function clearPendingAgentRelayRepliesForTunnel(tunnelId: string): void {
  const cleanTunnelId = String(tunnelId || "").trim();
  if (!cleanTunnelId) {
    return;
  }
  writePendingAgentRelayReplies(readPendingAgentRelayReplies().filter((reply) => reply.tunnelId !== cleanTunnelId));
}

function rememberPendingAgentRelayReply(reply: LocalAgentPendingRelayReply): void {
  const clean = sanitizePendingAgentRelayReply(reply);
  if (!clean) {
    return;
  }
  const replies = readPendingAgentRelayReplies().filter((item) => item.id !== clean.id);
  writePendingAgentRelayReplies([clean, ...replies].slice(0, 8));
}

function recordPendingAgentRelayEvent(id: string, after: number, type: string, text: string): void {
  const cleanText = String(text || "").replace(/\r\n?/gu, "\n").trim().slice(0, 12_000);
  if (!cleanText) {
    updatePendingAgentRelayReply(id, { after });
    return;
  }
  const replies = readPendingAgentRelayReplies();
  const index = replies.findIndex((reply) => reply.id === cleanPendingRelayReplyId(id));
  if (index < 0) {
    return;
  }
  const current = replies[index];
  if (!current) {
    return;
  }
  const terminal = type === "agent_terminal";
  const values = [...new Set([...(terminal ? current.terminal : current.messages), cleanText])].slice(-maxCodexDialogMessages);
  const next: LocalAgentPendingRelayReply = {
    ...current,
    after: Math.max(current.after || 0, after),
    ...(terminal ? { terminal: values } : { messages: values })
  };
  replies[index] = next;
  writePendingAgentRelayReplies(replies);
}

function updatePendingAgentRelayReply(id: string, patch: Partial<LocalAgentPendingRelayReply>): void {
  const cleanId = cleanPendingRelayReplyId(id);
  if (!cleanId) {
    return;
  }
  const replies = readPendingAgentRelayReplies();
  const index = replies.findIndex((reply) => reply.id === cleanId);
  if (index < 0) {
    return;
  }
  const current = replies[index];
  if (!current) {
    return;
  }
  replies[index] = sanitizePendingAgentRelayReply({ ...current, ...patch }) || current;
  writePendingAgentRelayReplies(replies);
}

function readPendingAgentRelayReplies(): LocalAgentPendingRelayReply[] {
  try {
    const raw = JSON.parse(localStorage.getItem(pendingRelayRepliesStorageKey) || "[]") as unknown;
    if (!Array.isArray(raw)) {
      return [];
    }
    return raw
      .map((item) => sanitizePendingAgentRelayReply(item))
      .filter((item): item is LocalAgentPendingRelayReply => Boolean(item));
  } catch {
    return [];
  }
}

function writePendingAgentRelayReplies(replies: readonly LocalAgentPendingRelayReply[]): void {
  try {
    localStorage.setItem(pendingRelayRepliesStorageKey, JSON.stringify(replies.slice(0, 8)));
  } catch {
    // Resume is a convenience layer; the relay job itself keeps running without this cache.
  }
}

function sanitizePendingAgentRelayReply(value: unknown): LocalAgentPendingRelayReply | null {
  const record = value && typeof value === "object" ? value as Partial<LocalAgentPendingRelayReply> : {};
  const relayId = sanitizeRelayId(record.relayId || "");
  const id = cleanPendingRelayReplyId(record.id || "");
  const tunnelId = String(record.tunnelId || "").trim().slice(0, 180);
  if (!relayId || !id || !tunnelId) {
    return null;
  }
  const createdAt = Number.isFinite(record.createdAt) ? Number(record.createdAt) : Date.now();
  const timeoutAt = Number.isFinite(record.timeoutAt) ? Number(record.timeoutAt) : createdAt + 2 * 60 * 60_000;
  const cleanMessages = (items: readonly unknown[] | undefined) => Array.isArray(items)
    ? items
      .filter((item): item is string => typeof item === "string")
      .map((item) => item.replace(/\r\n?/gu, "\n").trim().slice(0, 12_000))
      .filter(Boolean)
      .slice(-maxCodexDialogMessages)
    : [];
  return {
    relayId,
    id,
    tunnelId,
    text: String(record.text || "").trim().slice(0, 1000),
    createdAt,
    timeoutAt,
    after: Math.max(0, Number.isFinite(record.after) ? Number(record.after) : 0),
    messages: cleanMessages(record.messages),
    terminal: cleanMessages(record.terminal)
  };
}

function cleanPendingRelayReplyId(value: string): string {
  const text = String(value || "").trim().slice(0, 120);
  return /^[A-Za-z0-9_.:-]{4,120}$/u.test(text) ? text : "";
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
  device: { readonly id?: string; readonly nick?: string } = {},
  releaseVersion = ""
): void {
  const relayId = ensureAgentRelayId();
  const base = `${window.location.origin}/agent`;
  if (isWindowsPlatform()) {
    const revision = sanitizeInstallerRevision(releaseVersion);
    downloadText(
      revision ? `install-soty-agent-machine-${revision}.cmd` : "install-soty-agent-machine.cmd",
      buildWindowsInstaller(base, relayId, device, revision),
      "application/bat"
    );
    return;
  }
  downloadText("install-soty-agent.sh", buildUnixInstaller(scope, base, relayId), "text/x-shellscript");
}

function buildWindowsInstaller(
  base: string,
  relayId: string,
  device: { readonly id?: string; readonly nick?: string } = {},
  revision = ""
): string {
  const deviceId = sanitizeWindowsCmdValue(device.id || "");
  const deviceNick = sanitizeWindowsCmdValue(device.nick || "");
  const installerRevision = sanitizeWindowsCmdValue(revision);
  const installerQuery = installerRevision ? "?v=%INSTALLER_REVISION%" : "";
  return [
    "@echo off",
    `rem soty-agent-machine-bootstrap:${installerRevision || "unknown"}`,
    "setlocal",
    `set "BASE=${base}"`,
    `set "RELAY=${relayId}"`,
    `set "DEVICE=${deviceId}"`,
    `set "NICK=${deviceNick}"`,
    `set "INSTALLER_REVISION=${installerRevision}"`,
    "",
    "echo Downloading Soty Agent installer %INSTALLER_REVISION%...",
    `powershell.exe -NoLogo -NoProfile -ExecutionPolicy Bypass -Command "$ErrorActionPreference='Stop'; [Net.ServicePointManager]::SecurityProtocol = [Net.SecurityProtocolType]::Tls12; $dir = Join-Path $env:TEMP 'soty-agent-machine'; New-Item -ItemType Directory -Force -Path $dir | Out-Null; $bootstrap = Join-Path $dir 'install-windows-machine-bootstrap.ps1'; $log = Join-Path $dir 'bootstrap.log'; 'soty-agent-machine:bootstrap-download:%INSTALLER_REVISION%' | Out-File -LiteralPath $log -Encoding ASCII; Invoke-WebRequest -Uri '%BASE%/install-windows-machine-bootstrap.ps1${installerQuery}' -UseBasicParsing -OutFile $bootstrap -TimeoutSec 45 -ErrorAction Stop; & powershell.exe -NoLogo -NoProfile -ExecutionPolicy Bypass -File $bootstrap -Base '%BASE%' -Revision '%INSTALLER_REVISION%' -RelayId '%RELAY%' -DeviceId '%DEVICE%' -DeviceNick '%NICK%'; exit $LASTEXITCODE"`,
    "if errorlevel 1 goto fail",
    "exit /b 0",
    "",
    ":fail",
    "echo.",
    "echo soty-agent machine install failed",
    "echo %ProgramData%\\soty-agent\\install.log",
    "echo %TEMP%\\soty-agent-machine\\bootstrap.log",
    "echo %ProgramData%\\Soty\\agent-install\\bootstrap-elevated.log",
    "echo.",
    "if exist \"%TEMP%\\soty-agent-machine\\bootstrap.log\" (",
    "  echo --- bootstrap.log ---",
    "  type \"%TEMP%\\soty-agent-machine\\bootstrap.log\"",
    ")",
    "if exist \"%ProgramData%\\Soty\\agent-install\\bootstrap-elevated.log\" (",
    "  echo.",
    "  echo --- bootstrap-elevated.log ---",
    "  type \"%ProgramData%\\Soty\\agent-install\\bootstrap-elevated.log\"",
    ")",
    "if exist \"%ProgramData%\\soty-agent\\install.log\" (",
    "  echo.",
    "  echo --- install.log tail ---",
    "  powershell.exe -NoLogo -NoProfile -ExecutionPolicy Bypass -Command \"Get-Content -LiteralPath '%ProgramData%\\soty-agent\\install.log' -Tail 80\"",
    ")",
    "if exist \"%ProgramData%\\soty-agent\\node-probe.err.log\" (",
    "  echo.",
    "  echo --- node-probe.err.log ---",
    "  type \"%ProgramData%\\soty-agent\\node-probe.err.log\"",
    ")",
    "if exist \"%ProgramData%\\soty-agent\\start-agent.status.log\" (",
    "  echo.",
    "  echo --- start-agent.status.log ---",
    "  powershell.exe -NoLogo -NoProfile -ExecutionPolicy Bypass -Command \"Get-Content -LiteralPath '%ProgramData%\\soty-agent\\start-agent.status.log' -Tail 40\"",
    ")",
    "if exist \"%ProgramData%\\soty-agent\\start-agent.err.log\" (",
    "  echo.",
    "  echo --- start-agent.err.log ---",
    "  powershell.exe -NoLogo -NoProfile -ExecutionPolicy Bypass -Command \"Get-Content -LiteralPath '%ProgramData%\\soty-agent\\start-agent.err.log' -Tail 80\"",
    ")",
    "pause",
    "exit /b 1",
    ""
  ].join("\r\n");
}

function sanitizeWindowsCmdValue(value: string): string {
  return value.replace(/[\r\n"%']/gu, "").slice(0, 160);
}

function sanitizeInstallerRevision(value: string): string {
  return value.replace(/[^A-Za-z0-9._-]/gu, "").slice(0, 40);
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
