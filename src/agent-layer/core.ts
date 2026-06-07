export const agentLayerSchema = "soty.agent-layer.v1";
export const agentRunEnvelopeSchema = "soty.agent-run-envelope.v1";
export const agentActionCardSchema = "soty.action-card.v2";

export type AgentSurface = "soty" | "personal-card" | "miniapp" | "admin" | "api";
export type AgentProviderKind = "soty-codex" | "custom-http" | "openai-compatible" | "local" | "pochinit";
export type AgentRisk = "low" | "medium" | "high";
export type AgentTargetKind =
  | "browser"
  | "server"
  | "ssh"
  | "windows-device"
  | "mac-device"
  | "linux-device"
  | "container"
  | "miniapp"
  | "unknown";
export type AgentOperatingSystem = "windows" | "macos" | "linux" | "unknown";
export type AgentShell = "powershell" | "cmd" | "bash" | "zsh" | "sh" | "unknown";

export interface AgentTargetEnvelope {
  readonly kind: AgentTargetKind;
  readonly os: AgentOperatingSystem;
  readonly shell: AgentShell;
  readonly hostId: string;
  readonly workspace: string;
  readonly label: string;
}

export interface AgentStyleConfig {
  readonly themeId: string;
  readonly density: "compact" | "comfortable";
  readonly accent: string;
}

export interface AgentRunEnvelope {
  readonly schema: typeof agentRunEnvelopeSchema;
  readonly v: 1;
  readonly runId: string;
  readonly createdAt: string;
  readonly userId: string;
  readonly projectId: string;
  readonly surface: AgentSurface;
  readonly provider: AgentProviderKind;
  readonly target: AgentTargetEnvelope;
  readonly intent: string;
  readonly capabilities: readonly string[];
  readonly risk: AgentRisk;
  readonly requiresApproval: boolean;
  readonly timeoutMs: number;
  readonly idempotencyKey: string;
  readonly proof: readonly string[];
  readonly style: AgentStyleConfig;
  readonly context: Readonly<Record<string, string>>;
}

export interface CreateAgentRunEnvelopeInput {
  readonly userId: string;
  readonly projectId?: string;
  readonly surface: AgentSurface;
  readonly provider: AgentProviderKind;
  readonly target?: Partial<AgentTargetEnvelope>;
  readonly intent: string;
  readonly capabilities?: readonly string[];
  readonly risk?: AgentRisk;
  readonly requiresApproval?: boolean;
  readonly timeoutMs?: number;
  readonly idempotencyKey?: string;
  readonly proof?: readonly string[];
  readonly style?: Partial<AgentStyleConfig>;
  readonly context?: Readonly<Record<string, string | number | boolean | null | undefined>>;
}

export interface AgentActionCard {
  readonly schema: typeof agentActionCardSchema;
  readonly id: string;
  readonly title: string;
  readonly intent: string;
  readonly targetPolicy: string;
  readonly firstMoves: readonly string[];
  readonly confirmBefore: readonly string[];
  readonly successProof: readonly string[];
  readonly avoid: readonly string[];
  readonly risk: AgentRisk;
  readonly requiresApproval: boolean;
  readonly runtime: Readonly<Record<string, unknown>>;
}

export interface AgentActionSource {
  readonly id: string;
  readonly title: string;
  readonly summary?: string;
  readonly tags?: readonly string[];
  readonly source?: string;
  readonly kind?: string;
  readonly runtime?: Readonly<Record<string, unknown>>;
}

export function createAgentRunEnvelope(input: CreateAgentRunEnvelopeInput): AgentRunEnvelope {
  const risk = normalizeRisk(input.risk);
  const runId = cleanToken(input.idempotencyKey || `run-${Date.now()}-${Math.random().toString(36).slice(2)}`, 160);
  const target = normalizeTargetEnvelope(input.target);
  const intent = cleanText(input.intent, 4000);
  const userId = cleanToken(input.userId, 160) || "anonymous";
  const projectId = cleanToken(input.projectId || "", 160);
  const capabilities = cleanStringList(input.capabilities || [], 80, 80);
  const proof = cleanStringList(input.proof || defaultProofForRisk(risk), 12, 120);
  const style = normalizeStyleConfig(input.style);
  const context = cleanContext(input.context || {});
  const idempotencyKey = cleanToken(input.idempotencyKey || stableKey([
    userId,
    projectId,
    input.surface,
    input.provider,
    target.kind,
    target.hostId,
    target.workspace,
    intent
  ]), 180);

  return {
    schema: agentRunEnvelopeSchema,
    v: 1,
    runId,
    createdAt: new Date().toISOString(),
    userId,
    projectId,
    surface: input.surface,
    provider: input.provider,
    target,
    intent,
    capabilities,
    risk,
    requiresApproval: input.requiresApproval ?? risk !== "low",
    timeoutMs: clampInt(input.timeoutMs ?? 2 * 60 * 60_000, 1000, 24 * 60 * 60_000),
    idempotencyKey,
    proof,
    style,
    context
  };
}

export function validateAgentRunEnvelope(envelope: AgentRunEnvelope): readonly string[] {
  const errors: string[] = [];
  if (envelope.schema !== agentRunEnvelopeSchema || envelope.v !== 1) {
    errors.push("Envelope schema is invalid.");
  }
  if (!envelope.userId) {
    errors.push("User identity is missing.");
  }
  if (!envelope.intent) {
    errors.push("Intent is empty.");
  }
  if (envelope.target.kind === "unknown" && envelope.risk !== "low") {
    errors.push("Target must be selected before medium or high risk work.");
  }
  if ((envelope.target.kind.includes("device") || envelope.target.kind === "ssh" || envelope.target.kind === "server") && envelope.target.os === "unknown" && envelope.risk !== "low") {
    errors.push("Operating system must be known before state-changing work.");
  }
  if (envelope.target.os !== "unknown" && envelope.target.shell === "unknown" && envelope.risk !== "low") {
    errors.push("Shell must be known before state-changing work.");
  }
  if (!envelope.idempotencyKey) {
    errors.push("Idempotency key is missing.");
  }
  if (envelope.timeoutMs < 1000) {
    errors.push("Timeout is too short.");
  }
  return errors;
}

export function createAgentActionCard(action: AgentActionSource): AgentActionCard {
  const runtime = cleanRuntimeObject(action.runtime || {});
  const runtimeRisk = typeof runtime.risk === "string" ? runtime.risk : "";
  const requiresConfirmation = runtime.requiresConfirmation === true || runtime.requiresApproval === true;
  const id = cleanToken(action.id, 160) || "action";
  const title = cleanText(action.title, 140) || id;
  const risk = normalizeRisk(runtimeRisk || inferredRisk(id, title, action.kind || ""));
  const firstMoves = preferredList(runtime.phases, runtime.actions, [
    "Resolve the exact target envelope.",
    "Run the smallest read-only preflight.",
    "Execute through the declared capability adapter.",
    "Verify proof before final reply."
  ]);
  const successProof = preferredList(runtime.proof, runtime.successProof, [
    "Final status is explicit.",
    "Observable state or artifact is attached.",
    "No duplicate long-running job was started."
  ]);
  return {
    schema: agentActionCardSchema,
    id,
    title,
    intent: cleanText(action.summary || title, 360),
    targetPolicy: cleanText(stringRuntime(runtime.targetPolicy) || defaultTargetPolicy(action.kind || id), 240),
    firstMoves,
    confirmBefore: preferredList(runtime.confirmBefore, null, defaultConfirmBefore(risk, requiresConfirmation)),
    successProof,
    avoid: preferredList(runtime.avoid, null, [
      "Do not guess OS, shell, host, or workspace for state-changing work.",
      "Do not reuse stale device context.",
      "Do not start duplicate durable jobs after timeout.",
      "Do not treat command exit alone as user-visible proof."
    ]),
    risk,
    requiresApproval: requiresConfirmation || risk !== "low",
    runtime
  };
}

export function serializeAgentActionPrompt(
  card: AgentActionCard,
  userComment: string,
  contextLabel: string
): string {
  return [
    `Action: ${card.title}`,
    `User comment: ${cleanText(userComment, 1200) || "(none)"}`,
    `Current context: ${cleanText(contextLabel, 240) || "(unknown)"}`,
    "",
    "SOTY_ACTION_CARD:",
    JSON.stringify(card),
    "",
    "Use this as the private action contract. Before any OS, shell, SSH, container, browser, or device operation, resolve the target envelope: host, OS, shell, workspace, permissions, risk, timeout, idempotency, rollback, and success proof.",
    "For low-risk read-only work, gather compact proof and answer. For state-changing work, stop for confirmation when the card or fresh facts require it. Prefer durable idempotent actions for long jobs."
  ].join("\n");
}

export function serializeAgentEnvelopeForPrompt(envelope: AgentRunEnvelope): string {
  return [
    "SOTY_AGENT_RUN_ENVELOPE:",
    JSON.stringify(envelope),
    "",
    "Preflight contract: never assume Mac/Windows/Linux, shell dialect, host, current directory, device access, or admin permission. Resolve the target first, run the smallest safe probe, execute through declared capabilities, and return proof."
  ].join("\n");
}

export function normalizeRisk(value: unknown): AgentRisk {
  return value === "high" || value === "medium" || value === "low" ? value : "low";
}

export function cleanText(value: unknown, maxLength: number): string {
  return String(value || "")
    .replace(/[\u0000-\u001F\u007F]/gu, " ")
    .replace(/\s+/gu, " ")
    .trim()
    .slice(0, maxLength);
}

export function cleanToken(value: unknown, maxLength: number): string {
  return cleanText(value, maxLength)
    .replace(/[^A-Za-z0-9._:@/-]+/gu, "-")
    .replace(/-+/gu, "-")
    .replace(/^-|-$/gu, "")
    .slice(0, maxLength);
}

export function cleanStringList(value: readonly unknown[], maxItems: number, maxLength: number): readonly string[] {
  return value
    .map((item) => cleanText(item, maxLength))
    .filter(Boolean)
    .slice(0, maxItems);
}

export function stableKey(parts: readonly unknown[]): string {
  const text = parts.map((part) => cleanText(part, 400)).join("|");
  let hash = 2166136261;
  for (let index = 0; index < text.length; index += 1) {
    hash ^= text.charCodeAt(index);
    hash = Math.imul(hash, 16777619);
  }
  return `agent-${(hash >>> 0).toString(36)}`;
}

function normalizeTargetEnvelope(input: Partial<AgentTargetEnvelope> | undefined): AgentTargetEnvelope {
  const kind = normalizeTargetKind(input?.kind);
  const os = normalizeOperatingSystem(input?.os);
  return {
    kind,
    os,
    shell: normalizeShell(input?.shell, os),
    hostId: cleanToken(input?.hostId || "", 180),
    workspace: cleanText(input?.workspace || "", 260),
    label: cleanText(input?.label || "", 120)
  };
}

function normalizeTargetKind(value: unknown): AgentTargetKind {
  const allowed: readonly AgentTargetKind[] = ["browser", "server", "ssh", "windows-device", "mac-device", "linux-device", "container", "miniapp", "unknown"];
  return allowed.includes(value as AgentTargetKind) ? value as AgentTargetKind : "unknown";
}

function normalizeOperatingSystem(value: unknown): AgentOperatingSystem {
  return value === "windows" || value === "macos" || value === "linux" || value === "unknown" ? value : "unknown";
}

function normalizeShell(value: unknown, os: AgentOperatingSystem): AgentShell {
  if (value === "powershell" || value === "cmd" || value === "bash" || value === "zsh" || value === "sh" || value === "unknown") {
    return value;
  }
  if (os === "windows") {
    return "powershell";
  }
  if (os === "macos") {
    return "zsh";
  }
  if (os === "linux") {
    return "bash";
  }
  return "unknown";
}

function normalizeStyleConfig(input: Partial<AgentStyleConfig> | undefined): AgentStyleConfig {
  const density = input?.density === "compact" ? "compact" : "comfortable";
  return {
    themeId: cleanToken(input?.themeId || "soty-current", 80) || "soty-current",
    density,
    accent: cleanText(input?.accent || "#000000", 32) || "#000000"
  };
}

function cleanContext(input: Readonly<Record<string, string | number | boolean | null | undefined>>): Readonly<Record<string, string>> {
  const output: Record<string, string> = {};
  for (const [key, raw] of Object.entries(input).slice(0, 32)) {
    const cleanKey = cleanToken(key, 80);
    const cleanValue = cleanText(raw ?? "", 500);
    if (cleanKey && cleanValue) {
      output[cleanKey] = cleanValue;
    }
  }
  return output;
}

function clampInt(value: number, min: number, max: number): number {
  if (!Number.isFinite(value)) {
    return min;
  }
  return Math.max(min, Math.min(max, Math.round(value)));
}

function defaultProofForRisk(risk: AgentRisk): readonly string[] {
  if (risk === "high") {
    return ["preflight", "approval", "durable status", "final artifact"];
  }
  if (risk === "medium") {
    return ["preflight", "result state", "final status"];
  }
  return ["compact proof", "final status"];
}

function cleanRuntimeObject(value: Readonly<Record<string, unknown>>): Readonly<Record<string, unknown>> {
  const output: Record<string, unknown> = {};
  for (const [key, raw] of Object.entries(value).slice(0, 48)) {
    const cleanKey = cleanToken(key, 80);
    if (!cleanKey) {
      continue;
    }
    if (typeof raw === "string") {
      output[cleanKey] = cleanText(raw, 500);
    } else if (typeof raw === "number" || typeof raw === "boolean") {
      output[cleanKey] = raw;
    } else if (Array.isArray(raw)) {
      output[cleanKey] = cleanStringList(raw, 24, 180);
    }
  }
  return output;
}

function preferredList(primary: unknown, secondary: unknown, fallback: readonly string[]): readonly string[] {
  const first = Array.isArray(primary) ? cleanStringList(primary, 10, 180) : [];
  if (first.length > 0) {
    return first;
  }
  const second = Array.isArray(secondary) ? cleanStringList(secondary, 10, 180) : [];
  return second.length > 0 ? second : fallback;
}

function stringRuntime(value: unknown): string {
  return typeof value === "string" ? cleanText(value, 240) : "";
}

function inferredRisk(id: string, title: string, kind: string): AgentRisk {
  const haystack = `${id} ${title} ${kind}`.toLowerCase();
  if (/(reinstall|reset|wipe|format|delete|admin|system|boot|reboot)/u.test(haystack)) {
    return "high";
  }
  if (/(repair|install|update|copy|move|wallpaper|write|change)/u.test(haystack)) {
    return "medium";
  }
  return "low";
}

function defaultTargetPolicy(kind: string): string {
  if (kind === "mini-app") {
    return "Use the active mini-app context and only its declared capabilities.";
  }
  if (kind.includes("computer") || kind.includes("runtime") || kind.includes("route")) {
    return "Use the currently selected Soty target only after resolving exact host, OS, shell, access, and workspace.";
  }
  return "Use the current Soty context, then ask for target selection before state-changing work.";
}

function defaultConfirmBefore(risk: AgentRisk, requiresConfirmation: boolean): readonly string[] {
  if (risk === "high" || requiresConfirmation) {
    return ["destructive changes", "admin elevation", "reboot", "wipe", "format", "payment", "credential changes"];
  }
  if (risk === "medium") {
    return ["cross-device writes", "long-running jobs", "public UI changes"];
  }
  return ["unexpected state-changing work"];
}
