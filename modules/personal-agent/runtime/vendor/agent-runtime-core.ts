export type RuntimeMessage = { role: "system" | "user" | "assistant" | "tool"; content: string | null; reasoning_content?: string; tool_call_id?: string; tool_calls?: RuntimeProviderToolCall[] };

export type RuntimeAdapterContract<Role extends string, Capability extends string> = {
  id: string;
  roles: readonly Role[];
  capabilities: readonly Capability[];
  applicability: string;
  systemContext: (terminalToolName: string) => string;
  isActionHrefAllowed: (href: string) => boolean;
};

export type RuntimeProviderToolCall = { id: string; type?: "function"; function: { name: string; arguments: string } };
export type RuntimeProviderOptions = { toolChoice?: string; parallelToolCalls?: boolean; maxTokens?: number };
export type RuntimeProviderResponse = { choices?: Array<{ finish_reason?: string; message?: { content?: string | null; reasoning_content?: string; tool_calls?: RuntimeProviderToolCall[] } }> };

export type RuntimeToolDefinition<Capability extends string = string> = {
  description: string;
  capability: Capability;
  inputSchema?: Record<string, unknown>;
  validate: (args: unknown) => boolean;
  execute: (args: unknown, signal: AbortSignal) => Promise<unknown> | unknown;
};

export type RuntimeProvider<Tool extends RuntimeToolDefinition = RuntimeToolDefinition> = (
  messages: RuntimeMessage[],
  signal: AbortSignal,
  tools?: Record<string, Tool>,
  options?: RuntimeProviderOptions,
) => Promise<RuntimeProviderResponse>;

export type RuntimeEvidence = { id: string; title: string; observedAt: string | null; sourceStatus: string; data?: Record<string, unknown> };
export type RuntimeTruthClaim = { kind: "fact" | "inference" | "projection"; statement: string; evidenceIds: string[]; freshness: string; confidence: string; applicability: string };

export type RuntimeContextRecord<Role extends string> = {
  version: 1;
  id: string;
  role: Role;
  history: string[];
  state: Record<string, unknown>;
  createdAt: number;
  updatedAt: number;
  expiresAt: number;
};

const OPAQUE_CONTEXT_ID = /^ctx_[A-Za-z0-9_-]{16,124}$/u;

export function assertAdapterRole<Role extends string>(adapter: RuntimeAdapterContract<Role, string>, value: string): Role {
  if (!adapter.roles.includes(value as Role)) throw new Error(`unsupported-role:${value}`);
  return value as Role;
}

export function createIsolatedContextStore<Role extends string>(options: {
  roles: readonly Role[];
  idFactory?: () => string;
  now?: () => number;
  ttlMs?: number;
  maxHistoryEntries?: number;
  maxStateEntries?: number;
  maxContexts?: number;
}) {
  const byId = new Map<string, RuntimeContextRecord<Role>>();
  const idFactory = options.idFactory ?? (() => `ctx_${crypto.randomUUID().replaceAll("-", "")}`);
  const now = options.now ?? Date.now;
  const ttlMs = options.ttlMs ?? 30 * 60_000;
  const maxHistory = options.maxHistoryEntries ?? 8;
  const maxState = options.maxStateEntries ?? 16;
  const maxContexts = options.maxContexts ?? 1_000;
  if (!Number.isFinite(ttlMs) || ttlMs < 1_000 || !Number.isInteger(maxHistory) || maxHistory < 0 || !Number.isInteger(maxState) || maxState < 0 || !Number.isInteger(maxContexts) || maxContexts < 1) throw new Error("invalid-context-bounds");

  const validRole = (value: string): value is Role => options.roles.includes(value as Role);
  const purgeExpired = () => {
    const current = now();
    for (const [id, record] of byId) if (record.expiresAt <= current) byId.delete(id);
  };
  const touch = (record: RuntimeContextRecord<Role>) => {
    record.updatedAt = now();
    record.expiresAt = record.updatedAt + ttlMs;
  };
  const create = (role: Role) => {
    purgeExpired();
    if (byId.size >= maxContexts) {
      const oldest = [...byId.values()].sort((left, right) => left.updatedAt - right.updatedAt)[0];
      if (oldest) byId.delete(oldest.id);
    }
    const id = idFactory();
    if (!OPAQUE_CONTEXT_ID.test(id) || byId.has(id)) throw new Error("invalid-context-id");
    const timestamp = now();
    const record: RuntimeContextRecord<Role> = { version: 1, id, role, history: [], state: {}, createdAt: timestamp, updatedAt: timestamp, expiresAt: timestamp + ttlMs };
    byId.set(id, record);
    return record;
  };
  const required = (id: string) => {
    if (!OPAQUE_CONTEXT_ID.test(id)) throw new Error("invalid-context-id");
    purgeExpired();
    const record = byId.get(id);
    if (!record) throw new Error("unknown-context-id");
    touch(record);
    return record;
  };

  return {
    resolve(role: Role, requestedId?: string) {
      if (!validRole(role)) throw new Error(`unsupported-role:${role}`);
      if (!requestedId) {
        const record = create(role);
        return { id: record.id, role: record.role };
      }
      const record = required(requestedId);
      if (record.role !== role) throw new Error("context-role-mismatch");
      return { id: record.id, role: record.role };
    },
    switchRole(currentId: string, newRole: Role) {
      if (!validRole(newRole)) throw new Error(`unsupported-role:${newRole}`);
      const current = required(currentId);
      if (current.role === newRole) return { id: current.id, role: current.role };
      const record = create(newRole);
      return { id: record.id, role: record.role };
    },
    append(id: string, entry: string) {
      const record = required(id);
      if (typeof entry !== "string" || entry.length > 8_000) throw new Error("invalid-context-entry");
      record.history.push(entry);
      if (record.history.length > maxHistory) record.history.splice(0, record.history.length - maxHistory);
    },
    replaceHistory(id: string, entries: readonly string[]) {
      const record = required(id);
      if (!Array.isArray(entries) || entries.length > maxHistory || entries.some((entry) => typeof entry !== "string" || entry.length > 8_000)) throw new Error("invalid-context-history");
      record.history = entries.slice(-maxHistory);
      touch(record);
    },
    setState(id: string, key: string, value: unknown) {
      const record = required(id);
      if (!(key in record.state) && Object.keys(record.state).length >= maxState) throw new Error("context-state-limit");
      record.state[key] = structuredClone(value);
      touch(record);
    },
    snapshot(id: string): RuntimeContextRecord<Role> | undefined {
      try { return structuredClone(required(id)); } catch { return undefined; }
    },
    clear(id: string) { byId.delete(id); },
    size() { purgeExpired(); return byId.size; },
  };
}

export function runtimeConversation(history: readonly string[]): RuntimeMessage[] {
  return history.flatMap((entry) => {
    try {
      const value = JSON.parse(entry) as { role?: unknown; content?: unknown };
      return (value.role === "user" || value.role === "assistant") && typeof value.content === "string"
        ? [{ role: value.role, content: value.content } as RuntimeMessage]
        : [];
    } catch { return []; }
  });
}

export function authorizeRuntimeAction<Capability extends string>(action: string, capability: string | undefined, allowedCapabilities: readonly Capability[]) {
  const forbidden = /(?:book(?:ing)?|reserv|payment|pay|crm|send|delete|remove|pii|transfer|submit|publish|write|create|update|брон|оплат|отправ|удал|передач|публикац|созда|измен)/iu;
  if (forbidden.test(action) || !capability || !allowedCapabilities.includes(capability as Capability)) {
    return { allowed: false as const, handoff: { action, preview: true as const, requiresHumanSubmit: true as const } };
  }
  return { allowed: true as const };
}

export type SafeFailureClass = "timeout" | "rate_limited" | "transient_upstream" | "transport" | "non_transient";

export function classifyProviderFailure(error: unknown): { retryable: boolean; reasonClass: SafeFailureClass } {
  if (error && typeof error === "object" && "status" in error && typeof (error as { status?: unknown }).status === "number") {
    const status = (error as { status: number }).status;
    if (status === 429) return { retryable: true, reasonClass: "rate_limited" };
    if ([408, 425, 500, 502, 503, 504].includes(status)) return { retryable: true, reasonClass: "transient_upstream" };
    return { retryable: false, reasonClass: "non_transient" };
  }
  if (error instanceof TypeError) return { retryable: true, reasonClass: "transport" };
  return { retryable: false, reasonClass: "non_transient" };
}

export type RuntimeCard = {
  kind: string;
  title: string;
  href?: string;
  freshness: string;
  asOf: string | null;
  sourceStatus: string;
  evidenceIds: string[];
  data?: Record<string, unknown>;
};

export type RuntimeAction = { label: string; href: string; evidenceIds: string[] };

export type RuntimeResponse<Card extends RuntimeCard = RuntimeCard> = {
  status: "complete" | "degraded" | "blocked";
  answer: string;
  contextId: string;
  evidence: RuntimeEvidence[];
  claims: RuntimeTruthClaim[];
  cards: Card[];
  actions: RuntimeAction[];
  telemetry: RuntimeTelemetry;
};

export type RuntimeTelemetry = { correlationId: string; reasonClass: SafeFailureClass | "ok" | "policy_blocked" };

export type RuntimePersistenceBoundary<Role extends string, VisibleMessage> = {
  version: number;
  ttlMs: number;
  maxMessages: number;
  maxBytes: number;
  encode: (value: { role: Role; contextId: string; messages: VisibleMessage[] }, now?: number) => string | null;
  decode: (value: string, roles: readonly Role[], now?: number) => { role: Role; contextId: string; messages: VisibleMessage[] } | null;
};

type PortableContextStore<Role extends string> = {
  resolve: (role: Role, requestedId?: string) => { id: string; role: Role };
  append: (id: string, entry: string) => void;
  snapshot: (id: string) => RuntimeContextRecord<Role> | undefined;
};

function portableProviderMessage(response: RuntimeProviderResponse) {
  const message = response.choices?.[0]?.message;
  if (!message) throw new Error("provider-envelope-invalid");
  return message;
}

function portableToolResult(value: unknown): { evidence: RuntimeEvidence[]; cards: RuntimeCard[]; actions: RuntimeAction[] } {
  if (!value || typeof value !== "object" || Array.isArray(value)) throw new Error("tool-result-invalid");
  let serialized: string;
  try { serialized = JSON.stringify(value); } catch { throw new Error("tool-result-invalid"); }
  if (new TextEncoder().encode(serialized).byteLength > 64_000) throw new Error("tool-result-too-large");
  const result = value as { evidence?: unknown; cards?: unknown; actions?: unknown };
  const evidence = Array.isArray(result.evidence) ? result.evidence : [];
  const cards = Array.isArray(result.cards) ? result.cards : [];
  const actions = Array.isArray(result.actions) ? result.actions : [];
  if (evidence.some((item) => !item || typeof item !== "object" || typeof (item as RuntimeEvidence).id !== "string" || !(item as RuntimeEvidence).id || typeof (item as RuntimeEvidence).title !== "string" || !((item as RuntimeEvidence).observedAt === null || typeof (item as RuntimeEvidence).observedAt === "string") || typeof (item as RuntimeEvidence).sourceStatus !== "string")) throw new Error("tool-evidence-invalid");
  const evidenceIds = new Set((evidence as RuntimeEvidence[]).map((item) => item.id));
  if (evidenceIds.size !== evidence.length) throw new Error("tool-evidence-duplicate");
  const validEvidenceRefs = (ids: unknown) => Array.isArray(ids) && ids.length > 0 && ids.every((id) => typeof id === "string" && evidenceIds.has(id));
  if (cards.some((item) => !item || typeof item !== "object" || typeof (item as RuntimeCard).kind !== "string" || typeof (item as RuntimeCard).title !== "string" || typeof (item as RuntimeCard).freshness !== "string" || !((item as RuntimeCard).asOf === null || typeof (item as RuntimeCard).asOf === "string") || typeof (item as RuntimeCard).sourceStatus !== "string" || ((item as RuntimeCard).href !== undefined && typeof (item as RuntimeCard).href !== "string") || !validEvidenceRefs((item as RuntimeCard).evidenceIds))) throw new Error("tool-card-invalid");
  if (actions.some((item) => !item || typeof item !== "object" || typeof (item as RuntimeAction).label !== "string" || typeof (item as RuntimeAction).href !== "string" || !validEvidenceRefs((item as RuntimeAction).evidenceIds))) throw new Error("tool-action-invalid");
  return { evidence: structuredClone(evidence) as RuntimeEvidence[], cards: structuredClone(cards) as RuntimeCard[], actions: structuredClone(actions) as RuntimeAction[] };
}

const PORTABLE_TERMINAL_TOOL_NAME = "finish_answer";
const PORTABLE_TERMINAL_TOOL: RuntimeToolDefinition<string> = {
  description: "Finish with a validated answer and explicit evidence selection",
  capability: "__terminal__",
  inputSchema: {
    type: "object",
    additionalProperties: false,
    required: ["answer", "selectedEvidenceIds", "claims"],
    properties: {
      answer: { type: "string", minLength: 1, maxLength: 8_000 },
      selectedEvidenceIds: { type: "array", maxItems: 32, uniqueItems: true, items: { type: "string" } },
      claims: {
        type: "array", maxItems: 16, items: {
          type: "object", additionalProperties: false,
          required: ["kind", "statement", "evidenceIds", "freshness", "confidence", "applicability"],
          properties: {
            kind: { enum: ["fact", "inference", "projection"] }, statement: { type: "string", minLength: 1, maxLength: 2_000 },
            evidenceIds: { type: "array", maxItems: 16, uniqueItems: true, items: { type: "string" } },
            freshness: { type: "string", minLength: 1, maxLength: 80 }, confidence: { type: "string", minLength: 1, maxLength: 80 },
            applicability: { type: "string", minLength: 1, maxLength: 240 },
          },
        },
      },
    },
  },
  validate: () => false,
  execute: () => { throw new Error("terminal-tool-is-not-executable"); },
};

function validatePortableTerminal<Role extends string, Capability extends string>(
  response: RuntimeProviderResponse,
  adapter: RuntimeAdapterContract<Role, Capability>,
  output: { evidence: RuntimeEvidence[]; cards: RuntimeCard[]; actions: RuntimeAction[] },
) {
  const message = portableProviderMessage(response);
  const calls = message.tool_calls ?? [];
  if (calls.length !== 1 || calls[0]?.function.name !== PORTABLE_TERMINAL_TOOL_NAME || typeof calls[0].function.arguments !== "string" || calls[0].function.arguments.length > 32_000) throw new Error("terminal-envelope-invalid");
  let raw: unknown;
  try { raw = JSON.parse(calls[0].function.arguments); } catch { throw new Error("terminal-arguments-invalid"); }
  if (!raw || typeof raw !== "object" || Array.isArray(raw)) throw new Error("terminal-arguments-invalid");
  const value = raw as Record<string, unknown>;
  if (Object.keys(value).some((key) => !["answer", "selectedEvidenceIds", "claims"].includes(key)) || typeof value.answer !== "string" || !value.answer.trim() || value.answer.length > 8_000 || !Array.isArray(value.selectedEvidenceIds) || value.selectedEvidenceIds.length > 32 || !Array.isArray(value.claims) || value.claims.length > 16) throw new Error("terminal-arguments-invalid");
  const knownIds = new Set(output.evidence.map((item) => item.id));
  const selected = value.selectedEvidenceIds as unknown[];
  if (selected.some((id) => typeof id !== "string" || !knownIds.has(id)) || new Set(selected).size !== selected.length || (knownIds.size > 0 && selected.length === 0)) throw new Error("terminal-evidence-invalid");
  const selectedIds = new Set(selected as string[]);
  const claims = (value.claims as unknown[]).map((claim): RuntimeTruthClaim => {
    if (!claim || typeof claim !== "object" || Array.isArray(claim)) throw new Error("terminal-claim-invalid");
    const item = claim as Record<string, unknown>;
    if (Object.keys(item).some((key) => !["kind", "statement", "evidenceIds", "freshness", "confidence", "applicability"].includes(key)) || !["fact", "inference", "projection"].includes(String(item.kind)) || typeof item.statement !== "string" || !item.statement.trim() || item.statement.length > 2_000 || !Array.isArray(item.evidenceIds) || item.evidenceIds.length > 16 || typeof item.freshness !== "string" || !item.freshness || typeof item.confidence !== "string" || !item.confidence || item.applicability !== adapter.applicability) throw new Error("terminal-claim-invalid");
    if (item.evidenceIds.some((id) => typeof id !== "string" || !selectedIds.has(id)) || new Set(item.evidenceIds).size !== item.evidenceIds.length || (item.kind === "fact" && item.evidenceIds.length === 0)) throw new Error("terminal-claim-evidence-invalid");
    return item as RuntimeTruthClaim;
  });
  const selectedOutput = <T extends { evidenceIds: string[] }>(items: T[]) => items.filter((item) => item.evidenceIds.every((id) => selectedIds.has(id)));
  const cards = selectedOutput(output.cards);
  const actions = selectedOutput(output.actions);
  if (cards.some((card) => card.href && !adapter.isActionHrefAllowed(card.href)) || actions.some((action) => !adapter.isActionHrefAllowed(action.href))) throw new Error("terminal-action-invalid");
  return { answer: value.answer.trim(), selectedEvidenceIds: [...selectedIds], claims, cards, actions };
}

/**
 * Minimal reusable execution path used to prove an adapter can bind to the
 * neutral contracts. Product integrations may add stricter terminal schemas,
 * but may not weaken isolation, authorization, bounds or evidence validation.
 */
export async function runBoundedRuntimeTurn<Role extends string, Capability extends string>(options: {
  adapter: RuntimeAdapterContract<Role, Capability>;
  role: Role;
  prompt: string;
  contextStore: PortableContextStore<Role>;
  contextId?: string;
  provider: RuntimeProvider<RuntimeToolDefinition<Capability>>;
  tools: Record<string, RuntimeToolDefinition<Capability>>;
  timeoutMs?: number;
}): Promise<RuntimeResponse> {
  const role = assertAdapterRole(options.adapter, options.role);
  if (typeof options.prompt !== "string" || !options.prompt.trim() || options.prompt.length > 8_000) throw new Error("prompt-invalid");
  const context = options.contextStore.resolve(role, options.contextId);
  const controller = new AbortController();
  const timeoutMs = options.timeoutMs ?? 20_000;
  if (!Number.isFinite(timeoutMs) || timeoutMs < 100 || timeoutMs > 90_000) throw new Error("timeout-invalid");
  const timer = setTimeout(() => controller.abort(new Error("runtime-deadline")), timeoutMs);
  const correlationId = `runtime_${crypto.randomUUID().replaceAll("-", "")}`;
  const raceAbort = async <T>(work: Promise<T>): Promise<T> => {
    if (controller.signal.aborted) throw controller.signal.reason ?? new Error("runtime-deadline");
    let abort: (() => void) | undefined;
    const aborted = new Promise<never>((_resolve, reject) => {
      abort = () => reject(controller.signal.reason ?? new Error("runtime-deadline"));
      controller.signal.addEventListener("abort", abort, { once: true });
    });
    try { return await Promise.race([work, aborted]); }
    finally { if (abort) controller.signal.removeEventListener("abort", abort); }
  };
  const remember = (answer: string) => {
    options.contextStore.append(context.id, JSON.stringify({ role: "user", content: options.prompt }));
    options.contextStore.append(context.id, JSON.stringify({ role: "assistant", content: answer }));
  };
  try {
    const messages: RuntimeMessage[] = [
      { role: "system", content: options.adapter.systemContext("finish_answer") },
      ...runtimeConversation(options.contextStore.snapshot(context.id)?.history ?? []),
      { role: "user", content: options.prompt },
    ];
    const callProvider = async (providerMessages: RuntimeMessage[], tools: Record<string, RuntimeToolDefinition<string>>, providerOptions: RuntimeProviderOptions) => {
      let lastError: unknown;
      for (let attempt = 0; attempt < 3; attempt += 1) {
        try { return await raceAbort(options.provider(providerMessages, controller.signal, tools as Record<string, RuntimeToolDefinition<Capability>>, providerOptions)); }
        catch (error) {
          lastError = error;
          if (attempt >= 2 || controller.signal.aborted || !classifyProviderFailure(error).retryable) throw error;
        }
      }
      throw lastError;
    };
    const offeredTools: Record<string, RuntimeToolDefinition<string>> = { ...options.tools, [PORTABLE_TERMINAL_TOOL_NAME]: PORTABLE_TERMINAL_TOOL };
    // Both phases can emit a complete finish_answer envelope, including a draft.
    // The host still applies its smaller model-policy cap to this finite budget.
    const answerMaxTokens = 2048;
    const routedResponse = await callProvider(messages, offeredTools, { parallelToolCalls: false, maxTokens: answerMaxTokens });
    const routed = portableProviderMessage(routedResponse);
    const calls = routed.tool_calls ?? [];
    if (calls.length !== 1) throw new Error("provider-tool-count-invalid");
    const call = calls[0];
    if (call.function.name === PORTABLE_TERMINAL_TOOL_NAME) {
      const final = validatePortableTerminal(routedResponse, options.adapter, { evidence: [], cards: [], actions: [] });
      remember(final.answer);
      return { status: "complete", answer: final.answer, contextId: context.id, evidence: [], claims: final.claims, cards: [], actions: [], telemetry: { correlationId, reasonClass: "ok" } };
    }
    const tool = options.tools[call.function.name];
    if (!tool) throw new Error("provider-tool-unknown");
    const authorization = authorizeRuntimeAction(call.function.name, tool.capability, options.adapter.capabilities);
    if (!authorization.allowed) throw new Error("tool-not-authorized");
    let args: unknown;
    try { args = JSON.parse(call.function.arguments); } catch { throw new Error("tool-arguments-invalid"); }
    if (!tool.validate(args)) throw new Error("tool-arguments-invalid");
    const rawResult = await raceAbort(Promise.resolve(tool.execute(args, controller.signal)));
    const output = portableToolResult(rawResult);
    const terminalMessages: RuntimeMessage[] = [
      ...messages,
      { role: "assistant", content: routed.content ?? null, tool_calls: calls },
      { role: "tool", tool_call_id: call.id, content: JSON.stringify(rawResult) },
    ];
    const terminalResponse = await callProvider(terminalMessages, { [PORTABLE_TERMINAL_TOOL_NAME]: PORTABLE_TERMINAL_TOOL }, { toolChoice: PORTABLE_TERMINAL_TOOL_NAME, parallelToolCalls: false, maxTokens: answerMaxTokens });
    const final = validatePortableTerminal(terminalResponse, options.adapter, output);
    remember(final.answer);
    return { status: "complete", answer: final.answer, contextId: context.id, evidence: output.evidence.filter((item) => final.selectedEvidenceIds.includes(item.id)), claims: final.claims, cards: final.cards, actions: final.actions, telemetry: { correlationId, reasonClass: "ok" } };
  } finally {
    clearTimeout(timer);
  }
}
