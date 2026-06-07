import type { AgentRunEnvelope, AgentProviderKind } from "./core";
import { serializeAgentEnvelopeForPrompt } from "./core";

export type AgentProviderEventKind = "status" | "message" | "terminal" | "proof";

export interface AgentProviderEvent {
  readonly kind: AgentProviderEventKind;
  readonly text: string;
}

export interface AgentProviderRequest {
  readonly envelope: AgentRunEnvelope;
  readonly prompt: string;
  readonly context?: string;
  readonly signal?: AbortSignal;
  readonly emit?: (event: AgentProviderEvent) => void;
}

export interface AgentProviderResult {
  readonly ok: boolean;
  readonly message: string;
  readonly reply: string;
  readonly proof: readonly string[];
  readonly exitCode?: number;
}

export interface AgentProviderAdapter {
  readonly id: string;
  readonly kind: AgentProviderKind;
  readonly label: string;
  readonly summary: string;
  readonly capabilities: readonly string[];
  run(request: AgentProviderRequest): Promise<AgentProviderResult>;
}

export interface SotyCodexProviderOptions {
  readonly id?: string;
  readonly label?: string;
  readonly source?: unknown;
  readonly askReply: (
    text: string,
    context: string,
    source: unknown,
    timeoutMs: number,
    onMessage?: (message: string) => void,
    onTerminal?: (message: string) => void,
    signal?: AbortSignal
  ) => Promise<{ readonly ok: boolean; readonly text: string; readonly messages?: readonly string[]; readonly terminal?: readonly string[]; readonly exitCode?: number }>;
}

export interface CustomHttpProviderOptions {
  readonly id: string;
  readonly kind?: AgentProviderKind;
  readonly label: string;
  readonly summary?: string;
  readonly endpoint: string;
  readonly headers?: Readonly<Record<string, string>>;
  readonly capabilities?: readonly string[];
  readonly retries?: number;
  readonly retryBaseMs?: number;
}

export function createProviderRegistry(providers: readonly AgentProviderAdapter[]): ReadonlyMap<string, AgentProviderAdapter> {
  const registry = new Map<string, AgentProviderAdapter>();
  for (const provider of providers) {
    if (!registry.has(provider.id)) {
      registry.set(provider.id, provider);
    }
  }
  return registry;
}

export function createSotyCodexProvider(options: SotyCodexProviderOptions): AgentProviderAdapter {
  return {
    id: options.id || "soty-codex",
    kind: "soty-codex",
    label: options.label || "Soty Codex",
    summary: "Server Codex executor with Soty computer-use tools.",
    capabilities: ["soty-relay", "codex-exec", "computer", "durable-actions"],
    async run(request) {
      const prompt = [serializeAgentEnvelopeForPrompt(request.envelope), request.prompt].join("\n\n");
      request.emit?.({ kind: "status", text: "request sent to Soty Codex" });
      const reply = await options.askReply(
        prompt,
        request.context || "",
        options.source || {},
        request.envelope.timeoutMs,
        (message) => request.emit?.({ kind: "message", text: message }),
        (message) => request.emit?.({ kind: "terminal", text: message }),
        request.signal
      );
      const proof = [
        ...(reply.messages || []).slice(-4),
        ...(reply.terminal || []).slice(-4)
      ].filter(Boolean);
      return {
        ok: reply.ok,
        message: reply.ok ? "done" : "agent reply failed",
        reply: reply.text,
        proof,
        ...(typeof reply.exitCode === "number" ? { exitCode: reply.exitCode } : {})
      };
    }
  };
}

export function createCustomHttpProvider(options: CustomHttpProviderOptions): AgentProviderAdapter {
  const retries = Math.max(0, Math.min(4, Math.round(options.retries ?? 1)));
  const retryBaseMs = Math.max(100, Math.min(4000, Math.round(options.retryBaseMs ?? 450)));
  return {
    id: options.id,
    kind: options.kind || "custom-http",
    label: options.label,
    summary: options.summary || "User provided HTTP agent endpoint.",
    capabilities: options.capabilities || ["http-agent"],
    async run(request) {
      let lastError = "";
      for (let attempt = 0; attempt <= retries; attempt += 1) {
        if (request.signal?.aborted) {
          return cancelledResult();
        }
        try {
          request.emit?.({ kind: "status", text: attempt === 0 ? "calling custom agent" : `retry ${attempt}` });
          const result = await callCustomHttpAgent(options, request);
          if (result.ok || !isRetryableExit(result.exitCode)) {
            return result;
          }
          lastError = result.message;
        } catch (error) {
          lastError = error instanceof Error ? error.message : "custom agent failed";
        }
        if (attempt < retries) {
          await sleep(retryBaseMs * 2 ** attempt + Math.floor(Math.random() * 120), request.signal);
        }
      }
      return {
        ok: false,
        message: lastError || "custom agent unavailable",
        reply: lastError || "Custom agent did not return a usable response.",
        proof: [],
        exitCode: 127
      };
    }
  };
}

async function callCustomHttpAgent(options: CustomHttpProviderOptions, request: AgentProviderRequest): Promise<AgentProviderResult> {
  const init: RequestInit = {
    method: "POST",
    cache: "no-store",
    headers: {
      "Content-Type": "application/json",
      ...(options.headers || {})
    },
    body: JSON.stringify({
      schema: "soty.agent-provider-request.v1",
      envelope: request.envelope,
      prompt: request.prompt,
      context: request.context || ""
    }),
    ...(request.signal ? { signal: request.signal } : {})
  };
  const response = await fetch(options.endpoint, init);
  const payload = await response.json().catch(() => ({})) as {
    readonly ok?: boolean;
    readonly message?: unknown;
    readonly reply?: unknown;
    readonly text?: unknown;
    readonly proof?: unknown;
  };
  const proof = Array.isArray(payload.proof)
    ? payload.proof.map((item) => String(item || "").trim()).filter(Boolean).slice(0, 12)
    : [];
  const reply = typeof payload.reply === "string"
    ? payload.reply
    : typeof payload.text === "string"
      ? payload.text
      : "";
  return {
    ok: response.ok && payload.ok !== false,
    message: typeof payload.message === "string" ? payload.message : response.ok ? "done" : `http ${response.status}`,
    reply: reply || (response.ok ? "" : `Custom agent returned HTTP ${response.status}.`),
    proof,
    exitCode: response.ok ? 0 : response.status
  };
}

function cancelledResult(): AgentProviderResult {
  return {
    ok: false,
    message: "cancelled",
    reply: "Cancelled.",
    proof: [],
    exitCode: 130
  };
}

function isRetryableExit(exitCode: number | undefined): boolean {
  return exitCode === undefined || exitCode === 0 || exitCode === 408 || exitCode === 429 || exitCode >= 500;
}

function sleep(ms: number, signal: AbortSignal | undefined): Promise<void> {
  return new Promise((resolve, reject) => {
    if (signal?.aborted) {
      reject(new DOMException("Aborted", "AbortError"));
      return;
    }
    const timer = window.setTimeout(resolve, ms);
    signal?.addEventListener("abort", () => {
      window.clearTimeout(timer);
      reject(new DOMException("Aborted", "AbortError"));
    }, { once: true });
  });
}
