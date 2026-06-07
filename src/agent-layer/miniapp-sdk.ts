import type { AgentProviderKind, AgentRunEnvelope, AgentSurface } from "./core";
import { createAgentRunEnvelope, cleanText, cleanToken } from "./core";

export interface MiniAppAgentContext {
  readonly id: string;
  readonly label: string;
  readonly value: string;
  readonly private?: boolean;
}

export interface MiniAppAgentTool {
  readonly id: string;
  readonly label: string;
  readonly description: string;
  readonly risk: "low" | "medium" | "high";
  readonly inputSchema?: Readonly<Record<string, unknown>>;
}

export interface MiniAppAgentBridgeOptions {
  readonly appId: string;
  readonly appTitle: string;
  readonly userId: string;
  readonly provider?: AgentProviderKind;
  readonly surface?: AgentSurface;
}

export interface MiniAppAgentManifest {
  readonly schema: "soty.miniapp-agent-manifest.v1";
  readonly appId: string;
  readonly appTitle: string;
  readonly contexts: readonly MiniAppAgentContext[];
  readonly tools: readonly MiniAppAgentTool[];
}

export interface MiniAppAgentBridge {
  registerContext(context: MiniAppAgentContext): MiniAppAgentManifest;
  registerTool(tool: MiniAppAgentTool): MiniAppAgentManifest;
  manifest(): MiniAppAgentManifest;
  createEnvelope(intent: string, capabilityIds?: readonly string[]): AgentRunEnvelope;
  postIntent(intent: string, capabilityIds?: readonly string[]): void;
}

export function createMiniAppAgentBridge(options: MiniAppAgentBridgeOptions): MiniAppAgentBridge {
  const contexts = new Map<string, MiniAppAgentContext>();
  const tools = new Map<string, MiniAppAgentTool>();
  const appId = cleanToken(options.appId, 120) || "miniapp";
  const appTitle = cleanText(options.appTitle, 120) || appId;
  const userId = cleanToken(options.userId, 160) || "anonymous";
  const provider = options.provider || "soty-codex";
  const surface = options.surface || "miniapp";

  const api: MiniAppAgentBridge = {
    registerContext(context) {
      const normalized = normalizeContext(context);
      if (normalized.id) {
        contexts.set(normalized.id, normalized);
      }
      return api.manifest();
    },
    registerTool(tool) {
      const normalized = normalizeTool(tool);
      if (normalized.id) {
        tools.set(normalized.id, normalized);
      }
      return api.manifest();
    },
    manifest() {
      return {
        schema: "soty.miniapp-agent-manifest.v1",
        appId,
        appTitle,
        contexts: [...contexts.values()],
        tools: [...tools.values()]
      };
    },
    createEnvelope(intent, capabilityIds = []) {
      const manifest = api.manifest();
      const context: Record<string, string> = {
        appId: manifest.appId,
        appTitle: manifest.appTitle
      };
      for (const item of manifest.contexts) {
        if (!item.private) {
          context[`ctx:${item.id}`] = `${item.label}: ${item.value}`;
        }
      }
      return createAgentRunEnvelope({
        userId,
        surface,
        provider,
        target: {
          kind: "miniapp",
          os: "unknown",
          shell: "unknown",
          label: appTitle,
          hostId: appId
        },
        intent,
        capabilities: cleanCapabilityIds(capabilityIds.length > 0 ? capabilityIds : manifest.tools.map((tool) => tool.id)),
        risk: highestToolRisk(manifest.tools),
        proof: ["mini-app state acknowledged", "final reply"],
        context
      });
    },
    postIntent(intent, capabilityIds = []) {
      const envelope = api.createEnvelope(intent, capabilityIds);
      const boot = readMiniAppBoot();
      const manifest = api.manifest();
      const text = [
        `Mini-app intent: ${cleanText(intent, 1200)}`,
        "",
        "SOTY_AGENT_RUN_ENVELOPE:",
        JSON.stringify(envelope),
        "",
        "MINI_APP_AGENT_MANIFEST:",
        JSON.stringify(manifest)
      ].join("\n");
      window.parent?.postMessage({
        schema: boot.messageSchema || "soty.mini-app.v1",
        nonce: boot.nonce,
        type: "agent.invoke",
        text,
        visibleText: cleanText(intent, 500),
        envelope,
        manifest
      }, boot.targetOrigin || window.location.origin);
    }
  };

  return api;
}

function readMiniAppBoot(): { readonly nonce: string; readonly messageSchema: string; readonly targetOrigin: string } {
  const source = (window as Window & {
    readonly SOTY_MINI_APP?: {
      readonly nonce?: unknown;
      readonly messageSchema?: unknown;
      readonly targetOrigin?: unknown;
    };
  }).SOTY_MINI_APP;
  return {
    nonce: typeof source?.nonce === "string" ? source.nonce : "",
    messageSchema: typeof source?.messageSchema === "string" ? source.messageSchema : "",
    targetOrigin: typeof source?.targetOrigin === "string" ? source.targetOrigin : window.location.origin
  };
}

function normalizeContext(context: MiniAppAgentContext): MiniAppAgentContext {
  return {
    id: cleanToken(context.id, 80),
    label: cleanText(context.label, 80),
    value: cleanText(context.value, 1000),
    ...(context.private === true ? { private: true } : {})
  };
}

function normalizeTool(tool: MiniAppAgentTool): MiniAppAgentTool {
  return {
    id: cleanToken(tool.id, 80),
    label: cleanText(tool.label, 80),
    description: cleanText(tool.description, 240),
    risk: tool.risk === "high" || tool.risk === "medium" ? tool.risk : "low",
    ...(tool.inputSchema ? { inputSchema: tool.inputSchema } : {})
  };
}

function cleanCapabilityIds(value: readonly string[]): readonly string[] {
  return value
    .map((item) => cleanToken(item, 80))
    .filter(Boolean)
    .slice(0, 32);
}

function highestToolRisk(tools: readonly MiniAppAgentTool[]): "low" | "medium" | "high" {
  if (tools.some((tool) => tool.risk === "high")) {
    return "high";
  }
  if (tools.some((tool) => tool.risk === "medium")) {
    return "medium";
  }
  return "low";
}
