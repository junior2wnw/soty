import type { AgentStyleConfig } from "./core";
import { cleanText, cleanToken } from "./core";

export interface AgentStylePack {
  readonly id: string;
  readonly label: string;
  readonly description: string;
  readonly tokens: Readonly<Record<AgentStyleToken, string>>;
}

export type AgentStyleToken =
  | "bg"
  | "panel"
  | "text"
  | "muted"
  | "border"
  | "accent"
  | "accentText"
  | "soft"
  | "danger"
  | "radius"
  | "shadow";

export const defaultAgentStylePack: AgentStylePack = {
  id: "soty-current",
  label: "Soty",
  description: "Current Soty UI language: black, white, compact, direct.",
  tokens: {
    bg: "#f4f4f4",
    panel: "#ffffff",
    text: "#000000",
    muted: "#5f6368",
    border: "#000000",
    accent: "#000000",
    accentText: "#ffffff",
    soft: "#ededed",
    danger: "#b42318",
    radius: "8px",
    shadow: "0 18px 60px rgba(0, 0, 0, 0.16)"
  },
};

export const agentStylePacks: readonly AgentStylePack[] = [
  defaultAgentStylePack,
  {
    id: "operator-graph",
    label: "Operator",
    description: "Dense operations surface for logs, proof, and terminal state.",
    tokens: {
      bg: "#111315",
      panel: "#181b1f",
      text: "#f4f7f9",
      muted: "#a8b0b8",
      border: "#3a4148",
      accent: "#9ad36a",
      accentText: "#101410",
      soft: "#23282d",
      danger: "#ff7d73",
      radius: "6px",
      shadow: "0 18px 70px rgba(0, 0, 0, 0.38)"
    }
  },
  {
    id: "studio-clean",
    label: "Studio",
    description: "Light editorial workspace for personal cards and mini-app styling.",
    tokens: {
      bg: "#f8f7f2",
      panel: "#ffffff",
      text: "#1d1c18",
      muted: "#6e6a60",
      border: "#d6d0c2",
      accent: "#146c5f",
      accentText: "#ffffff",
      soft: "#ece7dc",
      danger: "#b42318",
      radius: "8px",
      shadow: "0 16px 44px rgba(37, 33, 25, 0.14)"
    }
  }
];

export function resolveAgentStylePack(styleId: string): AgentStylePack {
  const id = cleanToken(styleId, 80);
  return agentStylePacks.find((pack) => pack.id === id) || defaultAgentStylePack;
}

export function normalizeAgentStyleConfig(input: Partial<AgentStyleConfig> | undefined): AgentStyleConfig {
  const themeId = cleanToken(input?.themeId || "soty-current", 80) || "soty-current";
  return {
    themeId,
    density: input?.density === "compact" ? "compact" : "comfortable",
    accent: cleanText(input?.accent || resolveAgentStylePack(themeId).tokens.accent, 32)
  };
}

export function agentStyleVars(style: Partial<AgentStyleConfig> | undefined): Readonly<Record<string, string>> {
  const normalized = normalizeAgentStyleConfig(style);
  const pack = resolveAgentStylePack(normalized.themeId);
  return {
    "--soty-agent-bg": pack.tokens.bg,
    "--soty-agent-panel": pack.tokens.panel,
    "--soty-agent-text": pack.tokens.text,
    "--soty-agent-muted": pack.tokens.muted,
    "--soty-agent-border": pack.tokens.border,
    "--soty-agent-accent": normalized.accent || pack.tokens.accent,
    "--soty-agent-accent-text": pack.tokens.accentText,
    "--soty-agent-soft": pack.tokens.soft,
    "--soty-agent-danger": pack.tokens.danger,
    "--soty-agent-radius": pack.tokens.radius,
    "--soty-agent-shadow": pack.tokens.shadow,
    "--soty-agent-density": normalized.density === "compact" ? "8px" : "12px"
  };
}

export function applyAgentStyleVars(target: HTMLElement, style: Partial<AgentStyleConfig> | undefined): void {
  const vars = agentStyleVars(style);
  for (const [key, value] of Object.entries(vars)) {
    target.style.setProperty(key, value);
  }
}
