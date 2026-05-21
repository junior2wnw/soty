import { normalizeAppSurfaceId, resolveAppSurfaceUrl } from "trustlink-kernel";
import type { IconName } from "../icons";
import { cleanNick } from "../trustlink";

export type MiniAppScope = "account" | "chat" | "device";
export type MiniAppWindowLayout = "half" | "compact" | "large" | "full" | "floating";

export type MiniAppDefinition = {
  readonly id: string;
  readonly title: string;
  readonly url: string;
  readonly inlineHtml?: string;
  readonly summary: string;
  readonly icon: IconName;
  readonly layout?: MiniAppWindowLayout;
  readonly height?: string;
  readonly width?: string;
  readonly capabilities: readonly string[];
  readonly source?: "manifest" | "agent" | "room";
  readonly scope?: MiniAppScope;
  readonly targetDeviceId?: string;
  readonly tunnelId?: string;
  readonly revision?: string;
  readonly installedAt?: string;
  readonly updatedAt?: string;
};

export type MiniAppSession = {
  readonly app: MiniAppDefinition;
  readonly nonce: string;
  readonly layout: MiniAppWindowLayout;
  readonly height?: string;
  readonly width?: string;
  readonly collapsed: boolean;
};

export type PendingAttachment = {
  readonly id: string;
  readonly file: File;
  readonly name: string;
  readonly type: string;
  readonly size: number;
};

export type FileBundleAttachment = {
  readonly id: string;
  readonly name: string;
  readonly type: string;
  readonly size: number;
};

export type FileBundleMarker = {
  readonly id: string;
  readonly files: readonly FileBundleAttachment[];
};

export type MiniAppInstallResult = {
  readonly ok: boolean;
  readonly app?: MiniAppDefinition;
  readonly opened?: boolean;
  readonly error?: string;
};

export type MiniAppSanitizeOptions = {
  readonly baseUrl?: string;
};

const miniAppLayoutValues = ["half", "compact", "large", "full", "floating"] as const satisfies readonly MiniAppWindowLayout[];
const iconNames = new Set<IconName>([
  "install",
  "qr",
  "scan",
  "close",
  "check",
  "person",
  "clip",
  "remote",
  "download",
  "upload",
  "refresh",
  "copy",
  "bell",
  "shield",
  "send",
  "stop",
  "chess",
  "collapse",
  "expand"
]);

export function normalizeMiniAppScope(value: string): MiniAppScope {
  const clean = value.trim().toLowerCase();
  return clean === "chat" || clean === "device" ? clean : "account";
}

export function normalizeMiniAppLayout(value: string): MiniAppWindowLayout {
  const clean = value.trim().toLowerCase().replace(/_/gu, "-");
  if (clean === "full" || clean === "fullscreen" || clean === "full-screen") {
    return "full";
  }
  if (clean === "compact" || clean === "small" || clean === "mini") {
    return "compact";
  }
  if (clean === "large" || clean === "big" || clean === "wide") {
    return "large";
  }
  if (clean === "floating" || clean === "float" || clean === "free") {
    return "floating";
  }
  return "half";
}

export function miniAppLayouts(): readonly MiniAppWindowLayout[] {
  return miniAppLayoutValues;
}

export function safeMiniAppCssSize(value: string): string {
  const clean = value.trim().slice(0, 80);
  if (!clean || /[{};<>@"']/u.test(clean) || /url\s*\(/iu.test(clean)) {
    return "";
  }
  if (/^\d+(?:\.\d+)?(?:px|rem|em|%|vh|vw|svh|svw|dvh|dvw)$/iu.test(clean)) {
    return clean;
  }
  if (/^(?:clamp|min|max|calc)\([\w\s.+\-*/(),%]+(?:px|rem|em|%|vh|vw|svh|svw|dvh|dvw)[\w\s.+\-*/(),%]*\)$/iu.test(clean)) {
    return clean;
  }
  return "";
}

export function miniAppDefaultHeight(layout: MiniAppWindowLayout): string {
  if (layout === "compact") {
    return "clamp(190px, 30svh, 340px)";
  }
  if (layout === "large") {
    return "clamp(360px, 68svh, 820px)";
  }
  if (layout === "full") {
    return "calc(100svh - 124px)";
  }
  if (layout === "floating") {
    return "clamp(260px, 48svh, 620px)";
  }
  return "clamp(260px, 50svh, 620px)";
}

export function miniAppDefaultWidth(layout: MiniAppWindowLayout): string {
  return layout === "floating" ? "min(760px, calc(100% - 36px))" : "auto";
}

export function sanitizeMiniAppDefinition(value: unknown, options: MiniAppSanitizeOptions = {}): MiniAppDefinition | null {
  if (!isRecord(value)) {
    return null;
  }
  const id = normalizeAppSurfaceId(recordString(value, "id"));
  const title = cleanNick(recordString(value, "title") || id).slice(0, 80);
  const summary = cleanNick(recordString(value, "summary")).slice(0, 160);
  const iconName = recordString(value, "icon");
  const iconValue = isIconName(iconName) ? iconName : "remote";
  const inlineHtml = safeMiniAppInlineHtml(recordString(value, "inlineHtml") || recordString(value, "html"));
  const url = inlineHtml ? "about:srcdoc" : safeMiniAppUrl(recordString(value, "url"), options);
  if (!id || !title || !url) {
    return null;
  }
  const display = isRecord(value.display) ? value.display : value;
  const layout = normalizeMiniAppLayout(recordString(display, "layout") || recordString(value, "layout"));
  const height = safeMiniAppCssSize(recordString(display, "height") || recordString(value, "height"));
  const width = safeMiniAppCssSize(recordString(display, "width") || recordString(value, "width"));
  const capabilities = Array.isArray(value.capabilities)
    ? value.capabilities.filter((item): item is string => typeof item === "string").map((item) => item.slice(0, 80)).slice(0, 20)
    : [];
  return {
    id,
    title,
    url,
    ...(inlineHtml ? { inlineHtml } : {}),
    summary,
    icon: iconValue,
    ...(layout !== "half" ? { layout } : {}),
    ...(height ? { height } : {}),
    ...(width ? { width } : {}),
    capabilities
  };
}

export function sanitizeLocalMiniAppDefinition(value: unknown, options: MiniAppSanitizeOptions = {}): MiniAppDefinition | null {
  const definition = sanitizeMiniAppDefinition(value, options);
  if (!definition || !isRecord(value)) {
    return null;
  }
  const scope = normalizeMiniAppScope(recordString(value, "scope"));
  const tunnelId = recordString(value, "tunnelId").slice(0, 120);
  const targetDeviceId = recordString(value, "targetDeviceId").slice(0, 180);
  const revision = recordString(value, "revision").slice(0, 80);
  const installedAt = recordString(value, "installedAt").slice(0, 40);
  const updatedAt = recordString(value, "updatedAt").slice(0, 40);
  return {
    ...definition,
    source: "agent",
    scope,
    ...(scope === "chat" && tunnelId ? { tunnelId } : {}),
    ...(scope === "device" && targetDeviceId ? { targetDeviceId } : {}),
    ...(revision ? { revision } : {}),
    ...(installedAt ? { installedAt } : {}),
    ...(updatedAt ? { updatedAt } : {})
  };
}

export function sameMiniAppRecord(left: MiniAppDefinition, right: MiniAppDefinition): boolean {
  return left.id === right.id
    && (left.scope || "account") === (right.scope || "account")
    && (left.tunnelId || "") === (right.tunnelId || "")
    && (left.targetDeviceId || "") === (right.targetDeviceId || "");
}

export function dedupeMiniApps(apps: readonly MiniAppDefinition[]): MiniAppDefinition[] {
  const seen = new Set<string>();
  const result: MiniAppDefinition[] = [];
  for (const item of apps) {
    if (!item.id || seen.has(item.id)) {
      continue;
    }
    seen.add(item.id);
    result.push(item);
  }
  return result;
}

export function isIconName(value: string): value is IconName {
  return iconNames.has(value as IconName);
}

export function safeMiniAppUrl(value: string, options: MiniAppSanitizeOptions = {}): string {
  try {
    const resolved = resolveAppSurfaceUrl(value, {
      baseUrl: options.baseUrl || globalThis.location?.origin || "https://xn--n1afe0b.online",
      allowLoopbackHttp: true,
      allowTrustedHttps: true,
      kernelIntentSchemes: ["soty:"]
    });
    return resolved.requiresKernelProxy ? "" : resolved.url;
  } catch {
    return "";
  }
}

export function safeMiniAppInlineHtml(value: string): string {
  const raw = String(value || "")
    .replace(/[\u0000-\u0008\u000B\u000C\u000E-\u001F\u007F]/gu, "")
    .trim();
  return raw.length <= 300_000 ? raw : "";
}

function isRecord(value: unknown): value is Record<string, unknown> {
  return typeof value === "object" && value !== null && !Array.isArray(value);
}

function recordString(value: unknown, key: string): string {
  if (!isRecord(value)) {
    return "";
  }
  const item = value[key];
  return typeof item === "string" ? item : "";
}
