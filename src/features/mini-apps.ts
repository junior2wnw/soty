import { normalizeAppSurfaceId, resolveAppSurfaceUrl } from "trustlink-kernel";
import type { IconName } from "../icons";
import { cleanNick } from "../trustlink";

export type MiniAppScope = "account" | "chat" | "device";
export type MiniAppVisibility = "private" | "granted-cells" | "my-cells" | "public";
export type MiniAppPlacement = "inline" | "same-origin" | "remote-origin" | "device-local" | "kernel-proxy";
export type MiniAppWindowLayout = "half" | "compact" | "large" | "full" | "floating";

export type MiniAppDefinition = {
  readonly id: string;
  readonly title: string;
  readonly url: string;
  readonly inlineHtml?: string;
  readonly summary: string;
  readonly icon: IconName;
  readonly tags?: readonly string[];
  readonly profileId?: string;
  readonly profileTitle?: string;
  readonly placement?: MiniAppPlacement;
  readonly layout?: MiniAppWindowLayout;
  readonly height?: string;
  readonly width?: string;
  readonly capabilities: readonly string[];
  readonly source?: "manifest" | "agent" | "room";
  readonly scope?: MiniAppScope;
  readonly visibility?: MiniAppVisibility;
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
  "apps",
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

export function normalizeMiniAppVisibility(value: string): MiniAppVisibility {
  const clean = value.trim().toLowerCase().replace(/[\s_]+/gu, "-");
  if (["public", "everyone", "all", "global", "world"].includes(clean)) {
    return "public";
  }
  if (["my-cells", "cells", "added-cells", "contacts", "known-cells", "friends"].includes(clean)) {
    return "my-cells";
  }
  if (["granted-cells", "granted", "access", "allowed", "trusted", "invited", "shared"].includes(clean)) {
    return "granted-cells";
  }
  return "private";
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
  const surface = inlineHtml
    ? { url: "about:srcdoc", placement: "inline" as const }
    : safeMiniAppSurface(recordString(value, "url"), options);
  if (!id || !title || !surface.url) {
    return null;
  }
  const display = isRecord(value.display) ? value.display : value;
  const layout = normalizeMiniAppLayout(recordString(display, "layout") || recordString(value, "layout"));
  const height = safeMiniAppCssSize(recordString(display, "height") || recordString(value, "height"));
  const width = safeMiniAppCssSize(recordString(display, "width") || recordString(value, "width"));
  const tags = safeMiniAppTags(value.tags);
  const profile = miniAppProfile(value);
  const visibility = miniAppVisibility(value);
  const capabilities = Array.isArray(value.capabilities)
    ? value.capabilities.filter((item): item is string => typeof item === "string").map((item) => item.slice(0, 80)).slice(0, 20)
    : [];
  return {
    id,
    title,
    url: surface.url,
    ...(inlineHtml ? { inlineHtml } : {}),
    summary,
    icon: iconValue,
    ...(tags.length > 0 ? { tags } : {}),
    ...(profile.id ? { profileId: profile.id } : {}),
    ...(profile.title ? { profileTitle: profile.title } : {}),
    placement: surface.placement,
    ...(layout !== "half" ? { layout } : {}),
    ...(height ? { height } : {}),
    ...(width ? { width } : {}),
    ...(visibility ? { visibility } : {}),
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
    && (left.targetDeviceId || "") === (right.targetDeviceId || "")
    && (left.profileId || "") === (right.profileId || "");
}

export function miniAppRecordKey(app: MiniAppDefinition): string {
  return [
    app.scope || "account",
    app.tunnelId || "",
    app.targetDeviceId || "",
    app.profileId || "",
    app.id
  ].join("\u001F");
}

export function dedupeMiniApps(apps: readonly MiniAppDefinition[]): MiniAppDefinition[] {
  const seen = new Set<string>();
  const result: MiniAppDefinition[] = [];
  for (const item of apps) {
    const key = miniAppRecordKey(item);
    if (!item.id || seen.has(key)) {
      continue;
    }
    seen.add(key);
    result.push(item);
  }
  return result;
}

export function searchMiniApps(apps: readonly MiniAppDefinition[], query: string): MiniAppDefinition[] {
  const needle = miniAppSearchNeedle(query);
  const indexed = dedupeMiniApps(apps);
  if (!needle) {
    return indexed.sort(miniAppDefaultSort);
  }
  return indexed
    .map((appItem) => ({ appItem, score: miniAppSearchScore(appItem, needle) }))
    .filter((item) => item.score > 0)
    .sort((left, right) => right.score - left.score || miniAppDefaultSort(left.appItem, right.appItem))
    .map((item) => item.appItem);
}

export function miniAppSearchNeedle(value: string): string {
  return String(value || "")
    .toLowerCase()
    .replace(/[^\p{L}\p{N}]+/gu, " ")
    .trim();
}

export function isIconName(value: string): value is IconName {
  return iconNames.has(value as IconName);
}

export function safeMiniAppUrl(value: string, options: MiniAppSanitizeOptions = {}): string {
  return safeMiniAppSurface(value, options).url;
}

function safeMiniAppSurface(value: string, options: MiniAppSanitizeOptions = {}): { readonly url: string; readonly placement: MiniAppPlacement } {
  try {
    const resolved = resolveAppSurfaceUrl(value, {
      baseUrl: options.baseUrl || globalThis.location?.origin || "https://xn--n1afe0b.online",
      allowLoopbackHttp: true,
      allowTrustedHttps: true,
      kernelIntentSchemes: ["soty:"]
    });
    return resolved.requiresKernelProxy ? { url: "", placement: "kernel-proxy" } : { url: resolved.url, placement: resolved.mode };
  } catch {
    return { url: "", placement: "remote-origin" };
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

function safeMiniAppTags(value: unknown): readonly string[] {
  const raw = Array.isArray(value)
    ? value
    : typeof value === "string"
      ? value.split(/[,#;\n]/u)
      : [];
  const seen = new Set<string>();
  const tags: string[] = [];
  for (const item of raw) {
    const tag = cleanNick(String(item || ""))
      .replace(/^#+/u, "")
      .slice(0, 48);
    const key = miniAppSearchNeedle(tag);
    if (!tag || !key || seen.has(key)) {
      continue;
    }
    seen.add(key);
    tags.push(tag);
    if (tags.length >= 24) {
      break;
    }
  }
  return tags;
}

function miniAppProfile(value: Record<string, unknown>): { readonly id: string; readonly title: string } {
  const profile = isRecord(value.profile) ? value.profile : {};
  const rawTitle = recordString(value, "profileTitle")
    || recordString(profile, "title")
    || (typeof value.profile === "string" ? value.profile : "");
  const title = cleanNick(rawTitle).slice(0, 80);
  const id = normalizeAppSurfaceId(recordString(value, "profileId") || recordString(profile, "id") || title, 80);
  return { id, title };
}

function miniAppVisibility(value: Record<string, unknown>): MiniAppVisibility | undefined {
  const raw = recordString(value, "visibility")
    || recordString(value, "share")
    || recordString(value, "sharing");
  return raw ? normalizeMiniAppVisibility(raw) : undefined;
}

function miniAppSearchScore(appItem: MiniAppDefinition, needle: string): number {
  const words = needle.split(" ").filter(Boolean);
  const title = miniAppSearchNeedle(appItem.title);
  const tags = (appItem.tags || []).map(miniAppSearchNeedle);
  const profile = miniAppSearchNeedle(`${appItem.profileTitle || ""} ${appItem.profileId || ""}`);
  const summary = miniAppSearchNeedle(appItem.summary);
  const id = miniAppSearchNeedle(appItem.id);
  const url = miniAppSearchNeedle(appItem.url);
  const visibility = miniAppSearchNeedle(appItem.visibility || "");
  let score = textMatchScore(title, needle, words, 8000);
  score += Math.max(...tags.map((tag) => textMatchScore(tag, needle, words, 3600)), 0);
  score += textMatchScore(profile, needle, words, 1600);
  score += textMatchScore(summary, needle, words, 900);
  score += textMatchScore(id, needle, words, 700);
  score += textMatchScore(url, needle, words, 180);
  score += textMatchScore(visibility, needle, words, 120);
  return score;
}

function textMatchScore(text: string, needle: string, words: readonly string[], weight: number): number {
  if (!text) {
    return 0;
  }
  if (text === needle) {
    return weight + 400;
  }
  if (text.startsWith(needle)) {
    return weight + 240;
  }
  if (text.includes(needle)) {
    return weight + 120;
  }
  return words.reduce((score, word) => score + (text.includes(word) ? Math.round(weight / 8) : 0), 0);
}

function miniAppDefaultSort(left: MiniAppDefinition, right: MiniAppDefinition): number {
  const leftTime = Date.parse(left.updatedAt || left.installedAt || "");
  const rightTime = Date.parse(right.updatedAt || right.installedAt || "");
  if (Number.isFinite(leftTime) || Number.isFinite(rightTime)) {
    return (Number.isFinite(rightTime) ? rightTime : 0) - (Number.isFinite(leftTime) ? leftTime : 0);
  }
  return left.title.localeCompare(right.title) || left.id.localeCompare(right.id);
}
