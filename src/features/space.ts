import { icon } from "../icons";
import type { IconName } from "../icons";

export type SpaceMode = "dialog" | "wall" | "reputation";
export type SpaceEntryKind = "wall" | "wall-comment" | "reputation" | "reputation-comment";

export interface SpaceEntry {
  readonly id: string;
  readonly kind: SpaceEntryKind;
  readonly author: string;
  readonly authorId: string;
  readonly sourceId?: string;
  readonly text: string;
  readonly createdAt: string;
}

export interface SpaceModel {
  readonly id: string;
  readonly color: string;
  readonly mode: SpaceMode;
  readonly ownSpace?: boolean;
}

export interface SpaceComposerAccess {
  readonly canCompose: boolean;
  readonly entryKind: SpaceEntryKind | null;
  readonly placeholder: string;
}

export interface SpaceMarkDisplay {
  readonly role: "self" | "review";
  readonly icon: IconName;
  readonly className: string;
  readonly actionLabel: string;
  readonly activeLabel: string;
  readonly entryLabel: string;
}

export const spaceWallPrefix = "SOTY_SPACE_WALL:";
export const spaceWallCommentPrefix = "SOTY_SPACE_WALL_COMMENT:";
export const spaceReputationPrefix = "SOTY_SPACE_REPUTATION:";
export const spaceReputationCommentPrefix = "SOTY_SPACE_REPUTATION_COMMENT:";

const legacyCommentPrefix = "SOTY_SPACE_COMMENT:";
const legacyDirectPrefix = "SOTY_SPACE_DIRECT:";

const modes: readonly {
  readonly id: SpaceMode;
  readonly label: string;
  readonly hint: string;
  readonly ariaLabel: string;
  readonly icon: IconName;
}[] = [
  { id: "dialog", label: "Чат", hint: "Чат", ariaLabel: "Чат", icon: "mail" },
  { id: "wall", label: "Я", hint: "Я", ariaLabel: "Я: страница", icon: "hexagon" },
  { id: "reputation", label: "Отзывы", hint: "Отзывы", ariaLabel: "Отзывы", icon: "heart" }
];

const prefixByKind: Record<SpaceEntryKind, string> = {
  wall: spaceWallPrefix,
  "wall-comment": spaceWallCommentPrefix,
  reputation: spaceReputationPrefix,
  "reputation-comment": spaceReputationCommentPrefix
};

export function normalizeSpaceMode(value: string): SpaceMode {
  if (value === "dialog" || value === "wall" || value === "reputation") {
    return value;
  }
  if (value === "self" || value === "space") {
    return "wall";
  }
  if (value === "peer" || value === "comments") {
    return "reputation";
  }
  return "dialog";
}

export function makeSpaceEntryLine(kind: SpaceEntryKind, entry: Omit<SpaceEntry, "id" | "kind" | "createdAt">): string {
  return `${prefixByKind[kind]}${JSON.stringify({
    id: cleanSpaceText(entry.sourceId || "", 180) || crypto.randomUUID(),
    author: cleanSpaceText(entry.author, 48) || "Я",
    authorId: cleanSpaceText(entry.authorId, 120),
    sourceId: cleanSpaceText(entry.sourceId || "", 180),
    text: cleanSpaceText(entry.text, 1200),
    createdAt: new Date().toISOString()
  })}`;
}

export function spaceEntryKindForMessage(local: boolean, ownSpace: boolean): SpaceEntryKind {
  const ownerMessage = ownSpace ? local : !local;
  return ownerMessage ? "wall" : "reputation";
}

export function normalizeSpaceEntryKind(value: string): SpaceEntryKind | null {
  if (value === "wall" || value === "reputation") {
    return value;
  }
  return null;
}

export function spaceMarkDisplay(kind: SpaceEntryKind, ownSpace = true): SpaceMarkDisplay {
  if (kind === "wall" || kind === "wall-comment") {
    if (!ownSpace) {
      return {
        role: "self",
        icon: "hexagon",
        className: "is-hex",
        actionLabel: "На страницу",
        activeLabel: "На странице",
        entryLabel: "Страница"
      };
    }
    return {
      role: "self",
      icon: "hexagon",
      className: "is-hex",
      actionLabel: "Сохранить в Я",
      activeLabel: "В Я",
      entryLabel: "Я"
    };
  }
  return {
    role: "review",
    icon: "heart",
    className: "is-heart",
    actionLabel: "Сохранить как отзыв",
    activeLabel: "Отзыв сохранен",
    entryLabel: "Отзыв"
  };
}

export function parseSpaceEntryLine(line: string): SpaceEntry | null {
  const kind = entryKindForLine(line);
  if (!kind) {
    return null;
  }
  const prefix = entryPrefixForLine(line, kind);
  try {
    const payload = JSON.parse(line.slice(prefix.length)) as unknown;
    if (!isRecord(payload)) {
      return null;
    }
    const text = cleanSpaceText(readString(payload, "text"), 1200);
    if (!text) {
      return null;
    }
    return {
      id: cleanSpaceText(readString(payload, "id"), 120) || kind,
      kind,
      author: cleanSpaceText(readString(payload, "author"), 48) || "Гость",
      authorId: cleanSpaceText(readString(payload, "authorId"), 120),
      ...(readString(payload, "sourceId") ? { sourceId: cleanSpaceText(readString(payload, "sourceId"), 180) } : {}),
      text,
      createdAt: cleanSpaceText(readString(payload, "createdAt"), 40) || new Date().toISOString()
    };
  } catch {
    return null;
  }
}

export function spaceComposerAccess(mode: SpaceMode, label: string, ownSpace: boolean): SpaceComposerAccess {
  if (mode === "wall") {
    void label;
    return { canCompose: false, entryKind: null, placeholder: ownSpace ? "Отмеченное в Я" : "Страница" };
  }
  if (mode === "reputation") {
    return { canCompose: false, entryKind: null, placeholder: "Отзывы из сообщений" };
  }
  return { canCompose: true, entryKind: null, placeholder: ownSpace ? "Заметка" : "Сообщение" };
}

export function renderSpaceRail(model: SpaceModel): string {
  return `
    <div class="space-card" data-mode="${model.mode}" style="--space-color:${escapeAttr(model.color)}">
      <div class="space-map" role="tablist" aria-label="раздел соты">
        ${modes.map((mode) => {
          const display = modeDisplay(mode, model.ownSpace === true);
          return `
          <button class="${[mode.id === model.mode ? "is-active" : "", display.label ? "" : "is-symbol"].filter(Boolean).join(" ")}" type="button" data-space-mode="${mode.id}" role="tab" aria-selected="${mode.id === model.mode ? "true" : "false"}" aria-label="${escapeAttr(display.ariaLabel)}" data-tooltip="${escapeAttr(display.hint)}">
            ${icon(display.icon)}
            ${display.label ? `<b>${escapeHtml(display.label)}</b>` : ""}
          </button>
        `;
        }).join("")}
      </div>
    </div>
  `;
}

function modeDisplay(mode: typeof modes[number], ownSpace: boolean): typeof modes[number] {
  if (!ownSpace && mode.id === "wall") {
    return { ...mode, label: "Страница", hint: "Страница", ariaLabel: "Страница" };
  }
  return ownSpace && mode.id === "dialog"
    ? { ...mode, label: "Заметки", hint: "Заметки", ariaLabel: "Заметки" }
    : mode;
}

export function renderSpaceEntryBubble(entry: SpaceEntry, ownSpace = true): string {
  const label = entryLabel(entry.kind, ownSpace);
  const display = spaceMarkDisplay(entry.kind, ownSpace);
  return `
    <div class="space-entry-bubble" data-kind="${entry.kind}" data-role="${display.role}">
      <span>${escapeHtml(label)}</span>
      <b>${escapeHtml(entry.author)}</b>
      <small>${escapeHtml(shortDate(entry.createdAt))}</small>
      <p>${escapeHtml(entry.text)}</p>
    </div>
  `;
}

function entryKindForLine(line: string): SpaceEntryKind | null {
  if (line.startsWith(spaceWallPrefix)) {
    return "wall";
  }
  if (line.startsWith(spaceWallCommentPrefix)) {
    return "wall-comment";
  }
  if (line.startsWith(spaceReputationPrefix) || line.startsWith(legacyCommentPrefix) || line.startsWith(legacyDirectPrefix)) {
    return "reputation";
  }
  if (line.startsWith(spaceReputationCommentPrefix)) {
    return "reputation-comment";
  }
  return null;
}

function entryPrefixForLine(line: string, kind: SpaceEntryKind): string {
  if (kind === "reputation" && line.startsWith(legacyCommentPrefix)) {
    return legacyCommentPrefix;
  }
  if (kind === "reputation" && line.startsWith(legacyDirectPrefix)) {
    return legacyDirectPrefix;
  }
  return prefixByKind[kind];
}

function entryLabel(kind: SpaceEntryKind, ownSpace: boolean): string {
  if (kind === "wall-comment") {
    return "Комментарий";
  }
  if (kind === "reputation-comment") {
    return "Комментарий";
  }
  return spaceMarkDisplay(kind, ownSpace).entryLabel;
}

function shortDate(value: string): string {
  const date = new Date(value);
  if (Number.isNaN(date.valueOf())) {
    return "";
  }
  return `${date.getDate().toString().padStart(2, "0")}.${(date.getMonth() + 1).toString().padStart(2, "0")}`;
}

function cleanSpaceText(value: string, limit: number): string {
  return value.replace(/\s+/gu, " ").trim().slice(0, limit);
}

function isRecord(value: unknown): value is Record<string, unknown> {
  return typeof value === "object" && value !== null && !Array.isArray(value);
}

function readString(value: Record<string, unknown>, key: string): string {
  const item = value[key];
  return typeof item === "string" ? item : "";
}

function escapeAttr(value: string): string {
  return escapeHtml(value).replace(/"/gu, "&quot;");
}

function escapeHtml(value: string): string {
  return value
    .replace(/&/gu, "&amp;")
    .replace(/</gu, "&lt;")
    .replace(/>/gu, "&gt;")
    .replace(/"/gu, "&quot;")
    .replace(/'/gu, "&#39;");
}
