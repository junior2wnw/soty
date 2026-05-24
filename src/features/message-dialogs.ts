export type MessageDialogVisibility = "common" | "invited" | "my-cells" | "everyone";

export interface MessageDialogTarget {
  readonly visibility: MessageDialogVisibility;
  readonly invitedDeviceIds?: readonly string[];
  readonly cellIds?: readonly string[];
}

export interface MessageDialogEntry {
  readonly id: string;
  readonly chatId: string;
  readonly sourceId: string;
  readonly sourceText: string;
  readonly sourceAuthor: string;
  readonly author: string;
  readonly authorId: string;
  readonly text: string;
  readonly target: MessageDialogTarget;
  readonly createdAt: string;
}

export interface MessageDialogViewer {
  readonly deviceId: string;
  readonly cellIds?: readonly string[];
}

export const messageDialogPrefix = "SOTY_MESSAGE_DIALOG:";

export function commonMessageDialogTarget(): MessageDialogTarget {
  return { visibility: "common" };
}

export function messageDialogId(chatId: string, sourceId: string): string {
  return `msgdlg_${hashCompact(`${chatId}:${sourceId}`)}`;
}

export function createMessageDialogLine(input: {
  readonly chatId: string;
  readonly sourceId: string;
  readonly sourceText: string;
  readonly sourceAuthor: string;
  readonly author: string;
  readonly authorId: string;
  readonly text: string;
  readonly target?: MessageDialogTarget;
  readonly createdAt?: string;
}): string {
  const chatId = cleanToken(input.chatId, 180);
  const sourceId = cleanToken(input.sourceId, 220);
  const text = cleanText(input.text, 1800);
  if (!chatId || !sourceId || !text) {
    return "";
  }
  return `${messageDialogPrefix}${JSON.stringify({
    v: 1,
    id: messageDialogId(chatId, sourceId),
    chatId,
    sourceId,
    sourceText: cleanText(input.sourceText, 900),
    sourceAuthor: cleanText(input.sourceAuthor, 80),
    author: cleanText(input.author, 80) || "Me",
    authorId: cleanToken(input.authorId, 180),
    text,
    target: normalizeMessageDialogTarget(input.target),
    createdAt: cleanText(input.createdAt || new Date().toISOString(), 48)
  })}`;
}

export function parseMessageDialogLine(line: string): MessageDialogEntry | null {
  if (!isMessageDialogLine(line)) {
    return null;
  }
  try {
    const payload = JSON.parse(line.slice(messageDialogPrefix.length)) as unknown;
    if (!isRecord(payload)) {
      return null;
    }
    const chatId = cleanToken(readString(payload, "chatId"), 180);
    const sourceId = cleanToken(readString(payload, "sourceId"), 220);
    const text = cleanText(readString(payload, "text"), 1800);
    if (!chatId || !sourceId || !text) {
      return null;
    }
    return {
      id: cleanToken(readString(payload, "id"), 140) || messageDialogId(chatId, sourceId),
      chatId,
      sourceId,
      sourceText: cleanText(readString(payload, "sourceText"), 900),
      sourceAuthor: cleanText(readString(payload, "sourceAuthor"), 80),
      author: cleanText(readString(payload, "author"), 80) || "Guest",
      authorId: cleanToken(readString(payload, "authorId"), 180),
      text,
      target: normalizeMessageDialogTarget(readRecord(payload, "target")),
      createdAt: cleanText(readString(payload, "createdAt"), 48) || new Date().toISOString()
    };
  } catch {
    return null;
  }
}

export function isMessageDialogLine(line: string): boolean {
  return line.startsWith(messageDialogPrefix);
}

export function messageDialogVisibleForTarget(entry: MessageDialogEntry, viewer: MessageDialogViewer): boolean {
  const visibility = entry.target.visibility;
  if (visibility === "common" || visibility === "everyone") {
    return true;
  }
  if (visibility === "invited") {
    return Boolean(viewer.deviceId && entry.target.invitedDeviceIds?.includes(viewer.deviceId));
  }
  if (visibility === "my-cells") {
    const cells = new Set(viewer.cellIds ?? []);
    return Boolean(entry.target.cellIds?.some((id) => cells.has(id)));
  }
  return true;
}

function normalizeMessageDialogTarget(value: unknown): MessageDialogTarget {
  const record = isRecord(value) ? value : {};
  const rawVisibility = readString(record, "visibility");
  const visibility: MessageDialogVisibility =
    rawVisibility === "invited" || rawVisibility === "my-cells" || rawVisibility === "everyone"
      ? rawVisibility
      : "common";
  return {
    visibility,
    ...(visibility === "invited" ? { invitedDeviceIds: cleanList(record.invitedDeviceIds, 120) } : {}),
    ...(visibility === "my-cells" ? { cellIds: cleanList(record.cellIds, 120) } : {})
  };
}

function cleanList(value: unknown, limit: number): readonly string[] {
  if (!Array.isArray(value)) {
    return [];
  }
  return [...new Set(value.map((item) => cleanToken(String(item || ""), 180)).filter(Boolean))].slice(0, limit);
}

function cleanText(value: string, limit: number): string {
  return String(value || "").replace(/\s+/gu, " ").trim().slice(0, limit);
}

function cleanToken(value: string, limit: number): string {
  return String(value || "").replace(/[^A-Za-z0-9._:-]/gu, "_").slice(0, limit);
}

function hashCompact(value: string): string {
  let hash = 2166136261;
  for (let index = 0; index < value.length; index += 1) {
    hash ^= value.charCodeAt(index);
    hash = Math.imul(hash, 16777619);
  }
  return (hash >>> 0).toString(36);
}

function isRecord(value: unknown): value is Record<string, unknown> {
  return typeof value === "object" && value !== null && !Array.isArray(value);
}

function readRecord(value: Record<string, unknown>, key: string): Record<string, unknown> | null {
  const item = value[key];
  return isRecord(item) ? item : null;
}

function readString(value: Record<string, unknown>, key: string): string {
  const item = value[key];
  return typeof item === "string" ? item : "";
}
