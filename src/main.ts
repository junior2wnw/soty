import QRCode from "qrcode";
import { JoinRequest, ReceivedFile, RemoteRequest, TunnelSync, WriterActivity } from "./sync";
import { icon } from "./icons";
import { colorFor } from "./core/color";
import { clock } from "./core/time";
import { filesFrom, renderFileRail } from "./features/files";
import { loadRemoteEnabled, setRemoteEnabled } from "./features/remote";
import { openCounterpartyMenu } from "./ui/context-menu";
import { renderHexField } from "./ui/hex-field";
import {
  DeviceRecord,
  JoinInvite,
  TunnelRecord,
  captureJoinInviteFromLocation,
  clearPendingInvite,
  clearPendingJoin,
  cleanNick,
  createJoinKeyPair,
  createDevice,
  createTunnel,
  decryptAcceptedJoin,
  inviteUrl,
  isAppRuntime,
  loadDevice,
  loadPendingJoin,
  loadSelectedTunnelId,
  loadTunnels,
  markTunnel,
  publicJoinJwk,
  removeTunnel,
  rememberAppRuntime,
  saveSelectedTunnelId,
  saveTunnels,
  touchTunnel,
  tunnelFromAcceptedJoin,
  upsertTunnel
} from "./trustlink";
import "./style.css";

interface BeforeInstallPromptEvent extends Event {
  prompt(): Promise<void>;
}

const root = document.querySelector<HTMLDivElement>("#app");
if (!root) {
  throw new Error("App root missing");
}
const app: HTMLDivElement = root;

let installPrompt: BeforeInstallPromptEvent | null = null;
let device: DeviceRecord | null = null;
let tunnels: TunnelRecord[] = [];
let selectedId = "";
let textarea: HTMLTextAreaElement | null = null;
let textPaint: HTMLDivElement | null = null;
let lineGutter: HTMLDivElement | null = null;
let lineMeta: HTMLDivElement | null = null;
let fileInput: HTMLInputElement | null = null;
let deleteModeId = "";
const syncs = new Map<string, TunnelSync>();
const texts = new Map<string, string>();
const peers = new Map<string, string>();
const files = new Map<string, ReceivedFile[]>();
let remoteEnabled = loadRemoteEnabled();
const writerLines = new Map<string, Map<number, {
  readonly nick: string;
  readonly deviceId: string;
  readonly color: string;
  readonly time: string;
  readonly at: number;
}>>();
const activeWriters = new Map<string, string>();
const joinPrompts = new Set<string>();
let joinSocket: WebSocket | null = null;
let joinReconnectTimer = 0;
let joinCompleted = false;

window.addEventListener("beforeinstallprompt", (event) => {
  event.preventDefault();
  installPrompt = event as BeforeInstallPromptEvent;
  if (!isAppRuntime()) {
    renderInstall();
  }
});

window.addEventListener("appinstalled", () => {
  installPrompt = null;
  rememberAppRuntime();
  void boot();
});

void boot();

async function boot(): Promise<void> {
  if ("serviceWorker" in navigator) {
    await navigator.serviceWorker.register("/sw.js");
  }

  const capturedJoin = captureJoinInviteFromLocation();
  const appRuntime = isAppRuntime();
  if (capturedJoin) {
    window.history.replaceState({}, "", appRuntime ? "/?pwa=1" : "/");
  }
  clearPendingInvite();

  if (!appRuntime) {
    renderInstall();
    return;
  }

  device = await loadDevice();
  if (!device) {
    renderNick();
    return;
  }

  const pending = loadPendingJoin();
  if (pending) {
    renderJoinWaiting(pending);
    return;
  }

  tunnels = loadTunnels();
  if (tunnels.length === 0) {
    tunnels = upsertTunnel(createTunnel());
  }
  selectedId = loadSelectedTunnelId() || tunnels[0]?.id || "";
  if (selectedId) {
    saveSelectedTunnelId(selectedId);
  }
  renderApp();
}

function renderInstall(): void {
  app.innerHTML = `
    <section class="install">
      <div class="install-mark">${icon("install")}</div>
      <button class="install-button" type="button" aria-label="install">${icon("install")}</button>
      <p>PWA</p>
    </section>
  `;
  app.querySelector("button")?.addEventListener("click", () => {
    if (installPrompt) {
      void installPrompt.prompt();
      return;
    }
    rememberAppRuntime();
    void boot();
  });
}

function renderNick(): void {
  app.innerHTML = `
    <section class="nick-screen">
      <form class="nick-form">
        <span>${icon("person")}</span>
        <input name="nick" maxlength="32" autocomplete="nickname" autofocus />
        <button type="submit" aria-label="ok">${icon("check")}</button>
      </form>
    </section>
  `;
  const form = app.querySelector<HTMLFormElement>("form");
  const input = app.querySelector<HTMLInputElement>("input");
  input?.focus();
  form?.addEventListener("submit", async (event) => {
    event.preventDefault();
    const nick = cleanNick(new FormData(form).get("nick")?.toString() || "");
    device = await createDevice(nick);
    const pending = loadPendingJoin();
    if (pending) {
      renderJoinWaiting(pending);
      return;
    }
    tunnels = loadTunnels();
    if (tunnels.length === 0) {
      tunnels = upsertTunnel(createTunnel());
    }
    selectedId = tunnels[0]?.id || "";
    if (selectedId) {
      saveSelectedTunnelId(selectedId);
    }
    renderApp();
  });
}

function continueWithoutPending(): void {
  clearPendingJoin();
  window.history.replaceState({}, "", "/?pwa=1");
  tunnels = loadTunnels();
  if (device && tunnels.length === 0) {
    tunnels = upsertTunnel(createTunnel());
  }
  selectedId = loadSelectedTunnelId() || tunnels[0]?.id || "";
  renderApp();
}

function renderJoinWaiting(invite: JoinInvite): void {
  if (!device) {
    return;
  }
  joinSocket?.close();
  window.clearTimeout(joinReconnectTimer);
  joinCompleted = false;
  const nick = cleanNick(invite.fromNick);
  app.innerHTML = `
    <section class="pair-screen">
      <div class="counterparty-mark">
        <span>${escapeHtml(initials(nick))}</span>
        <b>${escapeHtml(nick)}</b>
      </div>
      <div class="pair-actions">
        <button class="icon-button deny-button" type="button" aria-label="close">${icon("close")}</button>
      </div>
    </section>
  `;
  app.querySelector(".deny-button")?.addEventListener("click", () => {
    joinCompleted = true;
    joinSocket?.close();
    continueWithoutPending();
  });
  void startJoinRequest(invite);
}

async function startJoinRequest(invite: JoinInvite): Promise<void> {
  if (!device) {
    return;
  }
  const requestId = `join_${crypto.randomUUID()}`;
  const pair = await createJoinKeyPair();
  const publicJwk = await publicJoinJwk(pair);
  const connect = () => {
    if (!device || joinCompleted) {
      return;
    }
    const protocol = window.location.protocol === "https:" ? "wss:" : "ws:";
    const ws = new WebSocket(`${protocol}//${window.location.host}/ws/${invite.roomId}`);
    joinSocket = ws;
    ws.onopen = () => {
      ws.send(JSON.stringify({
        type: "hello",
        deviceId: device?.id,
        nick: device?.nick,
        joinRequest: {
          requestId,
          publicJwk
        }
      }));
    };
    ws.onmessage = (event) => {
      void (async () => {
        let message: {
          readonly type: string;
          readonly requestId?: string;
          readonly accept?: import("./trustlink").JoinAcceptPayload;
        };
        try {
          message = JSON.parse(event.data as string) as {
            readonly type: string;
            readonly requestId?: string;
            readonly accept?: import("./trustlink").JoinAcceptPayload;
          };
        } catch {
          return;
        }
        if (message.requestId && message.requestId !== requestId) {
          return;
        }
        if (message.type === "join.accepted" && message.accept) {
          joinCompleted = true;
          const roomKey = await decryptAcceptedJoin(pair.privateKey, message.accept);
          const tunnel = tunnelFromAcceptedJoin(invite, roomKey);
          tunnels = upsertTunnel(tunnel);
          selectedId = tunnel.id;
          saveSelectedTunnelId(selectedId);
          clearPendingJoin();
          window.history.replaceState({}, "", "/?pwa=1");
          ws.close();
          renderApp();
        }
        if (message.type === "join.denied" || message.type === "closed") {
          joinCompleted = true;
          ws.close();
          continueWithoutPending();
        }
      })();
    };
    ws.onerror = () => {
      ws.close();
    };
    ws.onclose = () => {
      if (!joinCompleted) {
        joinReconnectTimer = window.setTimeout(connect, 1200);
      }
    };
  };
  connect();
}

function renderApp(): void {
  if (!device) {
    return;
  }
  tunnels = loadTunnels();
  if (tunnels.length === 0) {
    tunnels = upsertTunnel(createTunnel());
    selectedId = tunnels[0]?.id || "";
  }
  selectedId = selectedId || loadSelectedTunnelId() || tunnels[0]?.id || "";
  for (const tunnel of tunnels) {
    ensureSync(tunnel);
  }

  const topHeight = localStorage.getItem("soty:split:v1") || "168";
  const hasVisibleTunnels = sortedVisibleTunnels().length > 0;
  app.innerHTML = `
    <section class="shell" style="--top:${topHeight}px">
      <header class="tiles${hasVisibleTunnels ? "" : " empty"}">
        <div class="hex-field"></div>
      </header>
      <button class="splitter" type="button" aria-label="resize"></button>
      <section class="editor">
        <div class="text-paint" aria-hidden="true"><div class="text-paint-inner"></div></div>
        <div class="line-gutter"></div>
        <div class="line-meta"></div>
        <div class="writer-pop"></div>
        <div class="file-rail"></div>
        <textarea spellcheck="false" autocapitalize="sentences"></textarea>
        <input class="file-input" type="file" multiple />
      </section>
    </section>
  `;

  textarea = app.querySelector("textarea");
  textPaint = app.querySelector<HTMLDivElement>(".text-paint-inner");
  lineGutter = app.querySelector(".line-gutter");
  lineMeta = app.querySelector(".line-meta");
  fileInput = app.querySelector(".file-input");
  renderTiles();
  textarea?.addEventListener("input", () => {
    const sync = syncs.get(selectedId);
    if (sync && textarea) {
      const current = texts.get(selectedId) ?? "";
      const normalized = normalizeLocalEdit(current, textarea.value, textarea.selectionStart);
      if (normalized.text !== textarea.value) {
        textarea.value = normalized.text;
        textarea.setSelectionRange(normalized.caret, normalized.caret);
      }
      sync.setText(textarea.value);
      touchSelected();
      renderTextPaint();
    }
  });
  textarea?.addEventListener("scroll", () => {
    renderLineTags();
    renderTextPaint();
  });
  textarea?.addEventListener("dragover", (event) => {
    event.preventDefault();
    app.querySelector(".editor")?.classList.add("dropping");
  });
  textarea?.addEventListener("dragleave", () => {
    app.querySelector(".editor")?.classList.remove("dropping");
  });
  textarea?.addEventListener("drop", (event) => {
    event.preventDefault();
    app.querySelector(".editor")?.classList.remove("dropping");
    void sendFiles(event.dataTransfer?.files);
  });
  fileInput?.addEventListener("change", () => {
    void sendFiles(fileInput?.files);
    if (fileInput) {
      fileInput.value = "";
    }
  });
  setupSplitter();
  applySelectedText();
  renderFiles();
}

function renderTiles(): void {
  const field = app.querySelector<HTMLDivElement>(".hex-field");
  if (!field) {
    return;
  }
  tunnels = loadTunnels();
  const sorted = sortedVisibleTunnels();
  if (sorted.length === 0) {
    renderHexField(field, [], {
      select: () => undefined,
      menu: () => undefined,
      qr: () => {
        void showQr();
      }
    }, { emptyQr: true });
    void paintInlineQr(field);
    return;
  }
  renderHexField(field, sorted.map((tunnel) => ({
    id: tunnel.id,
    label: counterpartyLabel(tunnel),
    color: tunnel.color || colorFor(counterpartyLabel(tunnel) + tunnel.id),
    active: tunnel.id === selectedId,
    unread: tunnel.unread
  })), {
    select: (id) => {
      selectedId = id;
      saveSelectedTunnelId(id);
      tunnels = markTunnel(id, false);
      renderTiles();
      applySelectedText(true);
      renderFiles();
    },
    menu: (id, x, y) => {
      openCounterpartyMenu(x, y, {
        attach: () => {
          selectedId = id;
          saveSelectedTunnelId(id);
          fileInput?.click();
        },
        remote: () => {
          selectedId = id;
          saveSelectedTunnelId(id);
          syncs.get(id)?.requestRemote();
        },
        close: () => closeTunnel(id)
      }, {
        remoteEnabled: remoteEnabled.has(id)
      });
    }
  });
}

function sortedVisibleTunnels(): TunnelRecord[] {
  return loadTunnels()
    .filter((tunnel) => hasCounterparty(tunnel))
    .sort((a, b) => {
      const score = (b.score ?? 0) - (a.score ?? 0);
      if (score !== 0) {
        return score;
      }
      return Date.parse(b.lastActionAt || b.updatedAt) - Date.parse(a.lastActionAt || a.updatedAt);
    });
}

function hasCounterparty(tunnel: TunnelRecord): boolean {
  const label = cleanNick(peers.get(tunnel.id) || tunnel.label || "");
  return Boolean(tunnel.counterparty || peers.has(tunnel.id) || (label !== "." && label !== device?.nick));
}

function counterpartyLabel(tunnel: TunnelRecord): string {
  return cleanNick(peers.get(tunnel.id) || tunnel.label || ".");
}

function ensureSync(tunnel: TunnelRecord): void {
  if (!device || syncs.has(tunnel.id)) {
    return;
  }
  syncs.set(tunnel.id, new TunnelSync(tunnel, device, {
    onText: (text) => {
      texts.set(tunnel.id, text);
      if (tunnel.id === selectedId) {
        applySelectedText();
      }
    },
    onActivity: (activity) => {
      rememberWriter(tunnel.id, activity);
      touchSelected();
      if (tunnel.id === selectedId) {
        renderLineTags();
        renderTextPaint();
      }
    },
    onRemoteChange: (activity) => {
      rememberWriter(tunnel.id, activity);
      activeWriters.set(tunnel.id, activity.nick);
      window.setTimeout(() => {
        if (activeWriters.get(tunnel.id) === activity.nick) {
          activeWriters.delete(tunnel.id);
          if (tunnel.id === selectedId) {
            renderWriterPop();
          }
        }
      }, 1800);
      if (tunnel.id !== selectedId) {
        tunnels = markTunnel(tunnel.id, true);
        renderTiles();
      } else {
        tunnels = touchTunnel(tunnel.id);
        renderLineTags();
        renderTextPaint();
        renderWriterPop();
      }
    },
    onFile: (file) => {
      const next = [file, ...(files.get(tunnel.id) ?? []).filter((item) => item.id !== file.id)];
      files.set(tunnel.id, next);
      tunnels = tunnel.id === selectedId ? touchTunnel(tunnel.id) : markTunnel(tunnel.id, true);
      if (tunnel.id === selectedId) {
        renderFiles();
      }
      renderTiles();
    },
    onRemoteRequest: (request) => {
      renderRemoteConfirm(tunnel, request);
    },
    onRemoteResponse: (response) => {
      remoteEnabled = setRemoteEnabled(tunnel.id, response.accepted);
      renderTiles();
    },
    onPeers: (items) => {
      const label = items.map((item) => item.nick).filter(Boolean).join(" ");
      if (label) {
        setTunnelCounterparty(tunnel.id, label);
        renderTiles();
      }
    },
    onJoinRequest: (request) => {
      renderOwnerJoinConfirm(tunnel, request);
    },
    onClosed: () => {
      syncs.get(tunnel.id)?.destroy();
      syncs.delete(tunnel.id);
      writerLines.delete(tunnel.id);
      activeWriters.delete(tunnel.id);
      tunnels = removeTunnel(tunnel.id);
      selectedId = loadSelectedTunnelId() || tunnels[0]?.id || "";
      renderApp();
    },
    onState: () => undefined
  }));
}

function setTunnelCounterparty(tunnelId: string, label: string): void {
  const safeLabel = cleanNick(label);
  peers.set(tunnelId, safeLabel);
  const now = new Date().toISOString();
  const next = loadTunnels().map((item) => item.id === tunnelId
    ? {
      ...item,
      label: safeLabel,
      color: item.color || colorFor(safeLabel + tunnelId),
      counterparty: true,
      score: (item.score ?? 0) + 1,
      updatedAt: now,
      lastActionAt: now
    }
    : item);
  saveTunnels(next);
  tunnels = next;
}

function renderOwnerJoinConfirm(tunnel: TunnelRecord, request: JoinRequest): void {
  if (!device || joinPrompts.has(request.requestId) || request.deviceId === device.id) {
    return;
  }
  joinPrompts.add(request.requestId);
  const nick = cleanNick(request.nick);
  const overlay = document.createElement("div");
  overlay.className = "pair-modal";
  overlay.innerHTML = `
    <div class="pair-screen">
      <div class="counterparty-mark">
        <span>${escapeHtml(initials(nick))}</span>
        <b>${escapeHtml(nick)}</b>
      </div>
      <div class="pair-actions">
        <button class="icon-button deny-button" type="button" aria-label="close">${icon("close")}</button>
        <button class="icon-button accept-button" type="button" aria-label="ok">${icon("check")}</button>
      </div>
    </div>
  `;
  const remove = () => {
    joinPrompts.delete(request.requestId);
    overlay.remove();
  };
  overlay.querySelector(".accept-button")?.addEventListener("click", () => {
    setTunnelCounterparty(tunnel.id, nick);
    selectedId = tunnel.id;
    saveSelectedTunnelId(selectedId);
    syncs.get(tunnel.id)?.acceptJoin(request, device?.nick || ".");
    renderTiles();
    applySelectedText();
    remove();
  });
  overlay.querySelector(".deny-button")?.addEventListener("click", () => {
    syncs.get(tunnel.id)?.denyJoin(request);
    remove();
  });
  document.body.append(overlay);
}

function renderRemoteConfirm(tunnel: TunnelRecord, request: RemoteRequest): void {
  if (!device || request.deviceId === device.id) {
    return;
  }
  const nick = cleanNick(request.nick);
  const overlay = document.createElement("div");
  overlay.className = "pair-modal";
  overlay.innerHTML = `
    <div class="pair-screen">
      <div class="counterparty-mark">
        <span>${icon("remote")}</span>
        <b>${escapeHtml(nick)}</b>
      </div>
      <div class="pair-actions">
        <button class="icon-button deny-button" type="button" aria-label="close">${icon("close")}</button>
        <button class="icon-button accept-button" type="button" aria-label="ok">${icon("check")}</button>
      </div>
    </div>
  `;
  overlay.querySelector(".accept-button")?.addEventListener("click", () => {
    remoteEnabled = setRemoteEnabled(tunnel.id, true);
    syncs.get(tunnel.id)?.respondRemote(request.id, true);
    renderTiles();
    overlay.remove();
  });
  overlay.querySelector(".deny-button")?.addEventListener("click", () => {
    remoteEnabled = setRemoteEnabled(tunnel.id, false);
    syncs.get(tunnel.id)?.respondRemote(request.id, false);
    renderTiles();
    overlay.remove();
  });
  document.body.append(overlay);
}

function closeTunnel(id: string): void {
  const sync = syncs.get(id);
  sync?.closeForEveryone();
  syncs.delete(id);
  remoteEnabled = setRemoteEnabled(id, false);
  writerLines.delete(id);
  activeWriters.delete(id);
  files.delete(id);
  tunnels = removeTunnel(id);
  selectedId = tunnels[0]?.id || "";
  if (selectedId) {
    saveSelectedTunnelId(selectedId);
  }
  renderApp();
}

async function sendFiles(list?: FileList | null): Promise<void> {
  if (!selectedId) {
    return;
  }
  const sync = syncs.get(selectedId);
  if (!sync) {
    return;
  }
  for (const file of filesFrom(list)) {
    await sync.sendFile(file);
    const localFile: ReceivedFile = {
      id: `local_${crypto.randomUUID()}`,
      name: file.name,
      type: file.type || "application/octet-stream",
      size: file.size,
      bytes: new Uint8Array(),
      url: URL.createObjectURL(file),
      nick: device?.nick || "",
      deviceId: device?.id || "",
      createdAt: new Date().toISOString()
    };
    files.set(selectedId, [localFile, ...(files.get(selectedId) ?? [])]);
  }
  tunnels = touchTunnel(selectedId);
  renderTiles();
  renderFiles();
}

function renderFiles(): void {
  const rail = app.querySelector<HTMLDivElement>(".file-rail");
  if (!rail) {
    return;
  }
  const tunnel = loadTunnels().find((item) => item.id === selectedId);
  const color = tunnel?.color || colorFor((tunnel?.label || selectedId) + selectedId);
  renderFileRail(rail, files.get(selectedId) ?? [], color);
}

function touchSelected(): void {
  if (selectedId) {
    tunnels = touchTunnel(selectedId);
  }
}

function applySelectedText(focus = false): void {
  if (!textarea) {
    return;
  }
  const next = texts.get(selectedId) || "";
  if (textarea.value !== next) {
    const before = textarea.value;
    const start = textarea.selectionStart;
    const end = textarea.selectionEnd;
    const [changeAt, deleteCount, inserted] = diffPlain(before, next);
    const mapPosition = (position: number) => {
      if (position <= changeAt) {
        return position;
      }
      if (position >= changeAt + deleteCount) {
        return Math.max(0, position + inserted.length - deleteCount);
      }
      return changeAt + inserted.length;
    };
    textarea.value = next;
    textarea.setSelectionRange(
      Math.min(mapPosition(start), next.length),
      Math.min(mapPosition(end), next.length)
    );
  }
  if (focus) {
    textarea.focus();
  }
  renderLineTags();
  renderTextPaint();
  renderWriterPop();
}

function rememberWriter(tunnelId: string, activity: WriterActivity): void {
  const label = cleanNick(activity.nick);
  const text = tunnelId === selectedId && textarea ? textarea.value : texts.get(tunnelId) || "";
  const line = lineFromIndex(text, activity.index);
  const color = colorFor(`${label}:${activity.deviceId || tunnelId}`);
  const lines = writerLines.get(tunnelId) ?? new Map<number, {
    readonly nick: string;
    readonly deviceId: string;
    readonly color: string;
    readonly time: string;
    readonly at: number;
  }>();
  lines.set(line, {
    nick: label,
    deviceId: activity.deviceId,
    color,
    time: clock(),
    at: Date.now()
  });
  writerLines.set(tunnelId, lines);
}

function renderLineTags(): void {
  if (!textarea || !lineGutter || !lineMeta) {
    return;
  }
  const text = textarea.value;
  const scrollTop = textarea.scrollTop;
  const count = Math.max(1, text.split("\n").length);
  const labels = writerLines.get(selectedId) ?? new Map<number, { readonly nick: string; readonly color: string; readonly time: string }>();
  const lineHeight = Number.parseFloat(getComputedStyle(textarea).lineHeight) || 26;
  lineGutter.innerHTML = Array.from({ length: count }, (_item, line) => {
    const label = labels.get(line);
    return label
      ? `<span style="top:${line * lineHeight - scrollTop}px;--color:${label.color}">${escapeHtml(initials(label.nick))}</span>`
      : "";
  }).join("");
  lineMeta.innerHTML = Array.from({ length: count }, (_item, line) => {
    const label = labels.get(line);
    return label
      ? `<span style="top:${line * lineHeight - scrollTop}px;--color:${label.color}">${escapeHtml(label.time)}</span>`
      : "";
  }).join("");
}

function renderTextPaint(): void {
  if (!textarea || !textPaint) {
    return;
  }
  const labels = writerLines.get(selectedId) ?? new Map();
  const lines = textarea.value.split("\n");
  textPaint.style.transform = `translateY(${-textarea.scrollTop}px)`;
  textPaint.innerHTML = lines.map((line, index) => {
    const label = labels.get(index);
    const color = label?.color || "#141414";
    return `<span class="paint-line" style="--line-color:${color}">${line ? escapeHtml(line) : "&nbsp;"}</span>`;
  }).join("");
}

function renderWriterPop(): void {
  const pop = app.querySelector<HTMLDivElement>(".writer-pop");
  if (!pop) {
    return;
  }
  const nick = activeWriters.get(selectedId);
  pop.innerHTML = nick ? `<span>${escapeHtml(initials(nick))}</span><b>${escapeHtml(nick)}</b>` : "";
}

function lineFromIndex(text: string, index: number): number {
  const safeIndex = Math.max(0, Math.min(index, text.length));
  const adjusted = text[safeIndex] === "\n" ? safeIndex + 1 : safeIndex;
  return text.slice(0, adjusted).split("\n").length - 1;
}

function normalizeLocalEdit(before: string, next: string, caret: number): { readonly text: string; readonly caret: number } {
  if (!device || before === next) {
    return { text: next, caret };
  }
  const [start, deleteCount, insertText] = diffPlain(before, next);
  if (deleteCount > 0 || insertText.length === 0) {
    return { text: next, caret };
  }
  const line = lineFromIndex(before, start);
  const owner = writerLines.get(selectedId)?.get(line);
  const isFreshRemoteLine = owner && owner.deviceId !== device.id && Date.now() - owner.at < 8000;
  if (!isFreshRemoteLine) {
    return { text: next, caret };
  }
  const lineEnd = endOfLine(before, line);
  const separator = insertText.startsWith("\n") || (lineEnd > 0 && before[lineEnd - 1] === "\n") ? "" : "\n";
  const text = `${before.slice(0, lineEnd)}${separator}${insertText}${before.slice(lineEnd)}`;
  return {
    text,
    caret: lineEnd + separator.length + insertText.length
  };
}

function endOfLine(text: string, line: number): number {
  let cursor = 0;
  for (let current = 0; current < line; current += 1) {
    const nextBreak = text.indexOf("\n", cursor);
    if (nextBreak === -1) {
      return text.length;
    }
    cursor = nextBreak + 1;
  }
  const lineBreak = text.indexOf("\n", cursor);
  return lineBreak === -1 ? text.length : lineBreak;
}

function diffPlain(before: string, after: string): [number, number, string] {
  let start = 0;
  while (start < before.length && start < after.length && before[start] === after[start]) {
    start += 1;
  }
  let beforeEnd = before.length;
  let afterEnd = after.length;
  while (beforeEnd > start && afterEnd > start && before[beforeEnd - 1] === after[afterEnd - 1]) {
    beforeEnd -= 1;
    afterEnd -= 1;
  }
  return [start, beforeEnd - start, after.slice(start, afterEnd)];
}

async function showQr(): Promise<void> {
  if (!device) {
    return;
  }
  let tunnel = loadTunnels().find((item) => item.id === selectedId);
  if (!tunnel) {
    tunnel = createTunnel();
    tunnels = upsertTunnel(tunnel);
    selectedId = tunnel.id;
    renderApp();
  }
  const url = await inviteUrl(tunnel, device);
  const overlay = document.createElement("div");
  overlay.className = "qr-modal";
  overlay.innerHTML = `
    <div>
      <canvas></canvas>
      <button class="icon-button" type="button" aria-label="close">${icon("close")}</button>
    </div>
  `;
  document.body.append(overlay);
  const canvas = overlay.querySelector("canvas");
  if (canvas) {
    await QRCode.toCanvas(canvas, url, {
      margin: 1,
      scale: 8,
      color: {
        dark: "#141414",
        light: "#f8f7f2"
      }
    });
  }
  overlay.querySelector("button")?.addEventListener("click", () => overlay.remove());
}

async function paintInlineQr(field: HTMLElement): Promise<void> {
  if (!device) {
    return;
  }
  const canvas = field.querySelector<HTMLCanvasElement>(".qr-hex canvas");
  const fallback = field.querySelector<HTMLElement>(".qr-fallback");
  if (!canvas) {
    return;
  }
  let tunnel = loadTunnels().find((item) => item.id === selectedId) || loadTunnels()[0];
  if (!tunnel) {
    tunnel = createTunnel();
    tunnels = upsertTunnel(tunnel);
    selectedId = tunnel.id;
  }
  const url = await inviteUrl(tunnel, device);
  await QRCode.toCanvas(canvas, url, {
    margin: 1,
    scale: 5,
    color: {
      dark: "#141414",
      light: "#fbfaf6"
    }
  });
  if (fallback) {
    fallback.hidden = true;
  }
}

function setupSplitter(): void {
  const shell = app.querySelector<HTMLElement>(".shell");
  const splitter = app.querySelector<HTMLElement>(".splitter");
  if (!shell || !splitter) {
    return;
  }
  let dragging = false;
  const move = (clientY: number) => {
    const rect = shell.getBoundingClientRect();
    const top = Math.max(72, Math.min(rect.height * 0.45, clientY - rect.top));
    shell.style.setProperty("--top", `${top}px`);
    localStorage.setItem("soty:split:v1", String(Math.round(top)));
  };
  splitter.addEventListener("pointerdown", (event) => {
    dragging = true;
    splitter.setPointerCapture(event.pointerId);
  });
  splitter.addEventListener("pointermove", (event) => {
    if (dragging) {
      move(event.clientY);
    }
  });
  splitter.addEventListener("pointerup", () => {
    dragging = false;
  });
}

function escapeHtml(value: string): string {
  return value.replace(/[&<>"']/gu, (char) => ({
    "&": "&amp;",
    "<": "&lt;",
    ">": "&gt;",
    "\"": "&quot;",
    "'": "&#39;"
  })[char] || char);
}

function initials(value: string): string {
  const parts = cleanNick(value).split(" ").filter(Boolean);
  const letters = parts.length > 1
    ? `${parts[0]?.[0] ?? ""}${parts[1]?.[0] ?? ""}`
    : cleanNick(value).slice(0, 2);
  return letters || ".";
}
