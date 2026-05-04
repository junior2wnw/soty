import QRCode from "qrcode";
import jsQR from "jsqr";
import { JoinRequest, NoticeKnock, ReceivedFile, RemoteCommand, RemoteGrant, RemoteOutput, RemoteRequest, RemoteScript, TunnelSync, WriterActivity } from "./sync";
import { icon } from "./icons";
import { colorFor, safeColor } from "./core/color";
import { clock } from "./core/time";
import { checkLocalAgent, downloadAgentInstaller, isWindowsPlatform } from "./features/agent";
import type { LocalAgentStatus } from "./features/agent";
import { filesFrom, renderFileRail } from "./features/files";
import { clearRemoteSessionState, loadRemoteAccess, loadRemoteEnabled, setRemoteAccess, setRemoteEnabled } from "./features/remote";
import { openCounterpartyMenu } from "./ui/context-menu";
import { renderHexField } from "./ui/hex-field";
import { installTooltips } from "./ui/tooltips";
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
  resetLocalSotyState,
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

type BarcodeResult = {
  readonly rawValue?: string;
};

type BarcodeDetectorLike = {
  detect(source: HTMLVideoElement): Promise<BarcodeResult[]>;
};

type BarcodeDetectorConstructor = new (options: { formats: string[] }) => BarcodeDetectorLike;

const root = document.querySelector<HTMLDivElement>("#app");
if (!root) {
  throw new Error("App root missing");
}
const app: HTMLDivElement = root;
installTooltips();

let installPrompt: BeforeInstallPromptEvent | null = null;
let device: DeviceRecord | null = null;
let tunnels: TunnelRecord[] = [];
let selectedId = "";
let textarea: HTMLTextAreaElement | null = null;
let composer: HTMLTextAreaElement | null = null;
let textPaint: HTMLDivElement | null = null;
let lineGutter: HTMLDivElement | null = null;
let lineMeta: HTMLDivElement | null = null;
let fileInput: HTMLInputElement | null = null;
const syncs = new Map<string, TunnelSync>();
const texts = new Map<string, string>();
const peers = new Map<string, string>();
const files = new Map<string, ReceivedFile[]>();
const localDrafts = new Map<string, string>();
let remoteEnabled = loadRemoteEnabled();
let remoteAccess = loadRemoteAccess();
let terminalOpenId = "";
const terminalLogs = new Map<string, string[]>();
const terminalState = new Map<string, "idle" | "run" | "ok" | "bad" | "off">();
let localAgent: LocalAgentStatus = { ok: false };
let agentProbeTimer = 0;
let agentProbe: Promise<LocalAgentStatus> | null = null;
type WriterLine = {
  readonly nick: string;
  readonly deviceId: string;
  readonly color: string;
  readonly time: string;
  readonly at: number;
  readonly action: WriterActivity["action"];
  readonly preview: string;
};

const writerLines = new Map<string, Map<number, WriterLine>>();
const activeActivities = new Map<string, WriterActivity>();
const activeActivityTicks = new Map<string, number>();
const activeNoticeKeys = new Set<string>();
const lastTypingNoticeAt = new Map<string, number>();
const joinPrompts = new Set<string>();
let joinSocket: WebSocket | null = null;
let joinReconnectTimer = 0;
let joinCompleted = false;
let qrOverlay: HTMLDivElement | null = null;
let qrMode: "manual" | "auto" | null = null;
let qrResetClicks = 0;
let qrResetTimer = 0;
let qrScanStream: MediaStream | null = null;
let qrScanFrame = 0;
let operatorSocket: WebSocket | null = null;
let operatorReconnectTimer = 0;
const operatorPending = new Map<string, string>();
const operatorChatQueues = new Map<string, Promise<void>>();

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

document.addEventListener("visibilitychange", () => {
  if (document.visibilityState === "visible" && selectedId) {
    clearTunnelNotices(selectedId);
    tunnels = markTunnel(selectedId, false);
    renderTiles();
  }
});

void boot();

let serviceWorkerReloading = false;

async function boot(): Promise<void> {
  if (shouldResetLocalState()) {
    await resetLocalSotyState();
    clearRemoteSessionState();
    remoteEnabled = loadRemoteEnabled();
    remoteAccess = loadRemoteAccess();
    terminalOpenId = "";
    rememberAppRuntime();
    window.history.replaceState({}, "", "/?pwa=1");
  }

  await registerServiceWorker();

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

async function registerServiceWorker(): Promise<void> {
  if (!("serviceWorker" in navigator)) {
    return;
  }

  try {
    const hadController = Boolean(navigator.serviceWorker.controller);
    const registration = await navigator.serviceWorker.register("/sw.js");

    navigator.serviceWorker.addEventListener("controllerchange", () => {
      if (!hadController || serviceWorkerReloading) {
        return;
      }
      serviceWorkerReloading = true;
      window.location.reload();
    });

    if (registration.waiting) {
      registration.waiting.postMessage({ type: "skipWaiting" });
    }

    registration.addEventListener("updatefound", () => {
      const worker = registration.installing;
      worker?.addEventListener("statechange", () => {
        if (worker.state === "installed" && navigator.serviceWorker.controller) {
          worker.postMessage({ type: "skipWaiting" });
        }
      });
    });

    void registration.update();
  } catch (error) {
    console.warn("[soty] Service worker registration failed", error);
  }
}

function shouldResetLocalState(): boolean {
  const url = new URL(window.location.href);
  return url.searchParams.get("reset-local") === "1"
    || url.searchParams.get("soty-reset") === "1"
    || url.searchParams.get("repair") === "reset";
}

function renderInstall(): void {
  app.innerHTML = `
    <section class="install">
      <div class="install-mark">${icon("install")}</div>
      <button class="install-button" type="button" aria-label="install" data-tooltip="Установить Соты как приложение">${icon("install")}</button>
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
        <button type="submit" aria-label="ok" data-tooltip="Сохранить имя">${icon("check")}</button>
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
        <button class="icon-button deny-button" type="button" aria-label="close" data-tooltip="Отменить подключение">${icon("close")}</button>
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
  normalizeSelectedTunnel();
  for (const tunnel of tunnels) {
    ensureSync(tunnel);
  }

  const hasVisibleTunnels = sortedVisibleTunnels().length > 0;
  app.innerHTML = `
    <section class="shell retro-shell">
      <aside class="tiles hive-panel${hasVisibleTunnels ? "" : " empty"}">
        <div class="retro-brand">
          <span class="retro-brand-mark">S</span>
          <span>
            <b>SOTY</b>
            <small>LIVE TUNNELS</small>
          </span>
        </div>
        <button class="qr-open retro-icon-button" type="button" aria-label="qr" data-tooltip="Показать QR для подключения">${icon("qr")}</button>
        <div class="hex-field"></div>
      </aside>
      <main class="dialog-shell">
        <header class="dialog-head">
          <span class="dialog-avatar">.</span>
          <span class="dialog-copy">
            <b class="dialog-name">.</b>
            <small class="dialog-state">OFFLINE</small>
          </span>
          <span class="dialog-id">0000</span>
        </header>
        <section class="editor retro-screen">
          <div class="chat-scroll">
            <div class="text-paint" aria-live="polite"><div class="text-paint-inner chat-stream"></div></div>
          </div>
          <div class="line-gutter" aria-hidden="true"></div>
          <div class="line-meta" aria-hidden="true"></div>
          <textarea class="dialog-buffer" spellcheck="false" autocapitalize="sentences" aria-hidden="true" tabindex="-1"></textarea>
          <form class="composer-bar">
            <button class="composer-attach retro-icon-button" type="button" aria-label="attach" data-tooltip="Прикрепить файл">${icon("clip")}</button>
            <textarea class="chat-composer" rows="1" spellcheck="false" autocapitalize="sentences" aria-label="message"></textarea>
            <button class="send-button retro-icon-button" type="submit" aria-label="send" data-tooltip="Отправить сообщение">${icon("send")}</button>
          </form>
        <div class="terminal-panel" data-tooltip="Окно удаленных команд" data-tooltip-side="top">
          <div class="terminal-head">
            <span class="terminal-led"></span>
            <span class="terminal-peer"></span>
            <span class="terminal-glyph">$</span>
            <button class="terminal-close" type="button" aria-label="close" data-tooltip="Закрыть удаленные команды">${icon("close")}</button>
          </div>
          <div class="terminal-output"></div>
          <form class="terminal-form">
            <span>$</span>
            <input autocomplete="off" autocapitalize="off" spellcheck="false" data-tooltip="off" />
            <button type="submit" aria-label="run" data-tooltip="Выполнить команду">${icon("check")}</button>
          </form>
        </div>
        <input class="file-input" type="file" multiple />
      </section>
      </main>
      <aside class="side-panel">
        <section class="side-block live-block">
          <h2>LIVE</h2>
          <div class="writer-pop"></div>
        </section>
        <section class="side-block file-block">
          <h2>FILES</h2>
          <div class="file-rail"></div>
        </section>
        <section class="side-block action-block">
          <h2>TOOLS</h2>
          <div class="side-actions">
            <button class="side-action attach-action" type="button" aria-label="attach" data-tooltip="Отправить файл">${icon("clip")}<span>FILE</span></button>
            <button class="side-action knock-action" type="button" aria-label="knock" data-tooltip="Позвать собеседника">${icon("bell")}<span>PING</span></button>
            <button class="side-action remote-action" type="button" aria-label="remote" data-tooltip="Включить удаленное подключение">${icon("remote")}<span>LINK</span></button>
            <button class="side-action close-action" type="button" aria-label="close" data-tooltip="Закрыть соту">${icon("close")}<span>DROP</span></button>
          </div>
        </section>
      </aside>
    </section>
  `;

  textarea = app.querySelector(".dialog-buffer");
  composer = app.querySelector(".chat-composer");
  textPaint = app.querySelector<HTMLDivElement>(".text-paint-inner");
  lineGutter = app.querySelector(".line-gutter");
  lineMeta = app.querySelector(".line-meta");
  fileInput = app.querySelector(".file-input");
  app.querySelector<HTMLButtonElement>(".qr-open")?.addEventListener("click", () => {
    void showQr();
  });
  renderTiles();
  composer?.addEventListener("input", () => syncComposerDraft());
  composer?.addEventListener("keydown", (event) => {
    if (event.key === "Enter" && !event.shiftKey && !event.isComposing) {
      event.preventDefault();
      finalizeComposerDraft();
    }
  });
  app.querySelector<HTMLFormElement>(".composer-bar")?.addEventListener("submit", (event) => {
    event.preventDefault();
    finalizeComposerDraft();
  });
  app.querySelector<HTMLButtonElement>(".composer-attach")?.addEventListener("click", () => fileInput?.click());
  app.querySelector<HTMLElement>(".editor")?.addEventListener("dragover", (event) => {
    event.preventDefault();
    app.querySelector(".editor")?.classList.add("dropping");
  });
  app.querySelector<HTMLElement>(".editor")?.addEventListener("dragleave", () => {
    app.querySelector(".editor")?.classList.remove("dropping");
  });
  app.querySelector<HTMLElement>(".editor")?.addEventListener("drop", (event) => {
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
  app.querySelector<HTMLButtonElement>(".terminal-close")?.addEventListener("click", () => {
    if (selectedId) {
      closeRemoteMode(selectedId);
    }
  });
  app.querySelector<HTMLFormElement>(".terminal-form")?.addEventListener("submit", (event) => {
    event.preventDefault();
    void sendTerminalCommand();
  });
  app.querySelector<HTMLButtonElement>(".attach-action")?.addEventListener("click", () => fileInput?.click());
  app.querySelector<HTMLButtonElement>(".knock-action")?.addEventListener("click", () => {
    if (!selectedId) {
      return;
    }
    syncs.get(selectedId)?.sendKnock("*");
    tunnels = touchTunnel(selectedId);
    renderTiles();
  });
  app.querySelector<HTMLButtonElement>(".remote-action")?.addEventListener("click", () => {
    if (selectedId) {
      void toggleRemoteGrant(selectedId);
    }
  });
  app.querySelector<HTMLButtonElement>(".close-action")?.addEventListener("click", () => {
    if (selectedId) {
      closeTunnel(selectedId);
    }
  });
  setupSplitter();
  applySelectedText();
  renderFiles();
  renderTerminal();
  void ensureOperatorBridge();
}

function renderTiles(): void {
  const field = app.querySelector<HTMLDivElement>(".hex-field");
  if (!field) {
    return;
  }
  tunnels = loadTunnels();
  normalizeSelectedTunnel();
  const sorted = sortedVisibleTunnels();
  if (sorted.length === 0) {
    renderHexField(field, [], {
      select: () => undefined,
      menu: () => undefined
    });
    renderDialogChrome();
    void showQr(true);
    return;
  }
  if (qrMode === "auto") {
    closeQrOverlay();
  }
  renderHexField(field, sorted.map((tunnel) => ({
    id: tunnel.id,
    label: counterpartyLabel(tunnel),
    color: safeColor(tunnel.color, counterpartyLabel(tunnel) + tunnel.id),
    active: tunnel.id === selectedId,
    unread: tunnel.unread
  })), {
    select: (id) => {
      selectedId = id;
      saveSelectedTunnelId(id);
      clearTunnelNotices(id);
      tunnels = markTunnel(id, false);
      renderTiles();
      applySelectedText(true);
      renderFiles();
      renderTerminal();
    },
    menu: (id, x, y) => {
      openCounterpartyMenu(x, y, {
        attach: () => {
          selectedId = id;
          saveSelectedTunnelId(id);
          fileInput?.click();
        },
        knock: () => {
          selectedId = id;
          saveSelectedTunnelId(id);
          syncs.get(id)?.sendKnock("*");
          tunnels = touchTunnel(id);
          renderTiles();
        },
        remote: () => {
          selectedId = id;
          saveSelectedTunnelId(id);
          void toggleRemoteGrant(id);
        },
        close: () => closeTunnel(id)
      }, {
        remoteEnabled: remoteEnabled.has(id)
      });
    }
  });
  renderDialogChrome();
}

async function toggleRemoteGrant(id: string): Promise<void> {
  if (remoteEnabled.has(id)) {
    closeRemoteMode(id);
    return;
  }

  const agent = await refreshLocalAgent();
  if (!agent.ok) {
    renderAgentInstall(id);
    return;
  }

  remoteEnabled = setRemoteEnabled(id, true);
  syncs.get(id)?.grantRemote(true, "*");
  terminalOpenId = id;
  setTerminalState(id, "idle");
  renderTerminal();
  renderTiles();
}

function announceRemoteGrant(tunnelId: string, targetDeviceId = "*"): void {
  if (!remoteEnabled.has(tunnelId)) {
    return;
  }
  syncs.get(tunnelId)?.grantRemote(true, targetDeviceId);
}

async function refreshLocalAgent(): Promise<LocalAgentStatus> {
  window.clearTimeout(agentProbeTimer);
  agentProbe = agentProbe || checkLocalAgent().finally(() => {
    agentProbe = null;
  });
  localAgent = await agentProbe;
  document.querySelector(".agent-sheet")?.classList.toggle("is-ok", localAgent.ok);
  if (localAgent.ok) {
    agentProbeTimer = window.setTimeout(() => void refreshLocalAgent(), 30_000);
  }
  return localAgent;
}

function renderAgentInstall(tunnelId: string): void {
  document.querySelector(".agent-modal")?.remove();
  const overlay = document.createElement("div");
  overlay.className = "agent-modal";
  overlay.innerHTML = `
    <div class="agent-sheet${localAgent.ok ? " is-ok" : ""}" data-tooltip="Панель установки локального Soty-агента" data-tooltip-side="bottom">
      <span class="agent-mark" data-tooltip="Локальный агент нужен для команд Windows">${icon("remote")}</span>
      <button class="icon-button download-button" type="button" aria-label="download" data-tooltip="Скачать обычный установщик">${icon("download")}</button>
      ${isWindowsPlatform() ? `<button class="icon-button machine-button" type="button" aria-label="machine" data-tooltip="Установить с правами администратора">${icon("shield")}</button>` : ""}
      <button class="icon-button refresh-button" type="button" aria-label="refresh" data-tooltip="Проверить, запущен ли агент">${icon("refresh")}</button>
      <button class="icon-button close-button" type="button" aria-label="close" data-tooltip="Закрыть панель">${icon("close")}</button>
    </div>
  `;
  document.body.append(overlay);
  overlay.querySelector(".download-button")?.addEventListener("click", () => {
    downloadAgentInstaller();
  });
  overlay.querySelector(".machine-button")?.addEventListener("click", () => {
    downloadAgentInstaller("machine");
  });
  overlay.querySelector(".refresh-button")?.addEventListener("click", () => {
    void (async () => {
      const agent = await refreshLocalAgent();
      if (agent.ok) {
        overlay.remove();
        void toggleRemoteGrant(tunnelId);
      }
    })();
  });
  overlay.querySelector(".close-button")?.addEventListener("click", () => overlay.remove());
}

function closeRemoteMode(tunnelId: string): void {
  const sync = syncs.get(tunnelId);
  const hostDeviceId = remoteAccess.get(tunnelId);
  if (remoteEnabled.has(tunnelId)) {
    remoteEnabled = setRemoteEnabled(tunnelId, false);
    sync?.grantRemote(false, "*");
  }
  if (hostDeviceId) {
    remoteAccess = setRemoteAccess(tunnelId, "", false);
    if (hostDeviceId !== device?.id) {
      sync?.grantRemote(false, hostDeviceId);
    }
  }
  if (terminalOpenId === tunnelId) {
    terminalOpenId = "";
  }
  setTerminalState(tunnelId, "ok");
  renderTerminal();
  renderTiles();
  publishOperatorTargets();
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

function normalizeSelectedTunnel(): void {
  const all = loadTunnels();
  if (all.length === 0) {
    selectedId = "";
    return;
  }
  const visible = all.filter((tunnel) => hasCounterparty(tunnel));
  const pool = visible.length > 0 ? visible : all;
  const stored = loadSelectedTunnelId();
  if (pool.some((tunnel) => tunnel.id === selectedId)) {
    saveSelectedTunnelId(selectedId);
    return;
  }
  const next = pool.find((tunnel) => tunnel.id === stored) ?? pool[0];
  selectedId = next?.id || "";
  if (selectedId) {
    saveSelectedTunnelId(selectedId);
  }
}

function hasCounterparty(tunnel: TunnelRecord): boolean {
  const label = cleanNick(peers.get(tunnel.id) || tunnel.label || "");
  return Boolean(tunnel.counterparty || peers.has(tunnel.id) || (label !== "." && label !== device?.nick));
}

function counterpartyLabel(tunnel: TunnelRecord): string {
  return cleanNick(peers.get(tunnel.id) || tunnel.label || ".");
}

function renderDialogChrome(): void {
  const tunnel = loadTunnels().find((item) => item.id === selectedId);
  const label = tunnel ? counterpartyLabel(tunnel) : ".";
  const color = tunnel ? safeColor(tunnel.color, label + tunnel.id) : "#67e8f9";
  const avatar = app.querySelector<HTMLElement>(".dialog-avatar");
  const name = app.querySelector<HTMLElement>(".dialog-name");
  const state = app.querySelector<HTMLElement>(".dialog-state");
  const id = app.querySelector<HTMLElement>(".dialog-id");
  const shell = app.querySelector<HTMLElement>(".dialog-shell");
  const remoteButton = app.querySelector<HTMLButtonElement>(".remote-action");
  if (shell) {
    shell.style.setProperty("--peer-color", color);
  }
  if (avatar) {
    avatar.textContent = initials(label);
  }
  if (name) {
    name.textContent = label;
  }
  if (state) {
    const remote = remoteAccess.has(selectedId) ? "REMOTE READY" : remoteEnabled.has(selectedId) ? "HOST LINK" : "LIVE TEXT";
    state.textContent = remote;
  }
  if (id) {
    id.textContent = selectedId ? selectedId.slice(0, 8).toUpperCase() : "NO LINK";
  }
  if (remoteButton) {
    remoteButton.classList.toggle("is-on", remoteEnabled.has(selectedId));
    remoteButton.classList.toggle("has-access", remoteAccess.has(selectedId));
    remoteButton.dataset.tooltip = remoteEnabled.has(selectedId)
      ? "Выключить удаленное подключение"
      : remoteAccess.has(selectedId)
        ? "Открыть удаленные команды"
        : "Включить удаленное подключение";
  }
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
      activeActivities.set(tunnel.id, activity);
      const tick = Date.now();
      activeActivityTicks.set(tunnel.id, tick);
      window.setTimeout(() => {
        if (activeActivityTicks.get(tunnel.id) === tick) {
          activeActivities.delete(tunnel.id);
          activeActivityTicks.delete(tunnel.id);
          if (tunnel.id === selectedId) {
            renderWriterPop();
            renderTextPaint();
          }
        }
      }, 1800);
      touchSelected();
      if (tunnel.id === selectedId) {
        renderLineTags();
        renderTextPaint();
        renderWriterPop();
      }
    },
    onRemoteChange: (activity) => {
      const hadNotice = tunnelHasNotice(tunnel.id);
      maybeKnockForTyping(tunnel.id, activity, hadNotice);
      rememberWriter(tunnel.id, activity);
      activeActivities.set(tunnel.id, activity);
      const tick = Date.now();
      activeActivityTicks.set(tunnel.id, tick);
      window.setTimeout(() => {
        if (activeActivityTicks.get(tunnel.id) === tick) {
          activeActivities.delete(tunnel.id);
          activeActivityTicks.delete(tunnel.id);
          if (tunnel.id === selectedId) {
            renderWriterPop();
            renderTextPaint();
          }
        }
      }, 1800);
      if (tunnel.id !== selectedId || document.visibilityState === "hidden") {
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
    onFileDeleted: (fileId) => {
      files.set(tunnel.id, (files.get(tunnel.id) ?? []).filter((item) => item.id !== fileId));
      if (tunnel.id === selectedId) {
        renderFiles();
      }
    },
    onKnock: (knock) => {
      applyKnock(tunnel.id, knock);
    },
    onRemoteGrant: (grant) => {
      applyRemoteGrant(tunnel.id, grant);
    },
    onRemoteRequest: (request) => {
      applyRemoteRequest(tunnel.id, request);
    },
    onRemoteCommand: (command) => {
      applyRemoteCommand(tunnel.id, command);
    },
    onRemoteScript: (script) => {
      applyRemoteScript(tunnel.id, script);
    },
    onRemoteOutput: (output) => {
      applyRemoteOutput(tunnel.id, output);
    },
    onPeers: (items) => {
      const label = items.map((item) => item.nick).filter(Boolean).join(" ");
      if (label) {
        setTunnelCounterparty(tunnel.id, label);
        renderTiles();
      }
    },
    onJoinRequest: (request) => {
      const hadNotice = tunnelHasNotice(tunnel.id);
      vibrateHiddenOnce(`join:${request.requestId}`, tunnel.id, hadNotice);
      if (document.visibilityState === "hidden" && !hadNotice) {
        tunnels = markTunnel(tunnel.id, true);
        renderTiles();
      }
      renderOwnerJoinConfirm(tunnel, request);
    },
    onClosed: () => {
      syncs.get(tunnel.id)?.destroy();
      syncs.delete(tunnel.id);
      writerLines.delete(tunnel.id);
      activeActivities.delete(tunnel.id);
      activeActivityTicks.delete(tunnel.id);
      tunnels = removeTunnel(tunnel.id);
      normalizeSelectedTunnel();
      renderApp();
    },
    onState: (state) => {
      if (state === "open") {
        announceRemoteGrant(tunnel.id);
      }
    }
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
        <button class="icon-button deny-button" type="button" aria-label="close" data-tooltip="Отклонить подключение">${icon("close")}</button>
        <button class="icon-button accept-button" type="button" aria-label="ok" data-tooltip="Разрешить подключение">${icon("check")}</button>
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

function closeTunnel(id: string): void {
  const sync = syncs.get(id);
  sync?.closeForEveryone();
  syncs.delete(id);
  remoteEnabled = setRemoteEnabled(id, false);
  remoteAccess = setRemoteAccess(id, "", false);
  if (terminalOpenId === id) {
    terminalOpenId = "";
  }
  terminalLogs.delete(id);
  terminalState.delete(id);
  writerLines.delete(id);
  activeActivities.delete(id);
  activeActivityTicks.delete(id);
  localDrafts.delete(id);
  files.delete(id);
  tunnels = removeTunnel(id);
  normalizeSelectedTunnel();
  renderApp();
}

function rotateInviteTunnel(preserveSelection = false): TunnelRecord | null {
  if (!device) {
    return null;
  }
  const current = loadTunnels();
  const previousSelected = selectedId;
  for (const tunnel of current.filter((item) => !item.counterparty)) {
    const sync = syncs.get(tunnel.id);
    sync?.closeForEveryone();
    syncs.delete(tunnel.id);
    writerLines.delete(tunnel.id);
    activeActivities.delete(tunnel.id);
    activeActivityTicks.delete(tunnel.id);
    files.delete(tunnel.id);
  }
  const tunnel = createTunnel();
  const counterparties = current.filter((item) => item.counterparty);
  const next = [tunnel, ...counterparties];
  saveTunnels(next);
  if (preserveSelection && counterparties.some((item) => item.id === previousSelected)) {
    selectedId = previousSelected;
    saveSelectedTunnelId(previousSelected);
  } else {
    selectedId = tunnel.id;
    saveSelectedTunnelId(tunnel.id);
  }
  tunnels = next;
  ensureSync(tunnel);
  return tunnel;
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
    const localFile = await sync.sendFile(file);
    files.set(selectedId, [localFile, ...(files.get(selectedId) ?? []).filter((item) => item.id !== localFile.id)]);
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
  const color = safeColor(tunnel?.color, (tunnel?.label || selectedId) + selectedId);
  renderFileRail(rail, files.get(selectedId) ?? [], color, deleteFile);
}

function deleteFile(fileId: string): void {
  if (!selectedId) {
    return;
  }
  files.set(selectedId, (files.get(selectedId) ?? []).filter((item) => item.id !== fileId));
  syncs.get(selectedId)?.deleteFile(fileId);
  renderFiles();
}

function applyKnock(tunnelId: string, knock: NoticeKnock): void {
  if (!device || knock.deviceId === device.id || !grantTargetsThisDevice(knock.targetDeviceId)) {
    return;
  }
  const hadNotice = tunnelHasNotice(tunnelId);
  vibrateHiddenOnce(`knock:${knock.deviceId || knock.nick}`, tunnelId, hadNotice);
  tunnels = document.visibilityState === "hidden" || tunnelId !== selectedId
    ? markTunnel(tunnelId, true)
    : touchTunnel(tunnelId);
  renderTiles();
}

function applyRemoteRequest(tunnelId: string, request: RemoteRequest): void {
  if (!device || request.deviceId === device.id || !grantTargetsThisDevice(request.targetDeviceId)) {
    return;
  }
  const sync = syncs.get(tunnelId);
  if (remoteEnabled.has(tunnelId)) {
    sync?.grantRemote(true, request.deviceId);
    return;
  }
  selectedId = tunnelId;
  saveSelectedTunnelId(tunnelId);
  renderRemoteRequest(tunnelId, request);
}

function renderRemoteRequest(tunnelId: string, request: RemoteRequest): void {
  document.querySelector(".access-modal")?.remove();
  const tunnel = tunnels.find((item) => item.id === tunnelId);
  const requester = cleanNick(request.nick || (tunnel ? counterpartyLabel(tunnel) : ""));
  const overlay = document.createElement("div");
  overlay.className = "access-modal";
  overlay.innerHTML = `
    <div class="access-sheet">
      <span class="access-mark">${icon("remote")}</span>
      <b>Удалённое управление</b>
      <p>${escapeHtml(requester || "Оператор")} просит доступ к этому компьютеру.</p>
      <div class="access-actions">
        <button class="access-accept" type="button">Разрешить</button>
        <button class="access-deny" type="button">Не сейчас</button>
      </div>
    </div>
  `;
  document.body.append(overlay);
  overlay.querySelector(".access-accept")?.addEventListener("click", () => {
    void (async () => {
      const agent = await refreshLocalAgent();
      if (!agent.ok) {
        overlay.remove();
        renderAgentInstall(tunnelId);
        return;
      }
      remoteEnabled = setRemoteEnabled(tunnelId, true);
      syncs.get(tunnelId)?.grantRemote(true, request.deviceId);
      terminalOpenId = tunnelId;
      setTerminalState(tunnelId, "idle");
      overlay.remove();
      renderTiles();
      renderTerminal();
      publishOperatorTargets();
    })();
  });
  overlay.querySelector(".access-deny")?.addEventListener("click", () => overlay.remove());
}

function applyRemoteGrant(tunnelId: string, grant: RemoteGrant): void {
  if (!device || grant.deviceId === device.id || !grantTargetsThisDevice(grant.targetDeviceId)) {
    return;
  }
  if (!grant.enabled && remoteEnabled.has(tunnelId)) {
    remoteEnabled = setRemoteEnabled(tunnelId, false);
    syncs.get(tunnelId)?.grantRemote(false, "*");
  }
  remoteAccess = setRemoteAccess(tunnelId, grant.deviceId, grant.enabled);
  if (grant.enabled) {
    terminalOpenId = tunnelId;
    setTerminalState(tunnelId, "idle");
  } else if (terminalOpenId === tunnelId) {
    terminalOpenId = "";
    setTerminalState(tunnelId, "ok");
  }
  renderTiles();
  renderTerminal();
  void ensureOperatorBridge();
  publishOperatorTargets();
}

function applyRemoteCommand(tunnelId: string, command: RemoteCommand): void {
  if (!device || !remoteEnabled.has(tunnelId) || !grantTargetsThisDevice(command.targetDeviceId)) {
    return;
  }
  terminalOpenId = tunnelId;
  setTerminalState(tunnelId, "run");
  appendTerminalLine(tunnelId, `< ${command.command}`);
  void runLocalAgentCommand(tunnelId, command);
  renderTerminal();
}

function applyRemoteScript(tunnelId: string, script: RemoteScript): void {
  if (!device || !remoteEnabled.has(tunnelId) || !grantTargetsThisDevice(script.targetDeviceId)) {
    return;
  }
  terminalOpenId = tunnelId;
  setTerminalState(tunnelId, "run");
  appendTerminalLine(tunnelId, `< ${script.name || "script"}`);
  void runLocalAgentScript(tunnelId, script);
  renderTerminal();
}

function applyRemoteOutput(tunnelId: string, output: RemoteOutput): void {
  if (!device || !grantTargetsThisDevice(output.targetDeviceId)) {
    return;
  }
  if (output.text.trim()) {
    appendTerminalLine(tunnelId, output.text);
  }
  if (typeof output.exitCode === "number") {
    setTerminalState(tunnelId, output.exitCode === 0 ? "ok" : output.exitCode === 127 ? "off" : "bad");
    appendTerminalLine(tunnelId, `${output.exitCode === 0 ? "+" : "!"} ${output.exitCode}`);
  }
  const operatorId = operatorPending.get(output.commandId);
  if (operatorId) {
    sendOperatorOutput(operatorId, output.text, output.exitCode);
    if (typeof output.exitCode === "number") {
      operatorPending.delete(output.commandId);
    }
  }
  terminalOpenId = tunnelId;
  renderTerminal();
}

async function sendTerminalCommand(): Promise<void> {
  if (!selectedId || !device) {
    return;
  }
  const hostDeviceId = remoteAccess.get(selectedId);
  const sync = syncs.get(selectedId);
  const input = app.querySelector<HTMLInputElement>(".terminal-form input");
  const command = input?.value.trim() || "";
  if (!hostDeviceId || !sync || !command) {
    return;
  }
  if (input) {
    input.value = "";
  }
  setTerminalState(selectedId, "run");
  appendTerminalLine(selectedId, `$ ${command}`);
  renderTerminal();
  await sync.sendRemoteCommand(hostDeviceId, command);
}

async function ensureOperatorBridge(): Promise<void> {
  if (!hasOperatorTargets()) {
    closeOperatorBridge();
    return;
  }
  if (operatorSocket && (operatorSocket.readyState === WebSocket.OPEN || operatorSocket.readyState === WebSocket.CONNECTING)) {
    publishOperatorTargets();
    return;
  }
  window.clearTimeout(operatorReconnectTimer);
  const agent = await checkLocalAgent(650);
  if (!agent.ok || !hasOperatorTargets()) {
    return;
  }
  const ws = new WebSocket("ws://127.0.0.1:49424");
  operatorSocket = ws;
  ws.onopen = () => {
    ws.send(JSON.stringify({ type: "operator.attach" }));
    publishOperatorTargets();
  };
  ws.onmessage = (event) => {
    let message: {
      readonly type?: string;
      readonly id?: string;
      readonly target?: string;
      readonly command?: string;
      readonly name?: string;
      readonly shell?: string;
      readonly script?: string;
      readonly text?: string;
      readonly speed?: string;
      readonly persona?: string;
    };
    try {
      message = JSON.parse(event.data as string) as typeof message;
    } catch {
      return;
    }
    if (message.type === "operator.run") {
      void runOperatorCommand(message);
    }
    if (message.type === "operator.script") {
      void runOperatorScript(message);
    }
    if (message.type === "operator.chat") {
      void runOperatorChat(message);
    }
    if (message.type === "operator.access") {
      runOperatorAccess(message);
    }
    if (message.type === "operator.export") {
      runOperatorExport(message);
    }
  };
  ws.onclose = () => {
    if (operatorSocket === ws) {
      operatorSocket = null;
    }
    if (hasOperatorTargets()) {
      operatorReconnectTimer = window.setTimeout(() => void ensureOperatorBridge(), 1800);
    }
  };
  ws.onerror = () => {
    ws.close();
  };
}

function closeOperatorBridge(): void {
  window.clearTimeout(operatorReconnectTimer);
  operatorSocket?.close();
  operatorSocket = null;
  operatorPending.clear();
}

function hasOperatorTargets(): boolean {
  return operatorTargets().length > 0;
}

function publishOperatorTargets(): void {
  const ws = operatorSocket;
  if (!ws || ws.readyState !== WebSocket.OPEN) {
    return;
  }
  ws.send(JSON.stringify({
    type: "operator.targets",
    targets: operatorTargets()
  }));
}

function operatorTargets(): Array<{ readonly id: string; readonly label: string }> {
  return sortedVisibleTunnels()
    .map((tunnel) => ({
      id: tunnel.id,
      label: counterpartyLabel(tunnel)
    }));
}

async function runOperatorCommand(message: { readonly id?: string; readonly target?: string; readonly command?: string }): Promise<void> {
  const requestId = typeof message.id === "string" ? message.id : "";
  const command = typeof message.command === "string" ? message.command.trim() : "";
  if (!requestId || !command) {
    return;
  }
  const tunnel = findOperatorTarget(message.target || "");
  if (!tunnel) {
    sendOperatorOutput(requestId, "! target", 404);
    return;
  }
  const hostDeviceId = remoteAccess.get(tunnel.id);
  const sync = syncs.get(tunnel.id);
  if (!hostDeviceId || !sync) {
    sendOperatorOutput(requestId, "! tunnel", 409);
    return;
  }
  selectedId = tunnel.id;
  saveSelectedTunnelId(tunnel.id);
  terminalOpenId = tunnel.id;
  setTerminalState(tunnel.id, "run");
  appendTerminalLine(tunnel.id, `$ ${command}`);
  renderTiles();
  renderTerminal();
  try {
    const commandId = await sync.sendRemoteCommand(hostDeviceId, command);
    operatorPending.set(commandId, requestId);
  } catch {
    sendOperatorOutput(requestId, "! tunnel", 500);
    setTerminalState(tunnel.id, "bad");
    renderTerminal();
  }
}

async function runOperatorScript(message: {
  readonly id?: string;
  readonly target?: string;
  readonly name?: string;
  readonly shell?: string;
  readonly script?: string;
}): Promise<void> {
  const requestId = typeof message.id === "string" ? message.id : "";
  const script = typeof message.script === "string" ? message.script : "";
  if (!requestId || !script.trim()) {
    return;
  }
  const tunnel = findOperatorTarget(message.target || "");
  if (!tunnel) {
    sendOperatorOutput(requestId, "! target", 404);
    return;
  }
  const hostDeviceId = remoteAccess.get(tunnel.id);
  const sync = syncs.get(tunnel.id);
  if (!hostDeviceId || !sync) {
    sendOperatorOutput(requestId, "! tunnel", 409);
    return;
  }
  const name = cleanNick(message.name || "script") || "script";
  selectedId = tunnel.id;
  saveSelectedTunnelId(tunnel.id);
  terminalOpenId = tunnel.id;
  setTerminalState(tunnel.id, "run");
  appendTerminalLine(tunnel.id, `$ ${name}`);
  renderTiles();
  renderTerminal();
  try {
    const commandId = await sync.sendRemoteScript(hostDeviceId, {
      name,
      shell: message.shell || "",
      script
    });
    operatorPending.set(commandId, requestId);
  } catch {
    sendOperatorOutput(requestId, "! tunnel", 500);
    setTerminalState(tunnel.id, "bad");
    renderTerminal();
  }
}

async function runOperatorChat(message: {
  readonly id?: string;
  readonly target?: string;
  readonly text?: string;
  readonly speed?: string;
  readonly persona?: string;
}): Promise<void> {
  const requestId = typeof message.id === "string" ? message.id : "";
  const text = typeof message.text === "string" ? message.text.slice(0, 12_000) : "";
  if (!requestId || !text.trim()) {
    return;
  }
  const tunnel = findVisibleOperatorTarget(message.target || "");
  if (!tunnel) {
    sendOperatorOutput(requestId, "! target", 404);
    return;
  }
  const sync = syncs.get(tunnel.id);
  if (!sync) {
    sendOperatorOutput(requestId, "! tunnel", 409);
    return;
  }
  selectedId = tunnel.id;
  saveSelectedTunnelId(tunnel.id);
  renderTiles();
  applySelectedText();
  sendOperatorOutput(requestId, "typing\n");
  const previous = operatorChatQueues.get(tunnel.id) ?? Promise.resolve();
  const displayText = formatOperatorChat(text, message.persona || "operator");
  const next = previous
    .catch(() => undefined)
    .then(() => typeOperatorChat(tunnel.id, displayText, message.speed || ""));
  operatorChatQueues.set(tunnel.id, next);
  try {
    await next;
    sendOperatorOutput(requestId, "sent\n", 0);
  } catch {
    sendOperatorOutput(requestId, "! chat", 500);
  } finally {
    if (operatorChatQueues.get(tunnel.id) === next) {
      operatorChatQueues.delete(tunnel.id);
    }
  }
}

function formatOperatorChat(text: string, persona: string): string {
  const name = persona === "sysadmin" ? "Codex" : cleanNick(persona || "Оператор") || "Оператор";
  const body = text.trim();
  if (!body) {
    return "";
  }
  return [
    `${name} · ${clock()}`,
    ...body.split("\n"),
    "Ответ:"
  ].join("\n");
}

function runOperatorAccess(message: { readonly id?: string; readonly target?: string }): void {
  const requestId = typeof message.id === "string" ? message.id : "";
  if (!requestId) {
    return;
  }
  const tunnel = findVisibleOperatorTarget(message.target || "");
  if (!tunnel) {
    sendOperatorOutput(requestId, "! target", 404);
    return;
  }
  const sync = syncs.get(tunnel.id);
  if (!sync) {
    sendOperatorOutput(requestId, "! tunnel", 409);
    return;
  }
  selectedId = tunnel.id;
  saveSelectedTunnelId(tunnel.id);
  sync.requestRemote("*");
  renderTiles();
  sendOperatorOutput(requestId, "requested\n", 0);
}

function runOperatorExport(message: { readonly id?: string }): void {
  const requestId = typeof message.id === "string" ? message.id : "";
  if (!requestId) {
    return;
  }
  sendOperatorOutput(requestId, buildOperatorExport(), 0);
}

function findOperatorTarget(target: string): TunnelRecord | null {
  const needle = cleanNick(target).toLowerCase();
  if (!needle) {
    return null;
  }
  const items = loadTunnels().filter((tunnel) => remoteAccess.has(tunnel.id));
  return items.find((tunnel) => tunnel.id === target)
    || items.find((tunnel) => counterpartyLabel(tunnel).toLowerCase() === needle)
    || items.find((tunnel) => counterpartyLabel(tunnel).toLowerCase().includes(needle))
    || null;
}

function findVisibleOperatorTarget(target: string): TunnelRecord | null {
  const needle = cleanNick(target).toLowerCase();
  if (!needle) {
    return null;
  }
  const items = sortedVisibleTunnels();
  return items.find((tunnel) => tunnel.id === target)
    || items.find((tunnel) => counterpartyLabel(tunnel).toLowerCase() === needle)
    || items.find((tunnel) => counterpartyLabel(tunnel).toLowerCase().includes(needle))
    || null;
}

function sendOperatorOutput(id: string, text: string, exitCode?: number): void {
  const ws = operatorSocket;
  if (!ws || ws.readyState !== WebSocket.OPEN) {
    return;
  }
  ws.send(JSON.stringify({
    type: "operator.output",
    id,
    text,
    ...(typeof exitCode === "number" ? { exitCode } : {})
  }));
}

function renderTerminal(): void {
  const panel = app.querySelector<HTMLDivElement>(".terminal-panel");
  const output = app.querySelector<HTMLDivElement>(".terminal-output");
  const editor = app.querySelector<HTMLElement>(".editor");
  const form = app.querySelector<HTMLFormElement>(".terminal-form");
  const peer = app.querySelector<HTMLSpanElement>(".terminal-peer");
  if (!panel || !output || !editor || !form || !peer) {
    return;
  }
  if (!terminalOpenId && selectedId && remoteAccess.has(selectedId)) {
    terminalOpenId = selectedId;
  }
  const controller = Boolean(selectedId && terminalOpenId === selectedId && remoteAccess.has(selectedId));
  const host = Boolean(selectedId && terminalOpenId === selectedId && remoteEnabled.has(selectedId));
  const active = controller || host;
  const state = terminalState.get(selectedId) ?? "idle";
  editor.classList.toggle("terminal-active", active);
  editor.classList.toggle("terminal-controller", controller);
  editor.classList.toggle("terminal-host", host && !controller);
  panel.dataset.state = state;
  form.hidden = !controller;
  const tunnel = loadTunnels().find((item) => item.id === selectedId);
  peer.textContent = tunnel ? initials(counterpartyLabel(tunnel)) : ".";
  output.innerHTML = (terminalLogs.get(selectedId) ?? [])
    .map((line) => `<pre>${escapeHtml(line || " ")}</pre>`)
    .join("");
  if (active) {
    output.scrollTop = output.scrollHeight;
    window.setTimeout(() => app.querySelector<HTMLInputElement>(".terminal-form input")?.focus(), 0);
  }
}

function appendTerminalLine(tunnelId: string, line: string): void {
  const next = [...(terminalLogs.get(tunnelId) ?? []), line].slice(-600);
  terminalLogs.set(tunnelId, next);
}

function setTerminalState(tunnelId: string, state: "idle" | "run" | "ok" | "bad" | "off"): void {
  terminalState.set(tunnelId, state);
}

function grantTargetsThisDevice(targetDeviceId: string): boolean {
  return targetDeviceId === "*" || targetDeviceId === device?.id;
}

function tunnelHasNotice(tunnelId: string): boolean {
  return loadTunnels().some((tunnel) => tunnel.id === tunnelId && tunnel.unread);
}

function vibrateHiddenOnce(reason: string, tunnelId: string, hadNotice: boolean): void {
  if (document.visibilityState !== "hidden" || hadNotice) {
    return;
  }
  const key = `${tunnelId}:${reason}`;
  if (activeNoticeKeys.has(key)) {
    return;
  }
  activeNoticeKeys.add(key);
  navigator.vibrate?.([45, 70, 45]);
}

function clearTunnelNotices(tunnelId: string): void {
  for (const key of [...activeNoticeKeys]) {
    if (key.startsWith(`${tunnelId}:`)) {
      activeNoticeKeys.delete(key);
    }
  }
}

function maybeKnockForTyping(tunnelId: string, activity: WriterActivity, hadNotice: boolean): void {
  if (activity.local) {
    return;
  }
  const writer = activity.deviceId || activity.nick;
  if (!writer) {
    return;
  }
  const key = `${tunnelId}:${writer}`;
  const now = Date.now();
  const last = lastTypingNoticeAt.get(key) || 0;
  if (now - last < 60_000) {
    return;
  }
  lastTypingNoticeAt.set(key, now);
  vibrateHiddenOnce(`typing:${writer}`, tunnelId, hadNotice);
}

function runLocalAgentCommand(tunnelId: string, command: RemoteCommand): void {
  const sync = syncs.get(tunnelId);
  if (!sync || !command.deviceId) {
    return;
  }
  let opened = false;
  let finished = false;
  const ws = new WebSocket("ws://127.0.0.1:49424");
  const fail = () => {
    if (finished || opened) {
      return;
    }
    finished = true;
    setTerminalState(tunnelId, "off");
    appendTerminalLine(tunnelId, "! 127.0.0.1:49424");
    renderTerminal();
    window.clearTimeout(timer);
    void sync.sendRemoteOutput(command.deviceId, command.id, "! 127.0.0.1:49424", 127);
  };
  const timer = window.setTimeout(() => {
    fail();
    ws.close();
  }, 2500);
  ws.onopen = () => {
    opened = true;
    window.clearTimeout(timer);
    ws.send(JSON.stringify({
      type: "run",
      id: command.id,
      command: command.command
    }));
  };
  ws.onmessage = (event) => {
    let message: { readonly type?: string; readonly text?: string; readonly exitCode?: number };
    try {
      message = JSON.parse(event.data as string) as { readonly type?: string; readonly text?: string; readonly exitCode?: number };
    } catch {
      return;
    }
    if (message.type === "start") {
      setTerminalState(tunnelId, "run");
      renderTerminal();
      return;
    }
    if (message.type === "error") {
      finished = true;
      setTerminalState(tunnelId, "bad");
      const text = typeof message.text === "string" ? message.text : "!";
      appendTerminalLine(tunnelId, text);
      renderTerminal();
      void sync.sendRemoteOutput(command.deviceId, command.id, text, 1);
      return;
    }
    if (message.type !== "data" && message.type !== "exit") {
      return;
    }
    const text = typeof message.text === "string" ? message.text : "";
    const exitCode = typeof message.exitCode === "number" ? message.exitCode : undefined;
    if (text.trim()) {
      appendTerminalLine(tunnelId, text);
    }
    if (typeof exitCode === "number") {
      finished = true;
      setTerminalState(tunnelId, exitCode === 0 ? "ok" : "bad");
      appendTerminalLine(tunnelId, `${exitCode === 0 ? "+" : "!"} ${exitCode}`);
    }
    renderTerminal();
    void sync.sendRemoteOutput(command.deviceId, command.id, text, exitCode);
  };
  ws.onerror = () => fail();
  ws.onclose = () => window.clearTimeout(timer);
}

function runLocalAgentScript(tunnelId: string, script: RemoteScript): void {
  const sync = syncs.get(tunnelId);
  if (!sync || !script.deviceId) {
    return;
  }
  let opened = false;
  let finished = false;
  const ws = new WebSocket("ws://127.0.0.1:49424");
  const fail = () => {
    if (finished || opened) {
      return;
    }
    finished = true;
    setTerminalState(tunnelId, "off");
    appendTerminalLine(tunnelId, "! 127.0.0.1:49424");
    renderTerminal();
    window.clearTimeout(timer);
    void sync.sendRemoteOutput(script.deviceId, script.id, "! 127.0.0.1:49424", 127);
  };
  const timer = window.setTimeout(() => {
    fail();
    ws.close();
  }, 2500);
  ws.onopen = () => {
    opened = true;
    window.clearTimeout(timer);
    ws.send(JSON.stringify({
      type: "script",
      id: script.id,
      name: script.name,
      shell: script.shell,
      script: script.script
    }));
  };
  ws.onmessage = (event) => {
    let message: { readonly type?: string; readonly text?: string; readonly exitCode?: number };
    try {
      message = JSON.parse(event.data as string) as { readonly type?: string; readonly text?: string; readonly exitCode?: number };
    } catch {
      return;
    }
    if (message.type === "start") {
      setTerminalState(tunnelId, "run");
      renderTerminal();
      return;
    }
    if (message.type === "error") {
      finished = true;
      setTerminalState(tunnelId, "bad");
      const text = typeof message.text === "string" ? message.text : "!";
      appendTerminalLine(tunnelId, text);
      renderTerminal();
      void sync.sendRemoteOutput(script.deviceId, script.id, text, 1);
      return;
    }
    if (message.type !== "data" && message.type !== "exit") {
      return;
    }
    const text = typeof message.text === "string" ? message.text : "";
    const exitCode = typeof message.exitCode === "number" ? message.exitCode : undefined;
    if (text.trim()) {
      appendTerminalLine(tunnelId, text);
    }
    if (typeof exitCode === "number") {
      finished = true;
      setTerminalState(tunnelId, exitCode === 0 ? "ok" : "bad");
      appendTerminalLine(tunnelId, `${exitCode === 0 ? "+" : "!"} ${exitCode}`);
    }
    renderTerminal();
    void sync.sendRemoteOutput(script.deviceId, script.id, text, exitCode);
  };
  ws.onerror = () => fail();
  ws.onclose = () => window.clearTimeout(timer);
}

function touchSelected(): void {
  if (selectedId) {
    tunnels = touchTunnel(selectedId);
  }
}

function syncComposerDraft(): void {
  if (!selectedId || !composer || !textarea) {
    return;
  }
  const sync = syncs.get(selectedId);
  if (!sync) {
    return;
  }
  const draft = composer.value;
  const previous = localDrafts.get(selectedId) ?? "";
  if (!draft && !previous) {
    resizeComposer();
    return;
  }
  const current = texts.get(selectedId) ?? "";
  let next = current;
  if (previous) {
    const start = findDraftStart(current, previous);
    if (start >= 0) {
      next = `${current.slice(0, start)}${draft}${current.slice(start + previous.length)}`;
    } else {
      const separator = current.length > 0 && !current.endsWith("\n") && draft ? "\n" : "";
      next = `${current}${separator}${draft}`;
    }
  } else if (draft) {
    const separator = current.length > 0 && !current.endsWith("\n") ? "\n" : "";
    next = `${current}${separator}${draft}`;
  }
  if (draft) {
    localDrafts.set(selectedId, draft);
  } else {
    localDrafts.delete(selectedId);
  }
  textarea.value = next;
  texts.set(selectedId, next);
  sync.setText(next);
  touchSelected();
  resizeComposer();
  renderTiles();
  renderTextPaint();
  renderWriterPop();
}

function finalizeComposerDraft(): void {
  if (!selectedId || !composer || !textarea) {
    return;
  }
  const sync = syncs.get(selectedId);
  if (!sync) {
    return;
  }
  const draft = localDrafts.get(selectedId) ?? composer.value;
  if (!draft.trim()) {
    if (draft) {
      composer.value = "";
      syncComposerDraft();
    }
    return;
  }
  let next = texts.get(selectedId) ?? textarea.value;
  if (!next.endsWith("\n")) {
    next = `${next}\n`;
    textarea.value = next;
    texts.set(selectedId, next);
    sync.setText(next);
  }
  localDrafts.delete(selectedId);
  composer.value = "";
  touchSelected();
  resizeComposer();
  renderTiles();
  renderTextPaint();
  renderWriterPop();
}

function findDraftStart(text: string, draft: string): number {
  if (!draft) {
    return -1;
  }
  if (text.endsWith(draft)) {
    return text.length - draft.length;
  }
  return text.lastIndexOf(draft);
}

function resizeComposer(): void {
  if (!composer) {
    return;
  }
  composer.style.height = "auto";
  const next = Math.max(42, Math.min(148, composer.scrollHeight));
  composer.style.height = `${next}px`;
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
    composer?.focus();
  }
  if (composer) {
    composer.value = localDrafts.get(selectedId) ?? "";
    resizeComposer();
  }
  renderLineTags();
  renderTextPaint();
  renderWriterPop();
}

async function typeOperatorChat(tunnelId: string, rawText: string, speed: string): Promise<void> {
  const text = rawText.trimEnd();
  if (!text) {
    return;
  }
  const initial = texts.get(tunnelId) || "";
  const prefix = initial.length > 0 && !initial.endsWith("\n") ? "\n" : "";
  const suffix = text.endsWith("\n") ? "" : "\n";
  const chars = Array.from(`${prefix}${text}${suffix}`);
  for (let index = 0; index < chars.length; index += 1) {
    const char = chars[index] || "";
    const typo = speed === "human" && index > 2 && shouldMistype(char, index) ? typoFor(char) : "";
    if (typo) {
      appendOperatorChatText(tunnelId, typo);
      await wait(operatorDelay(typo, speed) + 90);
      removeOperatorChatSuffix(tunnelId, typo);
      await wait(45 + Math.round(Math.random() * 90));
    }
    appendOperatorChatText(tunnelId, char);
    await wait(operatorDelay(char, speed));
  }
}

function appendOperatorChatText(tunnelId: string, text: string): void {
  const sync = syncs.get(tunnelId);
  if (!sync || !text) {
    return;
  }
  const next = `${texts.get(tunnelId) || ""}${text}`;
  sync.setText(next);
  texts.set(tunnelId, next);
  if (tunnelId === selectedId && textarea) {
    textarea.value = next;
    textarea.setSelectionRange(next.length, next.length);
    renderLineTags();
    renderTextPaint();
  }
  tunnels = touchTunnel(tunnelId);
  renderTiles();
}

function removeOperatorChatSuffix(tunnelId: string, suffix: string): void {
  const sync = syncs.get(tunnelId);
  const current = texts.get(tunnelId) || "";
  if (!sync || !suffix || !current.endsWith(suffix)) {
    return;
  }
  const next = current.slice(0, -suffix.length);
  sync.setText(next);
  texts.set(tunnelId, next);
  if (tunnelId === selectedId && textarea) {
    textarea.value = next;
    textarea.setSelectionRange(next.length, next.length);
    renderLineTags();
    renderTextPaint();
  }
}

function shouldMistype(char: string, index: number): boolean {
  return /[0-9A-Za-zА-Яа-яЁё]/u.test(char) && index % 17 === 9 && Math.random() < 0.55;
}

function typoFor(char: string): string {
  const lower = char.toLowerCase();
  const ru = "йцукенгшщзхъфывапролджэячсмитьбю";
  const en = "qwertyuiopasdfghjklzxcvbnm";
  const source = ru.includes(lower) ? ru : en.includes(lower) ? en : "";
  if (!source) {
    return "";
  }
  const at = source.indexOf(lower);
  const next = source[Math.min(source.length - 1, at + 1)] || "";
  return char === lower ? next : next.toUpperCase();
}

function operatorDelay(char: string, speed: string): number {
  const multiplier = speed === "fast" ? 0.55 : speed === "slow" ? 1.6 : 1;
  const base = char === "\n" ? 260 : /[.!?,:;]/u.test(char) ? 135 : char === " " ? 48 : 34;
  return Math.round((base + Math.random() * base) * multiplier);
}

function buildOperatorExport(): string {
  const local: Record<string, string> = {};
  for (let index = 0; index < localStorage.length; index += 1) {
    const key = localStorage.key(index) || "";
    if (key === "device" || key.startsWith("soty:")) {
      local[key] = localStorage.getItem(key) || "";
    }
  }
  const safeDevice = device ? {
    id: device.id,
    nick: device.nick,
    publicJwk: device.publicJwk,
    createdAt: device.createdAt
  } : null;
  const payload = {
    schema: "soty.operator-export.v1",
    exportedAt: new Date().toISOString(),
    selectedId,
    device: safeDevice,
    localStorage: local,
    tunnels: loadTunnels().map((tunnel) => ({
      ...tunnel,
      counterpartyLabel: counterpartyLabel(tunnel),
      text: texts.get(tunnel.id) || "",
      files: (files.get(tunnel.id) || []).map((file) => ({
        id: file.id,
        name: file.name,
        type: file.type,
        size: file.size,
        nick: file.nick,
        deviceId: file.deviceId,
        createdAt: file.createdAt
      }))
    }))
  };
  return `${JSON.stringify(payload, null, 2)}\n`;
}

function rememberWriter(tunnelId: string, activity: WriterActivity): void {
  const label = cleanNick(activity.nick);
  const text = tunnelId === selectedId && textarea ? textarea.value : texts.get(tunnelId) || "";
  const line = lineFromIndex(text, activity.index);
  const color = colorFor(`${label}:${activity.deviceId || tunnelId}`);
  const lines = writerLines.get(tunnelId) ?? new Map<number, WriterLine>();
  lines.set(line, {
    nick: label,
    deviceId: activity.deviceId,
    color,
    time: clock(),
    at: Date.now(),
    action: activity.action,
    preview: activity.preview
  });
  writerLines.set(tunnelId, lines);
}

function renderLineTags(): void {
  if (!lineGutter || !lineMeta) {
    return;
  }
  const labels = writerLines.get(selectedId) ?? new Map();
  const last = [...labels.values()].sort((a, b) => b.at - a.at)[0];
  lineGutter.innerHTML = last ? `<span style="--color:${last.color}">${escapeHtml(activityCode(last.action))}</span>` : "";
  lineMeta.innerHTML = last ? `<span style="--color:${last.color}">${escapeHtml(last.time)}</span>` : "";
}

function renderTextPaint(): void {
  if (!textarea || !textPaint) {
    return;
  }
  renderDialogChrome();
  const scroll = app.querySelector<HTMLDivElement>(".chat-scroll");
  const stickToBottom = scroll ? scroll.scrollHeight - scroll.scrollTop - scroll.clientHeight < 180 : false;
  const labels = writerLines.get(selectedId) ?? new Map();
  const text = textarea.value;
  const lines = text.endsWith("\n") ? text.slice(0, -1).split("\n") : text.split("\n");
  const active = activeActivities.get(selectedId);
  const activeLine = active ? lineFromIndex(text, active.index) : -1;
  textPaint.style.transform = "";
  if (!text.trim()) {
    textPaint.innerHTML = `
      <div class="chat-empty">
        <span>READY</span>
        <b>${escapeHtml(counterpartyLabelForSelected())}</b>
      </div>
    `;
    return;
  }
  let operatorBlock = false;
  const bubbles: {
    key: string;
    side: string;
    nick: string;
    color: string;
    time: string;
    className: string;
    lines: string[];
    live: WriterActivity | null;
  }[] = [];
  for (let index = 0; index < lines.length; index += 1) {
    const line = lines[index] ?? "";
    const label = labels.get(index);
    const state = classifyChatLine(line, operatorBlock);
    operatorBlock = state.operatorBlock;
    if (!line.trim() && !label) {
      if (bubbles.length > 0) {
        bubbles[bubbles.length - 1]?.lines.push("");
      }
      continue;
    }
    const speaker = speakerForLine(line, state.className, label);
    const live = active && index === activeLine ? active : null;
    const key = `${speaker.side}:${speaker.nick}:${speaker.deviceId}:${state.className}`;
    const current = bubbles[bubbles.length - 1];
    if (current && current.key === key && !live) {
      current.lines.push(line);
      continue;
    }
    bubbles.push({
      key,
      side: speaker.side,
      nick: speaker.nick,
      color: speaker.color,
      time: label?.time || clock(),
      className: state.className,
      lines: [line],
      live
    });
  }
  textPaint.innerHTML = bubbles.map((bubble) => {
    const body = bubble.lines
      .map((line) => line ? `<span>${escapeHtml(line)}</span>` : "<br>")
      .join("");
    const live = bubble.live
      ? `<em class="live-chip">${escapeHtml(activityCode(bubble.live.action))}${bubble.live.preview ? ` ${escapeHtml(compactPreview(bubble.live.preview))}` : ""}</em>`
      : "";
    return `
      <article class="chat-bubble ${bubble.side} ${bubble.className}" style="--bubble-color:${bubble.color}">
        <div class="bubble-meta">
          <span>${escapeHtml(initials(bubble.nick))}</span>
          <b>${escapeHtml(bubble.nick)}</b>
          <small>${escapeHtml(bubble.time)}</small>
          ${live}
        </div>
        <p>${body}</p>
      </article>
    `;
  }).join("");
  if (scroll && stickToBottom) {
    window.setTimeout(() => {
      scroll.scrollTop = scroll.scrollHeight;
    }, 0);
  }
}

function speakerForLine(
  line: string,
  className: string,
  label?: {
    readonly nick: string;
    readonly deviceId: string;
    readonly color: string;
    readonly time: string;
  }
): { readonly nick: string; readonly deviceId: string; readonly color: string; readonly side: string } {
  if (label) {
    return {
      nick: label.nick || counterpartyLabelForSelected(),
      deviceId: label.deviceId,
      color: label.color,
      side: label.deviceId && label.deviceId === device?.id ? "local" : "remote"
    };
  }
  const operator = operatorNameFromLine(line);
  if (operator || className.startsWith("is-operator")) {
    const nick = operator || "Operator";
    return {
      nick,
      deviceId: "operator",
      color: colorFor(`operator:${nick}`),
      side: "remote"
    };
  }
  const nick = counterpartyLabelForSelected();
  return {
    nick,
    deviceId: "",
    color: safeColor(undefined, `${nick}:${selectedId}`),
    side: "surface"
  };
}

function operatorNameFromLine(line: string): string {
  const match = line.trim().match(/^(.+?)\s+В·\s+\d{1,2}:\d{2}$/u);
  return cleanNick(match?.[1] || "");
}

function counterpartyLabelForSelected(): string {
  const tunnel = loadTunnels().find((item) => item.id === selectedId);
  return tunnel ? counterpartyLabel(tunnel) : ".";
}

function activityCode(action: WriterActivity["action"]): string {
  if (action === "erase") {
    return "DEL";
  }
  if (action === "edit") {
    return "EDIT";
  }
  return "TYPE";
}

function compactPreview(value: string): string {
  return value.replace(/\s+/gu, " ").trim().slice(0, 24);
}

function classifyChatLine(line: string, inOperatorBlock: boolean): { readonly className: string; readonly operatorBlock: boolean } {
  const trimmed = line.trim();
  if (!trimmed) {
    return { className: "is-empty", operatorBlock: inOperatorBlock };
  }
  if (isOperatorHeader(trimmed)) {
    return { className: "is-operator-head", operatorBlock: true };
  }
  if (trimmed === "Ответ:" || trimmed === "Ответьте ниже:" || trimmed === "Reply:") {
    return { className: "is-operator-reply", operatorBlock: false };
  }
  if (trimmed.startsWith("┌ ")) {
    return { className: "is-operator-head", operatorBlock: true };
  }
  if (trimmed.startsWith("│ ")) {
    return { className: "is-operator-body", operatorBlock: true };
  }
  if (trimmed.startsWith("└ ")) {
    return { className: "is-operator-reply", operatorBlock: false };
  }
  return { className: inOperatorBlock ? "is-operator-body" : "is-user-line", operatorBlock: inOperatorBlock };
}

function isOperatorHeader(line: string): boolean {
  return /^(Codex|Оператор|Operator)\s+·\s+\d{1,2}:\d{2}$/u.test(line);
}

function renderWriterPop(): void {
  const pop = app.querySelector<HTMLDivElement>(".writer-pop");
  if (!pop) {
    return;
  }
  const latest = latestWriterLine(selectedId);
  const activity = activeActivities.get(selectedId) ?? (latest && Date.now() - latest.at < 4200
    ? {
      deviceId: latest.deviceId,
      nick: latest.nick,
      index: 0,
      local: latest.deviceId === device?.id,
      action: latest.action,
      preview: latest.preview
    }
    : null);
  if (!activity) {
    pop.innerHTML = `<span class="idle-dot"></span><b>IDLE</b>`;
    return;
  }
  const nick = cleanNick(activity.nick) || counterpartyLabelForSelected();
  pop.innerHTML = `
    <span>${escapeHtml(initials(nick))}</span>
    <b>${escapeHtml(nick)}</b>
    <small>${escapeHtml(activityCode(activity.action))}</small>
  `;
}

function latestWriterLine(tunnelId: string): WriterLine | null {
  const lines = writerLines.get(tunnelId);
  if (!lines) {
    return null;
  }
  let latest: WriterLine | null = null;
  for (const line of lines.values()) {
    if (!latest || line.at > latest.at) {
      latest = line;
    }
  }
  return latest;
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
  const localLine = findFreshLineForDevice(selectedId, device.id);
  if (localLine !== null) {
    const localEnd = endOfLine(before, localLine);
    return {
      text: `${before.slice(0, localEnd)}${insertText}${before.slice(localEnd)}`,
      caret: localEnd + insertText.length
    };
  }
  const lineEnd = endOfLine(before, line);
  const separator = insertText.startsWith("\n") || (lineEnd > 0 && before[lineEnd - 1] === "\n") ? "" : "\n";
  const text = `${before.slice(0, lineEnd)}${separator}${insertText}${before.slice(lineEnd)}`;
  return {
    text,
    caret: lineEnd + separator.length + insertText.length
  };
}

function findFreshLineForDevice(tunnelId: string, deviceId: string): number | null {
  const lines = writerLines.get(tunnelId);
  if (!lines) {
    return null;
  }
  let bestLine: number | null = null;
  let bestAt = 0;
  const now = Date.now();
  for (const [line, label] of lines) {
    if (label.deviceId === deviceId && now - label.at < 8000 && label.at > bestAt) {
      bestLine = line;
      bestAt = label.at;
    }
  }
  return bestLine;
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

async function showQr(autoOpened = false): Promise<void> {
  if (!device) {
    return;
  }
  const currentDevice = device;
  const preserveSelection = sortedVisibleTunnels().length > 0;
  const tunnel = ensureInviteTunnel(preserveSelection);
  if (!tunnel) {
    return;
  }
  closeQrOverlay();
  const overlay = document.createElement("div");
  overlay.className = "qr-modal";
  overlay.innerHTML = `
    <div class="qr-sheet" data-tooltip="Окно приглашения по QR-коду" data-tooltip-side="bottom">
      <canvas data-tooltip="Покажи этот QR на втором устройстве"></canvas>
      <div class="qr-scanner" aria-live="polite">
        <video playsinline muted></video>
        <div class="qr-scan-status">Наведи камеру на QR</div>
      </div>
      <button class="icon-button refresh-button" type="button" aria-label="refresh" data-tooltip="Создать новый QR">${icon("refresh")}</button>
      <button class="icon-button scan-button" type="button" aria-label="scan" data-tooltip="Сканировать QR камерой">${icon("scan")}</button>
      <button class="icon-button copy-button" type="button" aria-label="copy" data-tooltip="Скопировать ссылку подключения">${icon("copy")}</button>
      <button class="icon-button close-button" type="button" aria-label="close" data-tooltip="Закрыть QR">${icon("close")}</button>
    </div>
  `;
  document.body.append(overlay);
  qrOverlay = overlay;
  qrMode = autoOpened ? "auto" : "manual";
  const canvas = overlay.querySelector("canvas");
  if (canvas) {
    attachQrResetGesture(canvas);
  }
  let currentUrl = "";
  const draw = async (nextTunnel: TunnelRecord) => {
    if (!canvas) {
      return;
    }
    const url = await inviteUrl(nextTunnel, currentDevice);
    currentUrl = url;
    await QRCode.toCanvas(canvas, url, {
      margin: 1,
      scale: 8,
      color: {
        dark: "#141414",
        light: "#f8f7f2"
      }
    });
  };
  await draw(tunnel);
  overlay.querySelector(".refresh-button")?.addEventListener("click", () => {
    const nextTunnel = rotateInviteTunnel(preserveSelection);
    if (nextTunnel) {
      void draw(nextTunnel);
    }
  });
  overlay.querySelector(".copy-button")?.addEventListener("click", () => {
    void copyText(currentUrl);
  });
  overlay.querySelector(".scan-button")?.addEventListener("click", () => {
    if (qrScanStream) {
      stopQrScanner();
      return;
    }
    void startQrScanner(overlay);
  });
  overlay.querySelector<HTMLButtonElement>(".close-button")?.addEventListener("click", () => closeQrOverlay());
}

async function startQrScanner(overlay: HTMLDivElement): Promise<void> {
  const scanner = overlay.querySelector<HTMLElement>(".qr-scanner");
  const video = overlay.querySelector<HTMLVideoElement>(".qr-scanner video");
  const status = overlay.querySelector<HTMLElement>(".qr-scan-status");
  const Detector = (window as Window & { BarcodeDetector?: BarcodeDetectorConstructor }).BarcodeDetector;
  if (!scanner || !video || !status) {
    return;
  }
  if (!navigator.mediaDevices?.getUserMedia) {
    status.textContent = "Камера недоступна";
    overlay.classList.add("is-scanning");
    return;
  }

  stopQrScanner();
  overlay.classList.add("is-scanning");
  status.textContent = "Наведи камеру на QR";
  try {
    qrScanStream = await navigator.mediaDevices.getUserMedia({
      audio: false,
      video: {
        facingMode: { ideal: "environment" }
      }
    });
    video.srcObject = qrScanStream;
    await video.play();
  } catch {
    status.textContent = "Не удалось открыть камеру";
    stopQrScanner();
    overlay.classList.add("is-scanning");
    return;
  }

  const detector = Detector ? new Detector({ formats: ["qr_code"] }) : null;
  const frame = document.createElement("canvas");
  const scan = async () => {
    if (qrOverlay !== overlay || !qrScanStream) {
      return;
    }
    try {
      const raw = await detectQrFromVideo(video, detector, frame);
      const joinCode = joinCodeFromScannedQr(raw);
      if (joinCode) {
        status.textContent = "QR найден";
        stopQrScanner();
        window.location.assign(`/?j=${encodeURIComponent(joinCode)}`);
        return;
      }
      if (raw) {
        status.textContent = "Это не QR Соты";
      }
    } catch {
      status.textContent = "Ищу QR";
    }
    qrScanFrame = window.requestAnimationFrame(scan);
  };
  qrScanFrame = window.requestAnimationFrame(scan);
}

function stopQrScanner(): void {
  if (qrScanFrame) {
    window.cancelAnimationFrame(qrScanFrame);
    qrScanFrame = 0;
  }
  qrScanStream?.getTracks().forEach((track) => track.stop());
  qrScanStream = null;
  qrOverlay?.classList.remove("is-scanning");
  const video = qrOverlay?.querySelector<HTMLVideoElement>(".qr-scanner video");
  if (video) {
    video.pause();
    video.srcObject = null;
  }
}

async function detectQrFromVideo(
  video: HTMLVideoElement,
  detector: BarcodeDetectorLike | null,
  frame: HTMLCanvasElement
): Promise<string> {
  if (detector) {
    const codes = await detector.detect(video);
    const raw = codes.find((item) => item.rawValue)?.rawValue || "";
    if (raw) {
      return raw;
    }
  }
  const width = video.videoWidth;
  const height = video.videoHeight;
  if (width <= 0 || height <= 0) {
    return "";
  }
  frame.width = width;
  frame.height = height;
  const context = frame.getContext("2d", { willReadFrequently: true });
  if (!context) {
    return "";
  }
  context.drawImage(video, 0, 0, width, height);
  const image = context.getImageData(0, 0, width, height);
  return jsQR(image.data, width, height)?.data || "";
}

function joinCodeFromScannedQr(value: string): string {
  const trimmed = value.trim();
  if (!trimmed) {
    return "";
  }
  try {
    return new URL(trimmed, window.location.origin).searchParams.get("j") || "";
  } catch {
    return "";
  }
}

function attachQrResetGesture(canvas: HTMLCanvasElement): void {
  canvas.addEventListener("click", (event) => {
    event.preventDefault();
    window.clearTimeout(qrResetTimer);
    qrResetClicks += 1;
    if (qrResetClicks >= 10) {
      resetQrResetGesture();
      window.location.assign("/?pwa=1&reset-local=1");
      return;
    }
    qrResetTimer = window.setTimeout(resetQrResetGesture, 6500);
  });
}

function resetQrResetGesture(): void {
  qrResetClicks = 0;
  window.clearTimeout(qrResetTimer);
  qrResetTimer = 0;
}

async function copyText(value: string): Promise<void> {
  if (!value) {
    return;
  }
  try {
    await navigator.clipboard.writeText(value);
    return;
  } catch {
    const input = document.createElement("textarea");
    input.value = value;
    input.style.position = "fixed";
    input.style.opacity = "0";
    document.body.append(input);
    input.focus();
    input.select();
    document.execCommand("copy");
    input.remove();
  }
}

function ensureInviteTunnel(preserveSelection: boolean): TunnelRecord | null {
  if (!device) {
    return null;
  }
  const current = loadTunnels();
  let tunnel = current.find((item) => !item.counterparty);
  if (!tunnel) {
    tunnel = createTunnel();
    const previousSelected = selectedId;
    const next = [tunnel, ...current];
    saveTunnels(next);
    tunnels = next;
    if (!preserveSelection) {
      selectedId = tunnel.id;
      saveSelectedTunnelId(tunnel.id);
    } else if (previousSelected) {
      selectedId = previousSelected;
      saveSelectedTunnelId(previousSelected);
    }
  } else if (!preserveSelection) {
    selectedId = tunnel.id;
    saveSelectedTunnelId(tunnel.id);
  }
  ensureSync(tunnel);
  return tunnel;
}

function closeQrOverlay(): void {
  resetQrResetGesture();
  stopQrScanner();
  qrOverlay?.remove();
  qrOverlay = null;
  qrMode = null;
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

function wait(ms: number): Promise<void> {
  return new Promise((resolve) => window.setTimeout(resolve, ms));
}

function initials(value: string): string {
  const parts = cleanNick(value).split(" ").filter(Boolean);
  const letters = parts.length > 1
    ? `${parts[0]?.[0] ?? ""}${parts[1]?.[0] ?? ""}`
    : cleanNick(value).slice(0, 2);
  return letters || ".";
}
