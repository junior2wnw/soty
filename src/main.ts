import QRCode from "qrcode";
import jsQR from "jsqr";
import { JoinRequest, LiveDraft, NoticeKnock, PeerInfo, ReceivedFile, RemoteCancel, RemoteCommand, RemoteGrant, RemoteOutput, RemoteRequest, RemoteScript, SyncedChessState, TerminalSnapshot, TunnelSync, WriterActivity } from "./sync";
import { icon } from "./icons";
import { colorFor, safeColor } from "./core/color";
import { clock } from "./core/time";
import { adoptAgentRelayFromUrl, askLocalAgentReply, bindLocalAgentRelay, checkAgentSourceWorker, checkLocalAgent, checkLocalCompanionAgent, downloadAgentInstaller, downloadAgentInstallerForDevice, grantAgentSourceAccess, hasAgentRelayId } from "./features/agent";
import type { LocalAgentOperatorTarget, LocalAgentReply, LocalAgentRequestSource, LocalAgentStatus } from "./features/agent";
import { agentSide, applyChessMove, boardSquares, buildGeniusLine, chessFromSnapshot, chooseAgentMove, createChessSnapshot, geniusCoach, isAgentTurn, isSquare, legalMovesForSquare, normalizeChessSnapshot, pieceGlyph, promotionChoices, sideName, statusText, withCoach } from "./features/chess";
import type { ChessCoach, ChessMode, ChessSnapshot } from "./features/chess";
import { filesFrom, formatFileSize, maxFileBytes, oversizedFilesFrom, renderFileRail } from "./features/files";
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
  selectedKey,
  saveSelectedTunnelId,
  saveTunnels,
  touchTunnel,
  tunnelsKey,
  tunnelFromAcceptedJoin,
  upsertTunnel
} from "./trustlink";
import "./style.css";
import type { Color, Move, PieceSymbol, Square } from "chess.js";

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

interface OperatorExportPayload {
  readonly schema?: string;
  readonly selectedId?: string;
  readonly device?: {
    readonly nick?: string;
  } | null;
  readonly localStorage?: Readonly<Record<string, unknown>>;
  readonly tunnels?: readonly unknown[];
}

interface RestoreResult {
  readonly count: number;
  readonly texts: Map<string, string>;
}

const root = document.querySelector<HTMLDivElement>("#app");
if (!root) {
  throw new Error("App root missing");
}
const app: HTMLDivElement = root;
installTooltips();

const agentDialogLabel = "Агент";
const legacyAgentDialogLabel = "Codex";
const agentDialogMinVersion = "0.3.16";

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
const peerDevices = new Map<string, readonly PeerInfo[]>();
const files = new Map<string, ReceivedFile[]>();
const fileNotices = new Map<string, { readonly text: string; readonly until: number }>();
const localDrafts = new Map<string, string>();
const liveDrafts = new Map<string, Map<string, LiveDraftState>>();
const liveDraftTimers = new Map<string, number>();
const liveDraftSendTimers = new Map<string, number>();
let remoteEnabled = loadRemoteEnabled();
let remoteAccess = loadRemoteAccess();
let terminalOpenId = "";
const terminalLogs = new Map<string, string[]>();
const terminalState = new Map<string, "idle" | "run" | "ok" | "bad" | "off">();
const chessStoreKey = "soty:chess:v1";
const chessGames = new Map<string, ChessSnapshot>();
const chessFlipped = new Set<string>();
const chessAgentTimers = new Map<string, number>();
const chessWelcomedGames = new Set<string>();
let chessOpenId = "";
let chessSelectedSquare: Square | "" = "";
let chessPromotion: { readonly tunnelId: string; readonly from: Square; readonly to: Square } | null = null;
let localAgent: LocalAgentStatus = { ok: false };
let agentProbeTimer = 0;
let agentProbe: Promise<LocalAgentStatus> | null = null;
let companionProbe: Promise<LocalAgentStatus> | null = null;
let agentSourceGrantRefreshAt = 0;
const agentSourceGrantRefreshMs = 30_000;
type WriterLine = {
  readonly nick: string;
  readonly deviceId: string;
  readonly color: string;
  readonly time: string;
  readonly at: number;
  readonly action: WriterActivity["action"];
  readonly preview: string;
};

type LiveDraftState = LiveDraft & {
  readonly at: number;
  readonly color: string;
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
let operatorBridgeAllowEmpty = false;
const operatorPending = new Map<string, string>();
let operatorBridgeEpoch = 0;
type OperatorRemoteRun = {
  readonly commandId: string;
  readonly tunnelId: string;
  readonly hostDeviceId: string;
  readonly startedAt: number;
  readonly timeoutMs: number;
  readonly kind: "run" | "script";
  readonly label: string;
};
const operatorRemoteRuns = new Map<string, OperatorRemoteRun>();
const operatorStartingTunnels = new Set<string>();
const localAgentRuns = new Map<string, WebSocket>();
const operatorChatQueues = new Map<string, Promise<void>>();
const operatorBridgeProtocol = "soty.operator-bridge.v2";
const agentReplyQueues = new Map<string, Promise<void>>();
const agentThinking = new Set<string>();
let agentSourceControlTunnelId = "";
let agentSourcePollTimer = 0;
let agentSourcePolling = false;
let agentSourcePollEpoch = 0;
let agentSourcePollController: AbortController | null = null;

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
  adoptAgentRelayFromUrl();

  if (shouldResetLocalState()) {
    await resetLocalSotyState();
    clearRemoteSessionState();
    remoteEnabled = loadRemoteEnabled();
    remoteAccess = loadRemoteAccess();
    terminalOpenId = "";
    chessOpenId = "";
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
        <button class="restore-button" type="button" aria-label="restore" data-tooltip="Восстановить backup Сот">${icon("upload")}</button>
        <button type="submit" aria-label="ok" data-tooltip="Сохранить имя">${icon("check")}</button>
        <input class="restore-file" type="file" accept="application/json,.json" />
      </form>
    </section>
  `;
  const form = app.querySelector<HTMLFormElement>("form");
  const input = app.querySelector<HTMLInputElement>("input");
  const restoreButton = app.querySelector<HTMLButtonElement>(".restore-button");
  const restoreFile = app.querySelector<HTMLInputElement>(".restore-file");
  input?.focus();
  void ensureOperatorBridge(true);
  restoreButton?.addEventListener("click", () => {
    restoreFile?.click();
  });
  restoreFile?.addEventListener("change", () => {
    const file = restoreFile.files?.[0];
    if (file) {
      void restoreFromOperatorExportText(file.text(), input);
    }
    restoreFile.value = "";
  });
  form?.addEventListener("submit", async (event) => {
    event.preventDefault();
    const nick = cleanNick(new FormData(form).get("nick")?.toString() || "");
    device = await createDevice(nick);
    finishDeviceBoot();
  });
}

function finishDeviceBoot(restoredTexts = new Map<string, string>()): void {
  operatorBridgeAllowEmpty = false;
  const pending = loadPendingJoin();
  if (pending) {
    renderJoinWaiting(pending);
    return;
  }
  tunnels = loadTunnels();
  if (tunnels.length === 0) {
    tunnels = upsertTunnel(createTunnel());
  }
  selectedId = loadSelectedTunnelId() || selectedId || tunnels[0]?.id || "";
  if (selectedId) {
    saveSelectedTunnelId(selectedId);
  }
  renderApp();
  applyRestoredTextSnapshots(restoredTexts);
}

async function restoreFromOperatorExportText(textOrPromise: string | Promise<string>, nickInput?: HTMLInputElement | null): Promise<RestoreResult | null> {
  try {
    const payload = parseOperatorExportPayload(await textOrPromise);
    const restored = await restoreOperatorExportPayload(payload);
    finishDeviceBoot(restored.texts);
    return restored;
  } catch (error) {
    console.warn("[soty] operator export restore failed", error);
    if (nickInput) {
      nickInput.value = "";
      nickInput.placeholder = "backup?";
      nickInput.focus();
    }
    return null;
  }
}

async function restoreOperatorExportPayload(payload: OperatorExportPayload): Promise<RestoreResult> {
  if (!device) {
    device = await createDevice(cleanNick(payload.device?.nick || "Soty"));
  }

  const restored = restoredTunnelsFromPayload(payload);
  if (restored.tunnels.length > 0) {
    saveTunnels(restored.tunnels);
    const selected = restoredSelectedId(payload, restored.tunnels) || restored.tunnels[0]?.id || "";
    selectedId = selected;
    if (selected) {
      saveSelectedTunnelId(selected);
    }
  }

  rememberAppRuntime();
  clearRemoteSessionState();
  remoteEnabled = loadRemoteEnabled();
  remoteAccess = loadRemoteAccess();
  terminalOpenId = "";
  chessOpenId = "";
  return {
    count: restored.tunnels.length,
    texts: restored.texts
  };
}

function parseOperatorExportPayload(text: string): OperatorExportPayload {
  const cleanText = text.charCodeAt(0) === 0xfeff ? text.slice(1) : text;
  const parsed: unknown = JSON.parse(cleanText);
  if (!isRecord(parsed) || parsed.schema !== "soty.operator-export.v1") {
    throw new Error("Unsupported Soty backup file");
  }
  return parsed as OperatorExportPayload;
}

function restoredTunnelsFromPayload(payload: OperatorExportPayload): { readonly tunnels: TunnelRecord[]; readonly texts: Map<string, string> } {
  const rawTunnels = payload.tunnels?.length
    ? payload.tunnels
    : parseStoredTunnelList(recordString(payload.localStorage, tunnelsKey));
  const now = new Date().toISOString();
  const seen = new Set<string>();
  const textsByTunnel = new Map<string, string>();
  const tunnelsToRestore: TunnelRecord[] = [];

  for (const raw of rawTunnels) {
    const tunnel = normalizeImportedTunnel(raw, now);
    if (!tunnel || seen.has(tunnel.id)) {
      continue;
    }
    seen.add(tunnel.id);
    tunnelsToRestore.push(tunnel);
    if (isRecord(raw) && typeof raw.text === "string" && raw.text.length > 0) {
      textsByTunnel.set(tunnel.id, raw.text.slice(0, 200_000));
    }
  }

  return {
    tunnels: tunnelsToRestore,
    texts: textsByTunnel
  };
}

function parseStoredTunnelList(value: string): readonly unknown[] {
  if (!value) {
    return [];
  }
  try {
    const parsed: unknown = JSON.parse(value);
    return Array.isArray(parsed) ? parsed : [];
  } catch {
    return [];
  }
}

function normalizeImportedTunnel(raw: unknown, now: string): TunnelRecord | null {
  if (!isRecord(raw)) {
    return null;
  }
  const id = recordString(raw, "id");
  const key = recordString(raw, "key");
  if (!id || !key || id.length > 256 || key.length > 2048) {
    return null;
  }
  const label = cleanNick(recordString(raw, "label") || recordString(raw, "counterpartyLabel") || ".");
  const color = recordString(raw, "color");
  const score = typeof raw.score === "number" && Number.isFinite(raw.score)
    ? Math.max(0, Math.min(Math.round(raw.score), 1_000_000))
    : 0;
  const counterparty = typeof raw.counterparty === "boolean" ? raw.counterparty : label !== ".";
  return {
    id,
    key,
    label,
    ...(color ? { color } : {}),
    counterparty,
    archived: raw.archived === true,
    ...(raw.agent === true ? { agent: true } : {}),
    score,
    lastActionAt: recordString(raw, "lastActionAt") || now,
    createdAt: recordString(raw, "createdAt") || now,
    updatedAt: recordString(raw, "updatedAt") || now,
    unread: raw.unread === true
  };
}

function restoredSelectedId(payload: OperatorExportPayload, restoredTunnels: readonly TunnelRecord[]): string {
  const wanted = payload.selectedId || recordString(payload.localStorage, selectedKey);
  if (wanted && restoredTunnels.some((tunnel) => tunnel.id === wanted)) {
    return wanted;
  }
  return "";
}

function applyRestoredTextSnapshots(restoredTexts: Map<string, string>): void {
  if (restoredTexts.size === 0) {
    return;
  }
  for (const [tunnelId, text] of restoredTexts) {
    texts.set(tunnelId, text);
    syncs.get(tunnelId)?.setText(text);
  }
  if (selectedId && restoredTexts.has(selectedId)) {
    applySelectedText();
  }
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
        <button class="agent-open retro-icon-button" type="button" aria-label="поговорить с агентом" data-tooltip="Поговорить с агентом">${icon("person")}</button>
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
          <button class="clear-dialog-button retro-icon-button" type="button" aria-label="clear dialog" data-tooltip="Очистить диалог">${icon("refresh")}</button>
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
        <div class="chess-panel" data-mode="peer">
          <div class="chess-head">
            <span class="chess-led"></span>
            <b class="chess-title">CHESS</b>
            <small class="chess-status">READY</small>
            <button class="chess-coach" type="button" data-tooltip="Гений">${icon("person")}<span>ГЕНИЙ</span></button>
            <button class="chess-flip" type="button" aria-label="flip" data-tooltip="Развернуть доску">${icon("refresh")}</button>
            <button class="chess-new" type="button" aria-label="new chess game" data-tooltip="Новая партия">${icon("check")}</button>
            <button class="chess-close" type="button" aria-label="close chess" data-tooltip="Закрыть шахматы">${icon("close")}</button>
          </div>
          <div class="chess-body">
            <div class="chess-board" aria-label="chess board"></div>
            <aside class="chess-desk">
              <div class="chess-turn"></div>
              <div class="chess-stats"></div>
              <ol class="chess-moves"></ol>
            </aside>
          </div>
          <div class="chess-promotion" hidden></div>
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
            <button class="side-action agent-action" type="button" aria-label="поговорить с агентом" data-tooltip="Поговорить с агентом">${icon("person")}<span>AGENT</span></button>
            <button class="side-action remote-action" type="button" aria-label="remote" data-tooltip="Включить удаленное подключение">${icon("remote")}<span>LINK</span></button>
            <button class="side-action close-action" type="button" aria-label="close" data-tooltip="Закрыть соту">${icon("close")}<span>DROP</span></button>
            <button class="side-action chess-action" type="button" aria-label="chess" data-tooltip="Шахматы">${icon("chess")}<span>CHESS</span></button>
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
  app.querySelector<HTMLButtonElement>(".agent-open")?.addEventListener("click", () => {
    void startAgentDialog();
  });
  app.querySelector<HTMLButtonElement>(".clear-dialog-button")?.addEventListener("click", () => {
    startFreshDialog();
  });
  renderTiles();
  composer?.addEventListener("input", () => rememberComposerDraft());
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
    const tunnelId = activeTerminalTunnelId();
    if (tunnelId) {
      closeRemoteMode(tunnelId);
    }
  });
  app.querySelector<HTMLFormElement>(".terminal-form")?.addEventListener("submit", (event) => {
    event.preventDefault();
    void sendTerminalCommand();
  });
  app.querySelector<HTMLDivElement>(".chess-panel")?.addEventListener("click", (event) => {
    handleChessPanelClick(event);
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
  app.querySelector<HTMLButtonElement>(".agent-action")?.addEventListener("click", () => {
    void startAgentDialog();
  });
  app.querySelector<HTMLButtonElement>(".chess-action")?.addEventListener("click", () => {
    void openChessForSelected();
  });
  app.querySelector<HTMLButtonElement>(".remote-action")?.addEventListener("click", () => {
    if (selectedId) {
      if (isAgentTunnelId(selectedId)) {
        void toggleAgentRemoteGrant(selectedId);
      } else {
        void toggleRemoteGrant(selectedId);
      }
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
  renderChess();
  void ensureOperatorBridge();
  resumeAgentSourceControl();
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
    renderEmptyHiveActions(field);
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
      selectTunnel(id);
      clearTunnelNotices(id);
      tunnels = markTunnel(id, false);
      renderTiles();
      applySelectedText(true);
      renderFiles();
      renderTerminal();
      renderChess();
    },
    menu: (id, x, y) => {
      openCounterpartyMenu(x, y, {
        attach: () => {
          selectTunnel(id);
          fileInput?.click();
        },
        knock: () => {
          selectTunnel(id);
          syncs.get(id)?.sendKnock("*");
          tunnels = touchTunnel(id);
          renderTiles();
        },
        remote: () => {
          selectTunnel(id);
          if (isAgentTunnelId(id)) {
            void toggleAgentRemoteGrant(id);
          } else {
            void toggleRemoteGrant(id);
          }
        },
        close: () => closeTunnel(id)
      }, {
        remoteEnabled: remoteEnabled.has(id)
      });
    }
  });
  renderDialogChrome();
}

function renderEmptyHiveActions(field: HTMLDivElement): void {
  const actions = document.createElement("div");
  actions.className = "empty-hive-actions";
  actions.innerHTML = `
    <button class="empty-hive-action empty-agent-action" type="button" aria-label="поговорить с агентом" data-tooltip="Поговорить с агентом">
      ${icon("person")}
      <span>AGENT</span>
    </button>
    <button class="empty-hive-action empty-qr-action" type="button" aria-label="qr" data-tooltip="Показать QR для подключения">
      ${icon("qr")}
      <span>QR</span>
    </button>
  `;
  field.append(actions);
  actions.addEventListener("pointerdown", (event) => {
    event.stopPropagation();
  });
  actions.querySelector<HTMLButtonElement>(".empty-agent-action")?.addEventListener("click", () => {
    void startAgentDialog();
  });
  actions.querySelector<HTMLButtonElement>(".empty-qr-action")?.addEventListener("click", () => {
    void showQr();
  });
}

async function toggleRemoteGrant(id: string): Promise<void> {
  if (remoteEnabled.has(id)) {
    closeRemoteMode(id);
    return;
  }

  await enableRemoteGrant(id);
}

async function enableRemoteGrant(id: string, targetDeviceId = "*"): Promise<boolean> {
  const agent = await refreshLocalCompanion();
  if (!agent.ok) {
    renderAgentInstall(id);
    return false;
  }

  remoteEnabled = setRemoteEnabled(id, true);
  syncs.get(id)?.grantRemote(true, targetDeviceId);
  terminalOpenId = id;
  setTerminalState(id, "idle");
  renderTerminal();
  renderTiles();
  return true;
}

async function toggleAgentRemoteGrant(agentTunnelId: string): Promise<void> {
  if (remoteEnabled.has(agentTunnelId)) {
    closeRemoteMode(agentTunnelId);
    return;
  }
  if (!device) {
    return;
  }
  const companion = await ensureAgentSourceCompanion();
  if (!companion.ok) {
    renderAgentInstall(agentTunnelId, () => {
      void toggleAgentRemoteGrant(agentTunnelId);
    }, isAgentSourceCompanionReady, ensureAgentSourceCompanion, device);
    return;
  }
  if (!isAgentSourceCompanionReady(companion)) {
    renderAgentInstall(agentTunnelId, () => {
      void toggleAgentRemoteGrant(agentTunnelId);
    }, isAgentSourceCompanionReady, ensureAgentSourceCompanion, device);
    return;
  }
  const granted = await grantAgentSourceAccess(device.id, device.nick, true, agentSourceClientState());
  if (!granted) {
    await typeOperatorChat(
      agentTunnelId,
      formatOperatorChat("LINK не смог подключить командный канал агента. Проверь интернет и попробуй нажать LINK ещё раз.", "sysadmin"),
      "fast"
    );
    return;
  }
  remoteEnabled = setRemoteEnabled(agentTunnelId, true);
  agentSourceGrantRefreshAt = Date.now() + agentSourceGrantRefreshMs;
  terminalOpenId = agentTunnelId;
  setTerminalState(agentTunnelId, "idle");
  appendTerminalLine(agentTunnelId, "+ agent link");
  renderTerminal();
  renderTiles();
  startAgentSourceControl(agentTunnelId);
}

function isAgentSourceCompanionReady(agent: LocalAgentStatus): boolean {
  return agent.ok === true && agent.relay === true && agent.sourceWorker === true;
}

async function ensureAgentSourceCompanion(): Promise<LocalAgentStatus> {
  let agent = await refreshLocalCompanion();
  if (device && !isAgentSourceCompanionReady(agent)) {
    const sourceAgent = await checkAgentSourceWorker(device.id, 1200);
    if (isAgentSourceCompanionReady(sourceAgent)) {
      localAgent = sourceAgent;
      return sourceAgent;
    }
  }
  if (!device || isAgentSourceCompanionReady(agent)) {
    return agent;
  }
  if (agent.ok) {
    await bindLocalAgentRelay(device, 1800).catch(() => false);
    const deadline = Date.now() + 4500;
    do {
      await wait(350);
      const sourceAgent = await checkAgentSourceWorker(device.id, 1200);
      agent = isAgentSourceCompanionReady(sourceAgent) ? sourceAgent : await checkLocalAgent(1200);
      localAgent = agent;
      if (isAgentSourceCompanionReady(agent)) {
        return agent;
      }
    } while (Date.now() < deadline);
  }
  return agent;
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

async function refreshLocalCompanion(): Promise<LocalAgentStatus> {
  window.clearTimeout(agentProbeTimer);
  companionProbe = companionProbe || checkLocalCompanionAgent().finally(() => {
    companionProbe = null;
  });
  localAgent = await companionProbe;
  document.querySelector(".agent-sheet")?.classList.toggle("is-ok", localAgent.ok);
  if (localAgent.ok) {
    agentProbeTimer = window.setTimeout(() => void refreshLocalCompanion(), 30_000);
  }
  return localAgent;
}

function renderAgentInstall(
  tunnelId: string,
  onReady?: () => void,
  isReady: (agent: LocalAgentStatus) => boolean = (agent) => agent.ok,
  refresh: () => Promise<LocalAgentStatus> = refreshLocalCompanion,
  sourceDeviceForInstaller?: { readonly id?: string; readonly nick?: string }
): void {
  document.querySelector(".agent-modal")?.remove();
  const overlay = document.createElement("div");
  overlay.className = "agent-modal";
  overlay.innerHTML = `
    <div class="agent-sheet${isReady(localAgent) ? " is-ok" : ""}" data-tooltip="Панель установки локального Soty-агента" data-tooltip-side="bottom">
      <span class="agent-mark" data-tooltip="Локальный агент нужен для команд Windows">${icon("remote")}</span>
      <button class="icon-button chess-install-button" type="button" aria-label="chess" data-tooltip="Играть с агентом в шахматы">${icon("chess")}</button>
      <button class="icon-button download-button" type="button" aria-label="download" data-tooltip="Скачать Soty Agent">${icon("download")}</button>
      <button class="icon-button refresh-button" type="button" aria-label="refresh" data-tooltip="Проверить, запущен ли агент">${icon("refresh")}</button>
      <button class="icon-button close-button" type="button" aria-label="close" data-tooltip="Закрыть панель">${icon("close")}</button>
    </div>
  `;
  document.body.append(overlay);
  overlay.querySelector(".download-button")?.addEventListener("click", () => {
    if (sourceDeviceForInstaller?.id) {
      downloadAgentInstallerForDevice("machine", sourceDeviceForInstaller);
      return;
    }
    downloadAgentInstaller();
  });
  overlay.querySelector(".chess-install-button")?.addEventListener("click", () => {
    overlay.remove();
    void startAgentChess();
  });
  overlay.querySelector(".refresh-button")?.addEventListener("click", () => {
    void (async () => {
      const agent = await refresh();
      if (isReady(agent)) {
        overlay.remove();
        if (onReady) {
          onReady();
          return;
        }
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
    if (isAgentTunnelId(tunnelId) && device) {
      void grantAgentSourceAccess(device.id, device.nick, false);
      stopAgentSourceControl(tunnelId);
    }
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
    .filter((tunnel) => !tunnel.archived && hasCounterparty(tunnel))
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
  const visible = all.filter((tunnel) => !tunnel.archived && hasCounterparty(tunnel));
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

function selectTunnel(id: string): void {
  selectedId = id;
  saveSelectedTunnelId(id);
}

function hasVisibleSelection(id = selectedId): boolean {
  return Boolean(id && loadTunnels().some((tunnel) => !tunnel.archived && tunnel.id === id && hasCounterparty(tunnel)));
}

function shouldAutoSelectTunnel(id: string): boolean {
  return selectedId === id || !hasVisibleSelection();
}

function hasCounterparty(tunnel: TunnelRecord): boolean {
  const label = cleanNick(peers.get(tunnel.id) || tunnel.label || "");
  return Boolean(tunnel.counterparty || peers.has(tunnel.id) || (label !== "." && label !== device?.nick));
}

function counterpartyLabel(tunnel: TunnelRecord): string {
  const label = rawCounterpartyLabel(tunnel);
  if (tunnel.agent === true || label.toLowerCase() === legacyAgentDialogLabel.toLowerCase()) {
    return agentDialogLabel;
  }
  return label;
}

function startFreshDialog(): void {
  const active = loadTunnels().find((tunnel) => tunnel.id === selectedId);
  if (active && !isAgentTunnel(active) && hasCounterparty(active)) {
    clearCurrentDialog(active.id);
    return;
  }
  const fresh = active && isAgentTunnel(active)
    ? createFreshDialog(agentDialogLabel, { agent: true })
    : createFreshDialog();
  if (!fresh) {
    return;
  }
  if (active && isAgentTunnel(active)) {
    moveAgentLinkToFreshDialog(active.id, fresh.id);
  }
  renderApp();
}

function moveAgentLinkToFreshDialog(previousId: string, freshId: string): void {
  if (!device || previousId === freshId || !remoteEnabled.has(previousId)) {
    return;
  }
  remoteEnabled = setRemoteEnabled(previousId, false);
  remoteEnabled = setRemoteEnabled(freshId, true);
  terminalOpenId = freshId;
  terminalLogs.delete(freshId);
  terminalState.delete(previousId);
  setTerminalState(freshId, "idle");
  appendTerminalLine(freshId, "+ agent link");
  void grantAgentSourceAccess(device.id, device.nick, true, agentSourceClientState());
  startAgentSourceControl(freshId);
}

function clearCurrentDialog(tunnelId: string): void {
  let sync = syncs.get(tunnelId);
  if (!sync) {
    const tunnel = loadTunnels().find((item) => item.id === tunnelId);
    if (tunnel) {
      ensureSync(tunnel);
      sync = syncs.get(tunnelId);
    }
  }
  if (!sync) {
    return;
  }
  sync.setText("");
  texts.set(tunnelId, "");
  localDrafts.delete(tunnelId);
  writerLines.delete(tunnelId);
  activeActivities.delete(tunnelId);
  activeActivityTicks.delete(tunnelId);
  clearLiveDraftState(tunnelId);
  agentThinking.delete(tunnelId);
  void sync.sendLiveDraft("");
  if (tunnelId === selectedId) {
    if (textarea) {
      textarea.value = "";
    }
    if (composer) {
      composer.value = "";
      resizeComposer();
    }
    applySelectedText(true);
  }
  tunnels = touchTunnel(tunnelId);
  renderTiles();
  renderTextPaint();
  renderWriterPop();
}

async function startAgentDialog(): Promise<void> {
  let tunnel = findActiveAgentDialog();
  if (!tunnel) {
    tunnel = createFreshDialog(agentDialogLabel, { agent: true, archiveCurrent: false });
  } else {
    tunnel = normalizeAgentDialog(tunnel.id) || tunnel;
    selectedId = tunnel.id;
    saveSelectedTunnelId(tunnel.id);
    tunnels = markTunnel(tunnel.id, false);
  }
  if (!tunnel) {
    return;
  }
  renderApp();
  const agent = await refreshLocalAgent();
  if (agent.ok && !agent.relay) {
    void bindLocalAgentRelay(device || undefined).then((bound) => {
      if (bound) {
        void refreshLocalAgent();
      }
    });
  }
  if (!agentSupportsDialogInbox(agent)) {
    renderAgentInstall(tunnel.id, () => {
      void ensureOperatorBridge();
      publishOperatorTargets();
      composer?.focus();
    }, agentSupportsDialogInbox, refreshLocalAgent);
    return;
  }
  await ensureOperatorBridge();
  publishOperatorTargets();
  composer?.focus();
}

function findActiveAgentDialog(): TunnelRecord | null {
  return sortedVisibleTunnels().find((tunnel) => isAgentTunnel(tunnel))
    || loadTunnels().find((tunnel) => !tunnel.archived && isAgentTunnel(tunnel))
    || null;
}

function isAgentTunnel(tunnel: TunnelRecord): boolean {
  return tunnel.agent === true || rawCounterpartyLabel(tunnel).toLowerCase() === legacyAgentDialogLabel.toLowerCase();
}

function createFreshDialog(
  labelOverride = "",
  options: { readonly agent?: boolean; readonly archiveCurrent?: boolean } = {}
): TunnelRecord | null {
  if (!device) {
    return null;
  }
  const current = loadTunnels();
  const active = current.find((tunnel) => tunnel.id === selectedId);
  const activeLabel = cleanNick(active ? counterpartyLabel(active) : "");
  const rawRequestedLabel = cleanNick(labelOverride);
  const requestedLabel = rawRequestedLabel === "." ? "" : rawRequestedLabel;
  const label = requestedLabel || (activeLabel && activeLabel !== "." && activeLabel !== device.nick ? activeLabel : agentDialogLabel);
  const now = new Date().toISOString();
  const isAgent = options.agent === true || (!requestedLabel && active?.agent === true);
  const archiveCurrent = options.archiveCurrent !== false;
  const fresh = {
    ...createTunnel(label, true),
    ...(isAgent ? { agent: true } : {}),
    color: (archiveCurrent ? active?.color : "") || colorFor(`${label}:${now}`)
  };
  const next = [
    fresh,
    ...current.map((tunnel) => tunnel.id === selectedId
      ? { ...tunnel, archived: archiveCurrent, unread: false, updatedAt: now, lastActionAt: now }
      : tunnel)
  ];
  saveTunnels(next);
  selectedId = fresh.id;
  saveSelectedTunnelId(fresh.id);
  tunnels = next;
  localDrafts.delete(active?.id || "");
  texts.set(fresh.id, "");
  ensureSync(fresh);
  return fresh;
}

function rawCounterpartyLabel(tunnel: TunnelRecord): string {
  return cleanNick(peers.get(tunnel.id) || tunnel.label || ".");
}

function normalizeAgentDialog(tunnelId: string): TunnelRecord | null {
  const current = loadTunnels();
  const now = new Date().toISOString();
  let normalized: TunnelRecord | null = null;
  const next = current.map((tunnel) => {
    if (tunnel.id !== tunnelId) {
      return tunnel;
    }
    if (tunnel.agent === true && tunnel.label === agentDialogLabel) {
      normalized = tunnel;
      return tunnel;
    }
    normalized = {
      ...tunnel,
      agent: true,
      label: agentDialogLabel,
      updatedAt: now
    };
    return normalized;
  });
  if (normalized) {
    saveTunnels(next);
    tunnels = next;
  }
  return normalized;
}

function agentSupportsDialogInbox(agent: LocalAgentStatus): boolean {
  return agent.ok
    && (agent.relay === true || compareVersion(agent.version || "0.0.0", agentDialogMinVersion) >= 0);
}

function compareVersion(left: string, right: string): number {
  const a = left.split(".").map((part) => Number.parseInt(part, 10) || 0);
  const b = right.split(".").map((part) => Number.parseInt(part, 10) || 0);
  const length = Math.max(a.length, b.length);
  for (let index = 0; index < length; index += 1) {
    const diff = (a[index] || 0) - (b[index] || 0);
    if (diff !== 0) {
      return diff;
    }
  }
  return 0;
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
    onTerminal: (terminal) => {
      applySyncedTerminal(tunnel.id, terminal);
    },
    onChess: (chess) => {
      applySyncedChess(tunnel.id, chess);
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
      if (tunnel.id === selectedId) {
        touchSelected();
        renderLineTags();
        renderTextPaint();
        renderWriterPop();
      }
    },
    onLiveDraft: (draft) => {
      applyLiveDraft(tunnel.id, draft);
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
    onRemoteCancel: (cancel) => {
      applyRemoteCancel(tunnel.id, cancel);
    },
    onRemoteOutput: (output) => {
      applyRemoteOutput(tunnel.id, output);
    },
    onPeers: (items) => {
      peerDevices.set(tunnel.id, items);
      const accessChanged = syncRemoteAccessWithPeers(tunnel.id, items);
      const label = items.map((item) => item.nick).filter(Boolean).join(" ");
      if (label) {
        setTunnelCounterparty(tunnel.id, label);
        renderTiles();
      } else if (accessChanged) {
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
      peerDevices.delete(tunnel.id);
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

function syncRemoteAccessWithPeers(tunnelId: string, items: readonly { readonly id: string }[]): boolean {
  const hostDeviceId = remoteAccess.get(tunnelId);
  if (!hostDeviceId || items.some((item) => item.id === hostDeviceId)) {
    return false;
  }
  remoteAccess = setRemoteAccess(tunnelId, "", false);
  if (terminalOpenId === tunnelId) {
    terminalOpenId = "";
  }
  setTerminalState(tunnelId, "off");
  renderTerminal();
  publishOperatorTargets();
  return true;
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
    const shouldFocus = shouldAutoSelectTunnel(tunnel.id);
    setTunnelCounterparty(tunnel.id, nick);
    if (shouldFocus) {
      selectTunnel(tunnel.id);
    } else {
      tunnels = markTunnel(tunnel.id, true);
    }
    syncs.get(tunnel.id)?.acceptJoin(request, device?.nick || ".");
    renderTiles();
    if (shouldFocus) {
      applySelectedText();
    }
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
  if (chessOpenId === id) {
    chessOpenId = "";
  }
  const chessTimer = chessAgentTimers.get(id);
  if (chessTimer) {
    window.clearTimeout(chessTimer);
    chessAgentTimers.delete(id);
  }
  terminalLogs.delete(id);
  terminalState.delete(id);
  chessGames.delete(id);
  chessFlipped.delete(id);
  forgetChessSnapshot(id);
  writerLines.delete(id);
  activeActivities.delete(id);
  activeActivityTicks.delete(id);
  clearLiveDraftState(id);
  agentThinking.delete(id);
  localDrafts.delete(id);
  files.delete(id);
  fileNotices.delete(id);
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
    clearLiveDraftState(tunnel.id);
    agentThinking.delete(tunnel.id);
    chessGames.delete(tunnel.id);
    chessFlipped.delete(tunnel.id);
    forgetChessSnapshot(tunnel.id);
    files.delete(tunnel.id);
    fileNotices.delete(tunnel.id);
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
  const tunnelId = selectedId;
  const sync = syncs.get(tunnelId);
  if (!sync) {
    return;
  }
  const accepted = filesFrom(list);
  const oversized = oversizedFilesFrom(list);
  let failed = 0;
  for (const file of accepted) {
    try {
      const localFile = await sync.sendFile(file);
      files.set(tunnelId, [localFile, ...(files.get(tunnelId) ?? []).filter((item) => item.id !== localFile.id)]);
    } catch {
      failed += 1;
    }
  }
  if (oversized.length > 0 || failed > 0) {
    const parts = [
      oversized.length > 0 ? `Слишком большой файл: максимум ${formatFileSize(maxFileBytes)}` : "",
      failed > 0 ? "Не отправилось, связь восстановится и можно повторить" : ""
    ].filter(Boolean);
    setFileNotice(tunnelId, parts.join(". "));
  }
  tunnels = touchTunnel(tunnelId);
  renderTiles();
  if (tunnelId === selectedId) {
    renderFiles();
  }
}

function renderFiles(): void {
  const rail = app.querySelector<HTMLDivElement>(".file-rail");
  if (!rail) {
    return;
  }
  const tunnel = loadTunnels().find((item) => item.id === selectedId);
  const color = safeColor(tunnel?.color, (tunnel?.label || selectedId) + selectedId);
  renderFileRail(rail, files.get(selectedId) ?? [], color, deleteFile);
  renderFileNotice(rail, color);
}

function setFileNotice(tunnelId: string, text: string): void {
  fileNotices.set(tunnelId, { text, until: Date.now() + 9000 });
  window.setTimeout(() => {
    const notice = fileNotices.get(tunnelId);
    if (notice && notice.until <= Date.now()) {
      fileNotices.delete(tunnelId);
      if (tunnelId === selectedId) {
        renderFiles();
      }
    }
  }, 9200);
}

function renderFileNotice(rail: HTMLDivElement, color: string): void {
  const notice = fileNotices.get(selectedId);
  if (!notice) {
    return;
  }
  if (notice.until <= Date.now()) {
    fileNotices.delete(selectedId);
    return;
  }
  const chip = document.createElement("div");
  chip.className = "file-chip file-notice";
  chip.style.setProperty("--color", color);
  chip.textContent = notice.text;
  rail.prepend(chip);
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
  if (shouldAutoSelectTunnel(tunnelId)) {
    selectTunnel(tunnelId);
    renderTiles();
    applySelectedText();
  } else {
    tunnels = markTunnel(tunnelId, true);
    renderTiles();
  }
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
      const agent = await refreshLocalCompanion();
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
  if (!device || !grantTargetsThisDevice(command.targetDeviceId)) {
    return;
  }
  if (!remoteEnabled.has(tunnelId)) {
    void syncs.get(tunnelId)?.sendRemoteOutput(command.deviceId, command.id, "! access", 409);
    return;
  }
  selectedId = tunnelId;
  saveSelectedTunnelId(tunnelId);
  terminalOpenId = tunnelId;
  setTerminalState(tunnelId, "run");
  appendTerminalLine(tunnelId, `< ${command.command}`);
  clearTunnelNotices(tunnelId);
  tunnels = markTunnel(tunnelId, false);
  renderTiles();
  applySelectedText(true);
  renderFiles();
  void runLocalAgentCommand(tunnelId, command);
  renderTerminal();
}

function applyRemoteScript(tunnelId: string, script: RemoteScript): void {
  if (!device || !grantTargetsThisDevice(script.targetDeviceId)) {
    return;
  }
  if (!remoteEnabled.has(tunnelId)) {
    void syncs.get(tunnelId)?.sendRemoteOutput(script.deviceId, script.id, "! access", 409);
    return;
  }
  selectedId = tunnelId;
  saveSelectedTunnelId(tunnelId);
  terminalOpenId = tunnelId;
  setTerminalState(tunnelId, "run");
  appendTerminalLine(tunnelId, `< ${script.name || "script"}`);
  clearTunnelNotices(tunnelId);
  tunnels = markTunnel(tunnelId, false);
  renderTiles();
  applySelectedText(true);
  renderFiles();
  void runLocalAgentScript(tunnelId, script);
  renderTerminal();
}

function applyRemoteCancel(tunnelId: string, cancel: RemoteCancel): void {
  if (!device || !grantTargetsThisDevice(cancel.targetDeviceId)) {
    return;
  }
  if (!remoteEnabled.has(tunnelId)) {
    return;
  }
  const stopped = stopLocalAgentRun(cancel.commandId);
  appendTerminalLine(tunnelId, "! stop requested");
  if (stopped) {
    void syncs.get(tunnelId)?.sendRemoteOutput(cancel.deviceId, cancel.commandId, "! cancelled\n", 130);
  }
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
    appendTerminalExitLine(tunnelId, output.exitCode);
  }
  const operatorId = operatorPending.get(output.commandId);
  if (operatorId) {
    sendOperatorOutput(operatorId, output.text, output.exitCode);
    if (typeof output.exitCode === "number") {
      operatorPending.delete(output.commandId);
      operatorRemoteRuns.delete(operatorId);
    }
  }
  terminalOpenId = tunnelId;
  renderTerminal();
}

async function sendTerminalCommand(): Promise<void> {
  const tunnelId = activeTerminalTunnelId();
  if (!tunnelId || !device) {
    return;
  }
  const hostDeviceId = remoteAccess.get(tunnelId);
  const sync = syncs.get(tunnelId);
  const input = app.querySelector<HTMLInputElement>(".terminal-form input");
  const command = input?.value.trim() || "";
  if (!hostDeviceId || !sync || !command) {
    return;
  }
  if (input) {
    input.value = "";
  }
  setTerminalState(tunnelId, "run");
  appendTerminalLine(tunnelId, `$ ${command}`);
  renderTerminal();
  await sync.sendRemoteCommand(hostDeviceId, command);
}

async function ensureOperatorBridge(allowEmpty = false): Promise<void> {
  operatorBridgeAllowEmpty = operatorBridgeAllowEmpty || allowEmpty;
  if (!operatorBridgeAllowEmpty && !hasOperatorTargets()) {
    closeOperatorBridge();
    return;
  }
  if (operatorSocket && (operatorSocket.readyState === WebSocket.OPEN || operatorSocket.readyState === WebSocket.CONNECTING)) {
    publishOperatorTargets();
    resumeAgentSourceControl();
    return;
  }
  window.clearTimeout(operatorReconnectTimer);
  const agent = await checkLocalCompanionAgent(1500);
  if (!agent.ok || (!operatorBridgeAllowEmpty && !hasOperatorTargets())) {
    if (operatorBridgeAllowEmpty || hasOperatorTargets()) {
      operatorReconnectTimer = window.setTimeout(() => void ensureOperatorBridge(operatorBridgeAllowEmpty), 1800);
    }
    return;
  }
  const ws = new WebSocket("ws://127.0.0.1:49424");
  operatorSocket = ws;
  ws.onopen = () => {
    ws.send(JSON.stringify({
      type: "operator.attach",
      visible: document.visibilityState === "visible",
      protocol: operatorBridgeProtocol,
      capabilities: ["agent-new", "agent-message", "export-tail", "fast-fresh-dialog"]
    }));
    publishOperatorTargets();
    resumeAgentSourceControl();
  };
  ws.onmessage = (event) => {
    let message: {
      readonly type?: string;
      readonly id?: string;
      readonly target?: string;
      readonly sourceDeviceId?: string;
      readonly command?: string;
      readonly name?: string;
      readonly shell?: string;
      readonly script?: string;
      readonly runAs?: string;
      readonly text?: string;
      readonly speed?: string;
      readonly persona?: string;
      readonly version?: string;
      readonly timeoutMs?: number;
      readonly tailChars?: number;
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
    if (message.type === "operator.cancel") {
      void runOperatorCancel(message);
    }
    if (message.type === "operator.chat") {
      void runOperatorChat(message);
    }
    if (message.type === "operator.agent-message") {
      void runOperatorAgentMessage(message);
    }
    if (message.type === "operator.agent-new") {
      void runOperatorAgentNew(message);
    }
    if (message.type === "operator.terminal") {
      runOperatorTerminal(message);
    }
    if (message.type === "operator.access") {
      runOperatorAccess(message);
    }
    if (message.type === "operator.export") {
      runOperatorExport(message);
    }
    if (message.type === "operator.import") {
      void runOperatorImport(message);
    }
    if (message.type === "operator.updating") {
      cancelAllOperatorRemoteRuns("! agent updating");
    }
  };
  ws.onclose = () => {
    if (operatorSocket === ws) {
      operatorSocket = null;
      operatorBridgeEpoch += 1;
      cancelAllOperatorRemoteRuns("! operator bridge closed");
    }
    if (operatorBridgeAllowEmpty || hasOperatorTargets()) {
      operatorReconnectTimer = window.setTimeout(() => void ensureOperatorBridge(operatorBridgeAllowEmpty), 1800);
    }
  };
  ws.onerror = () => {
    ws.close();
  };
}

function closeOperatorBridge(): void {
  window.clearTimeout(operatorReconnectTimer);
  operatorBridgeEpoch += 1;
  cancelAllOperatorRemoteRuns("! operator bridge closed");
  operatorSocket?.close();
  operatorSocket = null;
  operatorBridgeAllowEmpty = false;
  operatorPending.clear();
  operatorRemoteRuns.clear();
  operatorStartingTunnels.clear();
}

function operatorTargetBusy(tunnelId: string): boolean {
  if (operatorStartingTunnels.has(tunnelId)) {
    return true;
  }
  for (const run of operatorRemoteRuns.values()) {
    if (run.tunnelId === tunnelId) {
      return true;
    }
  }
  return false;
}

function safeOperatorTimeoutMs(value: unknown): number {
  const timeoutMs = typeof value === "number" ? value : 0;
  if (!Number.isFinite(timeoutMs) || timeoutMs <= 0) {
    return 0;
  }
  return Math.max(1000, Math.min(Math.trunc(timeoutMs), 2 * 60 * 60_000));
}

function cancelAllOperatorRemoteRuns(reason: string): void {
  const runs = [...operatorRemoteRuns.entries()];
  operatorStartingTunnels.clear();
  for (const [requestId, run] of runs) {
    cancelOperatorRemoteRun(requestId, run, reason);
  }
  if (runs.length > 0) {
    renderTerminal();
  }
}

function cancelOperatorRemoteRun(requestId: string, run: OperatorRemoteRun, reason: string): void {
  appendTerminalLine(run.tunnelId, reason);
  setTerminalState(run.tunnelId, "bad");
  const sync = syncs.get(run.tunnelId);
  if (sync) {
    void sync.sendRemoteCancel(run.hostDeviceId, run.commandId).catch(() => undefined);
  }
  sendOperatorOutput(requestId, `${reason}\n`, 130);
  operatorRemoteRuns.delete(requestId);
  operatorPending.delete(run.commandId);
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
    deviceId: device?.id || "",
    deviceNick: device?.nick || "",
    targets: operatorTargets()
  }));
}

function operatorTargets(): LocalAgentOperatorTarget[] {
  return sortedVisibleTunnels()
    .filter((tunnel) => !isAgentTunnel(tunnel))
    .map((tunnel, index) => {
      const deviceIds = [...new Set((peerDevices.get(tunnel.id) ?? []).map((peer) => peer.id).filter(Boolean))];
      const hostDeviceId = remoteAccess.get(tunnel.id) || "";
      return {
        id: tunnel.id,
        label: counterpartyLabel(tunnel),
        deviceIds,
        hostDeviceId,
        access: remoteAccess.has(tunnel.id),
        host: remoteEnabled.has(tunnel.id),
        selected: tunnel.id === selectedId,
        rank: index + 1,
        lastActionAt: tunnel.lastActionAt || tunnel.updatedAt
      };
    });
}

function preferredAgentOperatorTargetForDialog(
  text: string,
  targets: readonly LocalAgentOperatorTarget[]
): LocalAgentOperatorTarget | null {
  const accessTargets = targets.filter((target) => target.access === true);
  const byPrefix = targetMentionedAtStart(text, accessTargets)
    || targetMentionedAtStart(text, targets);
  if (byPrefix?.access === true) {
    return byPrefix;
  }
  return null;
}

function targetMentionedAtStart(text: string, targets: readonly LocalAgentOperatorTarget[]): LocalAgentOperatorTarget | null {
  const body = normalizeChatMessage(text).trim();
  const match = /^([^:\n]{1,80})\s*:/u.exec(body);
  if (!match) {
    return null;
  }
  const needle = cleanNick(match[1] || "").toLowerCase();
  if (!needle) {
    return null;
  }
  return targets.find((target) => cleanNick(target.label).toLowerCase() === needle)
    || targets.find((target) => target.id.toLowerCase() === needle)
    || targets.find((target) => cleanNick(target.label).toLowerCase().includes(needle))
    || null;
}

async function runOperatorCommand(message: { readonly id?: string; readonly target?: string; readonly sourceDeviceId?: string; readonly command?: string; readonly runAs?: string; readonly timeoutMs?: number }): Promise<void> {
  const requestId = typeof message.id === "string" ? message.id : "";
  const command = typeof message.command === "string" ? message.command.trim() : "";
  const sourceDeviceId = typeof message.sourceDeviceId === "string" ? message.sourceDeviceId.trim() : "";
  if (!requestId || !command) {
    return;
  }
  const tunnel = findOperatorTarget(message.target || "");
  if (!tunnel) {
    sendOperatorOutput(requestId, "! target", 404);
    return;
  }
  if (sourceDeviceId && !operatorTargetMatchesDevice(tunnel.id, sourceDeviceId)) {
    sendOperatorOutput(requestId, "! source-target", 403);
    return;
  }
  const hostDeviceId = remoteAccess.get(tunnel.id);
  const sync = syncs.get(tunnel.id);
  if (!sync) {
    sendOperatorOutput(requestId, "! tunnel", 409);
    return;
  }
  if (!hostDeviceId) {
    sendOperatorOutput(requestId, "! access", 409);
    return;
  }
  if (operatorTargetBusy(tunnel.id)) {
    appendTerminalLine(tunnel.id, "! busy");
    sendOperatorOutput(requestId, "! busy", 409);
    renderTerminal();
    return;
  }
  const bridgeEpoch = operatorBridgeEpoch;
  const timeoutMs = safeOperatorTimeoutMs(message.timeoutMs);
  operatorStartingTunnels.add(tunnel.id);
  const keepCurrentDialog = shouldKeepCurrentDialogForOperatorTarget(sourceDeviceId);
  if (!keepCurrentDialog) {
    selectedId = tunnel.id;
    saveSelectedTunnelId(tunnel.id);
  }
  terminalOpenId = tunnel.id;
  setTerminalState(tunnel.id, "run");
  appendTerminalLine(tunnel.id, `$ ${command}`);
  clearTunnelNotices(tunnel.id);
  tunnels = markTunnel(tunnel.id, false);
  renderTiles();
  if (!keepCurrentDialog) {
    applySelectedText(true);
    renderFiles();
  }
  renderTerminal();
  try {
    const commandId = await sync.sendRemoteCommand(hostDeviceId, command, timeoutMs, message.runAs || "");
    if (bridgeEpoch !== operatorBridgeEpoch || !operatorSocket || operatorSocket.readyState !== WebSocket.OPEN) {
      await sync.sendRemoteCancel(hostDeviceId, commandId).catch(() => undefined);
      sendOperatorOutput(requestId, "! operator bridge closed", 130);
      return;
    }
    operatorPending.set(commandId, requestId);
    operatorRemoteRuns.set(requestId, {
      commandId,
      tunnelId: tunnel.id,
      hostDeviceId,
      startedAt: Date.now(),
      timeoutMs,
      kind: "run",
      label: command.slice(0, 120)
    });
  } catch {
    sendOperatorOutput(requestId, "! tunnel", 500);
    setTerminalState(tunnel.id, "bad");
    renderTerminal();
  } finally {
    operatorStartingTunnels.delete(tunnel.id);
  }
}

async function runOperatorScript(message: {
  readonly id?: string;
  readonly target?: string;
  readonly sourceDeviceId?: string;
  readonly name?: string;
  readonly shell?: string;
  readonly script?: string;
  readonly runAs?: string;
  readonly timeoutMs?: number;
}): Promise<void> {
  const requestId = typeof message.id === "string" ? message.id : "";
  const script = typeof message.script === "string" ? message.script : "";
  const sourceDeviceId = typeof message.sourceDeviceId === "string" ? message.sourceDeviceId.trim() : "";
  if (!requestId || !script.trim()) {
    return;
  }
  const tunnel = findOperatorTarget(message.target || "");
  if (!tunnel) {
    sendOperatorOutput(requestId, "! target", 404);
    return;
  }
  if (sourceDeviceId && !operatorTargetMatchesDevice(tunnel.id, sourceDeviceId)) {
    sendOperatorOutput(requestId, "! source-target", 403);
    return;
  }
  const hostDeviceId = remoteAccess.get(tunnel.id);
  const sync = syncs.get(tunnel.id);
  if (!sync) {
    sendOperatorOutput(requestId, "! tunnel", 409);
    return;
  }
  if (!hostDeviceId) {
    sendOperatorOutput(requestId, "! access", 409);
    return;
  }
  if (operatorTargetBusy(tunnel.id)) {
    appendTerminalLine(tunnel.id, "! busy");
    sendOperatorOutput(requestId, "! busy", 409);
    renderTerminal();
    return;
  }
  const name = cleanNick(message.name || "script") || "script";
  const bridgeEpoch = operatorBridgeEpoch;
  const timeoutMs = safeOperatorTimeoutMs(message.timeoutMs);
  operatorStartingTunnels.add(tunnel.id);
  const keepCurrentDialog = shouldKeepCurrentDialogForOperatorTarget(sourceDeviceId);
  if (!keepCurrentDialog) {
    selectedId = tunnel.id;
    saveSelectedTunnelId(tunnel.id);
  }
  terminalOpenId = tunnel.id;
  setTerminalState(tunnel.id, "run");
  appendTerminalLine(tunnel.id, `$ ${name}`);
  clearTunnelNotices(tunnel.id);
  tunnels = markTunnel(tunnel.id, false);
  renderTiles();
  if (!keepCurrentDialog) {
    applySelectedText(true);
    renderFiles();
  }
  renderTerminal();
  try {
    const commandId = await sync.sendRemoteScript(hostDeviceId, {
      name,
      shell: message.shell || "",
      script,
      runAs: message.runAs || "",
      timeoutMs
    });
    if (bridgeEpoch !== operatorBridgeEpoch || !operatorSocket || operatorSocket.readyState !== WebSocket.OPEN) {
      await sync.sendRemoteCancel(hostDeviceId, commandId).catch(() => undefined);
      sendOperatorOutput(requestId, "! operator bridge closed", 130);
      return;
    }
    operatorPending.set(commandId, requestId);
    operatorRemoteRuns.set(requestId, {
      commandId,
      tunnelId: tunnel.id,
      hostDeviceId,
      startedAt: Date.now(),
      timeoutMs,
      kind: "script",
      label: name.slice(0, 120)
    });
  } catch {
    sendOperatorOutput(requestId, "! tunnel", 500);
    setTerminalState(tunnel.id, "bad");
    renderTerminal();
  } finally {
    operatorStartingTunnels.delete(tunnel.id);
  }
}

async function runOperatorCancel(message: { readonly id?: string }): Promise<void> {
  const requestId = typeof message.id === "string" ? message.id : "";
  const run = requestId ? operatorRemoteRuns.get(requestId) : null;
  if (!requestId || !run) {
    return;
  }
  const sync = syncs.get(run.tunnelId);
  if (!sync) {
    sendOperatorOutput(requestId, "! tunnel", 409);
    return;
  }
  appendTerminalLine(run.tunnelId, "! stop requested");
  renderTerminal();
  await sync.sendRemoteCancel(run.hostDeviceId, run.commandId).catch(() => undefined);
  operatorRemoteRuns.delete(requestId);
  operatorPending.delete(run.commandId);
  sendOperatorOutput(requestId, "! stopped\n", 130);
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
  const tunnel = findVisibleOperatorTarget(message.target || "") || findAgentOperatorTarget(message.target || "");
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

async function runOperatorAgentMessage(message: {
  readonly id?: string;
  readonly target?: string;
  readonly sourceDeviceId?: string;
  readonly sourceDeviceNick?: string;
  readonly text?: string;
}): Promise<void> {
  const requestId = typeof message.id === "string" ? message.id : "";
  const body = normalizeChatMessage(typeof message.text === "string" ? message.text : "");
  if (!requestId || !body) {
    return;
  }
  const tunnel = findAgentOperatorTarget(message.target || "");
  if (!tunnel) {
    sendOperatorOutput(requestId, "! agent-target", 404);
    return;
  }
  ensureSync(tunnel);
  const sync = syncs.get(tunnel.id);
  if (!sync) {
    sendOperatorOutput(requestId, "! tunnel", 409);
    return;
  }
  selectedId = tunnel.id;
  saveSelectedTunnelId(tunnel.id);
  const current = texts.get(tunnel.id) || "";
  const separator = current.length > 0 && !current.endsWith("\n") ? "\n" : "";
  const next = `${current}${separator}${body}\n`;
  texts.set(tunnel.id, next);
  sync.setText(next);
  localDrafts.delete(tunnel.id);
  clearLiveDraftState(tunnel.id);
  void sync.sendLiveDraft("");
  touchSelected();
  renderTiles();
  applySelectedText();
  renderTextPaint();
  renderWriterPop();
  try {
    await sendAgentDialogMessage(tunnel.id, body);
    sendOperatorOutput(requestId, "done\n", 0);
  } catch {
    sendOperatorOutput(requestId, "! agent-message", 500);
  }
}

async function runOperatorAgentNew(message: { readonly id?: string }): Promise<void> {
  const requestId = typeof message.id === "string" ? message.id : "";
  if (!requestId) {
    return;
  }
  const previous = findActiveAgentDialog();
  if (previous) {
    selectedId = previous.id;
    saveSelectedTunnelId(previous.id);
  }
  const fresh = createFreshDialog(agentDialogLabel, {
    agent: true,
    archiveCurrent: Boolean(previous)
  });
  if (!fresh) {
    sendOperatorOutput(requestId, "! agent-new\n", 500);
    return;
  }
  if (previous) {
    moveAgentLinkToFreshDialog(previous.id, fresh.id);
  }
  renderApp();
  publishOperatorTargets();
  sendOperatorOutput(requestId, `agent ${fresh.id}\n`, 0);
  void (async () => {
    const agent = await refreshLocalAgent().catch(() => null);
    if (agent?.ok && !agent.relay) {
      const bound = await bindLocalAgentRelay(device || undefined).catch(() => false);
      if (bound) {
        await refreshLocalAgent().catch(() => null);
      }
    }
    await ensureOperatorBridge(true).catch(() => undefined);
    publishOperatorTargets();
  })();
}

function runOperatorTerminal(message: {
  readonly id?: string;
  readonly target?: string;
  readonly text?: string;
}): void {
  const tunnel = findAgentOperatorTarget(message.target || "");
  const text = typeof message.text === "string" ? message.text : "";
  if (!tunnel || !text.trim()) {
    return;
  }
  appendAgentTerminalTranscript(tunnel.id, text);
}

function formatOperatorChat(text: string, persona: string): string {
  const name = persona === "sysadmin" ? agentDialogLabel : cleanNick(persona || "Оператор") || "Оператор";
  const body = normalizeChatMessage(text);
  if (!body) {
    return "";
  }
  return [
    `${name} · ${clock()}`,
    ...body.split("\n")
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

function runOperatorExport(message: { readonly id?: string; readonly target?: string; readonly tailChars?: number }): void {
  const requestId = typeof message.id === "string" ? message.id : "";
  if (!requestId) {
    return;
  }
  const tailChars = Number.isSafeInteger(message.tailChars) ? Number(message.tailChars) : undefined;
  sendOperatorOutput(requestId, buildOperatorExport({
    target: typeof message.target === "string" ? message.target : "",
    ...(tailChars === undefined ? {} : { tailChars })
  }), 0);
}

async function runOperatorImport(message: { readonly id?: string; readonly text?: string }): Promise<void> {
  const requestId = typeof message.id === "string" ? message.id : "";
  const text = typeof message.text === "string" ? message.text : "";
  if (!requestId || !text.trim()) {
    return;
  }
  const restored = await restoreFromOperatorExportText(text);
  if (!restored) {
    sendOperatorOutput(requestId, "! import", 500);
    return;
  }
  sendOperatorOutput(requestId, `restored ${restored.count}\n`, 0);
}

function findAgentOperatorTarget(target: string): TunnelRecord | null {
  const needle = cleanNick(target).toLowerCase();
  const items = loadTunnels().filter((tunnel) => !tunnel.archived && isAgentTunnel(tunnel));
  if (needle) {
    const found = items.find((tunnel) => tunnel.id === target)
      || items.find((tunnel) => tunnel.id.toLowerCase() === needle)
      || items.find((tunnel) => counterpartyLabel(tunnel).toLowerCase() === needle);
    if (found) {
      return found;
    }
  }
  return findActiveAgentDialog();
}

function findOperatorTarget(target: string): TunnelRecord | null {
  const needle = cleanNick(target).toLowerCase();
  if (!needle) {
    return null;
  }
  const items = sortedVisibleTunnels().filter((tunnel) => remoteAccess.has(tunnel.id));
  return items.find((tunnel) => tunnel.id === target)
    || items.find((tunnel) => counterpartyLabel(tunnel).toLowerCase() === needle)
    || items.find((tunnel) => counterpartyLabel(tunnel).toLowerCase().includes(needle))
    || null;
}

function operatorTargetMatchesDevice(tunnelId: string, sourceDeviceId: string): boolean {
  const sourceId = sourceDeviceId.trim();
  if (!sourceId) {
    return false;
  }
  const hostDeviceId = remoteAccess.get(tunnelId);
  if (hostDeviceId === sourceId) {
    return true;
  }
  return (peerDevices.get(tunnelId) ?? []).some((peer) => peer.id === sourceId);
}

function shouldKeepCurrentDialogForOperatorTarget(sourceDeviceId: string): boolean {
  return Boolean(sourceDeviceId.trim() && selectedId && isAgentTunnelId(selectedId));
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
  const tunnelId = activeTerminalTunnelId();
  const chessActive = Boolean(activeChessTunnelId());
  const controller = Boolean(tunnelId && remoteAccess.has(tunnelId));
  const host = Boolean(tunnelId && remoteEnabled.has(tunnelId));
  const active = !chessActive && (controller || host);
  const state = tunnelId ? terminalState.get(tunnelId) ?? "idle" : "idle";
  editor.classList.toggle("terminal-active", active);
  editor.classList.toggle("terminal-controller", controller);
  editor.classList.toggle("terminal-host", host && !controller);
  panel.dataset.state = state;
  form.hidden = !controller;
  const tunnel = loadTunnels().find((item) => item.id === tunnelId);
  peer.textContent = tunnel ? initials(counterpartyLabel(tunnel)) : ".";
  output.innerHTML = (terminalLogs.get(tunnelId) ?? [])
    .map((line) => `<pre>${escapeHtml(line || " ")}</pre>`)
    .join("");
  if (active) {
    output.scrollTop = output.scrollHeight;
    window.setTimeout(() => app.querySelector<HTMLInputElement>(".terminal-form input")?.focus(), 0);
  }
}

function activeTerminalTunnelId(): string {
  if (terminalOpenId && (remoteAccess.has(terminalOpenId) || remoteEnabled.has(terminalOpenId) || terminalLogs.has(terminalOpenId))) {
    return terminalOpenId;
  }
  if (selectedId && (remoteAccess.has(selectedId) || isAgentLinkedTunnel(selectedId))) {
    terminalOpenId = selectedId;
    return selectedId;
  }
  return "";
}

function isAgentLinkedTunnel(tunnelId: string): boolean {
  return isAgentTunnelId(tunnelId) && remoteEnabled.has(tunnelId);
}

function applySyncedTerminal(tunnelId: string, terminal: TerminalSnapshot): void {
  terminalLogs.set(tunnelId, [...terminal.lines]);
  terminalState.set(tunnelId, terminal.state);
  if (terminal.lines.length > 0 && !terminalOpenId && (tunnelId === selectedId || remoteAccess.has(tunnelId) || remoteEnabled.has(tunnelId))) {
    terminalOpenId = tunnelId;
  }
  if (tunnelId === selectedId || terminalOpenId === tunnelId) {
    renderTerminal();
    renderTiles();
  }
}

function appendTerminalLine(tunnelId: string, line: string, publish = true): void {
  const next = [...(terminalLogs.get(tunnelId) ?? []), line].slice(-600);
  terminalLogs.set(tunnelId, next);
  if (publish) {
    syncs.get(tunnelId)?.appendTerminalLine(line);
  }
}

function appendTerminalExitLine(tunnelId: string, exitCode: number): void {
  if (exitCode !== 0) {
    appendTerminalLine(tunnelId, `! ${exitCode}`);
  }
}

function setTerminalState(tunnelId: string, state: "idle" | "run" | "ok" | "bad" | "off", publish = true): void {
  terminalState.set(tunnelId, state);
  if (publish) {
    syncs.get(tunnelId)?.setTerminalState(state);
  }
}

function activeChessTunnelId(): string {
  return chessOpenId && chessOpenId === selectedId ? chessOpenId : "";
}

async function openChessForSelected(): Promise<void> {
  if (!selectedId) {
    return;
  }
  if (activeChessTunnelId() === selectedId) {
    closeChessPanel();
    return;
  }
  if (isAgentTunnelId(selectedId)) {
    await startAgentChess();
    return;
  }
  chessOpenId = selectedId;
  chessSelectedSquare = "";
  chessPromotion = null;
  ensureChessSnapshot(selectedId, "peer");
  renderTerminal();
  renderChess();
}

async function startAgentChess(): Promise<void> {
  let tunnel = findActiveAgentDialog();
  if (!tunnel) {
    tunnel = createFreshDialog(agentDialogLabel, { agent: true, archiveCurrent: false });
  } else {
    tunnel = normalizeAgentDialog(tunnel.id) || tunnel;
    selectedId = tunnel.id;
    saveSelectedTunnelId(tunnel.id);
    tunnels = markTunnel(tunnel.id, false);
  }
  if (!tunnel) {
    return;
  }
  renderApp();
  chessOpenId = tunnel.id;
  chessSelectedSquare = "";
  chessPromotion = null;
  const snapshot = ensureChessSnapshot(tunnel.id, "agent");
  maybeWelcomeChessAgent(tunnel.id, snapshot);
  renderTerminal();
  renderChess();
  scheduleChessAgentMove(tunnel.id);
}

function closeChessPanel(): void {
  if (chessOpenId) {
    const timer = chessAgentTimers.get(chessOpenId);
    if (timer) {
      window.clearTimeout(timer);
      chessAgentTimers.delete(chessOpenId);
    }
  }
  chessOpenId = "";
  chessSelectedSquare = "";
  chessPromotion = null;
  renderChess();
  renderTerminal();
}

function handleChessPanelClick(event: MouseEvent): void {
  const target = event.target instanceof Element ? event.target : null;
  if (!target) {
    return;
  }
  const promotionButton = target.closest<HTMLButtonElement>("[data-promotion]");
  if (promotionButton) {
    const piece = promotionButton.dataset.promotion;
    if (isChessPromotionPiece(piece) && chessPromotion && chessPromotion.tunnelId === activeChessTunnelId()) {
      makeChessMove(chessPromotion.tunnelId, chessPromotion.from, chessPromotion.to, piece);
    }
    return;
  }
  if (target.closest(".chess-close")) {
    closeChessPanel();
    return;
  }
  if (target.closest(".chess-new")) {
    const tunnelId = activeChessTunnelId();
    if (tunnelId) {
      restartChessGame(tunnelId);
    }
    return;
  }
  if (target.closest(".chess-flip")) {
    const tunnelId = activeChessTunnelId();
    if (tunnelId) {
      if (chessFlipped.has(tunnelId)) {
        chessFlipped.delete(tunnelId);
      } else {
        chessFlipped.add(tunnelId);
      }
      renderChess();
    }
    return;
  }
  if (target.closest(".chess-coach")) {
    const tunnelId = activeChessTunnelId();
    if (tunnelId) {
      toggleChessCoach(tunnelId);
    }
    return;
  }
  const squareButton = target.closest<HTMLButtonElement>("[data-square]");
  const square = squareButton?.dataset.square || "";
  if (isSquare(square)) {
    handleChessSquare(square);
  }
}

function renderChess(): void {
  const panel = app.querySelector<HTMLDivElement>(".chess-panel");
  const editor = app.querySelector<HTMLElement>(".editor");
  const action = app.querySelector<HTMLButtonElement>(".chess-action");
  if (!panel || !editor) {
    return;
  }
  const tunnelId = activeChessTunnelId();
  const active = Boolean(tunnelId);
  editor.classList.toggle("chess-active", active);
  action?.classList.toggle("is-on", active);
  if (action) {
    action.dataset.tooltip = active ? "Закрыть шахматы" : "Шахматы";
  }
  if (!active) {
    return;
  }

  const snapshot = ensureChessSnapshot(tunnelId, chessModeForTunnel(tunnelId));
  const game = chessFromSnapshot(snapshot);
  if (chessSelectedSquare) {
    const piece = game.get(chessSelectedSquare);
    if (!piece || piece.color !== game.turn()) {
      chessSelectedSquare = "";
      chessPromotion = null;
    }
  }

  const title = panel.querySelector<HTMLElement>(".chess-title");
  const status = panel.querySelector<HTMLElement>(".chess-status");
  const board = panel.querySelector<HTMLDivElement>(".chess-board");
  const turn = panel.querySelector<HTMLDivElement>(".chess-turn");
  const stats = panel.querySelector<HTMLDivElement>(".chess-stats");
  const moves = panel.querySelector<HTMLOListElement>(".chess-moves");
  const coach = panel.querySelector<HTMLButtonElement>(".chess-coach");
  const promotion = panel.querySelector<HTMLDivElement>(".chess-promotion");
  if (!board || !turn || !stats || !moves || !coach || !promotion) {
    return;
  }

  panel.dataset.mode = snapshot.mode;
  panel.dataset.result = snapshot.result || "play";
  if (title) {
    title.textContent = snapshot.mode === "agent" ? "CHESS / ГЕНИЙ" : "CHESS";
  }
  if (status) {
    status.textContent = snapshot.result ? "DONE" : game.isCheck() ? "CHECK" : `${game.moveNumber()}`;
  }
  coach.hidden = snapshot.mode !== "agent";
  coach.classList.toggle("is-on", snapshot.coach === geniusCoach);

  const selectedMoves = chessSelectedSquare ? legalMovesForSquare(snapshot, chessSelectedSquare) : [];
  const legalTargets = new Set(selectedMoves.map((move) => move.to));
  const canMove = canMoveOnChessBoard(snapshot);
  const orientation: Color = chessFlipped.has(tunnelId) ? "b" : "w";
  board.innerHTML = boardSquares(orientation).map((square) => {
    const piece = game.get(square);
    const selected = chessSelectedSquare === square;
    const legal = legalTargets.has(square);
    const last = snapshot.lastMove?.from === square || snapshot.lastMove?.to === square;
    const classes = [
      "chess-square",
      chessSquareTone(square),
      piece ? `has-piece ${piece.color === "w" ? "white-piece" : "black-piece"}` : "",
      selected ? "is-selected" : "",
      legal ? "is-legal" : "",
      legal && piece ? "is-capture" : "",
      last ? "is-last" : ""
    ].filter(Boolean).join(" ");
    const label = piece ? `${sideName(piece.color)} ${piece.type} ${square}` : square;
    return `<button class="${classes}" type="button" data-square="${square}" aria-label="${escapeHtml(label)}"${canMove ? "" : " disabled"}>${pieceGlyph(piece)}</button>`;
  }).join("");

  turn.innerHTML = `
    <b>${escapeHtml(statusText(snapshot))}</b>
    <small>${escapeHtml(snapshot.mode === "agent" ? "ГЕНИЙ" : "ЛЮДИ")}</small>
  `;
  stats.innerHTML = renderChessStats(snapshot);
  moves.innerHTML = renderChessMoves(snapshot.history);
  renderChessPromotion(snapshot, promotion);
  if (isAgentTurn(snapshot)) {
    scheduleChessAgentMove(tunnelId);
  }
}

function handleChessSquare(square: Square): void {
  const tunnelId = activeChessTunnelId();
  if (!tunnelId) {
    return;
  }
  const snapshot = ensureChessSnapshot(tunnelId, chessModeForTunnel(tunnelId));
  if (!canMoveOnChessBoard(snapshot)) {
    return;
  }
  const game = chessFromSnapshot(snapshot);
  const piece = game.get(square);
  if (chessSelectedSquare) {
    if (chessSelectedSquare === square) {
      chessSelectedSquare = "";
      chessPromotion = null;
      renderChess();
      return;
    }
    const choices = promotionChoices(snapshot, chessSelectedSquare, square);
    if (choices.length > 0) {
      chessPromotion = { tunnelId, from: chessSelectedSquare, to: square };
      renderChess();
      return;
    }
    if (makeChessMove(tunnelId, chessSelectedSquare, square)) {
      return;
    }
  }
  if (piece && piece.color === game.turn()) {
    chessSelectedSquare = square;
    chessPromotion = null;
    renderChess();
  }
}

function makeChessMove(tunnelId: string, from: Square, to: Square, promotion: PieceSymbol = "q"): boolean {
  const snapshot = ensureChessSnapshot(tunnelId, chessModeForTunnel(tunnelId));
  const moved = applyChessMove(snapshot, from, to, promotion);
  if (!moved) {
    chessPromotion = null;
    renderChess();
    return false;
  }
  chessSelectedSquare = "";
  chessPromotion = null;
  publishChessSnapshot(tunnelId, moved.snapshot);
  renderChess();
  if (moved.snapshot.mode === "agent") {
    if (moved.snapshot.result && moved.snapshot.coach === geniusCoach) {
      appendAgentChatMessage(tunnelId, buildGeniusLine(moved.snapshot, moved.move, null));
    } else {
      scheduleChessAgentMove(tunnelId, moved.move);
    }
  }
  return true;
}

function scheduleChessAgentMove(tunnelId: string, humanMove: Move | null = null): void {
  const snapshot = chessGames.get(tunnelId);
  if (!snapshot || !isAgentTurn(snapshot)) {
    return;
  }
  const previous = chessAgentTimers.get(tunnelId);
  if (previous) {
    window.clearTimeout(previous);
  }
  const timer = window.setTimeout(() => {
    chessAgentTimers.delete(tunnelId);
    runChessAgentMove(tunnelId, humanMove);
  }, humanMove ? 520 : 760);
  chessAgentTimers.set(tunnelId, timer);
}

function runChessAgentMove(tunnelId: string, humanMove: Move | null): void {
  const snapshot = ensureChessSnapshot(tunnelId, "agent");
  if (!isAgentTurn(snapshot)) {
    return;
  }
  const agentMove = chooseAgentMove(snapshot);
  if (!agentMove) {
    return;
  }
  const moved = applyChessMove(snapshot, agentMove.from, agentMove.to, agentMove.promotion ?? "q");
  if (!moved) {
    return;
  }
  publishChessSnapshot(tunnelId, moved.snapshot);
  renderChess();
  if (moved.snapshot.coach === geniusCoach) {
    appendAgentChatMessage(tunnelId, buildGeniusLine(moved.snapshot, humanMove, moved.move));
  }
}

function restartChessGame(tunnelId: string): void {
  const previous = ensureChessSnapshot(tunnelId, chessModeForTunnel(tunnelId));
  const names = chessNames(tunnelId);
  const snapshot = createChessSnapshot({
    mode: previous.mode,
    localNick: names.local,
    opponentNick: names.opponent,
    stats: previous.stats,
    coach: previous.coach
  });
  chessSelectedSquare = "";
  chessPromotion = null;
  publishChessSnapshot(tunnelId, snapshot);
  if (snapshot.mode === "agent") {
    maybeWelcomeChessAgent(tunnelId, snapshot);
  }
  renderChess();
}

function toggleChessCoach(tunnelId: string): void {
  const snapshot = ensureChessSnapshot(tunnelId, "agent");
  const nextCoach: ChessCoach = snapshot.coach === geniusCoach ? "quiet" : geniusCoach;
  const next = withCoach(snapshot, nextCoach);
  publishChessSnapshot(tunnelId, next);
  if (nextCoach === geniusCoach) {
    appendAgentChatMessage(tunnelId, "ГЕНИЙ: тренер включен. Будут советы, стеб и иногда воспитательная работа.");
  }
  renderChess();
}

function renderChessPromotion(snapshot: ChessSnapshot, promotion: HTMLDivElement): void {
  if (!chessPromotion || chessPromotion.tunnelId !== activeChessTunnelId()) {
    promotion.hidden = true;
    promotion.innerHTML = "";
    return;
  }
  const choices = promotionChoices(snapshot, chessPromotion.from, chessPromotion.to);
  if (choices.length === 0) {
    promotion.hidden = true;
    promotion.innerHTML = "";
    return;
  }
  const color = chessFromSnapshot(snapshot).get(chessPromotion.from)?.color ?? "w";
  promotion.hidden = false;
  promotion.innerHTML = `
    <span>Пешка</span>
    ${choices.map((piece) => `<button type="button" data-promotion="${piece}">${pieceGlyph({ color, type: piece })}</button>`).join("")}
  `;
}

function renderChessStats(snapshot: ChessSnapshot): string {
  const stats = snapshot.stats;
  if (snapshot.mode === "agent") {
    return `
      <span><b>${stats.humanWins}</b><small>YOU</small></span>
      <span><b>${stats.draws}</b><small>DRAW</small></span>
      <span><b>${stats.agentWins}</b><small>GENIUS</small></span>
      <span><b>${stats.longestPly}</b><small>PLY</small></span>
    `;
  }
  return `
    <span><b>${stats.whiteWins}</b><small>WHITE</small></span>
    <span><b>${stats.draws}</b><small>DRAW</small></span>
    <span><b>${stats.blackWins}</b><small>BLACK</small></span>
    <span><b>${stats.longestPly}</b><small>PLY</small></span>
  `;
}

function renderChessMoves(history: readonly string[]): string {
  if (history.length === 0) {
    return `<li class="is-empty"><b>1</b><span>START</span><span></span></li>`;
  }
  const rows: string[] = [];
  for (let index = 0; index < history.length; index += 2) {
    const white = history[index] || "";
    const black = history[index + 1] || "";
    rows.push(`<li><b>${Math.floor(index / 2) + 1}</b><span>${escapeHtml(white)}</span><span>${escapeHtml(black)}</span></li>`);
  }
  return rows.slice(-80).join("");
}

function ensureChessSnapshot(tunnelId: string, mode: ChessMode): ChessSnapshot {
  const names = chessNames(tunnelId);
  const stored = chessGames.get(tunnelId) ?? loadStoredChessSnapshot(tunnelId);
  let snapshot = stored
    ? normalizeChessSnapshot(stored, { mode, localNick: names.local, opponentNick: names.opponent })
    : null;
  if (!snapshot || snapshot.mode !== mode) {
    snapshot = createChessSnapshot({
      mode,
      localNick: names.local,
      opponentNick: names.opponent,
      stats: snapshot?.stats,
      coach: mode === "agent" ? snapshot?.coach ?? geniusCoach : "quiet"
    });
    publishChessSnapshot(tunnelId, snapshot);
  } else {
    chessGames.set(tunnelId, snapshot);
    rememberChessSnapshot(tunnelId, snapshot);
  }
  return snapshot;
}

function publishChessSnapshot(tunnelId: string, snapshot: ChessSnapshot): void {
  chessGames.set(tunnelId, snapshot);
  rememberChessSnapshot(tunnelId, snapshot);
  syncs.get(tunnelId)?.setChessSnapshot(snapshot as unknown as SyncedChessState);
  tunnels = touchTunnel(tunnelId);
  renderTiles();
}

function applySyncedChess(tunnelId: string, chess: SyncedChessState | null): void {
  if (!chess) {
    return;
  }
  const mode: ChessMode = chess.mode === "agent" || chessModeForTunnel(tunnelId) === "agent" ? "agent" : "peer";
  const names = chessNames(tunnelId);
  const snapshot = normalizeChessSnapshot(chess, { mode, localNick: names.local, opponentNick: names.opponent });
  chessGames.set(tunnelId, snapshot);
  rememberChessSnapshot(tunnelId, snapshot);
  if (tunnelId === selectedId || tunnelId === chessOpenId) {
    renderChess();
    renderTiles();
  }
}

function chessModeForTunnel(tunnelId: string): ChessMode {
  return isAgentTunnelId(tunnelId) ? "agent" : "peer";
}

function chessNames(tunnelId: string): { readonly local: string; readonly opponent: string } {
  const tunnel = loadTunnels().find((item) => item.id === tunnelId);
  return {
    local: cleanNick(device?.nick || "") || "Я",
    opponent: tunnel ? counterpartyLabel(tunnel) : "Черные"
  };
}

function canMoveOnChessBoard(snapshot: ChessSnapshot): boolean {
  return !snapshot.result && !(snapshot.mode === "agent" && chessFromSnapshot(snapshot).turn() === agentSide(snapshot));
}

function maybeWelcomeChessAgent(tunnelId: string, snapshot: ChessSnapshot): void {
  if (snapshot.mode !== "agent" || snapshot.history.length > 0 || snapshot.coach !== geniusCoach || chessWelcomedGames.has(snapshot.gameId)) {
    return;
  }
  chessWelcomedGames.add(snapshot.gameId);
  appendAgentChatMessage(tunnelId, "ГЕНИЙ: доска на месте. Ты за белых, я за черных. Все просто: ходи.");
}

function loadStoredChessSnapshot(tunnelId: string): unknown {
  return loadStoredChessSnapshots()[tunnelId] ?? null;
}

function loadStoredChessSnapshots(): Record<string, unknown> {
  try {
    const parsed = JSON.parse(localStorage.getItem(chessStoreKey) || "{}") as unknown;
    return isRecord(parsed) ? parsed : {};
  } catch {
    return {};
  }
}

function rememberChessSnapshot(tunnelId: string, snapshot: ChessSnapshot): void {
  try {
    const stored = loadStoredChessSnapshots();
    stored[tunnelId] = snapshot;
    localStorage.setItem(chessStoreKey, JSON.stringify(stored));
  } catch {
    // Chess still lives in the synced room if local storage is unavailable.
  }
}

function forgetChessSnapshot(tunnelId: string): void {
  try {
    const stored = loadStoredChessSnapshots();
    delete stored[tunnelId];
    localStorage.setItem(chessStoreKey, JSON.stringify(stored));
  } catch {
    // Best effort only.
  }
}

function chessSquareTone(square: Square): string {
  const file = square.charCodeAt(0) - 96;
  const rank = Number(square[1]);
  return (file + rank) % 2 === 0 ? "dark" : "light";
}

function isChessPromotionPiece(value: unknown): value is PieceSymbol {
  return value === "q" || value === "r" || value === "b" || value === "n";
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

function startAgentSourceControl(tunnelId: string): void {
  agentSourceControlTunnelId = tunnelId;
  agentSourcePollEpoch += 1;
  agentSourcePollController?.abort();
  agentSourcePollController = null;
  agentSourcePolling = false;
  window.clearTimeout(agentSourcePollTimer);
  agentSourceGrantRefreshAt = 0;
  void pollAgentSourceControl(agentSourcePollEpoch);
}

function resumeAgentSourceControl(): void {
  if (!device) {
    return;
  }
  const agentTunnel = sortedVisibleTunnels().find((tunnel) => remoteEnabled.has(tunnel.id) && isAgentTunnel(tunnel));
  if (!agentTunnel) {
    return;
  }
  terminalOpenId = terminalOpenId || agentTunnel.id;
  setTerminalState(agentTunnel.id, "idle");
  void grantAgentSourceAccess(device.id, device.nick, true, agentSourceClientState());
  startAgentSourceControl(agentTunnel.id);
}

function stopAgentSourceControl(tunnelId: string): void {
  if (agentSourceControlTunnelId === tunnelId) {
    agentSourceControlTunnelId = "";
  }
  agentSourcePollEpoch += 1;
  agentSourcePollController?.abort();
  agentSourcePollController = null;
  agentSourcePolling = false;
  agentSourceGrantRefreshAt = 0;
  window.clearTimeout(agentSourcePollTimer);
}

async function pollAgentSourceControl(epoch = agentSourcePollEpoch): Promise<void> {
  if (agentSourcePolling || !device || !agentSourceControlTunnelId || !isAgentTunnelId(agentSourceControlTunnelId)) {
    return;
  }
  const tunnelId = agentSourceControlTunnelId;
  const controller = new AbortController();
  agentSourcePollController = controller;
  agentSourcePolling = true;
  try {
    await refreshAgentSourceGrant(tunnelId);
    if (agentSourcePollEpoch !== epoch || controller.signal.aborted) {
      return;
    }
  } finally {
    if (agentSourcePollController === controller) {
      agentSourcePollController = null;
    }
    if (agentSourcePollEpoch === epoch) {
      agentSourcePolling = false;
      if (device && agentSourceControlTunnelId === tunnelId && isAgentTunnelId(tunnelId)) {
        agentSourcePollTimer = window.setTimeout(() => void pollAgentSourceControl(epoch), agentSourceGrantRefreshMs);
      }
    }
  }
}

async function refreshAgentSourceGrant(tunnelId: string): Promise<void> {
  if (!device || !isAgentTunnelId(tunnelId)) {
    return;
  }
  const now = Date.now();
  if (agentSourceGrantRefreshAt > now) {
    return;
  }
  agentSourceGrantRefreshAt = now + agentSourceGrantRefreshMs;
  localAgent = await ensureAgentSourceCompanion();
  if (!isAgentSourceCompanionReady(localAgent)) {
    agentSourceGrantRefreshAt = now + 5000;
    return;
  }
  const ok = await grantAgentSourceAccess(device.id, device.nick, true, agentSourceClientState());
  if (!ok) {
    agentSourceGrantRefreshAt = now + 5000;
  }
}

function localAgentRunTimeoutMs(value: unknown): number {
  return safeOperatorTimeoutMs(value) || 30 * 60_000;
}

function agentSourceClientState(): { readonly localAgent: LocalAgentStatus } {
  return { localAgent };
}

function runLocalAgentCommand(tunnelId: string, command: RemoteCommand): void {
  const sync = syncs.get(tunnelId);
  if (!sync || !command.deviceId) {
    return;
  }
  let opened = false;
  let finished = false;
  const timeoutMs = localAgentRunTimeoutMs(command.timeoutMs);
  let watchdogTimer = 0;
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
    window.clearTimeout(watchdogTimer);
    void sync.sendRemoteOutput(command.deviceId, command.id, "! 127.0.0.1:49424", 127);
  };
  const timer = window.setTimeout(() => {
    fail();
    ws.close();
  }, 2500);
  ws.onopen = () => {
    opened = true;
    window.clearTimeout(timer);
    localAgentRuns.set(command.id, ws);
    watchdogTimer = window.setTimeout(() => {
      if (finished) {
        return;
      }
      finished = true;
      stopLocalAgentRun(command.id);
      setTerminalState(tunnelId, "bad");
      appendTerminalLine(tunnelId, "! timeout");
      renderTerminal();
      if (localAgentRuns.get(command.id) === ws) {
        localAgentRuns.delete(command.id);
      }
      void sync.sendRemoteOutput(command.deviceId, command.id, "! timeout\n", 124);
      ws.close();
    }, timeoutMs + 1500);
    ws.send(JSON.stringify({
      type: "run",
      id: command.id,
      command: command.command,
      runAs: command.runAs || "",
      timeoutMs
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
      window.clearTimeout(watchdogTimer);
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
      window.clearTimeout(watchdogTimer);
      setTerminalState(tunnelId, exitCode === 0 ? "ok" : "bad");
      appendTerminalExitLine(tunnelId, exitCode);
    }
    renderTerminal();
    void sync.sendRemoteOutput(command.deviceId, command.id, text, exitCode);
  };
  ws.onerror = () => fail();
  ws.onclose = () => {
    window.clearTimeout(timer);
    window.clearTimeout(watchdogTimer);
    if (opened && !finished) {
      finished = true;
      setTerminalState(tunnelId, "bad");
      appendTerminalLine(tunnelId, "! agent disconnected");
      renderTerminal();
      void sync.sendRemoteOutput(command.deviceId, command.id, "! agent disconnected\n", 127);
    }
    if (localAgentRuns.get(command.id) === ws) {
      localAgentRuns.delete(command.id);
    }
  };
}

function runLocalAgentScript(tunnelId: string, script: RemoteScript): void {
  const sync = syncs.get(tunnelId);
  if (!sync || !script.deviceId) {
    return;
  }
  let opened = false;
  let finished = false;
  const timeoutMs = localAgentRunTimeoutMs(script.timeoutMs);
  let watchdogTimer = 0;
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
    window.clearTimeout(watchdogTimer);
    void sync.sendRemoteOutput(script.deviceId, script.id, "! 127.0.0.1:49424", 127);
  };
  const timer = window.setTimeout(() => {
    fail();
    ws.close();
  }, 2500);
  ws.onopen = () => {
    opened = true;
    window.clearTimeout(timer);
    localAgentRuns.set(script.id, ws);
    watchdogTimer = window.setTimeout(() => {
      if (finished) {
        return;
      }
      finished = true;
      stopLocalAgentRun(script.id);
      setTerminalState(tunnelId, "bad");
      appendTerminalLine(tunnelId, "! timeout");
      renderTerminal();
      if (localAgentRuns.get(script.id) === ws) {
        localAgentRuns.delete(script.id);
      }
      void sync.sendRemoteOutput(script.deviceId, script.id, "! timeout\n", 124);
      ws.close();
    }, timeoutMs + 1500);
    ws.send(JSON.stringify({
      type: "script",
      id: script.id,
      name: script.name,
      shell: script.shell,
      script: script.script,
      runAs: script.runAs || "",
      timeoutMs
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
      window.clearTimeout(watchdogTimer);
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
      window.clearTimeout(watchdogTimer);
      setTerminalState(tunnelId, exitCode === 0 ? "ok" : "bad");
      appendTerminalExitLine(tunnelId, exitCode);
    }
    renderTerminal();
    void sync.sendRemoteOutput(script.deviceId, script.id, text, exitCode);
  };
  ws.onerror = () => fail();
  ws.onclose = () => {
    window.clearTimeout(timer);
    window.clearTimeout(watchdogTimer);
    if (opened && !finished) {
      finished = true;
      setTerminalState(tunnelId, "bad");
      appendTerminalLine(tunnelId, "! agent disconnected");
      renderTerminal();
      void sync.sendRemoteOutput(script.deviceId, script.id, "! agent disconnected\n", 127);
    }
    if (localAgentRuns.get(script.id) === ws) {
      localAgentRuns.delete(script.id);
    }
  };
}

function stopLocalAgentRun(commandId: string): boolean {
  const ws = localAgentRuns.get(commandId);
  if (!ws || ws.readyState !== WebSocket.OPEN) {
    return false;
  }
  ws.send(JSON.stringify({ type: "stop", id: commandId }));
  return true;
}

function touchSelected(): void {
  if (selectedId) {
    tunnels = touchTunnel(selectedId);
  }
}

function rememberComposerDraft(): void {
  if (!selectedId || !composer) {
    return;
  }
  // Drafts stay local; Enter or the send button is the only publish path.
  const draft = composer.value;
  if (draft) {
    localDrafts.set(selectedId, draft);
  } else {
    localDrafts.delete(selectedId);
  }
  scheduleLiveDraft(selectedId, draft);
  resizeComposer();
}

function scheduleLiveDraft(tunnelId: string, draft: string): void {
  const sync = syncs.get(tunnelId);
  if (!sync) {
    return;
  }
  const previous = liveDraftSendTimers.get(tunnelId);
  if (previous) {
    window.clearTimeout(previous);
  }
  const timer = window.setTimeout(() => {
    liveDraftSendTimers.delete(tunnelId);
    void sync.sendLiveDraft(draft);
  }, 90);
  liveDraftSendTimers.set(tunnelId, timer);
}

async function finalizeComposerDraft(): Promise<void> {
  const tunnelId = selectedId;
  if (!tunnelId || !composer || !textarea) {
    return;
  }
  const sync = syncs.get(tunnelId);
  const tunnel = loadTunnels().find((item) => item.id === tunnelId);
  if (!sync) {
    return;
  }
  const draft = composer.value || localDrafts.get(tunnelId) || "";
  const message = normalizeChatMessage(draft);
  if (!message) {
    if (draft) {
      composer.value = "";
      rememberComposerDraft();
    }
    return;
  }
  const current = texts.get(tunnelId) ?? textarea.value;
  const separator = current.length > 0 && !current.endsWith("\n") ? "\n" : "";
  const next = `${current}${separator}${message}\n`;
  textarea.value = next;
  texts.set(tunnelId, next);
  sync.setText(next);
  const pendingLiveDraftTimer = liveDraftSendTimers.get(tunnelId);
  if (pendingLiveDraftTimer) {
    window.clearTimeout(pendingLiveDraftTimer);
    liveDraftSendTimers.delete(tunnelId);
  }
  void sync.sendLiveDraft("");
  if (tunnel && isAgentTunnel(tunnel)) {
    await prepareAgentSourceForDialog(tunnelId, tunnel);
    void sendAgentDialogMessage(tunnelId, message);
  }
  localDrafts.delete(tunnelId);
  composer.value = "";
  touchSelected();
  resizeComposer();
  renderTiles();
  renderTextPaint();
  renderWriterPop();
}

function sendAgentDialogMessage(tunnelId: string, text: string): Promise<void> {
  const tunnel = loadTunnels().find((item) => item.id === tunnelId);
  if (!tunnel || !isAgentTunnel(tunnel) || !text.trim()) {
    return Promise.resolve();
  }
  const targets = operatorTargets();
  const preferredTarget = preferredAgentOperatorTargetForDialog(text, targets);
  const context = cleanAgentContext(texts.get(tunnelId) || "").slice(-16_000);
  const source: LocalAgentRequestSource = {
    tunnelId,
    tunnelLabel: counterpartyLabel(tunnel),
    deviceId: device?.id || "",
    deviceNick: device?.nick || "",
    appOrigin: window.location.origin,
    preferredTargetId: preferredTarget?.id || "",
    preferredTargetLabel: preferredTarget?.label || "",
    operatorTargets: targets
  };
  const previous = agentReplyQueues.get(tunnelId) ?? Promise.resolve();
  const next = previous
    .catch(() => undefined)
    .then(async () => {
      setAgentThinking(tunnelId, true);
      let reply: LocalAgentReply;
      const streamedMessages: string[] = [];
      const streamedTerminal: string[] = [];
      try {
        await prepareAgentSourceForDialog(tunnelId, tunnel);
        reply = await askLocalAgentReply(text, context, source, 2 * 60 * 60_000, (message) => {
          const streamed = normalizeChatMessage(cleanAgentReplyText(message));
          if (!streamed || streamedMessages[streamedMessages.length - 1] === streamed) {
            return;
          }
          streamedMessages.push(streamed);
          appendAgentChatMessage(tunnelId, streamed);
        }, (message) => {
          const streamed = cleanTerminalTranscript(message);
          if (!streamed || streamedTerminal[streamedTerminal.length - 1] === streamed) {
            return;
          }
          streamedTerminal.push(streamed);
          appendAgentTerminalTranscript(tunnelId, streamed);
        });
      } finally {
        setAgentThinking(tunnelId, false);
      }
      let finalReply = reply;
      let body = normalizeChatMessage(cleanAgentReplyText(reply.text));
      if (streamedMessages.length > 0) {
        const delivered = new Set(streamedMessages);
        const remainingMessages = (reply.messages ?? [])
          .map((message) => normalizeChatMessage(cleanAgentReplyText(message)))
          .filter((message) => message && !delivered.has(message));
        finalReply = {
          ...reply,
          text: "",
          ...(remainingMessages.length > 0 ? { messages: remainingMessages } : { messages: [] })
        };
        body = "";
      }
      const deliveredTerminal = new Set(streamedTerminal);
      for (const message of reply.terminal ?? []) {
        const terminal = cleanTerminalTranscript(message);
        if (terminal && !deliveredTerminal.has(terminal)) {
          appendAgentTerminalTranscript(tunnelId, terminal);
        }
      }
      if (shouldOfferAgentInstall(reply)) {
        renderAgentInstall(tunnelId, () => composer?.focus(), agentSupportsDialogInbox, refreshLocalAgent);
      }
      const appended = appendAgentReplyMessages(tunnelId, finalReply, body);
      if (!appended && !reply.ok && body) {
        appendAgentChatMessage(tunnelId, userVisibleAgentFailureText(body));
        appendTerminalLine(tunnelId, `! codex bridge: ${body}`);
      }
    });
  agentReplyQueues.set(tunnelId, next);
  void next.finally(() => {
    if (agentReplyQueues.get(tunnelId) === next) {
      agentReplyQueues.delete(tunnelId);
    }
  });
  return next;
}

async function prepareAgentSourceForDialog(tunnelId: string, tunnel: TunnelRecord): Promise<void> {
  if (!device || !isAgentTunnel(tunnel)) {
    return;
  }
  if (!remoteEnabled.has(tunnelId)) {
    remoteEnabled = setRemoteEnabled(tunnelId, true);
  }
  terminalOpenId = tunnelId;
  if (!terminalState.has(tunnelId)) {
    setTerminalState(tunnelId, "idle");
  }
  renderTiles();
  renderTerminal();
  publishOperatorTargets();
  localAgent = await checkLocalAgent(900);
  await grantAgentSourceAccess(device.id, device.nick, true, agentSourceClientState(), 2500).catch(() => false);
  startAgentSourceControl(tunnelId);
  publishOperatorTargets();
}

function appendAgentReplyMessages(tunnelId: string, reply: LocalAgentReply, fallback: string): boolean {
  const messages = (reply.messages ?? [])
    .map((message) => cleanAgentReplyText(message))
    .filter(Boolean);
  if (messages.length > 0) {
    return appendAgentChatMessage(tunnelId, messages.join("\n\n"));
  }
  if (reply.ok && fallback) {
    return appendAgentChatMessage(tunnelId, fallback);
  }
  return false;
}

function userVisibleAgentFailureText(value: string): string {
  return cleanAgentReplyText(value) || "! agent: no reply";
}

function appendAgentChatMessage(tunnelId: string, rawText: string): boolean {
  const sync = syncs.get(tunnelId);
  const message = cleanAgentReplyText(rawText);
  if (!sync || !message) {
    return false;
  }
  const before = texts.get(tunnelId) || "";
  const separator = before.length > 0 && !before.endsWith("\n") ? "\n" : "";
  const firstLine = message.split(/\r?\n/u, 1)[0]?.trim() || "";
  const displayText = isOperatorHeader(firstLine) ? message : formatOperatorChat(message, "sysadmin");
  const insertText = `${separator}${displayText}\n`;
  const next = `${before}${insertText}`;
  const index = before.length;
  const activity: WriterActivity = {
    deviceId: "codex",
    nick: agentDialogLabel,
    index,
    local: false,
    action: "write",
    preview: message.replace(/\s+/gu, " ").trim().slice(0, 48),
    insertText,
    deleteCount: 0,
    startLine: lineFromIndex(before, index),
    startColumn: columnFromIndex(before, index),
    lineDelta: lineBreakCount(insertText)
  };
  sync.setText(next);
  texts.set(tunnelId, next);
  rememberWriter(tunnelId, activity);
  clearLiveDraftState(tunnelId);
  if (tunnelId === selectedId && textarea) {
    textarea.value = next;
    textarea.setSelectionRange(next.length, next.length);
    renderLineTags();
    renderTextPaint();
    renderWriterPop();
  }
  tunnels = touchTunnel(tunnelId);
  renderTiles();
  return true;
}

function appendAgentTerminalTranscript(tunnelId: string, rawText: string): boolean {
  const text = cleanTerminalTranscript(rawText);
  if (!text) {
    return false;
  }
  terminalOpenId = tunnelId;
  if (!terminalState.has(tunnelId)) {
    setTerminalState(tunnelId, "run");
  }
  appendTerminalLine(tunnelId, text);
  renderTerminal();
  renderTiles();
  return true;
}

function setAgentThinking(tunnelId: string, active: boolean): void {
  if (active) {
    agentThinking.add(tunnelId);
  } else {
    agentThinking.delete(tunnelId);
  }
  if (tunnelId === selectedId) {
    renderTextPaint();
    renderWriterPop();
  }
}

function cleanTerminalTranscript(value: string): string {
  return value
    .replace(/\r\n?/gu, "\n")
    .replace(/[\u0000-\u0008\u000B\u000C\u000E-\u001F\u007F]/gu, "")
    .replace(/\n{5,}/gu, "\n\n\n\n")
    .trim()
    .slice(0, 12_000);
}

function cleanAgentReplyText(value: string): string {
  return value
    .replace(/\r\n?/gu, "\n")
    .split("\n")
    .filter((line) => !isInternalAgentReceiptLine(line))
    .join("\n")
    .replace(/\n{3,}/gu, "\n\n")
    .trim();
}

function isInternalAgentReceiptLine(line: string): boolean {
  const text = line.trim();
  return /^`?(learning_delta|proof|final_line|finish_skill_edit)\s*=/iu.test(text)
    || /^`?ops-memory\s*:/iu.test(text)
    || /^`?soty-memory\s*:/iu.test(text)
    || /^ops:\s*`?(learning_delta|proof|final_line)\s*=/iu.test(text)
    || isInternalAgentRouteLine(text);
}

function isInternalAgentRouteLine(text: string): boolean {
  if (!text) {
    return false;
  }
  return /(?:использую\s+`?\$ops`?|`?\$ops`?\s+подтвердил|горячий маршрут|маршрутизатор|action_packet|helper_fit|source-scoped|soty\s+mcp|operator(?:ский)?\s+bridge|preflight|managed\s+staging|рантайм|серверном рантайме|ворот[ауы]? готовности|маршрут требует|маршрут подтвердил|точный технический блокер|agent-source\s+\d+|exitCode|timeoutMs)/iu.test(text);
}

function normalizeChatMessage(value: string): string {
  return value
    .replace(/\r\n?/gu, "\n")
    .split("\n")
    .map((line) => line.trimEnd())
    .join("\n")
    .replace(/\n{2,}/gu, "\n")
    .trim();
}

function shouldOfferAgentInstall(reply: LocalAgentReply): boolean {
  return !reply.ok
    && reply.exitCode === 127
    && /127\.0\.0\.1:49424/u.test(reply.text)
    && !hasAgentRelayId();
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
  const text = normalizeChatMessage(rawText);
  if (!text) {
    return;
  }
  const initial = texts.get(tunnelId) || "";
  const prefix = initial.length > 0 && !initial.endsWith("\n") ? "\n" : "";
  const suffix = text.endsWith("\n") ? "" : "\n";
  if (speed === "instant") {
    appendOperatorChatText(tunnelId, `${prefix}${text}${suffix}`);
    return;
  }
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
  const before = texts.get(tunnelId) || "";
  const next = `${before}${text}`;
  sync.setText(next);
  texts.set(tunnelId, next);
  rememberWriter(tunnelId, {
    deviceId: "operator",
    nick: isAgentTunnelId(tunnelId) ? agentDialogLabel : "Operator",
    index: before.length,
    local: false,
    action: "write",
    preview: text.replace(/\s+/gu, " ").trim().slice(0, 48),
    insertText: text,
    deleteCount: 0,
    startLine: lineFromIndex(before, before.length),
    startColumn: columnFromIndex(before, before.length),
    lineDelta: 0
  });
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

function buildOperatorExport(options: { readonly target?: string; readonly tailChars?: number } = {}): string {
  const target = cleanNick(options.target || "");
  const tailChars = Number.isSafeInteger(options.tailChars)
    ? Math.max(0, Math.min(200_000, Number(options.tailChars)))
    : 0;
  const focused = Boolean(target);
  const local: Record<string, string> = {};
  if (!focused) {
    for (let index = 0; index < localStorage.length; index += 1) {
      const key = localStorage.key(index) || "";
      if (key === "device" || key.startsWith("soty:")) {
        local[key] = localStorage.getItem(key) || "";
      }
    }
  } else {
    for (const key of ["soty:selected:v1", "soty:agent:relay-id"]) {
      const value = localStorage.getItem(key);
      if (value) {
        local[key] = value;
      }
    }
  }
  const safeDevice = device ? {
    id: device.id,
    nick: device.nick,
    publicJwk: device.publicJwk,
    createdAt: device.createdAt
  } : null;
  const tunnels = loadTunnels();
  const needle = target.toLowerCase();
  const exportedTunnels = focused
    ? tunnels.filter((tunnel) => {
      const labels = [
        tunnel.id,
        cleanNick(tunnel.label),
        cleanNick(counterpartyLabel(tunnel))
      ].map((value) => value.toLowerCase());
      return labels.includes(needle);
    })
    : tunnels;
  const payload = {
    schema: "soty.operator-export.v1",
    exportedAt: new Date().toISOString(),
    focused,
    target: focused ? target : undefined,
    tailChars: focused ? tailChars : undefined,
    selectedId,
    device: safeDevice,
    localStorage: local,
    tunnels: exportedTunnels.map((tunnel) => {
      const text = texts.get(tunnel.id) || "";
      return {
        ...tunnel,
        counterpartyLabel: counterpartyLabel(tunnel),
        text: tailChars > 0 ? text.slice(-tailChars) : text,
        chess: chessGames.get(tunnel.id) ?? loadStoredChessSnapshot(tunnel.id),
        files: (files.get(tunnel.id) || []).map((file) => ({
          id: file.id,
          name: file.name,
          type: file.type,
          size: file.size,
          nick: file.nick,
          deviceId: file.deviceId,
          createdAt: file.createdAt
        }))
      };
    })
  };
  return `${JSON.stringify(payload, null, 2)}\n`;
}

function rememberWriter(tunnelId: string, activity: WriterActivity): void {
  const label = cleanNick(activity.nick);
  const text = tunnelId === selectedId && textarea ? textarea.value : texts.get(tunnelId) || "";
  const color = colorFor(`${label}:${activity.deviceId || tunnelId}`);
  const startLine = Number.isSafeInteger(activity.startLine)
    ? Math.max(0, Number(activity.startLine))
    : lineFromIndex(text, activity.index);
  const lineDelta = Number.isSafeInteger(activity.lineDelta) ? Number(activity.lineDelta) : 0;
  const shiftFrom = Number(activity.startColumn || 0) === 0 ? startLine : startLine + 1;
  const lines = rebaseWriterLines(tunnelId, shiftFrom, lineDelta);
  if (activity.action === "erase" && !(activity.insertText || "").trim()) {
    writerLines.set(tunnelId, lines);
    return;
  }
  const startColumn = Number(activity.startColumn || 0);
  const insertText = activity.insertText || "";
  const labelStartLine = startLine + (startColumn > 0 && insertText.startsWith("\n") ? 1 : 0);
  const span = insertedLineSpan(insertText || activity.preview, startColumn > 0);
  const writer: WriterLine = {
    nick: label,
    deviceId: activity.deviceId,
    color,
    time: clock(),
    at: Date.now(),
    action: activity.action,
    preview: activity.preview
  };
  for (let offset = 0; offset < span; offset += 1) {
    lines.set(labelStartLine + offset, writer);
  }
  writerLines.set(tunnelId, lines);
}

function rebaseWriterLines(tunnelId: string, fromLine: number, lineDelta: number): Map<number, WriterLine> {
  const current = writerLines.get(tunnelId) ?? new Map<number, WriterLine>();
  if (lineDelta === 0 || current.size === 0) {
    return current;
  }
  const shifted = new Map<number, WriterLine>();
  for (const [line, writer] of current) {
    if (line < fromLine) {
      shifted.set(line, writer);
      continue;
    }
    const nextLine = line + lineDelta;
    if (nextLine >= 0) {
      shifted.set(nextLine, writer);
    }
  }
  return shifted;
}

function insertedLineSpan(text: string, dropLeadingBreak = false): number {
  if (!text) {
    return 1;
  }
  const withoutLeading = dropLeadingBreak && text.startsWith("\n") ? text.slice(1) : text;
  const body = withoutLeading.endsWith("\n") ? withoutLeading.slice(0, -1) : withoutLeading;
  if (!body) {
    return 1;
  }
  return body.split("\n").length;
}

function lineBreakCount(text: string): number {
  return (text.match(/\n/gu) ?? []).length;
}

function applyLiveDraft(tunnelId: string, draft: LiveDraft): void {
  if (draft.deviceId && draft.deviceId === device?.id) {
    return;
  }
  const key = draft.deviceId || draft.nick || "remote";
  const current = liveDrafts.get(tunnelId) ?? new Map<string, LiveDraftState>();
  const existing = current.get(key);
  if (existing && draft.seq > 0 && existing.seq > draft.seq) {
    return;
  }
  const tunnel = loadTunnels().find((item) => item.id === tunnelId);
  const fallbackNick = tunnel ? counterpartyLabel(tunnel) : counterpartyLabelForSelected();
  const nick = cleanNick(draft.nick) || fallbackNick;
  const timerKey = `${tunnelId}:${key}`;
  const previousTimer = liveDraftTimers.get(timerKey);
  if (previousTimer) {
    window.clearTimeout(previousTimer);
  }
  if (!draft.active || !draft.text.trim()) {
    current.set(key, {
      ...draft,
      text: "",
      active: false,
      nick,
      at: Date.now(),
      color: existing?.color || colorFor(`${nick}:${draft.deviceId || tunnelId}`)
    });
    const timer = window.setTimeout(() => {
      const latest = liveDrafts.get(tunnelId)?.get(key);
      if (latest && latest.seq === draft.seq) {
        liveDrafts.get(tunnelId)?.delete(key);
        if (liveDrafts.get(tunnelId)?.size === 0) {
          liveDrafts.delete(tunnelId);
        }
      }
      liveDraftTimers.delete(timerKey);
    }, 6500);
    liveDraftTimers.set(timerKey, timer);
  } else {
    current.set(key, {
      ...draft,
      nick,
      at: Date.now(),
      color: colorFor(`${nick}:${draft.deviceId || tunnelId}`)
    });
    const timer = window.setTimeout(() => {
      const latest = liveDrafts.get(tunnelId)?.get(key);
      if (latest && latest.seq === draft.seq) {
        liveDrafts.get(tunnelId)?.delete(key);
        if (liveDrafts.get(tunnelId)?.size === 0) {
          liveDrafts.delete(tunnelId);
        }
        if (tunnelId === selectedId) {
          renderTextPaint();
          renderWriterPop();
        }
      }
      liveDraftTimers.delete(timerKey);
    }, 6500);
    liveDraftTimers.set(timerKey, timer);
  }
  if (current.size > 0) {
    liveDrafts.set(tunnelId, current);
  } else {
    liveDrafts.delete(tunnelId);
  }
  if (tunnelId === selectedId) {
    renderTextPaint();
    renderWriterPop();
  }
}

function clearLiveDraftState(tunnelId: string): void {
  liveDrafts.delete(tunnelId);
  const sendTimer = liveDraftSendTimers.get(tunnelId);
  if (sendTimer) {
    window.clearTimeout(sendTimer);
    liveDraftSendTimers.delete(tunnelId);
  }
  const timerPrefix = `${tunnelId}:`;
  for (const [key, timer] of liveDraftTimers) {
    if (key.startsWith(timerPrefix)) {
      window.clearTimeout(timer);
      liveDraftTimers.delete(key);
    }
  }
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
  const hideAgentChrome = loadTunnels().some((item) => item.id === selectedId && isAgentTunnel(item));
  const text = textarea.value;
  const lines = text.endsWith("\n") ? text.slice(0, -1).split("\n") : text.split("\n");
  const active = activeActivities.get(selectedId);
  const activeLine = active ? lineFromIndex(text, active.index) : -1;
  const drafts = liveDraftsForSelected();
  textPaint.style.transform = "";
  if (!text.trim() && drafts.length === 0) {
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
    let state = classifyChatLine(line, operatorBlock);
    if (label && label.deviceId !== "operator" && !isAgentChromeLineClass(state.className)) {
      state = { className: "is-user-line", operatorBlock: false };
    }
    operatorBlock = state.operatorBlock;
    if (hideAgentChrome && isAgentChromeLineClass(state.className)) {
      continue;
    }
    if (!line.trim()) {
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
  if (agentThinking.has(selectedId)) {
    bubbles.push({
      key: "agent-thinking",
      side: "remote",
      nick: agentDialogLabel,
      color: colorFor(`agent-thinking:${selectedId}`),
      time: clock(),
      className: "is-agent-thinking",
      lines: ["думаю"],
      live: null
    });
  }
  for (const draft of drafts) {
    const nick = cleanNick(draft.nick) || counterpartyLabelForSelected();
    bubbles.push({
      key: `live:${draft.deviceId || nick}`,
      side: draft.deviceId === device?.id ? "local" : "remote",
      nick,
      color: draft.color,
      time: clock(new Date(draft.createdAt)),
      className: "is-live-draft",
      lines: draft.text.split("\n"),
      live: {
        deviceId: draft.deviceId,
        nick,
        index: draft.index,
        local: draft.deviceId === device?.id,
        action: "write",
        preview: draft.text
      }
    });
  }
  textPaint.innerHTML = bubbles.map((bubble) => {
    const body = bubble.className === "is-agent-thinking"
      ? `<span class="thinking-label">${escapeHtml(bubble.lines[0] || "думаю")}</span><span class="thinking-rig" aria-hidden="true"><i></i><i></i><i></i><i></i><i></i></span>`
      : bubble.lines
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

function liveDraftsForSelected(): LiveDraftState[] {
  return [...(liveDrafts.get(selectedId)?.values() ?? [])]
    .filter((draft) => draft.active && draft.text.trim())
    .sort((a, b) => a.at - b.at);
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
  const operator = operatorNameFromLine(line);
  if (label && label.deviceId !== "operator" && className !== "is-operator-head" && className !== "is-operator-reply") {
    return {
      nick: label.nick || counterpartyLabelForSelected(),
      deviceId: label.deviceId,
      color: label.color,
      side: label.deviceId && label.deviceId === device?.id ? "local" : "remote"
    };
  }
  if (operator || className === "is-operator-head" || className === "is-operator-body" || className === "is-operator-reply" || label?.deviceId === "operator") {
    const nick = operator || label?.nick || (isAgentTunnelId(selectedId) ? agentDialogLabel : "Operator");
    return {
      nick,
      deviceId: "operator",
      color: colorFor(`operator:${nick}`),
      side: "remote"
    };
  }
  if (label) {
    return {
      nick: label.nick || counterpartyLabelForSelected(),
      deviceId: label.deviceId,
      color: label.color,
      side: label.deviceId && label.deviceId === device?.id ? "local" : "remote"
    };
  }
  const nick = counterpartyLabelForSelected();
  if (isAgentTunnelId(selectedId)) {
    const localNick = cleanNick(device?.nick || "") || "Я";
    return {
      nick: localNick,
      deviceId: device?.id || "",
      color: colorFor(`local:${device?.id || selectedId}`),
      side: "local"
    };
  }
  return {
    nick,
    deviceId: "",
    color: safeColor(undefined, `${nick}:${selectedId}`),
    side: "surface"
  };
}

function operatorNameFromLine(line: string): string {
  const match = line.trim().match(/^(.+?)\s+·\s+\d{1,2}:\d{2}$/u);
  return cleanNick(match?.[1] || "");
}

function counterpartyLabelForSelected(): string {
  const tunnel = loadTunnels().find((item) => item.id === selectedId);
  return tunnel ? counterpartyLabel(tunnel) : ".";
}

function isAgentTunnelId(tunnelId: string): boolean {
  const tunnel = loadTunnels().find((item) => item.id === tunnelId);
  return Boolean(tunnel && isAgentTunnel(tunnel));
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

function isAgentChromeLineClass(className: string): boolean {
  return className === "is-operator-head" || className === "is-operator-reply";
}

function isOperatorHeader(line: string): boolean {
  return /^(Агент|Codex|Оператор|Operator)\s+·\s+\d{1,2}:\d{2}$/u.test(line);
}

function cleanAgentContext(value: string): string {
  return value
    .split(/\r?\n/u)
    .filter((line) => !isAgentContextChromeLine(line))
    .join("\n")
    .replace(/\n{3,}/gu, "\n\n")
    .trim();
}

function isAgentContextChromeLine(line: string): boolean {
  const trimmed = line.trim();
  return isOperatorHeader(trimmed)
    || trimmed === "Ответ:"
    || trimmed === "Ответьте ниже:"
    || trimmed === "Reply:"
    || trimmed === "TYPE"
    || trimmed === "EDIT"
    || trimmed === "DEL";
}

function renderWriterPop(): void {
  const pop = app.querySelector<HTMLDivElement>(".writer-pop");
  if (!pop) {
    return;
  }
  const draft = latestLiveDraft(selectedId);
  const latest = latestWriterLine(selectedId);
  const activity = activeActivities.get(selectedId) ?? (draft
    ? {
      deviceId: draft.deviceId,
      nick: draft.nick,
      index: draft.index,
      local: draft.deviceId === device?.id,
      action: "write" as const,
      preview: draft.text
    }
    : latest && Date.now() - latest.at < 4200
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

function latestLiveDraft(tunnelId: string): LiveDraftState | null {
  return liveDraftsForTunnel(tunnelId)
    .sort((a, b) => b.at - a.at)[0] ?? null;
}

function liveDraftsForTunnel(tunnelId: string): LiveDraftState[] {
  return [...(liveDrafts.get(tunnelId)?.values() ?? [])]
    .filter((draft) => draft.active && draft.text.trim());
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

function columnFromIndex(text: string, index: number): number {
  const safeIndex = Math.max(0, Math.min(index, text.length));
  const lineStart = text.lastIndexOf("\n", Math.max(0, safeIndex - 1)) + 1;
  return safeIndex - lineStart;
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
