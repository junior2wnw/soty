import QRCode from "qrcode";
import jsQR from "jsqr";
import {
  appSurfaceAllowedOrigin,
  appSurfaceInstallSchema,
  normalizeAppSurfaceId,
  normalizeAppSurfaceInstallRequest,
  resolveAppSurfaceUrl
} from "trustlink-kernel";
import { JoinRequest, LiveDraft, NoticeKnock, PeerInfo, ReceivedFile, RemoteCancel, RemoteCommand, RemoteGrant, RemoteOutput, RemoteRequest, RemoteScript, SyncedChessState, SyncedMiniApp, TerminalSnapshot, TunnelSync, WriterActivity } from "./sync";
import { icon } from "./icons";
import type { IconName } from "./icons";
import { colorFor, safeColor } from "./core/color";
import { clock } from "./core/time";
import { adoptAgentRelayFromUrl, askLocalAgentReply, bindLocalAgentRelay, checkAgentSourceMachineAgent, checkAgentSourceWorker, checkLocalAgent, checkLocalCompanionAgent, clearPendingAgentRelayReply, clearPendingAgentRelayRepliesForTunnel, downloadAgentInstallerForDevice, grantAgentSourceAccess, hasAgentRelayId, loadPendingAgentRelayReplies, resumeAgentRelayReply } from "./features/agent";
import type { LocalAgentDeviceNetwork, LocalAgentOperatorTarget, LocalAgentPendingRelayReply, LocalAgentReply, LocalAgentRequestSource, LocalAgentStatus } from "./features/agent";
import { agentSide, applyChessMove, boardSquares, buildGeniusLine, chessFromSnapshot, chooseAgentMove, createChessSnapshot, geniusCoach, isAgentTurn, isSquare, legalMovesForSquare, normalizeChessSnapshot, pieceGlyph, promotionChoices, sideName, statusText, withCoach } from "./features/chess";
import type { ChessCoach, ChessMode, ChessSnapshot } from "./features/chess";
import { downloadReceivedFile, filesFrom, formatFileSize, maxFileBytes, oversizedFilesFrom, renderFileRail } from "./features/files";
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

type QuickAction = {
  readonly id: string;
  readonly title: string;
  readonly label: string;
  readonly summary: string;
  readonly tags: readonly string[];
  readonly agentCard: {
    readonly intent: string;
    readonly targetPolicy: string;
    readonly firstMoves: readonly string[];
    readonly confirmBefore: readonly string[];
    readonly successProof: readonly string[];
    readonly avoid: readonly string[];
  };
};

const root = document.querySelector<HTMLDivElement>("#app");
if (!root) {
  throw new Error("App root missing");
}
const app: HTMLDivElement = root;
installTooltips();

const agentDialogLabel = "Агент";
const agentDialogMinVersion = "0.3.16";
const agentReleaseCheckTtlMs = 60_000;

type AgentButtonMode = "download" | "update" | "link";

type AgentRelease = {
  readonly version: string;
  readonly sha256?: string;
};

type MiniAppDefinition = {
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

type MiniAppSession = {
  readonly app: MiniAppDefinition;
  readonly nonce: string;
  readonly layout: MiniAppWindowLayout;
  readonly height?: string;
  readonly width?: string;
  readonly collapsed: boolean;
};

type MiniAppScope = "account" | "chat" | "device";
type MiniAppWindowLayout = "half" | "compact" | "large" | "full" | "floating";

type PendingAttachment = {
  readonly id: string;
  readonly file: File;
  readonly name: string;
  readonly type: string;
  readonly size: number;
};

type FileBundleAttachment = {
  readonly id: string;
  readonly name: string;
  readonly type: string;
  readonly size: number;
};

type FileBundleMarker = {
  readonly id: string;
  readonly files: readonly FileBundleAttachment[];
};

type MiniAppInstallResult = {
  readonly ok: boolean;
  readonly app?: MiniAppDefinition;
  readonly opened?: boolean;
  readonly error?: string;
};

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
const syncStates = new Map<string, "open" | "closed" | "connecting">();
const files = new Map<string, ReceivedFile[]>();
const fileNotices = new Map<string, { readonly text: string; readonly until: number }>();
const pendingAttachments = new Map<string, PendingAttachment[]>();
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
const terminalCollapsedKey = "soty:terminal-collapsed:v1";
const textSnapshotsKey = "soty:text-snapshots:v1";
const chatScrollKey = "soty:chat-scroll:v1";
const autoDownloadedFilesKey = "soty:auto-downloaded-files:v1";
const miniAppsManifestUrl = "/mini-apps/manifest.json";
const miniAppsRegistryKey = "soty:mini-apps:v1";
const miniAppProtocol = "soty.mini-app.v1";
const miniAppContextProtocol = "soty.mini-app.context.v1";
const staticMiniAppsEnabled = false;
const mobileAppkaCreationEnabled = false;
const fileBundlePrefix = "SOTY_FILE_BUNDLE:";
const agentAttachmentLimit = 10;
let miniApps: MiniAppDefinition[] = [];
let manifestMiniApps: MiniAppDefinition[] = [];
const roomMiniApps = new Map<string, MiniAppDefinition[]>();
let miniAppsProbe: Promise<readonly MiniAppDefinition[]> | null = null;
let miniAppsLoadedAt = 0;
let miniAppSession: MiniAppSession | null = null;
let miniAppOverlay: HTMLDivElement | null = null;
const quickActions: readonly QuickAction[] = [
  {
    id: "windows-reinstall",
    title: "Переустановка Windows",
    label: "WIN",
    summary: "Подготовить, проверить, подтвердить и сопровождать установку.",
    tags: ["windows", "винда", "переустановка", "usb", "флешка", "драйверы"],
    agentCard: {
      intent: "Safely prepare and guide a Windows reinstall/reset on the selected/current device.",
      targetPolicy: "Use the current dialog/source device unless the user explicitly names another linked device.",
      firstMoves: [
        "Ask only for missing install mode, USB presence, and USB erase permission.",
        "Use managed reinstall capability for prepare/status/repair/arm when available.",
        "Keep long work durable and continue polling until completed, blocked, or waiting for final confirmation."
      ],
      confirmBefore: ["erasing USB media", "starting the final reboot/install step"],
      successProof: ["fresh reinstall status", "prepared media proof", "final user confirmation before destructive step"],
      avoid: ["manual ISO path requests when managed media download is available", "starting duplicate prepare jobs", "treating a healthy running job as a blocker"]
    }
  },
  {
    id: "wallpaper",
    title: "Поставить обои",
    label: "WALL",
    summary: "Создать или взять картинку и поставить на нужный рабочий стол.",
    tags: ["обои", "wallpaper", "рабочий стол", "картинка", "image"],
    agentCard: {
      intent: "Create or use an image and set it as wallpaper on the correct target desktop.",
      targetPolicy: "Never switch to an unnamed linked device. Use the current/source computer unless the current dialog or comment names another device.",
      firstMoves: [
        "Resolve image source: generate, use attached file, or use named existing file.",
        "Resolve target device from current dialog and user wording.",
        "Apply wallpaper through the best available interactive/user route and verify the actual wallpaper path or visible state."
      ],
      confirmBefore: [],
      successProof: ["target device identity", "file path/hash or generated image proof", "wallpaper readback on the same target"],
      avoid: ["claiming success without readback", "using the only linked device as implicit target", "hiding a failed apply behind a generic done message"]
    }
  },
  {
    id: "copy-file",
    title: "Скопировать файл",
    label: "COPY",
    summary: "Перенести файл между текущим и подключенным устройством.",
    tags: ["копировать", "файл", "download", "upload", "передать", "скачать"],
    agentCard: {
      intent: "Copy a file between the current computer and a selected/mentioned linked device.",
      targetPolicy: "Infer direction from wording and current dialog: in a device chat, source is that device and destination is the user's current computer unless stated otherwise.",
      firstMoves: [
        "Identify exact source file and destination.",
        "Use the native Soty artifact/file route for cross-device transfer.",
        "Verify size/hash or destination listing."
      ],
      confirmBefore: ["overwriting an existing file", "copying large/sensitive folders"],
      successProof: ["source path", "destination path", "size/hash readback"],
      avoid: ["printing raw secrets from files", "copying ambiguous paths", "claiming transfer before destination proof"]
    }
  },
  {
    id: "check-device",
    title: "Проверить устройство",
    label: "CHECK",
    summary: "Понять состояние агента, сети, диска, процессов и доступа.",
    tags: ["проверить", "статус", "диагностика", "агент", "сеть", "диск"],
    agentCard: {
      intent: "Run a compact health/status diagnostic on the current or named device.",
      targetPolicy: "Probe the selected/current device first; only inspect another device if the user names it.",
      firstMoves: [
        "Collect identity, agent/link status, OS, disk, network, and recent task status.",
        "Keep probes short and non-destructive.",
        "Summarize one concrete blocker or the next useful action."
      ],
      confirmBefore: [],
      successProof: ["device identity", "fresh timestamped status", "specific failed component if any"],
      avoid: ["large inventories before a focused probe", "raw transport jargon in user-facing answer", "mixing statuses from different devices"]
    }
  },
  {
    id: "agent-repair",
    title: "Починить агент",
    label: "AGENT",
    summary: "Проверить установку, обновление, автозапуск и связь агента.",
    tags: ["агент", "установить", "обновить", "починить", "bridge", "relay"],
    agentCard: {
      intent: "Repair or update the Soty agent on the current/named device with proof.",
      targetPolicy: "Prefer the current device context; for linked devices use only explicit current dialog target or named target.",
      firstMoves: [
        "Check agent health/version/autostart before reinstalling.",
        "Use the official Soty installer/update path.",
        "Verify local health and relay/source status after changes."
      ],
      confirmBefore: ["privileged install/update prompts", "stopping user-visible active work"],
      successProof: ["agent version", "health endpoint or relay status", "source worker/machine link readiness when relevant"],
      avoid: ["installing duplicate agents", "masking PATH/runtime issues as missing Codex", "leaving the user without a clear next action"]
    }
  },
  {
    id: "native-window-chrome",
    title: "Окно без хедера",
    label: "HDR",
    summary: "Спрятать системную полосу PWA, закрепить watcher и проверить, что окно не уходит под Пуск.",
    tags: ["окно", "хедер", "titlebar", "pwa", "chrome", "frameless", "свернуть", "двигать", "resize"],
    agentCard: {
      intent: "Enable and verify the frameless Soty PWA window on the selected/current device.",
      targetPolicy: "Prefer the current/source device. Use another linked device only when the current dialog or user wording names it.",
      firstMoves: [
        "Run the native-window-chrome computer operation in install/apply mode with ClientTitlebarHeight=32.",
        "Verify caption=false, frameless=true, clientTitlebar.hidden=true, and watcher persistence.",
        "Check that the visible window stays inside the working area and can still be resized from the side edges."
      ],
      confirmBefore: [],
      successProof: ["caption=false", "frameless=true", "clientTitlebar.hidden=true", "clientTitlebar.bottomInsideWorkingArea=true", "persistence includes scheduled-task or hkcu-run"],
      avoid: ["changing unrelated Chrome windows", "leaving duplicate watcher processes", "claiming success without fresh status JSON"]
    }
  },
  {
    id: "soty-export",
    title: "Экспорт Сот",
    label: "EXPORT",
    summary: "Собрать перенос состояния в один файл и восстановить из него.",
    tags: ["экспорт", "импорт", "backup", "перенос", "флешка", "соты"],
    agentCard: {
      intent: "Help the user export/import Soty state as a single portable file.",
      targetPolicy: "This is normally a current-browser/current-device action unless the user names another device.",
      firstMoves: [
        "Find the current available export/import mechanism.",
        "Guide or perform the export/import with one file.",
        "Verify dialogs/devices restored after import."
      ],
      confirmBefore: ["overwriting current local Soty state during import"],
      successProof: ["export file exists or import count", "selected device/dialog state after import"],
      avoid: ["automatic hidden backup during OS reinstall", "splitting state across many files", "pretending an import happened without count/readback"]
    }
  }
];
const chessGames = new Map<string, ChessSnapshot>();
const chessFlipped = new Set<string>();
const chessAgentTimers = new Map<string, number>();
const chessWelcomedGames = new Set<string>();
const textSnapshotTimers = new Map<string, number>();
let chessOpenId = "";
let chessSelectedSquare: Square | "" = "";
let chessPromotion: { readonly tunnelId: string; readonly from: Square; readonly to: Square } | null = null;
let localAgent: LocalAgentStatus = { ok: false };
let agentButtonAgent: LocalAgentStatus = { ok: false };
let agentProbeTimer = 0;
let agentProbe: Promise<LocalAgentStatus> | null = null;
let companionProbe: Promise<LocalAgentStatus> | null = null;
let agentRelease: AgentRelease | null = null;
let agentReleaseProbe: Promise<AgentRelease | null> | null = null;
let agentReleaseCheckedAt = 0;
let agentButtonProbe: Promise<AgentButtonMode> | null = null;
let agentButtonWatchTimer = 0;
let agentButtonWatchFastUntil = 0;
const agentButtonWatchFastMs = 2_000;
const agentButtonWatchNormalMs = 8_000;
const agentButtonWatchLinkMs = 20_000;
const agentButtonWatchHiddenMs = 60_000;
const agentButtonInstallWatchMs = 10 * 60_000;
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
let joinHeartbeatTimer = 0;
let joinLastSeenAt = 0;
let joinCompleted = false;
let joinWakeCleanup: (() => void) | null = null;
const joinHeartbeatIntervalMs = 8000;
const joinStaleMs = 22_000;
let qrOverlay: HTMLDivElement | null = null;
let actionOverlay: HTMLDivElement | null = null;
let actionSearchText = "";
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
let terminalCollapsed = loadTerminalCollapsed();
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
const operatorRemoteRunTimers = new Map<string, number>();
const operatorStartingTunnels = new Set<string>();
const localAgentRuns = new Map<string, WebSocket>();
const operatorChatQueues = new Map<string, Promise<void>>();
type SotyFileStreamState = {
  readonly tunnelId: string;
  readonly commandId: string;
  readonly fileId: string;
  readonly name: string;
  readonly type: string;
  readonly size: number;
  readonly total: number;
  readonly autoDownload: boolean;
  readonly delivery: string;
  readonly sourceCommandId: string;
  sent: number;
};
const sotyFileLineBuffers = new Map<string, string>();
const sotyFileStreams = new Map<string, SotyFileStreamState>();
const operatorBridgeProtocol = "soty.operator-bridge.v2";
const agentReplyQueues = new Map<string, Promise<LocalAgentReply | null | void>>();
const agentReplyControllers = new Map<string, AbortController>();
const agentThinking = new Set<string>();
let agentDoneAudio: AudioContext | null = null;
let agentSourceControlTunnelId = "";
let agentSourcePollTimer = 0;
let agentSourcePolling = false;
let agentSourcePollEpoch = 0;
let agentSourcePollController: AbortController | null = null;
let serviceWorkerReloading = false;
let serviceWorkerUpdateTimer = 0;
let appBundleReloading = false;
let appBundleWatchTimer = 0;
let sameDeviceWindowSyncStarted = false;
let sameDeviceWindowSyncTimer = 0;
const serviceWorkerUpdateMs = 60_000;
const appBundleWatchVisibleMs = 45_000;
const appBundleWatchHiddenMs = 90_000;
const appBundlePath = currentAppBundlePath();
const nativeWindowChromeKey = "soty:native-window-chrome:v1";

window.addEventListener("beforeinstallprompt", (event) => {
  event.preventDefault();
});

window.addEventListener("message", (event) => {
  handleMiniAppMessage(event);
});

window.addEventListener("storage", (event) => {
  if (event.key === miniAppsRegistryKey) {
    handleMiniAppRegistryChange();
  } else if (event.key === nativeWindowChromeKey) {
    applyNativeWindowChromePreference();
  }
});

window.addEventListener("appinstalled", () => {
  rememberAppRuntime();
  void boot();
});

document.addEventListener("visibilitychange", () => {
  sendOperatorVisibility();
  if (document.visibilityState === "hidden") {
    void ensureOperatorBridge();
    return;
  }
  if (document.visibilityState === "visible" && selectedId) {
    clearTunnelNotices(selectedId);
    tunnels = markTunnel(selectedId, false);
    renderTiles();
    startAgentButtonWatcher(true);
    startAppBundleWatcher(true);
  }
});

window.addEventListener("online", () => {
  startAgentButtonWatcher(true);
  startAppBundleWatcher(true);
});

void boot();

async function boot(): Promise<void> {
  adoptAgentRelayFromUrl();
  captureNativeWindowChromePreference();
  applyNativeWindowChromePreference();
  startSameDeviceWindowSync();
  void refreshMiniApps(true);

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
  startAppBundleWatcher(true);

  const capturedJoin = captureJoinInviteFromLocation();
  if (capturedJoin) {
    window.history.replaceState({}, "", "/");
  }
  clearPendingInvite();

  if (!isAppRuntime()) {
    rememberAppRuntime();
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
  restorePendingAgentDialogSelection();
  if (selectedId) {
    saveSelectedTunnelId(selectedId);
  }
  renderApp();
  startAgentButtonWatcher(true);
  resumePendingAgentDialogReplies();
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
    scheduleServiceWorkerUpdate(registration);
  } catch (error) {
    console.warn("[soty] Service worker registration failed", error);
  }
}

function captureNativeWindowChromePreference(): void {
  const url = new URL(window.location.href);
  const value = url.searchParams.get("native-window-chrome") || "";
  if (!value) {
    return;
  }
  try {
    if (value === "1" || value === "true" || value === "on") {
      localStorage.setItem(nativeWindowChromeKey, "1");
    } else if (value === "0" || value === "false" || value === "off") {
      localStorage.removeItem(nativeWindowChromeKey);
    }
  } catch {
    // Non-critical: the window can still use the native browser chrome.
  }
  url.searchParams.delete("native-window-chrome");
  window.history.replaceState({}, "", `${url.pathname}${url.search}${url.hash}`);
}

function applyNativeWindowChromePreference(): void {
  let enabled = false;
  try {
    enabled = localStorage.getItem(nativeWindowChromeKey) === "1";
  } catch {
    enabled = false;
  }
  document.body.classList.toggle("native-window-chrome", enabled);
}

function scheduleServiceWorkerUpdate(registration: ServiceWorkerRegistration): void {
  window.clearTimeout(serviceWorkerUpdateTimer);
  if (serviceWorkerReloading) {
    return;
  }
  serviceWorkerUpdateTimer = window.setTimeout(() => {
    void registration.update().catch(() => undefined).finally(() => {
      scheduleServiceWorkerUpdate(registration);
    });
  }, serviceWorkerUpdateMs);
}

function currentAppBundlePath(): string {
  try {
    return new URL(import.meta.url).pathname;
  } catch {
    return "";
  }
}

function startAppBundleWatcher(force = false): void {
  if (!appBundlePath.startsWith("/assets/") || appBundleReloading) {
    return;
  }
  window.clearTimeout(appBundleWatchTimer);
  appBundleWatchTimer = window.setTimeout(
    () => void runAppBundleWatcher(),
    force ? 3000 : nextAppBundleWatchDelay()
  );
}

function nextAppBundleWatchDelay(): number {
  return document.visibilityState === "hidden" ? appBundleWatchHiddenMs : appBundleWatchVisibleMs;
}

async function runAppBundleWatcher(): Promise<void> {
  if (!appBundlePath.startsWith("/assets/") || appBundleReloading) {
    return;
  }
  try {
    const latestPath = await fetchLatestAppBundlePath();
    if (latestPath && latestPath !== appBundlePath) {
      appBundleReloading = true;
      window.location.reload();
      return;
    }
  } catch {
    // Best-effort stale-tab recovery; normal chat flow should not depend on it.
  }
  startAppBundleWatcher();
}

async function fetchLatestAppBundlePath(): Promise<string> {
  const controller = new AbortController();
  const timer = window.setTimeout(() => controller.abort(), 3000);
  try {
    const response = await fetch(`/?soty-app-check=${Date.now()}`, {
      cache: "no-store",
      headers: { Accept: "text/html" },
      signal: controller.signal
    });
    if (!response.ok) {
      return "";
    }
    const html = await response.text();
    return mainModulePathFromHtml(html);
  } finally {
    window.clearTimeout(timer);
  }
}

function mainModulePathFromHtml(html: string): string {
  const tags = html.match(/<script\b[^>]*>/giu) || [];
  for (const tag of tags) {
    if (!/\btype\s*=\s*["']module["']/iu.test(tag)) {
      continue;
    }
    const src = tag.match(/\bsrc\s*=\s*["']([^"']+)["']/iu)?.[1] || "";
    if (!src) {
      continue;
    }
    try {
      return new URL(src, window.location.href).pathname;
    } catch {
      return "";
    }
  }
  return "";
}

function startSameDeviceWindowSync(): void {
  if (sameDeviceWindowSyncStarted) {
    return;
  }
  sameDeviceWindowSyncStarted = true;
  window.addEventListener("storage", (event) => {
    if (!event.key || event.key === tunnelsKey || event.key === selectedKey) {
      scheduleSameDeviceWindowState(event.key || "storage", false);
    }
  });
  window.addEventListener("focus", () => scheduleSameDeviceWindowState("focus", true));
  window.addEventListener("pageshow", () => scheduleSameDeviceWindowState("pageshow", true));
}

function scheduleSameDeviceWindowState(reason: string, followSelected: boolean): void {
  window.clearTimeout(sameDeviceWindowSyncTimer);
  sameDeviceWindowSyncTimer = window.setTimeout(() => {
    applySameDeviceWindowState(reason, followSelected);
  }, 40);
}

function applySameDeviceWindowState(reason: string, followSelected: boolean): void {
  if (!device || !app.querySelector(".shell")) {
    return;
  }
  const previousSignature = tunnelListSignature(tunnels);
  const nextTunnels = loadTunnels();
  const nextSignature = tunnelListSignature(nextTunnels);
  const storedSelected = loadSelectedTunnelId() || "";
  const currentStillExists = nextTunnels.some((tunnel) => tunnel.id === selectedId);
  tunnels = nextTunnels;
  if ((followSelected || reason === selectedKey || !currentStillExists) && storedSelected) {
    selectedId = storedSelected;
  }
  ensurePermanentAgentDialog();
  normalizeSelectedTunnel();
  const activeIds = new Set(tunnels.map((tunnel) => tunnel.id));
  for (const [id, sync] of syncs) {
    if (!activeIds.has(id)) {
      sync.destroy();
      syncs.delete(id);
      peerDevices.delete(id);
      syncStates.delete(id);
    }
  }
  for (const tunnel of tunnels) {
    ensureSync(tunnel);
  }
  if (previousSignature !== nextSignature || reason === selectedKey || followSelected) {
    applySelectedText();
    renderTiles();
    renderComposerAttachments();
    renderTerminal();
    renderChess();
    renderMiniAppPanel();
    publishMiniAppContext();
    publishOperatorTargets();
  }
}

function tunnelListSignature(items: readonly TunnelRecord[]): string {
  return items
    .map((item) => [
      item.id,
      item.label,
      item.updatedAt,
      item.lastActionAt || "",
      item.unread ? "1" : "0",
      item.archived ? "1" : "0",
      item.agent ? "1" : "0"
    ].join(":"))
    .join("|");
}

function shouldResetLocalState(): boolean {
  const url = new URL(window.location.href);
  return url.searchParams.get("reset-local") === "1"
    || url.searchParams.get("soty-reset") === "1"
    || url.searchParams.get("repair") === "reset";
}

function renderNick(): void {
  app.innerHTML = `
    ${pwaTitlebarMarkup()}
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
  bindPwaTitlebar();
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
  restorePendingAgentDialogSelection();
  if (selectedId) {
    saveSelectedTunnelId(selectedId);
  }
  const snapshots = loadTextSnapshots();
  for (const tunnel of tunnels) {
    if (!restoredTexts.has(tunnel.id)) {
      const snapshot = snapshots.get(tunnel.id);
      if (snapshot) {
        restoredTexts.set(tunnel.id, snapshot);
      }
    }
  }
  renderApp();
  applyRestoredTextSnapshots(restoredTexts);
  startAgentButtonWatcher(true);
  resumePendingAgentDialogReplies();
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
    saveTextSnapshotNow(tunnelId, text);
    syncs.get(tunnelId)?.setText(text);
  }
  if (selectedId && restoredTexts.has(selectedId)) {
    applySelectedText();
  }
}

function loadTextSnapshots(): Map<string, string> {
  try {
    const parsed: unknown = JSON.parse(localStorage.getItem(textSnapshotsKey) || "{}");
    if (!isRecord(parsed)) {
      return new Map();
    }
    const result = new Map<string, string>();
    for (const [tunnelId, record] of Object.entries(parsed)) {
      if (!isRecord(record) || typeof record.text !== "string") {
        continue;
      }
      result.set(tunnelId, record.text.slice(0, 200_000));
    }
    return result;
  } catch {
    return new Map();
  }
}

function saveTextSnapshotNow(tunnelId: string, text: string): void {
  if (!tunnelId) {
    return;
  }
  try {
    const parsed: unknown = JSON.parse(localStorage.getItem(textSnapshotsKey) || "{}");
    const current = isRecord(parsed) ? parsed : {};
    const now = Date.now();
    const next: Record<string, { readonly text: string; readonly at: number }> = {};
    next[tunnelId] = { text: text.slice(-200_000), at: now };
    for (const [id, record] of Object.entries(current)) {
      if (id === tunnelId || !isRecord(record) || typeof record.text !== "string") {
        continue;
      }
      const at = typeof record.at === "number" && Number.isFinite(record.at) ? record.at : 0;
      next[id] = { text: record.text.slice(-200_000), at };
    }
    const keep = Object.entries(next)
      .sort((left, right) => right[1].at - left[1].at)
      .slice(0, 40);
    localStorage.setItem(textSnapshotsKey, JSON.stringify(Object.fromEntries(keep)));
  } catch {
    // Local snapshots are best-effort; realtime sync is still the source of truth.
  }
}

function scheduleTextSnapshot(tunnelId: string, text: string): void {
  if (!tunnelId) {
    return;
  }
  const previous = textSnapshotTimers.get(tunnelId);
  if (previous) {
    window.clearTimeout(previous);
  }
  const timer = window.setTimeout(() => {
    textSnapshotTimers.delete(tunnelId);
    saveTextSnapshotNow(tunnelId, text);
  }, 180);
  textSnapshotTimers.set(tunnelId, timer);
}

function loadChatScroll(): Record<string, number> {
  try {
    const parsed: unknown = JSON.parse(localStorage.getItem(chatScrollKey) || "{}");
    if (!isRecord(parsed)) {
      return {};
    }
    const result: Record<string, number> = {};
    for (const [id, value] of Object.entries(parsed)) {
      if (typeof value === "number" && Number.isFinite(value) && value >= 0) {
        result[id] = Math.round(value);
      }
    }
    return result;
  } catch {
    return {};
  }
}

function saveChatScroll(tunnelId: string, top: number): void {
  if (!tunnelId || !Number.isFinite(top)) {
    return;
  }
  try {
    const current = loadChatScroll();
    current[tunnelId] = Math.max(0, Math.round(top));
    const keep = Object.entries(current).slice(-60);
    localStorage.setItem(chatScrollKey, JSON.stringify(Object.fromEntries(keep)));
  } catch {
    // Scroll memory is cosmetic.
  }
}

function rememberCurrentChatScroll(): void {
  const scroll = app.querySelector<HTMLDivElement>(".chat-scroll");
  if (scroll && selectedId) {
    saveChatScroll(selectedId, scroll.scrollTop);
  }
}

function restoreSelectedChatScroll(): void {
  const scroll = app.querySelector<HTMLDivElement>(".chat-scroll");
  if (!scroll || !selectedId) {
    return;
  }
  const top = loadChatScroll()[selectedId];
  if (typeof top !== "number" || !Number.isFinite(top)) {
    return;
  }
  window.setTimeout(() => {
    const current = app.querySelector<HTMLDivElement>(".chat-scroll");
    if (current && selectedId) {
      current.scrollTop = Math.min(Math.max(0, top), current.scrollHeight);
    }
  }, 0);
}

function openActionMenu(): void {
  closeActionMenu();
  const query = actionSearchText.trim();
  const actions = visibleQuickActions(query);
  const comment = selectedId
    ? normalizeChatMessage(composer?.value || localDrafts.get(selectedId) || "")
    : "";
  const overlay = document.createElement("div");
  overlay.className = "action-modal";
  overlay.innerHTML = `
    <section class="action-sheet" role="dialog" aria-modal="true" aria-label="actions">
      <header class="action-head">
        <span class="action-mark">${icon("check")}</span>
        <span>
          <b>ДЕЙСТВИЯ</b>
          <small>${escapeHtml(counterpartyLabelForSelected())}</small>
        </span>
        <button class="action-close icon-button" type="button" aria-label="close" data-tooltip="Закрыть">${icon("close")}</button>
      </header>
      <input class="action-search" type="search" value="${escapeHtml(actionSearchText)}" placeholder="что сделать" />
      ${comment ? `<div class="action-comment"><b>Комментарий</b><span>${escapeHtml(comment.slice(0, 180))}</span></div>` : ""}
      <div class="action-list">
        ${actions.map((action) => quickActionRowHtml(action)).join("")}
      </div>
      ${actions.length === 0 ? `<output class="action-empty">Ничего не найдено</output>` : ""}
    </section>
  `;
  document.body.append(overlay);
  actionOverlay = overlay;
  overlay.addEventListener("click", (event) => {
    if (event.target === overlay) {
      closeActionMenu();
    }
  });
  overlay.querySelector<HTMLButtonElement>(".action-close")?.addEventListener("click", () => closeActionMenu());
  overlay.querySelector<HTMLInputElement>(".action-search")?.addEventListener("input", (event) => {
    actionSearchText = (event.currentTarget as HTMLInputElement).value.slice(0, 120);
    openActionMenu();
    actionOverlay?.querySelector<HTMLInputElement>(".action-search")?.focus();
  });
  overlay.querySelectorAll<HTMLButtonElement>(".quick-action-run").forEach((button) => {
    button.addEventListener("click", () => {
      void runQuickAction(button.dataset.actionId || "");
    });
  });
  overlay.querySelector<HTMLInputElement>(".action-search")?.focus();
}

function closeActionMenu(): void {
  actionOverlay?.remove();
  actionOverlay = null;
}

async function refreshMiniApps(force = false): Promise<readonly MiniAppDefinition[]> {
  if (!staticMiniAppsEnabled) {
    miniApps = currentMiniApps();
    renderDialogChrome();
    return miniApps;
  }
  const scopedApps = scopedMiniAppsForSelected();
  const now = Date.now();
  if (!force && miniAppsLoadedAt && now - miniAppsLoadedAt < 60_000) {
    miniApps = mergeMiniApps(manifestMiniApps, scopedApps);
    return miniApps;
  }
  if (miniAppsProbe) {
    return miniAppsProbe;
  }
  miniAppsProbe = fetch(miniAppsManifestUrl, { cache: "no-store", headers: { Accept: "application/json" } })
    .then(async (response) => {
      if (!response.ok) {
        return [];
      }
      const payload = await response.json() as unknown;
      const next = sanitizeMiniAppsManifest(payload);
      manifestMiniApps = next.map((item) => ({ ...item, source: "manifest" as const }));
      miniApps = currentMiniApps();
      miniAppsLoadedAt = Date.now();
      renderDialogChrome();
      return miniApps;
    })
    .catch(() => {
      miniApps = currentMiniApps();
      return miniApps;
    })
    .finally(() => {
      miniAppsProbe = null;
    });
  return miniAppsProbe;
}

function sanitizeMiniAppsManifest(payload: unknown): MiniAppDefinition[] {
  const rawItems = Array.isArray((payload as { readonly apps?: unknown })?.apps)
    ? (payload as { readonly apps: readonly unknown[] }).apps
    : [];
  return rawItems
    .map(sanitizeMiniAppDefinition)
    .filter((item): item is MiniAppDefinition => Boolean(item));
}

function sanitizeMiniAppDefinition(value: unknown): MiniAppDefinition | null {
  if (!isRecord(value)) {
    return null;
  }
  const id = normalizeAppSurfaceId(recordString(value, "id"));
  const title = cleanNick(recordString(value, "title") || id).slice(0, 80);
  const summary = cleanNick(recordString(value, "summary")).slice(0, 160);
  const iconName = recordString(value, "icon");
  const iconValue = isIconName(iconName) ? iconName : "remote";
  const inlineHtml = safeMiniAppInlineHtml(recordString(value, "inlineHtml") || recordString(value, "html"));
  const url = inlineHtml ? "about:srcdoc" : safeMiniAppUrl(recordString(value, "url"));
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

function mergeMiniApps(staticApps: readonly MiniAppDefinition[], localApps: readonly MiniAppDefinition[]): MiniAppDefinition[] {
  const seen = new Set<string>();
  const result: MiniAppDefinition[] = [];
  for (const item of [...localApps, ...staticApps]) {
    if (!item.id || seen.has(item.id)) {
      continue;
    }
    seen.add(item.id);
    result.push(item);
  }
  return result;
}

function currentMiniApps(): MiniAppDefinition[] {
  miniApps = mergeMiniApps(manifestMiniApps, scopedMiniAppsForSelected());
  return miniApps;
}

function loadLocalMiniApps(): MiniAppDefinition[] {
  try {
    const parsed: unknown = JSON.parse(localStorage.getItem(miniAppsRegistryKey) || "{}");
    const rawItems = Array.isArray((parsed as { readonly apps?: unknown })?.apps)
      ? (parsed as { readonly apps: readonly unknown[] }).apps
      : [];
    return rawItems
      .map(sanitizeLocalMiniAppDefinition)
      .filter((item): item is MiniAppDefinition => Boolean(item));
  } catch {
    return [];
  }
}

function sanitizeLocalMiniAppDefinition(value: unknown): MiniAppDefinition | null {
  const definition = sanitizeMiniAppDefinition(value);
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

function scopedMiniAppsForSelected(): MiniAppDefinition[] {
  return [
    ...roomMiniAppsForSelected(),
    ...localMiniAppsForSelected()
  ];
}

function saveLocalMiniApps(apps: readonly MiniAppDefinition[]): void {
  try {
    const keep = apps
      .filter((item) => item.source === "agent")
      .slice()
      .sort((left, right) => String(right.updatedAt || right.installedAt || "").localeCompare(String(left.updatedAt || left.installedAt || "")))
      .slice(0, 60);
    localStorage.setItem(miniAppsRegistryKey, JSON.stringify({
      schema: "soty.mini-apps.local.v1",
      apps: keep
    }));
  } catch {
    // Mini-app registry is local convenience state; failing storage must not break chat.
  }
}

function localMiniAppsForSelected(): MiniAppDefinition[] {
  const targetDeviceId = selectedMiniAppTargetDeviceId();
  return loadLocalMiniApps().filter((item) => {
    const scope = item.scope || "account";
    if (scope === "account") {
      return true;
    }
    if (scope === "chat") {
      return Boolean(selectedId && item.tunnelId === selectedId);
    }
    if (scope === "device") {
      return Boolean(targetDeviceId && (!item.targetDeviceId || item.targetDeviceId === targetDeviceId));
    }
    return false;
  });
}

function roomMiniAppsForSelected(): MiniAppDefinition[] {
  const targetDeviceId = selectedMiniAppTargetDeviceId();
  return (roomMiniApps.get(selectedId) ?? []).filter((item) => {
    const scope = item.scope || "chat";
    if (scope === "chat") {
      return true;
    }
    if (scope === "device") {
      return Boolean(targetDeviceId && (!item.targetDeviceId || item.targetDeviceId === targetDeviceId));
    }
    return false;
  });
}

function miniAppFromSynced(appItem: SyncedMiniApp): MiniAppDefinition | null {
  const definition = sanitizeMiniAppDefinition(appItem);
  if (!definition) {
    return null;
  }
  return {
    ...definition,
    source: "room",
    scope: appItem.scope,
    ...(appItem.targetDeviceId ? { targetDeviceId: appItem.targetDeviceId } : {}),
    ...(appItem.revision ? { revision: appItem.revision } : {}),
    installedAt: appItem.installedAt,
    updatedAt: appItem.updatedAt
  };
}

function toSyncedMiniApp(appItem: MiniAppDefinition): SyncedMiniApp {
  const scope = appItem.scope === "device" ? "device" : "chat";
  const installedAt = appItem.installedAt || new Date().toISOString();
  return {
    id: appItem.id,
    title: appItem.title,
    url: appItem.inlineHtml ? "about:srcdoc" : appItem.url,
    ...(appItem.inlineHtml ? { inlineHtml: appItem.inlineHtml } : {}),
    summary: appItem.summary || appItem.id,
    icon: appItem.icon,
    ...(appItem.layout && appItem.layout !== "half" ? { layout: appItem.layout } : {}),
    ...(appItem.height ? { height: appItem.height } : {}),
    ...(appItem.width ? { width: appItem.width } : {}),
    capabilities: appItem.capabilities,
    scope,
    ...(scope === "device" && appItem.targetDeviceId ? { targetDeviceId: appItem.targetDeviceId } : {}),
    ...(appItem.revision ? { revision: appItem.revision } : {}),
    installedAt,
    updatedAt: appItem.updatedAt || installedAt
  };
}

function selectedMiniAppTargetDeviceId(): string {
  if (!selectedId) {
    return "";
  }
  if (isAgentTunnelId(selectedId)) {
    return device?.id || "";
  }
  return remoteAccess.get(selectedId) || "";
}

function normalizeMiniAppScope(value: string): MiniAppScope {
  const clean = value.trim().toLowerCase();
  return clean === "chat" || clean === "device" ? clean : "account";
}

function normalizeMiniAppLayout(value: string): MiniAppWindowLayout {
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

function safeMiniAppCssSize(value: string): string {
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

function miniAppDefaultHeight(layout: MiniAppWindowLayout): string {
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

function miniAppDefaultWidth(layout: MiniAppWindowLayout): string {
  return layout === "floating" ? "min(760px, calc(100% - 36px))" : "auto";
}

function miniAppLayouts(): readonly MiniAppWindowLayout[] {
  return ["half", "compact", "large", "full", "floating"];
}

function installMiniAppFromConnector(value: unknown): MiniAppInstallResult {
  try {
    const source = connectorMiniAppRecord(value);
    const plan = normalizeAppSurfaceInstallRequest(value, {
      baseUrl: window.location.origin,
      allowLoopbackHttp: true,
      allowTrustedHttps: true,
      kernelIntentSchemes: ["soty:"],
      allowedScopes: ["account", "chat", "device"],
      allowKernelProxy: false,
      allowInlineHtml: true,
      maximumInlineHtmlBytes: 300_000,
      now: new Date().toISOString()
    });
    if (plan.schema !== appSurfaceInstallSchema) {
      return { ok: false, error: "unsupported-mini-app-schema" };
    }
    const iconName = recordString(source, "icon");
    const sourceDisplay = isRecord(source.display) ? source.display : source;
    const planDisplay = isRecord(plan.definition.display) ? plan.definition.display : {};
    const layout = normalizeMiniAppLayout(recordString(planDisplay, "layout") || recordString(sourceDisplay, "layout"));
    const height = safeMiniAppCssSize(recordString(planDisplay, "height") || recordString(sourceDisplay, "height"));
    const width = safeMiniAppCssSize(recordString(planDisplay, "width") || recordString(sourceDisplay, "width"));
    const scope = normalizeMiniAppScope(plan.scope);
    const now = new Date().toISOString();
    const appItem: MiniAppDefinition = {
      id: plan.definition.id,
      title: plan.definition.title,
      url: plan.definition.url,
      ...(plan.definition.inlineHtml ? { inlineHtml: plan.definition.inlineHtml } : {}),
      summary: plan.definition.summary || plan.definition.id,
      icon: isIconName(iconName) ? iconName : "remote",
      ...(layout !== "half" ? { layout } : {}),
      ...(height ? { height } : {}),
      ...(width ? { width } : {}),
      capabilities: plan.definition.capabilities,
      source: "agent",
      scope,
      ...(scope === "chat" && selectedId ? { tunnelId: selectedId } : {}),
      ...(scope === "device" ? { targetDeviceId: plan.targetDeviceId || selectedMiniAppTargetDeviceId() } : {}),
      ...(plan.revision ? { revision: plan.revision } : {}),
      installedAt: plan.installedAt,
      updatedAt: now
    };
    if (scope === "chat" && !appItem.tunnelId) {
      return { ok: false, error: "chat-scope-requires-selected-chat" };
    }
    if (scope === "device" && !appItem.targetDeviceId) {
      return { ok: false, error: "device-scope-requires-selected-device" };
    }
    if (scope === "account") {
      const next = [
        appItem,
        ...loadLocalMiniApps().filter((item) => !sameMiniAppRecord(item, appItem))
      ];
      saveLocalMiniApps(next);
    } else {
      const sync = syncs.get(selectedId);
      if (!sync) {
        return { ok: false, error: "selected-chat-sync-unavailable" };
      }
      const roomApp = {
        ...appItem,
        source: "room" as const
      };
      sync.setMiniApp(toSyncedMiniApp(roomApp));
      roomMiniApps.set(selectedId, [
        roomApp,
        ...(roomMiniApps.get(selectedId) ?? []).filter((item) => !sameMiniAppRecord(item, roomApp))
      ]);
    }
    miniApps = currentMiniApps();
    renderDialogChrome();
    if (plan.open) {
      openMiniApp(appItem.id);
    }
    return { ok: true, app: appItem, opened: plan.open };
  } catch (error) {
    return { ok: false, error: error instanceof Error ? error.message : "mini-app-install-failed" };
  }
}

function sameMiniAppRecord(left: MiniAppDefinition, right: MiniAppDefinition): boolean {
  return left.id === right.id
    && (left.scope || "account") === (right.scope || "account")
    && (left.tunnelId || "") === (right.tunnelId || "")
    && (left.targetDeviceId || "") === (right.targetDeviceId || "");
}

function connectorMiniAppRecord(value: unknown): Record<string, unknown> {
  if (!isRecord(value)) {
    return {};
  }
  return isRecord(value.app) ? value.app : value;
}

function handleMiniAppRegistryChange(): void {
  miniApps = currentMiniApps();
  if (miniAppSession && !miniApps.some((item) => item.id === miniAppSession?.app.id)) {
    miniAppSession = null;
    renderMiniAppPanel();
  }
  renderDialogChrome();
}

function isIconName(value: string): value is IconName {
  return ["install", "qr", "scan", "close", "check", "person", "clip", "remote", "download", "upload", "refresh", "copy", "bell", "shield", "send", "stop", "chess", "collapse", "expand"].includes(value);
}

function safeMiniAppUrl(value: string): string {
  try {
    const resolved = resolveAppSurfaceUrl(value, {
      baseUrl: window.location.origin,
      allowLoopbackHttp: true,
      allowTrustedHttps: true,
      kernelIntentSchemes: ["soty:"]
    });
    return resolved.requiresKernelProxy ? "" : resolved.url;
  } catch {
    return "";
  }
}

function safeMiniAppInlineHtml(value: string): string {
  const raw = String(value || "")
    .replace(/[\u0000-\u0008\u000B\u000C\u000E-\u001F\u007F]/gu, "")
    .trim();
  return raw.length <= 300_000 ? raw : "";
}

async function openMiniAppLauncher(): Promise<void> {
  closeMiniAppLauncher();
  const apps = await refreshMiniApps(true);
  if (apps.length === 0) {
    renderDialogChrome();
    return;
  }
  const overlay = document.createElement("div");
  overlay.className = "action-modal mini-launcher-modal";
  overlay.innerHTML = `
    <section class="action-sheet mini-launcher-sheet" role="dialog" aria-modal="true" aria-label="mini apps">
      <header class="action-head">
        <span class="action-mark">${icon("remote")}</span>
        <span>
          <b>MINI APPS</b>
          <small>${escapeHtml(counterpartyLabelForSelected())}</small>
        </span>
        <button class="action-close icon-button" type="button" aria-label="close" data-tooltip="Close">${icon("close")}</button>
      </header>
      <div class="action-list">
        ${apps.map((appItem) => miniAppRowHtml(appItem)).join("")}
      </div>
      ${apps.length === 0 ? `<output class="action-empty">No apps</output>` : ""}
    </section>
  `;
  document.body.append(overlay);
  miniAppOverlay = overlay;
  overlay.addEventListener("click", (event) => {
    if (event.target === overlay) {
      closeMiniAppLauncher();
    }
  });
  overlay.querySelector<HTMLButtonElement>(".action-close")?.addEventListener("click", () => closeMiniAppLauncher());
  overlay.querySelectorAll<HTMLButtonElement>(".mini-app-run").forEach((button) => {
    button.addEventListener("click", () => {
      openMiniApp(button.dataset.appId || "");
    });
  });
}

function closeMiniAppLauncher(): void {
  miniAppOverlay?.remove();
  miniAppOverlay = null;
}

function miniAppRowHtml(appItem: MiniAppDefinition): string {
  return `
    <button class="quick-action-run mini-app-run" type="button" data-app-id="${escapeHtml(appItem.id)}">
      <span class="quick-action-label">${icon(appItem.icon)}</span>
      <span class="quick-action-copy">
        <b>${escapeHtml(appItem.title)}</b>
        <small>${escapeHtml(appItem.summary || appItem.id)}</small>
      </span>
    </button>
  `;
}

function openMiniApp(appId: string): void {
  const appItem = currentMiniApps().find((item) => item.id === appId);
  if (!appItem) {
    return;
  }
  closeMiniAppLauncher();
  closeChessPanel();
  miniAppSession = {
    app: appItem,
    nonce: crypto.randomUUID(),
    layout: appItem.layout || "half",
    ...(appItem.height ? { height: appItem.height } : {}),
    ...(appItem.width ? { width: appItem.width } : {}),
    collapsed: false
  };
  renderMiniAppPanel();
  renderDialogChrome();
}

function clearMiniAppSession(): void {
  miniAppSession = null;
  renderMiniAppPanel();
  renderDialogChrome();
}

function collapseMiniApp(): void {
  if (!miniAppSession) {
    return;
  }
  miniAppSession = {
    ...miniAppSession,
    collapsed: true
  };
  renderMiniAppPanel();
  renderDialogChrome();
  publishMiniAppContext();
}

function restoreMiniApp(): void {
  if (!miniAppSession) {
    return;
  }
  miniAppSession = {
    ...miniAppSession,
    collapsed: false
  };
  renderMiniAppPanel();
  renderDialogChrome();
  publishMiniAppContext();
}

function resizeMiniAppWindow(layout: MiniAppWindowLayout, height = "", width = ""): void {
  if (!miniAppSession) {
    return;
  }
  const session = miniAppSession;
  miniAppSession = {
    app: session.app,
    nonce: session.nonce,
    layout,
    ...(height ? { height } : {}),
    ...(width ? { width } : {}),
    collapsed: false
  };
  renderMiniAppPanel();
  renderDialogChrome();
  publishMiniAppContext();
}

function reloadMiniApp(): void {
  const frame = app.querySelector<HTMLIFrameElement>(".mini-frame");
  if (!frame || !miniAppSession) {
    return;
  }
  if (miniAppSession.app.inlineHtml) {
    const session = miniAppSession;
    frame.srcdoc = "";
    window.setTimeout(() => {
      if (miniAppSession === session) {
        frame.srcdoc = miniAppInlineHtmlWithContext(session);
      }
    }, 0);
    return;
  }
  frame.src = miniAppUrlWithContext(miniAppSession);
}

function renderMiniAppPanel(): void {
  const panel = app.querySelector<HTMLDivElement>(".mini-frame-panel");
  const frame = app.querySelector<HTMLIFrameElement>(".mini-frame");
  const editor = app.querySelector<HTMLElement>(".editor");
  const title = app.querySelector<HTMLElement>(".mini-frame-title");
  const status = app.querySelector<HTMLElement>(".mini-frame-status");
  const collapseButton = app.querySelector<HTMLButtonElement>(".mini-frame-collapse");
  const dock = app.querySelector<HTMLButtonElement>(".mini-frame-dock");
  const dockTitle = app.querySelector<HTMLElement>(".mini-frame-dock-title");
  if (!panel || !frame || !editor) {
    return;
  }
  const active = Boolean(miniAppSession);
  editor.classList.toggle("mini-frame-active", active);
  editor.classList.toggle("mini-frame-collapsed", Boolean(miniAppSession?.collapsed));
  panel.classList.toggle("is-active", active);
  panel.classList.toggle("is-collapsed", Boolean(miniAppSession?.collapsed));
  if (dock) {
    dock.hidden = !miniAppSession?.collapsed;
  }
  if (!miniAppSession) {
    frame.removeAttribute("src");
    frame.removeAttribute("srcdoc");
    frame.removeAttribute("sandbox");
    panel.dataset.state = "idle";
    panel.dataset.layout = "half";
    editor.removeAttribute("data-mini-app-layout");
    editor.style.removeProperty("--mini-frame-height");
    editor.style.removeProperty("--mini-frame-width");
    return;
  }
  const session = miniAppSession;
  const height = session.height || miniAppDefaultHeight(session.layout);
  const width = session.width || miniAppDefaultWidth(session.layout);
  panel.dataset.state = "run";
  panel.dataset.layout = session.layout;
  editor.dataset.miniAppLayout = session.layout;
  editor.style.setProperty("--mini-frame-height", height);
  if (width === "auto") {
    editor.style.removeProperty("--mini-frame-width");
  } else {
    editor.style.setProperty("--mini-frame-width", width);
  }
  if (title) {
    title.textContent = session.app.title.toUpperCase();
  }
  if (status) {
    status.textContent = session.layout === "full"
      ? "FULL"
      : session.layout === "floating"
        ? "FLOAT"
        : (selectedId ? selectedId.slice(0, 8).toUpperCase() : "NO CHAT");
  }
  if (collapseButton) {
    collapseButton.innerHTML = icon(session.collapsed ? "expand" : "collapse");
    collapseButton.setAttribute("aria-label", session.collapsed ? "expand mini app" : "collapse mini app");
    collapseButton.dataset.tooltip = session.collapsed ? "Развернуть мини-апп" : "Свернуть мини-апп";
  }
  if (dockTitle) {
    dockTitle.textContent = session.app.title.toUpperCase();
  }
  if (session.collapsed) {
    return;
  }
  if (session.app.inlineHtml) {
    frame.setAttribute("sandbox", "allow-scripts allow-forms allow-popups allow-downloads");
    frame.removeAttribute("src");
    const nextHtml = miniAppInlineHtmlWithContext(session);
    if (frame.srcdoc !== nextHtml) {
      frame.srcdoc = nextHtml;
    }
  } else {
    frame.removeAttribute("sandbox");
    frame.removeAttribute("srcdoc");
    const nextUrl = miniAppUrlWithContext(session);
    if (frame.src !== nextUrl) {
      frame.src = nextUrl;
    }
  }
}

function miniAppUrlWithContext(session: MiniAppSession): string {
  const url = new URL(session.app.url, window.location.href);
  url.searchParams.set("sotyMiniApp", session.app.id);
  url.searchParams.set("sotyNonce", session.nonce);
  return url.href;
}

function miniAppInlineHtmlWithContext(session: MiniAppSession): string {
  const boot = `<script>window.SOTY_MINI_APP=${JSON.stringify({
    schema: "soty.mini-app.bootstrap.v1",
    appId: session.app.id,
    nonce: session.nonce,
    messageSchema: miniAppProtocol,
    contextSchema: miniAppContextProtocol,
    targetOrigin: "*",
    window: {
      defaultLayout: session.layout,
      layouts: miniAppLayouts()
    }
  })};</script>`;
  const html = session.app.inlineHtml || "";
  if (/<head(?:\s[^>]*)?>/iu.test(html)) {
    return html.replace(/<head(?:\s[^>]*)?>/iu, (match) => `${match}${boot}`);
  }
  if (/<html(?:\s[^>]*)?>/iu.test(html)) {
    return html.replace(/<html(?:\s[^>]*)?>/iu, (match) => `${match}<head>${boot}</head>`);
  }
  return `<!doctype html><html><head>${boot}</head><body>${html}</body></html>`;
}

function handleMiniAppMessage(event: MessageEvent): void {
  if (!miniAppSession) {
    return;
  }
  const frame = app.querySelector<HTMLIFrameElement>(".mini-frame");
  if (!frame?.contentWindow || event.source !== frame.contentWindow || !isAllowedMiniAppOrigin(event.origin, miniAppSession.app)) {
    return;
  }
  const message = event.data;
  if (!isRecord(message) || message.schema !== miniAppProtocol || message.nonce !== miniAppSession.nonce) {
    return;
  }
  const type = recordString(message, "type");
  if (type === "ready") {
    publishMiniAppContext();
    return;
  }
  if (type === "window.collapse" || type === "collapse") {
    collapseMiniApp();
    return;
  }
  if (type === "window.resize" || type === "surface.resize") {
    resizeMiniAppWindow(
      normalizeMiniAppLayout(recordString(message, "layout")),
      safeMiniAppCssSize(recordString(message, "height")),
      safeMiniAppCssSize(recordString(message, "width"))
    );
    return;
  }
  if (type === "chat.append") {
    if (!miniAppHasCapability("chat.append")) {
      postMiniAppEvent("capability.error", { capability: "chat.append" });
      return;
    }
    const text = normalizeChatMessage(recordString(message, "text"));
    if (selectedId && text) {
      appendUserMessageToDialog(selectedId, text);
      renderTextPaint();
      renderWriterPop();
    }
    return;
  }
  if (type === "agent.invoke") {
    if (!miniAppHasCapability("agent.invoke")) {
      postMiniAppEvent("capability.error", { capability: "agent.invoke" });
      return;
    }
    void invokeAgentFromMiniApp(message);
    return;
  }
  if (type === "terminal.run") {
    if (!miniAppHasCapability("terminal.run")) {
      postMiniAppEvent("capability.error", { capability: "terminal.run" });
      return;
    }
    void runTerminalFromMiniApp(message);
  }
}

function isAllowedMiniAppOrigin(origin: string, appItem: MiniAppDefinition): boolean {
  if (appItem.inlineHtml) {
    return origin === "null";
  }
  return appSurfaceAllowedOrigin(origin, appItem.url, window.location.href, {
    kernelIntentSchemes: ["soty:"]
  });
}

function publishMiniAppContext(): void {
  if (!miniAppSession) {
    return;
  }
  const frame = app.querySelector<HTMLIFrameElement>(".mini-frame");
  if (!frame?.contentWindow) {
    return;
  }
  const tunnel = loadTunnels().find((item) => item.id === selectedId) || null;
  const label = tunnel ? counterpartyLabel(tunnel) : "";
  const message = {
    schema: miniAppContextProtocol,
    nonce: miniAppSession.nonce,
    appId: miniAppSession.app.id,
    device: device ? { id: device.id, nick: device.nick } : null,
    selected: tunnel ? {
      tunnelId: tunnel.id,
      label,
      color: safeColor(tunnel.color, label + tunnel.id),
      agent: isAgentTunnel(tunnel),
      remoteController: remoteAccess.has(tunnel.id),
      remoteHost: remoteEnabled.has(tunnel.id),
      syncState: syncStates.get(tunnel.id) || "connecting"
    } : null,
    window: {
      layout: miniAppSession.layout,
      height: miniAppSession.height || miniAppDefaultHeight(miniAppSession.layout),
      width: miniAppSession.width || miniAppDefaultWidth(miniAppSession.layout),
      collapsed: miniAppSession.collapsed,
      layouts: miniAppLayouts()
    },
    appka: {
      mobileCreationEnabled: mobileAppkaCreationEnabled
    },
    capabilities: miniAppGrantedCapabilities(miniAppSession.app)
  };
  frame.contentWindow.postMessage(message, miniAppTargetOrigin(miniAppSession));
}

function miniAppTargetOrigin(session: MiniAppSession): string {
  return session.app.inlineHtml ? "*" : new URL(session.app.url, window.location.href).origin;
}

function miniAppHasCapability(capability: string): boolean {
  return Boolean(miniAppSession && miniAppGrantedCapabilities(miniAppSession.app).includes(capability));
}

function miniAppGrantedCapabilities(appItem: MiniAppDefinition): readonly string[] {
  const requested = new Set(appItem.capabilities);
  return ["chat.append", "agent.invoke", "terminal.run", "window.collapse", "window.resize"].filter((capability) =>
    capability === "window.collapse" || capability === "window.resize" || requested.has(capability)
  );
}

async function invokeAgentFromMiniApp(message: Record<string, unknown>): Promise<void> {
  if (!selectedId) {
    return;
  }
  const tunnel = loadTunnels().find((item) => item.id === selectedId);
  if (!tunnel) {
    return;
  }
  const text = normalizeChatMessage(recordString(message, "text"));
  const visible = normalizeChatMessage(recordString(message, "visibleText"));
  if (!text) {
    return;
  }
  if (visible) {
    appendUserMessageToDialog(selectedId, visible);
  }
  await sendAgentDialogMessage(selectedId, text, {
    ...(isAgentTunnel(tunnel) ? {} : { explicitMention: true })
  });
}

async function runTerminalFromMiniApp(message: Record<string, unknown>): Promise<void> {
  const tunnelId = selectedId;
  const sync = tunnelId ? syncs.get(tunnelId) : null;
  const hostDeviceId = tunnelId ? remoteAccess.get(tunnelId) : "";
  const command = recordString(message, "command").trim();
  if (!tunnelId || !sync || !hostDeviceId || !command) {
    postMiniAppEvent("terminal.error", { error: "remote-not-ready" });
    return;
  }
  const timeoutMs = safeOperatorTimeoutMs(message.timeoutMs);
  const runAs = recordString(message, "runAs").slice(0, 40);
  terminalOpenId = tunnelId;
  terminalCollapsed = false;
  setTerminalState(tunnelId, "run");
  appendTerminalLine(tunnelId, `$ ${command}`);
  renderTerminal();
  try {
    const commandId = await sync.sendRemoteCommand(hostDeviceId, command, timeoutMs, runAs);
    postMiniAppEvent("terminal.started", { commandId });
  } catch (error) {
    setTerminalState(tunnelId, "bad");
    appendTerminalLine(tunnelId, `! ${error instanceof Error ? error.message : "failed"}`);
    renderTerminal();
    postMiniAppEvent("terminal.error", { error: "send-failed" });
  }
}

function postMiniAppEvent(type: string, detail: Record<string, unknown>): void {
  if (!miniAppSession) {
    return;
  }
  const frame = app.querySelector<HTMLIFrameElement>(".mini-frame");
  if (!frame?.contentWindow) {
    return;
  }
  frame.contentWindow.postMessage({
    schema: miniAppContextProtocol,
    nonce: miniAppSession.nonce,
    type,
    ...detail
  }, miniAppTargetOrigin(miniAppSession));
}

function visibleQuickActions(query: string): readonly QuickAction[] {
  const needle = actionSearchNeedle(query);
  if (!needle) {
    return quickActions;
  }
  return quickActions
    .map((action) => ({ action, score: quickActionMatchScore(action, needle) }))
    .filter((item) => item.score > 0)
    .sort((left, right) => right.score - left.score || left.action.title.localeCompare(right.action.title))
    .map((item) => item.action);
}

function quickActionMatchScore(action: QuickAction, needle: string): number {
  const haystack = actionSearchNeedle(`${action.title} ${action.summary} ${action.tags.join(" ")} ${action.agentCard.intent}`);
  if (haystack.includes(needle)) {
    return 1000 + needle.length;
  }
  const words = needle.split(" ").filter(Boolean);
  return words.reduce((score, word) => score + (haystack.includes(word) ? 100 : 0), 0);
}

function actionSearchNeedle(value: string): string {
  return String(value || "")
    .toLowerCase()
    .replace(/[^\p{L}\p{N}]+/gu, " ")
    .trim();
}

function quickActionRowHtml(action: QuickAction): string {
  return `
    <button class="quick-action-run" type="button" data-action-id="${escapeHtml(action.id)}">
      <span class="quick-action-label">${escapeHtml(action.label)}</span>
      <span class="quick-action-copy">
        <b>${escapeHtml(action.title)}</b>
        <small>${escapeHtml(action.summary)}</small>
      </span>
    </button>
  `;
}

function agentActionButtonHtml(action: QuickAction): string {
  return `
    <button class="agent-action-button" type="button" data-action-id="${escapeHtml(action.id)}" data-tooltip="${escapeHtml(action.title)}">
      <span>${escapeHtml(action.label)}</span>
      <b>${escapeHtml(action.title)}</b>
    </button>
  `;
}

async function runQuickAction(actionId: string): Promise<void> {
  const action = quickActions.find((item) => item.id === actionId);
  const tunnelId = selectedId;
  const tunnel = loadTunnels().find((item) => item.id === tunnelId);
  if (!action || !tunnelId || !tunnel) {
    return;
  }
  const comment = normalizeChatMessage(composer?.value || localDrafts.get(tunnelId) || "");
  const visible = quickActionVisibleMessage(action, comment);
  const agentTask = quickActionAgentMessage(action, comment, tunnel);
  closeActionMenu();
  appendUserMessageToDialog(tunnelId, visible);
  clearComposerDraftForTunnel(tunnelId);
  void sendAgentDialogMessage(tunnelId, agentTask, {
    ...(isAgentTunnel(tunnel) ? {} : { explicitMention: true })
  });
}

function quickActionVisibleMessage(action: QuickAction, comment: string): string {
  return [
    `Действие: ${action.title}`,
    `Комментарий: ${comment}`
  ].join("\n").trimEnd();
}

function quickActionAgentMessage(action: QuickAction, comment: string, tunnel: TunnelRecord): string {
  const card = {
    schema: "soty.action-card.v1",
    id: action.id,
    title: action.title,
    intent: action.agentCard.intent,
    targetPolicy: action.agentCard.targetPolicy,
    firstMoves: action.agentCard.firstMoves,
    confirmBefore: action.agentCard.confirmBefore,
    successProof: action.agentCard.successProof,
    avoid: action.agentCard.avoid
  };
  return [
    `Действие: ${action.title}`,
    `Комментарий пользователя: ${comment || "(нет)"}`,
    `Текущая сота: ${counterpartyLabel(tunnel)}`,
    "",
    "PRIVATE_ACTION_CARD:",
    JSON.stringify(card),
    "",
    "Используй карточку как приватное руководство, не показывай JSON пользователю. Действуй по текущему контексту и свежей проверке; если не хватает ровно одного критичного факта, спроси только его."
  ].join("\n");
}

function appendUserMessageToDialog(tunnelId: string, message: string): void {
  const sync = syncs.get(tunnelId);
  const current = texts.get(tunnelId) ?? textarea?.value ?? "";
  const separator = current.length > 0 && !current.endsWith("\n") ? "\n" : "";
  const next = `${current}${separator}${message}\n`;
  if (tunnelId === selectedId && textarea) {
    textarea.value = next;
  }
  texts.set(tunnelId, next);
  sync?.setText(next);
  saveTextSnapshotNow(tunnelId, next);
}

function clearComposerDraftForTunnel(tunnelId: string): void {
  if (composer && tunnelId === selectedId) {
    composer.value = "";
  }
  localDrafts.delete(tunnelId);
  const pendingLiveDraftTimer = liveDraftSendTimers.get(tunnelId);
  if (pendingLiveDraftTimer) {
    window.clearTimeout(pendingLiveDraftTimer);
    liveDraftSendTimers.delete(tunnelId);
  }
  void syncs.get(tunnelId)?.sendLiveDraft("");
  touchSelected();
  resizeComposer();
  renderTiles();
  renderTextPaint();
  renderWriterPop();
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
  clearJoinSocketTimers();
  clearJoinWakeListeners();
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
    clearJoinSocketTimers();
    clearJoinWakeListeners();
    joinSocket?.close();
    continueWithoutPending();
  });
  void startJoinRequest(invite);
}

function clearJoinSocketTimers(): void {
  window.clearTimeout(joinReconnectTimer);
  window.clearInterval(joinHeartbeatTimer);
  joinHeartbeatTimer = 0;
}

function clearJoinWakeListeners(): void {
  joinWakeCleanup?.();
  joinWakeCleanup = null;
}

async function startJoinRequest(invite: JoinInvite): Promise<void> {
  if (!device) {
    return;
  }
  const requestId = `join_${crypto.randomUUID()}`;
  const pair = await createJoinKeyPair();
  const publicJwk = await publicJoinJwk(pair);
  if (!device || joinCompleted) {
    return;
  }
  let reconnectDelay = 1000;
  const finish = () => {
    joinCompleted = true;
    clearJoinSocketTimers();
    clearJoinWakeListeners();
  };
  const pulseJoinSocket = () => {
    const ws = joinSocket;
    if (!ws || ws.readyState >= WebSocket.CLOSING) {
      connect();
      return;
    }
    if (ws.readyState !== WebSocket.OPEN) {
      return;
    }
    if (Date.now() - joinLastSeenAt > joinStaleMs) {
      ws.close();
      return;
    }
    try {
      ws.send(JSON.stringify({ type: "ping" }));
    } catch {
      ws.close();
    }
  };
  function wakeJoinSocket(): void {
    if (joinCompleted || document.visibilityState === "hidden") {
      return;
    }
    pulseJoinSocket();
  }
  function connect(): void {
    if (!device || joinCompleted) {
      return;
    }
    if (joinSocket && joinSocket.readyState < WebSocket.CLOSING) {
      return;
    }
    window.clearTimeout(joinReconnectTimer);
    window.clearInterval(joinHeartbeatTimer);
    const protocol = window.location.protocol === "https:" ? "wss:" : "ws:";
    const ws = new WebSocket(`${protocol}//${window.location.host}/ws/${invite.roomId}`);
    joinSocket = ws;
    ws.onopen = () => {
      joinLastSeenAt = Date.now();
      reconnectDelay = 1000;
      ws.send(JSON.stringify({
        type: "hello",
        deviceId: device?.id,
        nick: device?.nick,
        joinRequest: {
          requestId,
          publicJwk
        }
      }));
      joinHeartbeatTimer = window.setInterval(pulseJoinSocket, joinHeartbeatIntervalMs);
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
        joinLastSeenAt = Date.now();
        if (message.type === "pong" || message.type === "join.waiting") {
          return;
        }
        if (message.requestId && message.requestId !== requestId) {
          return;
        }
        if (message.type === "join.accepted" && message.accept) {
          finish();
          try {
            const roomKey = await decryptAcceptedJoin(pair.privateKey, message.accept);
            const tunnel = tunnelFromAcceptedJoin(invite, roomKey);
            tunnels = upsertTunnel(tunnel);
            selectedId = tunnel.id;
            saveSelectedTunnelId(selectedId);
            clearPendingJoin();
            window.history.replaceState({}, "", "/?pwa=1");
            ws.close();
            renderApp();
            applySelectedText(true);
          } catch {
            ws.close();
            continueWithoutPending();
          }
        }
        if (message.type === "join.denied" || message.type === "closed") {
          finish();
          ws.close();
          continueWithoutPending();
        }
      })();
    };
    ws.onerror = () => {
      ws.close();
    };
    ws.onclose = () => {
      if (joinSocket === ws) {
        window.clearInterval(joinHeartbeatTimer);
        joinHeartbeatTimer = 0;
      }
      if (!joinCompleted && joinSocket === ws) {
        const delay = reconnectDelay + Math.round(Math.random() * 700);
        reconnectDelay = Math.min(10_000, Math.round(reconnectDelay * 1.5));
        joinReconnectTimer = window.setTimeout(connect, delay);
      }
    };
  }
  window.addEventListener("online", wakeJoinSocket);
  window.addEventListener("focus", wakeJoinSocket);
  window.addEventListener("pageshow", wakeJoinSocket);
  document.addEventListener("visibilitychange", wakeJoinSocket);
  joinWakeCleanup = () => {
    window.removeEventListener("online", wakeJoinSocket);
    window.removeEventListener("focus", wakeJoinSocket);
    window.removeEventListener("pageshow", wakeJoinSocket);
    document.removeEventListener("visibilitychange", wakeJoinSocket);
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
  ensurePermanentAgentDialog();
  normalizeSelectedTunnel();
  for (const tunnel of tunnels) {
    ensureSync(tunnel);
  }

  const hasVisibleTunnels = sortedVisibleTunnels().length > 0;
  const localDeviceNick = cleanNick(device.nick) || "SOTY";
  app.innerHTML = `
    ${pwaTitlebarMarkup()}
    <section class="shell retro-shell">
      <aside class="tiles hive-panel${hasVisibleTunnels ? "" : " empty"}">
        <div class="retro-brand">
          <span class="retro-brand-mark">S</span>
          <span>
            <b>${escapeHtml(localDeviceNick)}</b>
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
          <span class="dialog-live" aria-live="polite">
            <span class="writer-pop"></span>
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
            <div class="composer-attachments" hidden></div>
            <button class="composer-attach retro-icon-button" type="button" aria-label="attach" data-tooltip="Прикрепить файл">${icon("clip")}</button>
            <textarea class="chat-composer" rows="1" spellcheck="false" autocapitalize="sentences" aria-label="message"></textarea>
            <button class="send-button retro-icon-button" type="submit" aria-label="send" data-tooltip="Отправить сообщение">${icon("send")}</button>
          </form>
        <div class="terminal-panel mini-app-panel" data-mini-app="commands" data-tooltip="Окно удаленных команд" data-tooltip-side="top">
          <div class="terminal-head">
            <span class="terminal-led"></span>
            <span class="terminal-peer"></span>
            <span class="terminal-title">COMMANDS</span>
            <span class="terminal-status">READY</span>
            <button class="terminal-collapse" type="button" aria-label="collapse" data-tooltip="Свернуть окно команд">${icon("collapse")}</button>
          </div>
          <div class="agent-action-strip" hidden>
            <div class="agent-action-strip-head">
              <span>Действия</span>
              <small>агент выполнит в текущей соте</small>
            </div>
            <div class="agent-action-grid"></div>
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
        <div class="mini-frame-panel" data-state="idle">
          <div class="mini-frame-head">
            <span class="mini-frame-led"></span>
            <b class="mini-frame-title">APP</b>
            <small class="mini-frame-status">READY</small>
            <button class="mini-frame-collapse" type="button" aria-label="collapse mini app" data-tooltip="Свернуть мини-апп">${icon("collapse")}</button>
          </div>
          <iframe class="mini-frame" title="Soty mini app" loading="lazy" referrerpolicy="no-referrer"></iframe>
        </div>
        <button class="mini-frame-dock" type="button" hidden aria-label="restore mini app" data-tooltip="Развернуть мини-апп">
          ${icon("expand")}<span class="mini-frame-dock-title">APP</span>
        </button>
        <input class="file-input" type="file" multiple />
      </section>
      </main>
    </section>
  `;

  textarea = app.querySelector(".dialog-buffer");
  composer = app.querySelector(".chat-composer");
  textPaint = app.querySelector<HTMLDivElement>(".text-paint-inner");
  lineGutter = app.querySelector(".line-gutter");
  lineMeta = app.querySelector(".line-meta");
  fileInput = app.querySelector(".file-input");
  bindPwaTitlebar();
  app.querySelector<HTMLDivElement>(".chat-scroll")?.addEventListener("scroll", () => {
    rememberCurrentChatScroll();
  }, { passive: true });
  app.querySelector<HTMLButtonElement>(".qr-open")?.addEventListener("click", () => {
    void showQr();
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
    stageFiles(event.dataTransfer?.files);
  });
  fileInput?.addEventListener("change", () => {
    stageFiles(fileInput?.files);
    if (fileInput) {
      fileInput.value = "";
    }
  });
  app.querySelector<HTMLButtonElement>(".terminal-collapse")?.addEventListener("click", () => {
    terminalCollapsed = !terminalCollapsed;
    saveTerminalCollapsed(terminalCollapsed);
    renderTerminal();
  });
  app.querySelector<HTMLFormElement>(".terminal-form")?.addEventListener("submit", (event) => {
    event.preventDefault();
    void sendTerminalCommand();
  });
  app.querySelector<HTMLDivElement>(".chess-panel")?.addEventListener("click", (event) => {
    handleChessPanelClick(event);
  });
  app.querySelector<HTMLButtonElement>(".mini-frame-collapse")?.addEventListener("click", () => {
    collapseMiniApp();
  });
  app.querySelector<HTMLButtonElement>(".mini-frame-dock")?.addEventListener("click", () => {
    restoreMiniApp();
  });
  setupSplitter();
  applySelectedText();
  renderComposerAttachments();
  renderTerminal();
  renderChess();
  renderMiniAppPanel();
  void ensureOperatorBridge(true);
  resumeAgentSourceControl();
}

function pwaTitlebarMarkup(): string {
  const title = cleanNick(device?.nick || "") || "SOTY";
  const mark = initials(title).slice(0, 2).toUpperCase();
  return `
    <div class="pwa-titlebar" aria-label="Soty window controls">
      <div class="pwa-titlebar-drag">
        <span class="pwa-titlebar-mark">${escapeHtml(mark)}</span>
        <b>${escapeHtml(title)}</b>
      </div>
      <button class="pwa-window-minimize retro-icon-button" type="button" aria-label="minimize window" data-tooltip="Свернуть окно">${icon("collapse")}</button>
      <button class="pwa-window-close retro-icon-button" type="button" aria-label="close window" data-tooltip="Закрыть окно">${icon("close")}</button>
    </div>
  `;
}

function bindPwaTitlebar(): void {
  app.querySelector<HTMLButtonElement>(".pwa-window-minimize")?.addEventListener("click", () => {
    window.blur();
    document.body.classList.add("pwa-window-minimize-pulse");
    window.setTimeout(() => document.body.classList.remove("pwa-window-minimize-pulse"), 180);
  });
  app.querySelector<HTMLButtonElement>(".pwa-window-close")?.addEventListener("click", () => {
    window.close();
  });
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
      renderComposerAttachments();
      renderTerminal();
      renderChess();
      renderMiniAppPanel();
      publishMiniAppContext();
    },
    menu: (id, x, y) => {
      selectTunnel(id);
      applySelectedText(true);
      renderComposerAttachments();
      renderTerminal();
      renderChess();
      renderMiniAppPanel();
      const tunnel = loadTunnels().find((item) => item.id === id);
      const canClose = !tunnel || !isAgentTunnel(tunnel);
      const availableMiniApps = currentMiniApps();
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
        actions: () => {
          selectTunnel(id);
          openActionMenu();
        },
        apps: () => {
          selectTunnel(id);
          void openMiniAppLauncher();
        },
        chess: () => {
          selectTunnel(id);
          void openChessForSelected();
        },
        agentInstall: () => {
          void (async () => {
            selectTunnel(id);
            const mode = await refreshAgentButtonState(true);
            if (mode !== "link") {
              requestAgentDownload(isAgentTunnelId(id) ? device || undefined : undefined);
            }
          })();
        },
        ...(canClose ? { close: () => closeTunnel(id) } : {})
      }, {
        remoteEnabled: remoteEnabled.has(id),
        canClose,
        hasMiniApps: availableMiniApps.length > 0,
        needsAgentInstall: agentButtonMode() !== "link"
      });
    }
  });
  renderDialogChrome();
}

function renderEmptyHiveActions(field: HTMLDivElement): void {
  const actions = document.createElement("div");
  actions.className = "empty-hive-actions";
  actions.innerHTML = `
    <button class="empty-hive-action empty-qr-action" type="button" aria-label="qr" data-tooltip="Показать QR для подключения">
      ${icon("qr")}
      <span>QR</span>
    </button>
  `;
  field.append(actions);
  actions.addEventListener("pointerdown", (event) => {
    event.stopPropagation();
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
  const mode = await refreshAgentButtonState(true);
  if (mode !== "link") {
    markAgentDownloadNeeded();
    requestAgentDownload();
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

function openRemoteCommands(id: string): void {
  terminalOpenId = id;
  terminalCollapsed = false;
  saveTerminalCollapsed(false);
  if (!terminalState.has(id)) {
    setTerminalState(id, "idle");
  }
  renderTerminal();
}

async function toggleAgentRemoteGrant(agentTunnelId: string): Promise<void> {
  if (remoteEnabled.has(agentTunnelId)) {
    closeRemoteMode(agentTunnelId);
    return;
  }
  if (!device) {
    return;
  }
  const mode = await refreshAgentButtonState(true);
  if (mode !== "link") {
    markAgentDownloadNeeded();
    requestAgentDownload(device);
    return;
  }
  const companion = await ensureAgentSourceCompanion();
  if (!companion.ok) {
    markAgentDownloadNeeded();
    requestAgentDownload(device);
    return;
  }
  if (!isAgentSourceCompanionReady(companion, device.id)) {
    markAgentDownloadNeeded();
    requestAgentDownload(device);
    return;
  }
  const granted = await grantAgentSourceAccess(device.id, device.nick, true, agentSourceClientState());
  if (!granted) {
    await typeOperatorChat(
      agentTunnelId,
      formatOperatorChat("Агент не смог подключить командный канал. Проверь интернет: чат сам повторит подключение.", "sysadmin"),
      "fast"
    );
    return;
  }
  remoteEnabled = setRemoteEnabled(agentTunnelId, true);
  agentSourceGrantRefreshAt = Date.now() + agentSourceGrantRefreshMs;
  terminalOpenId = agentTunnelId;
  setTerminalState(agentTunnelId, "idle");
  appendTerminalLine(agentTunnelId, "+ agent console");
  renderTerminal();
  renderTiles();
  startAgentSourceControl(agentTunnelId);
}

function isAgentSourceCompanionReady(agent: LocalAgentStatus, expectedDeviceId = ""): boolean {
  return agent.ok === true
    && agent.relay === true
    && agent.sourceWorker === true
    && (!expectedDeviceId || agent.deviceId === expectedDeviceId)
    && agent.autoUpdate !== false
    && (!agentRelease?.version || compareVersion(agent.version || "0.0.0", agentRelease.version) >= 0);
}

async function ensureAgentSourceCompanion(): Promise<LocalAgentStatus> {
  let agent = await refreshLocalCompanion();
  if (device && !isAgentSourceCompanionReady(agent, device.id)) {
    const sourceAgent = await checkAgentSourceWorker(device.id, 1200);
    if (isAgentSourceCompanionReady(sourceAgent, device.id)) {
      localAgent = sourceAgent;
      return sourceAgent;
    }
  }
  if (!device || isAgentSourceCompanionReady(agent, device.id)) {
    return agent;
  }
  if (agent.ok) {
    await bindLocalAgentRelay(device, 1800).catch(() => false);
    const deadline = Date.now() + 4500;
    do {
      await wait(350);
      const sourceAgent = await checkAgentSourceWorker(device.id, 1200);
      agent = isAgentSourceCompanionReady(sourceAgent, device.id) ? sourceAgent : await checkLocalAgent(1200);
      localAgent = agent;
      if (isAgentSourceCompanionReady(agent, device.id)) {
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
  renderDialogChrome();
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
  renderDialogChrome();
  if (localAgent.ok) {
    agentProbeTimer = window.setTimeout(() => void refreshLocalCompanion(), 30_000);
  }
  return localAgent;
}

async function refreshAgentRelease(force = false): Promise<AgentRelease | null> {
  const fresh = agentRelease && Date.now() - agentReleaseCheckedAt < agentReleaseCheckTtlMs;
  if (!force && fresh) {
    return agentRelease;
  }
  if (agentReleaseProbe) {
    return agentReleaseProbe;
  }
  agentReleaseProbe = fetchAgentRelease()
    .then((release) => {
      agentRelease = release;
      agentReleaseCheckedAt = Date.now();
      return release;
    })
    .finally(() => {
      agentReleaseProbe = null;
      renderDialogChrome();
    });
  return agentReleaseProbe;
}

async function fetchAgentRelease(): Promise<AgentRelease | null> {
  const controller = new AbortController();
  const timer = window.setTimeout(() => controller.abort(), 1500);
  try {
    const response = await fetch("/agent/manifest.json", {
      cache: "no-store",
      signal: controller.signal
    });
    if (!response.ok) {
      return null;
    }
    const payload = await response.json() as {
      readonly version?: unknown;
      readonly sha256?: unknown;
    };
    const version = typeof payload.version === "string" ? payload.version.trim() : "";
    if (!/^\d+\.\d+\.\d+(?:[-+][A-Za-z0-9_.-]+)?$/u.test(version)) {
      return null;
    }
    return {
      version,
      ...(typeof payload.sha256 === "string" && /^[a-f0-9]{64}$/iu.test(payload.sha256) ? { sha256: payload.sha256.toLowerCase() } : {})
    };
  } catch {
    return null;
  } finally {
    window.clearTimeout(timer);
  }
}

function startAgentButtonWatcher(force = false): void {
  if (!device) {
    return;
  }
  scheduleAgentButtonRefresh(force ? 0 : nextAgentButtonWatchDelay(), force);
}

function watchAgentInstallProgress(): void {
  agentButtonWatchFastUntil = Math.max(agentButtonWatchFastUntil, Date.now() + agentButtonInstallWatchMs);
  startAgentButtonWatcher(true);
}

function scheduleAgentButtonRefresh(delayMs: number, force = false): void {
  window.clearTimeout(agentButtonWatchTimer);
  if (!device) {
    return;
  }
  agentButtonWatchTimer = window.setTimeout(() => void runAgentButtonWatcher(force), Math.max(0, delayMs));
}

async function runAgentButtonWatcher(force = false): Promise<void> {
  if (!device) {
    return;
  }
  try {
    await refreshAgentButtonState(force);
  } catch {
    renderDialogChrome();
  } finally {
    if (device) {
      scheduleAgentButtonRefresh(nextAgentButtonWatchDelay());
    }
  }
}

function nextAgentButtonWatchDelay(): number {
  if (document.visibilityState === "hidden") {
    return agentButtonWatchHiddenMs;
  }
  if (Date.now() < agentButtonWatchFastUntil) {
    return agentButtonWatchFastMs;
  }
  return agentButtonMode() === "link" ? agentButtonWatchLinkMs : agentButtonWatchNormalMs;
}

async function refreshAgentButtonState(force = false): Promise<AgentButtonMode> {
  if (agentButtonProbe) {
    return agentButtonProbe;
  }
  const probe = (async () => {
    await refreshAgentRelease(force);
    let directAgent = await checkLocalCompanionAgent(1200);
    let relayedMachineAgent = device?.id
      ? await checkAgentSourceMachineAgent(device.id, 1200)
      : { ok: false };
    if (device?.id && directAgent.ok && !isAgentMachineLinkReady(directAgent, device.id)) {
      const bound = await bindLocalAgentRelay(device, 1800).catch(() => false);
      if (bound) {
        directAgent = await checkLocalCompanionAgent(1200);
        relayedMachineAgent = await checkAgentSourceMachineAgent(device.id, 1800);
      }
    }
    localAgent = directAgent;
    agentButtonAgent = chooseAgentButtonStatus(directAgent, relayedMachineAgent);
    renderDialogChrome();
    return agentButtonMode(agentButtonAgent);
  })();
  agentButtonProbe = probe;
  try {
    return await probe;
  } finally {
    if (agentButtonProbe === probe) {
      agentButtonProbe = null;
    }
  }
}

function chooseAgentButtonStatus(directAgent: LocalAgentStatus, relayedMachineAgent: LocalAgentStatus): LocalAgentStatus {
  if (isAgentMachineLinkReady(relayedMachineAgent, device?.id || "")) {
    return relayedMachineAgent;
  }
  return directAgent;
}

function agentButtonMode(agent: LocalAgentStatus = agentButtonAgent): AgentButtonMode {
  if (!agent.ok || agent.scope === "Relay") {
    return "download";
  }
  if (agent.autoUpdate === false) {
    return "update";
  }
  if (agentRelease?.version && compareVersion(agent.version || "0.0.0", agentRelease.version) < 0) {
    return "update";
  }
  if (!isAgentMachineLinkReady(agent, device?.id || "")) {
    return "download";
  }
  return "link";
}

function isAgentMachineLinkReady(agent: LocalAgentStatus, expectedDeviceId = ""): boolean {
  return agent.ok === true
    && agent.sourceWorker === true
    && agent.system === true
    && agent.scope === "Machine"
    && (!expectedDeviceId || agent.deviceId === expectedDeviceId)
    && agent.autoUpdate !== false
    && (!agentRelease?.version || compareVersion(agent.version || "0.0.0", agentRelease.version) >= 0);
}

function requestAgentDownload(sourceDeviceForInstaller?: { readonly id?: string; readonly nick?: string }): void {
  downloadAgentInstallerForDevice(
    "machine",
    sourceDeviceForInstaller?.id ? sourceDeviceForInstaller : device || {},
    agentRelease?.version || ""
  );
  watchAgentInstallProgress();
}

function markAgentDownloadNeeded(): void {
  void refreshAgentRelease(true);
  watchAgentInstallProgress();
  renderDialogChrome();
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
  rememberCurrentChatScroll();
  selectedId = id;
  saveSelectedTunnelId(id);
  publishMiniAppContext();
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
  if (tunnel.agent === true) {
    return agentDialogLabel;
  }
  return label;
}

function ensurePermanentAgentDialog(): void {
  const current = loadTunnels();
  const now = new Date().toISOString();
  const agentTunnels = current.filter((tunnel) => isAgentTunnel(tunnel));
  let changed = false;
  if (agentTunnels.length === 0) {
    const agent = {
      ...createTunnel(agentDialogLabel, true),
      agent: true,
      color: colorFor(`agent:${device?.id || now}`),
      score: -1
    };
    const next = [agent, ...current];
    saveTunnels(next);
    tunnels = next;
    if (!selectedId || !next.some((tunnel) => tunnel.id === selectedId)) {
      selectedId = agent.id;
      saveSelectedTunnelId(agent.id);
    }
    return;
  }
  const canonical = chooseCanonicalAgentDialog(agentTunnels);
  const duplicateAgentIds = agentTunnels
    .filter((tunnel) => tunnel.id !== canonical.id)
    .map((tunnel) => tunnel.id);
  const enabledAgentIds = agentTunnels.filter((tunnel) => remoteEnabled.has(tunnel.id)).map((tunnel) => tunnel.id);
  if (enabledAgentIds.some((id) => id !== canonical.id)) {
    remoteEnabled = setRemoteEnabled(canonical.id, true);
    for (const id of enabledAgentIds) {
      if (id !== canonical.id) {
        remoteEnabled = setRemoteEnabled(id, false);
      }
    }
  }
  const next = current.map((tunnel) => {
    if (!isAgentTunnel(tunnel)) {
      return tunnel;
    }
    const keepVisible = tunnel.id === canonical.id;
    if (!keepVisible) {
      changed = true;
      return null;
    }
    const normalized = {
      ...tunnel,
      agent: true,
      label: agentDialogLabel,
      counterparty: true,
      archived: false,
      unread: tunnel.unread,
      color: tunnel.color || colorFor(`agent:${device?.id || tunnel.id}`),
      updatedAt: tunnel.updatedAt || now,
      lastActionAt: tunnel.lastActionAt || tunnel.updatedAt || now
    };
    if (
      normalized.agent === tunnel.agent
      && normalized.label === tunnel.label
      && normalized.counterparty === tunnel.counterparty
      && normalized.archived === tunnel.archived
      && normalized.unread === tunnel.unread
      && normalized.color === tunnel.color
      && normalized.updatedAt === tunnel.updatedAt
      && normalized.lastActionAt === tunnel.lastActionAt
    ) {
      return tunnel;
    }
    changed = true;
    return normalized;
  }).filter((tunnel): tunnel is TunnelRecord => tunnel !== null);
  if (!changed) {
    tunnels = current;
  } else {
    saveTunnels(next);
    tunnels = next;
    forgetDuplicateAgentDialogs(duplicateAgentIds);
  }
  if (!selectedId || !next.some((tunnel) => tunnel.id === selectedId && !tunnel.archived)) {
    selectedId = canonical.id;
    if (selectedId) {
      saveSelectedTunnelId(selectedId);
    }
  }
}

function chooseCanonicalAgentDialog(agentTunnels: readonly TunnelRecord[]): TunnelRecord {
  const selected = agentTunnels.find((tunnel) => tunnel.id === selectedId);
  return mostRecentTunnel(agentTunnels.filter((tunnel) => remoteEnabled.has(tunnel.id)))
    || selected
    || mostRecentTunnel(agentTunnels.filter((tunnel) => !tunnel.archived))
    || mostRecentTunnel(agentTunnels)
    || agentTunnels[0]!;
}

function mostRecentTunnel(items: readonly TunnelRecord[]): TunnelRecord | null {
  return [...items].sort((a, b) => {
    const score = (b.score ?? 0) - (a.score ?? 0);
    if (score !== 0) {
      return score;
    }
    return tunnelTime(b) - tunnelTime(a);
  })[0] ?? null;
}

function tunnelTime(tunnel: TunnelRecord): number {
  return Date.parse(tunnel.lastActionAt || tunnel.updatedAt || tunnel.createdAt) || 0;
}

function forgetDuplicateAgentDialogs(ids: readonly string[]): void {
  if (ids.length === 0) {
    return;
  }
  for (const id of ids) {
    syncs.get(id)?.destroy();
    syncs.delete(id);
    syncStates.delete(id);
    peerDevices.delete(id);
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
    clearPendingAgentRelayRepliesForTunnel(id);
    agentThinking.delete(id);
    localDrafts.delete(id);
    pendingAttachments.delete(id);
    files.delete(id);
    fileNotices.delete(id);
    texts.delete(id);
  }
  removeTextSnapshots(ids);
}

function removeTextSnapshots(ids: readonly string[]): void {
  const remove = new Set(ids);
  if (remove.size === 0) {
    return;
  }
  try {
    const parsed: unknown = JSON.parse(localStorage.getItem(textSnapshotsKey) || "{}");
    if (!isRecord(parsed)) {
      return;
    }
    let changed = false;
    const next: Record<string, unknown> = {};
    for (const [id, record] of Object.entries(parsed)) {
      if (remove.has(id)) {
        changed = true;
        continue;
      }
      next[id] = record;
    }
    if (changed) {
      localStorage.setItem(textSnapshotsKey, JSON.stringify(next));
    }
  } catch {
    // Snapshot cleanup is best-effort; the canonical agent dialog is already selected.
  }
}

function startFreshDialog(): void {
  const active = loadTunnels().find((tunnel) => tunnel.id === selectedId);
  if (active && !isAgentTunnel(active) && hasCounterparty(active)) {
    clearCurrentDialog(active.id);
    return;
  }
  if (active && isAgentTunnel(active)) {
    clearCurrentDialog(active.id);
    renderApp();
    return;
  }
  const fresh = createFreshDialog();
  if (!fresh) {
    return;
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
  appendTerminalLine(freshId, "+ agent console");
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
  saveTextSnapshotNow(tunnelId, "");
  localDrafts.delete(tunnelId);
  pendingAttachments.delete(tunnelId);
  writerLines.delete(tunnelId);
  activeActivities.delete(tunnelId);
  activeActivityTicks.delete(tunnelId);
  clearLiveDraftState(tunnelId);
  agentThinking.delete(tunnelId);
  clearPendingAgentRelayRepliesForTunnel(tunnelId);
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
    markAgentDownloadNeeded();
    composer?.focus();
    return;
  }
  void prepareAgentSourceForDialog(tunnel.id, tunnel);
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
  return tunnel.agent === true;
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
  saveTextSnapshotNow(fresh.id, "");
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
    && agent.relay === true
    && agent.codex !== false
    && compareVersion(agent.version || "0.0.0", agentDialogMinVersion) >= 0;
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
  const sendButton = app.querySelector<HTMLButtonElement>(".send-button");
  const appsButton = app.querySelector<HTMLButtonElement>(".apps-action");
  const mode = agentButtonMode();
  const agentTunnel = tunnel ? isAgentTunnel(tunnel) : false;
  const availableMiniApps = currentMiniApps();
  if (miniAppSession && !availableMiniApps.some((item) => item.id === miniAppSession?.app.id)) {
    miniAppSession = null;
    renderMiniAppPanel();
  }
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
    let remote = "LIVE TEXT";
    if (mode === "download") {
      remote = "AGENT SETUP";
    } else if (mode === "update") {
      remote = "AGENT UPDATE";
    } else if (agentTunnel) {
      remote = remoteEnabled.has(selectedId) ? "AGENT READY" : "AGENT";
    } else if (remoteAccess.has(selectedId)) {
      remote = "REMOTE READY";
    } else if (remoteEnabled.has(selectedId)) {
      remote = "HOST READY";
    }
    const syncState = selectedId ? syncStates.get(selectedId) : "";
    const syncSuffix = syncState === "connecting" ? " / SYNCING" : syncState === "closed" ? " / OFFLINE" : "";
    state.textContent = `${remote}${syncSuffix}`;
  }
  if (id) {
    id.textContent = selectedId ? selectedId.slice(0, 8).toUpperCase() : "NO CHAT";
  }
  if (sendButton) {
    const stopping = agentThinking.has(selectedId);
    sendButton.classList.toggle("is-stop", stopping);
    sendButton.setAttribute("aria-label", stopping ? "stop" : "send");
    sendButton.dataset.tooltip = stopping ? "Stop agent" : "Send message";
    sendButton.innerHTML = icon(stopping ? "stop" : "send");
  }
  if (remoteButton) {
    const needsAgent = mode !== "link";
    remoteButton.hidden = !needsAgent;
    remoteButton.classList.toggle("is-on", false);
    remoteButton.classList.toggle("has-access", false);
    remoteButton.classList.toggle("needs-agent", needsAgent);
    if (needsAgent) {
      remoteButton.setAttribute("aria-label", mode === "update" ? "update" : "download");
      remoteButton.innerHTML = `${icon("download")}<span>${mode === "update" ? "UPDATE" : "DOWNLOAD"}</span>`;
      remoteButton.dataset.tooltip = mode === "update" ? "Download current Soty Agent" : "Download Soty Agent";
    }
  }
  if (appsButton) {
    appsButton.hidden = availableMiniApps.length === 0;
    appsButton.classList.toggle("is-on", Boolean(miniAppSession));
  }
  publishMiniAppContext();
}

function ensureSync(tunnel: TunnelRecord): void {
  if (!device || syncs.has(tunnel.id)) {
    return;
  }
  syncs.set(tunnel.id, new TunnelSync(tunnel, device, {
    onText: (text) => {
      texts.set(tunnel.id, text);
      scheduleTextSnapshot(tunnel.id, text);
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
      maybeAutoDownloadReceivedFile(tunnel.id, file);
      tunnels = tunnel.id === selectedId ? touchTunnel(tunnel.id) : markTunnel(tunnel.id, true);
      if (tunnel.id === selectedId) {
        renderTextPaint();
        renderComposerAttachments();
      }
      renderTiles();
    },
    onFileDeleted: (fileId) => {
      files.set(tunnel.id, (files.get(tunnel.id) ?? []).filter((item) => item.id !== fileId));
      if (tunnel.id === selectedId) {
        renderTextPaint();
        renderComposerAttachments();
      }
    },
    onMiniApps: (apps) => {
      roomMiniApps.set(tunnel.id, apps
        .map(miniAppFromSynced)
        .filter((item): item is MiniAppDefinition => Boolean(item)));
      if (tunnel.id === selectedId) {
        miniApps = currentMiniApps();
        if (miniAppSession && !miniApps.some((item) => item.id === miniAppSession?.app.id)) {
          miniAppSession = null;
          renderMiniAppPanel();
        }
        renderDialogChrome();
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
      syncStates.delete(tunnel.id);
      roomMiniApps.delete(tunnel.id);
      writerLines.delete(tunnel.id);
      activeActivities.delete(tunnel.id);
      activeActivityTicks.delete(tunnel.id);
      tunnels = removeTunnel(tunnel.id);
      normalizeSelectedTunnel();
      renderApp();
    },
    onState: (state) => {
      syncStates.set(tunnel.id, state);
      if (state === "open") {
        announceRemoteGrant(tunnel.id);
      }
      if (tunnel.id === selectedId) {
        renderDialogChrome();
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
    void (async () => {
      setTunnelCounterparty(tunnel.id, nick);
      selectTunnel(tunnel.id);
      clearTunnelNotices(tunnel.id);
      try {
        await syncs.get(tunnel.id)?.acceptJoin(request, device?.nick || ".");
      } finally {
        closeQrOverlay();
        remove();
        renderApp();
        applySelectedText(true);
      }
    })();
  });
  overlay.querySelector(".deny-button")?.addEventListener("click", () => {
    syncs.get(tunnel.id)?.denyJoin(request);
    remove();
  });
  document.body.append(overlay);
}

function closeTunnel(id: string): void {
  const tunnel = loadTunnels().find((item) => item.id === id);
  if (tunnel && isAgentTunnel(tunnel)) {
    selectTunnel(id);
    renderTiles();
    return;
  }
  const sync = syncs.get(id);
  sync?.closeForEveryone();
  syncs.delete(id);
  syncStates.delete(id);
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
  pendingAttachments.delete(id);
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
    syncStates.delete(tunnel.id);
    writerLines.delete(tunnel.id);
    activeActivities.delete(tunnel.id);
    activeActivityTicks.delete(tunnel.id);
    clearLiveDraftState(tunnel.id);
    agentThinking.delete(tunnel.id);
    chessGames.delete(tunnel.id);
    chessFlipped.delete(tunnel.id);
    forgetChessSnapshot(tunnel.id);
    pendingAttachments.delete(tunnel.id);
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
        renderComposerAttachments();
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
  renderComposerAttachments();
}

function stageFiles(list?: FileList | null): void {
  if (!selectedId) {
    return;
  }
  const tunnelId = selectedId;
  const accepted = filesFrom(list);
  const oversized = oversizedFilesFrom(list);
  const current = pendingAttachments.get(tunnelId) ?? [];
  const limit = isAgentTunnelId(tunnelId) ? agentAttachmentLimit : Number.POSITIVE_INFINITY;
  const openSlots = Math.max(0, limit - current.length);
  const nextFiles = accepted.slice(0, openSlots).map(pendingAttachmentFromFile);
  const rejectedByCount = accepted.length - nextFiles.length;
  if (nextFiles.length > 0) {
    pendingAttachments.set(tunnelId, [...current, ...nextFiles]);
  }
  if (oversized.length > 0 || rejectedByCount > 0) {
    const parts = [
      oversized.length > 0 ? `File is too large: current browser transfer limit is ${formatFileSize(maxFileBytes)}` : "",
      rejectedByCount > 0 ? `Agent messages accept up to ${agentAttachmentLimit} files at once` : ""
    ].filter(Boolean);
    setFileNotice(tunnelId, parts.join(". "));
  }
  renderComposerAttachments();
}

function pendingAttachmentFromFile(file: File): PendingAttachment {
  return {
    id: `pending_${crypto.randomUUID()}`,
    file,
    name: file.name || "file",
    type: file.type || "application/octet-stream",
    size: file.size
  };
}

async function sendPendingAttachments(tunnelId: string, sync: TunnelSync): Promise<ReceivedFile[]> {
  const pending = pendingAttachments.get(tunnelId) ?? [];
  if (pending.length === 0) {
    return [];
  }
  const limit = isAgentTunnelId(tunnelId) ? agentAttachmentLimit : Number.POSITIVE_INFINITY;
  const sending = pending.slice(0, limit);
  const remaining = pending.slice(sending.length);
  const sent: ReceivedFile[] = [];
  let failed = 0;
  for (const item of sending) {
    try {
      const localFile = await sync.sendFile(item.file);
      sent.push(localFile);
      files.set(tunnelId, [localFile, ...(files.get(tunnelId) ?? []).filter((file) => file.id !== localFile.id)]);
    } catch {
      failed += 1;
    }
  }
  if (remaining.length > 0) {
    pendingAttachments.set(tunnelId, remaining);
  } else {
    pendingAttachments.delete(tunnelId);
  }
  if (failed > 0) {
    setFileNotice(tunnelId, "Some files did not send. Keep the chat open and try again.");
  }
  return sent;
}

function renderComposerAttachments(): void {
  const root = app.querySelector<HTMLDivElement>(".composer-attachments");
  if (!root) {
    return;
  }
  const tunnel = loadTunnels().find((item) => item.id === selectedId);
  const color = safeColor(tunnel?.color, (tunnel?.label || selectedId) + selectedId);
  const pending = pendingAttachments.get(selectedId) ?? [];
  const notice = activeFileNotice(selectedId);
  root.hidden = pending.length === 0 && !notice;
  root.innerHTML = [
    notice ? `<div class="composer-file-notice" style="--color:${color}">${escapeHtml(notice)}</div>` : "",
    ...pending.map((item) => `
      <div class="composer-file-chip" style="--color:${color}" data-pending-id="${escapeHtml(item.id)}">
        <span>${icon("clip")}</span>
        <b>${escapeHtml(item.name)}</b>
        <small>${escapeHtml(formatFileSize(item.size))}</small>
        <button type="button" data-pending-id="${escapeHtml(item.id)}" aria-label="remove file" data-tooltip="Remove file">${icon("close")}</button>
      </div>
    `)
  ].join("");
  root.querySelectorAll<HTMLButtonElement>("button[data-pending-id]").forEach((button) => {
    button.addEventListener("click", (event) => {
      event.preventDefault();
      event.stopPropagation();
      removePendingAttachment(button.dataset.pendingId || "");
    });
  });
}

function removePendingAttachment(id: string): void {
  if (!selectedId || !id) {
    return;
  }
  const next = (pendingAttachments.get(selectedId) ?? []).filter((item) => item.id !== id);
  if (next.length > 0) {
    pendingAttachments.set(selectedId, next);
  } else {
    pendingAttachments.delete(selectedId);
  }
  renderComposerAttachments();
}

function activeFileNotice(tunnelId: string): string {
  const notice = fileNotices.get(tunnelId);
  if (!notice) {
    return "";
  }
  if (notice.until <= Date.now()) {
    fileNotices.delete(tunnelId);
    return "";
  }
  return notice.text;
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
        markAgentDownloadNeeded();
        requestAgentDownload();
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
  renderComposerAttachments();
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
  renderComposerAttachments();
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

function maybeAutoDownloadReceivedFile(tunnelId: string, file: ReceivedFile): void {
  if (file.autoDownload !== true || file.delivery !== "controller-browser-downloads") {
    return;
  }
  const createdAt = Date.parse(file.createdAt || "");
  if (!Number.isFinite(createdAt) || Date.now() - createdAt > 2 * 60_000) {
    return;
  }
  const key = `${tunnelId}:${file.id}`;
  const seen = loadAutoDownloadedFiles();
  if (seen.has(key)) {
    return;
  }
  seen.add(key);
  saveAutoDownloadedFiles(seen);
  downloadReceivedFile(file);
  appendTerminalLine(tunnelId, `+ saved to Downloads ${file.name}`);
  const operatorId = file.commandId ? operatorPending.get(file.commandId) : "";
  if (operatorId) {
    sendOperatorOutput(operatorId, `controllerDownload=${file.name}\n`);
  }
  renderTerminal();
}

function loadAutoDownloadedFiles(): Set<string> {
  try {
    const parsed: unknown = JSON.parse(localStorage.getItem(autoDownloadedFilesKey) || "[]");
    return new Set(Array.isArray(parsed) ? parsed.map((item) => String(item)).filter(Boolean) : []);
  } catch {
    return new Set();
  }
}

function saveAutoDownloadedFiles(seen: Set<string>): void {
  try {
    localStorage.setItem(autoDownloadedFilesKey, JSON.stringify([...seen].slice(-200)));
  } catch {
    // Ignore storage failures; duplicate prevention is only a convenience.
  }
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
      clearOperatorRemoteRunTimer(operatorId);
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
      capabilities: ["agent-new", "agent-message", "export-tail", "fast-fresh-dialog", "mini-app-install"]
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
      readonly app?: unknown;
      readonly appId?: string;
      readonly title?: string;
      readonly url?: string;
      readonly summary?: string;
      readonly icon?: string;
      readonly height?: string;
      readonly scope?: string;
      readonly targetDeviceId?: string;
      readonly revision?: string;
      readonly open?: boolean;
      readonly capabilities?: unknown;
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
    if (message.type === "operator.mini-app-install") {
      runOperatorMiniAppInstall(message);
    }
    if (message.type === "operator.terminal") {
      return;
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
  expireOperatorRemoteRunTimeouts();
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

function cancelOperatorRemoteRun(requestId: string, run: OperatorRemoteRun, reason: string, exitCode = 130): void {
  clearOperatorRemoteRunTimer(requestId);
  appendTerminalLine(run.tunnelId, reason);
  setTerminalState(run.tunnelId, "bad");
  const sync = syncs.get(run.tunnelId);
  if (sync) {
    void sync.sendRemoteCancel(run.hostDeviceId, run.commandId).catch(() => undefined);
  }
  sendOperatorOutput(requestId, `${reason}\n`, exitCode);
  operatorRemoteRuns.delete(requestId);
  operatorPending.delete(run.commandId);
}

function clearOperatorRemoteRunTimer(requestId: string): void {
  const timer = operatorRemoteRunTimers.get(requestId);
  if (timer) {
    window.clearTimeout(timer);
    operatorRemoteRunTimers.delete(requestId);
  }
}

function scheduleOperatorRemoteRunTimeout(requestId: string, run: OperatorRemoteRun): void {
  clearOperatorRemoteRunTimer(requestId);
  const timeoutMs = Math.max(1000, run.timeoutMs || 0);
  const timer = window.setTimeout(() => {
    const current = operatorRemoteRuns.get(requestId);
    if (!current) {
      return;
    }
    cancelOperatorRemoteRun(requestId, current, "! timeout", 124);
    renderTerminal();
  }, timeoutMs + 3000);
  operatorRemoteRunTimers.set(requestId, timer);
}

function expireOperatorRemoteRunTimeouts(now = Date.now()): void {
  let changed = false;
  for (const [requestId, run] of [...operatorRemoteRuns.entries()]) {
    const timeoutMs = Math.max(1000, run.timeoutMs || 0);
    if (now - run.startedAt <= timeoutMs + 3000) {
      continue;
    }
    cancelOperatorRemoteRun(requestId, run, "! timeout", 124);
    changed = true;
  }
  if (changed) {
    renderTerminal();
  }
}

function hasOperatorTargets(): boolean {
  return operatorTargets().length > 0;
}

function publishOperatorTargets(): void {
  const ws = operatorSocket;
  if (!ws || ws.readyState !== WebSocket.OPEN) {
    return;
  }
  const targets = operatorTargets();
  const activeTunnel = selectedId ? loadTunnels().find((item) => item.id === selectedId) || null : null;
  ws.send(JSON.stringify({
    type: "operator.targets",
    deviceId: device?.id || "",
    deviceNick: device?.nick || "",
    targets,
    deviceNetwork: agentDeviceNetworkContext(selectedId, activeTunnel, targets)
  }));
}

function sendOperatorVisibility(): void {
  const ws = operatorSocket;
  if (!ws || ws.readyState !== WebSocket.OPEN) {
    return;
  }
  ws.send(JSON.stringify({
    type: "operator.visibility",
    visible: document.visibilityState === "visible"
  }));
}

function operatorTargets(): LocalAgentOperatorTarget[] {
  return sortedVisibleTunnels()
    .filter((tunnel) => !isAgentTunnel(tunnel) && remoteAccess.has(tunnel.id))
    .map((tunnel, index) => {
      const deviceIds = [...new Set((peerDevices.get(tunnel.id) ?? []).map((peer) => peer.id).filter(Boolean))];
      const hostDeviceId = remoteAccess.get(tunnel.id) || "";
      return {
        id: tunnel.id,
        label: counterpartyLabel(tunnel),
        deviceIds,
        hostDeviceId,
        access: true,
        host: remoteEnabled.has(tunnel.id),
        selected: tunnel.id === selectedId,
        rank: index + 1,
        lastActionAt: tunnel.lastActionAt || tunnel.updatedAt
      };
    });
}

function agentDeviceNetworkContext(
  tunnelId: string,
  tunnel: TunnelRecord | null,
  targets: readonly LocalAgentOperatorTarget[] = operatorTargets()
): LocalAgentDeviceNetwork {
  const selectedTarget = tunnel && isAgentTunnel(tunnel)
    ? null
    : linkedOperatorTargetForTunnel(tunnelId, targets);
  const selectedTargetDeviceId = selectedTarget?.hostDeviceId || selectedTarget?.deviceIds?.[0] || "";
  const sourceCapabilities = isAgentSourceCompanionReady(localAgent, device?.id || "")
    ? ["source-device-agent"]
    : ["web-controller"];
  return {
    protocol: "soty-device-network.v1",
    controllerDeviceId: device?.id || "",
    controllerDeviceNick: device?.nick || "",
    activeTunnelId: tunnelId || "",
    activeTunnelLabel: tunnel ? counterpartyLabel(tunnel) : "",
    activeTunnelKind: tunnel && isAgentTunnel(tunnel) ? "agent" : "peer",
    selectedTargetId: selectedTarget?.id || "",
    selectedTargetLabel: selectedTarget?.label || "",
    selectedTargetDeviceId,
    selectedTargetAccess: selectedTarget?.access === true,
    selectedTargetLink: Boolean(selectedTarget),
    capabilities: [
      "chat-selected-target",
      "linked-device-actions",
      "room-file-transfer",
      "artifact-transfer",
      "desktop-actions",
      "multi-device-context",
      ...sourceCapabilities
    ],
    targets: [...targets]
  };
}

function linkedOperatorTargetForTunnel(tunnelId: string, targets: readonly LocalAgentOperatorTarget[]): LocalAgentOperatorTarget | null {
  if (!tunnelId) {
    return null;
  }
  return targets.find((target) => target.id === tunnelId) || null;
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
    renderComposerAttachments();
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
    const run: OperatorRemoteRun = {
      commandId,
      tunnelId: tunnel.id,
      hostDeviceId,
      startedAt: Date.now(),
      timeoutMs,
      kind: "run",
      label: command.slice(0, 120)
    };
    operatorRemoteRuns.set(requestId, run);
    scheduleOperatorRemoteRunTimeout(requestId, run);
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
    renderComposerAttachments();
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
    const run: OperatorRemoteRun = {
      commandId,
      tunnelId: tunnel.id,
      hostDeviceId,
      startedAt: Date.now(),
      timeoutMs,
      kind: "script",
      label: name.slice(0, 120)
    };
    operatorRemoteRuns.set(requestId, run);
    scheduleOperatorRemoteRunTimeout(requestId, run);
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
  clearOperatorRemoteRunTimer(requestId);
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
    if (!isAgentTunnel(tunnel) && containsAgentInvocation(text)) {
      void sendAgentDialogMessage(tunnel.id, text, { explicitMention: true });
    }
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
  saveTextSnapshotNow(tunnel.id, next);
  localDrafts.delete(tunnel.id);
  clearLiveDraftState(tunnel.id);
  void sync.sendLiveDraft("");
  touchSelected();
  renderTiles();
  applySelectedText();
  renderTextPaint();
  renderWriterPop();
  try {
    const reply = await sendAgentDialogMessage(tunnel.id, body);
    sendOperatorOutput(
      requestId,
      formatAgentReplyForOperator(reply),
      typeof reply?.exitCode === "number" ? reply.exitCode : (reply?.ok === false ? 1 : 0)
    );
  } catch {
    sendOperatorOutput(requestId, "! agent-message", 500);
  }
}

function formatAgentReplyForOperator(reply: LocalAgentReply | null | void): string {
  if (!reply) {
    return "done\n";
  }
  const parts: string[] = [];
  const seen = new Set<string>();
  const pushUnique = (value: string) => {
    const clean = normalizeChatMessage(value);
    const key = clean.replace(/\s+/gu, " ").trim();
    if (!clean || !key || seen.has(key)) {
      return;
    }
    seen.add(key);
    parts.push(clean);
  };
  for (const message of reply.messages ?? []) {
    pushUnique(cleanAgentReplyText(message));
  }
  const text = normalizeChatMessage(cleanAgentReplyText(reply.text));
  const messageBody = parts.join("\n\n").trim();
  const compactText = text.replace(/\s+/gu, " ").trim();
  const compactMessages = messageBody.replace(/\s+/gu, " ").trim();
  if (text && !parts.includes(text) && (!compactMessages || !compactText.includes(compactMessages))) {
    pushUnique(text);
  }
  const body = parts.join("\n\n").trim();
  if (body) {
    return `${body}\n`;
  }
  return reply.ok ? "done\n" : "! agent-message\n";
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
    const current = normalizeAgentDialog(previous.id) || previous;
    clearCurrentDialog(current.id);
    renderApp();
    publishOperatorTargets();
    sendOperatorOutput(requestId, `agent ${current.id}\n`, 0);
    ensureAgentDialogBridgeReady();
    return;
  }
  const fresh = createFreshDialog(agentDialogLabel, {
    agent: true,
    archiveCurrent: false
  });
  if (!fresh) {
    sendOperatorOutput(requestId, "! agent-new\n", 500);
    return;
  }
  renderApp();
  publishOperatorTargets();
  sendOperatorOutput(requestId, `agent ${fresh.id}\n`, 0);
  ensureAgentDialogBridgeReady();
}

function ensureAgentDialogBridgeReady(): void {
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

function runOperatorMiniAppInstall(message: {
  readonly id?: string;
  readonly target?: string;
  readonly app?: unknown;
  readonly appId?: string;
  readonly title?: string;
  readonly url?: string;
  readonly inlineHtml?: string;
  readonly html?: string;
  readonly summary?: string;
  readonly icon?: string;
  readonly layout?: string;
  readonly height?: string;
  readonly width?: string;
  readonly display?: unknown;
  readonly scope?: string;
  readonly targetDeviceId?: string;
  readonly revision?: string;
  readonly open?: boolean;
  readonly capabilities?: unknown;
}): void {
  const requestId = typeof message.id === "string" ? message.id : "";
  if (!requestId) {
    return;
  }
  const target = findVisibleOperatorTarget(message.target || "") || findAgentOperatorTarget(message.target || "");
  if (message.target && !target) {
    sendOperatorOutput(requestId, "! target", 404);
    return;
  }
  if (target) {
    selectedId = target.id;
    saveSelectedTunnelId(target.id);
    renderTiles();
    applySelectedText();
  }
  if (!selectedId) {
    sendOperatorOutput(requestId, "! selected", 409);
    return;
  }
  const result = installMiniAppFromConnector(operatorMiniAppPayload(message));
  if (!result.ok || !result.app) {
    sendOperatorOutput(requestId, `! mini-app ${result.error || "install-failed"}\n`, 400);
    return;
  }
  sendOperatorOutput(
    requestId,
    `mini-app ${result.app.id} ${result.opened ? "opened" : "installed"}\n`,
    0
  );
}

function operatorMiniAppPayload(message: {
  readonly app?: unknown;
  readonly appId?: string;
  readonly title?: string;
  readonly url?: string;
  readonly inlineHtml?: string;
  readonly html?: string;
  readonly summary?: string;
  readonly icon?: string;
  readonly layout?: string;
  readonly height?: string;
  readonly width?: string;
  readonly display?: unknown;
  readonly scope?: string;
  readonly targetDeviceId?: string;
  readonly revision?: string;
  readonly open?: boolean;
  readonly capabilities?: unknown;
}): Record<string, unknown> {
  const appRecord = isRecord(message.app) ? message.app : {};
  return {
    ...appRecord,
    id: recordString(appRecord, "id") || message.appId || "",
    title: recordString(appRecord, "title") || message.title || "",
    url: recordString(appRecord, "url") || message.url || "",
    inlineHtml: recordString(appRecord, "inlineHtml") || recordString(appRecord, "html") || message.inlineHtml || message.html || "",
    summary: recordString(appRecord, "summary") || message.summary || "",
    icon: recordString(appRecord, "icon") || message.icon || "",
    layout: recordString(appRecord, "layout") || message.layout || "",
    height: recordString(appRecord, "height") || message.height || "",
    width: recordString(appRecord, "width") || message.width || "",
    display: isRecord(message.display) ? message.display : (isRecord(appRecord.display) ? appRecord.display : undefined),
    scope: message.scope || recordString(appRecord, "scope") || "chat",
    targetDeviceId: message.targetDeviceId || recordString(appRecord, "targetDeviceId") || "",
    revision: message.revision || recordString(appRecord, "revision") || "",
    open: message.open !== false,
    capabilities: Array.isArray(message.capabilities)
      ? message.capabilities
      : (Array.isArray(appRecord.capabilities) ? appRecord.capabilities : [])
  };
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
  const title = app.querySelector<HTMLSpanElement>(".terminal-title");
  const status = app.querySelector<HTMLSpanElement>(".terminal-status");
  const collapseButton = app.querySelector<HTMLButtonElement>(".terminal-collapse");
  const actionStrip = app.querySelector<HTMLDivElement>(".agent-action-strip");
  const actionGrid = app.querySelector<HTMLDivElement>(".agent-action-grid");
  if (!panel || !output || !editor || !form || !peer) {
    return;
  }
  const tunnelId = activeTerminalTunnelId();
  const chessActive = Boolean(activeChessTunnelId());
  const controller = Boolean(tunnelId && remoteAccess.has(tunnelId));
  const host = Boolean(tunnelId && remoteEnabled.has(tunnelId));
  const agentCommandsMiniApp = Boolean(tunnelId && isAgentTunnelId(tunnelId));
  const active = !chessActive && (controller || host || agentCommandsMiniApp);
  const state = tunnelId ? terminalState.get(tunnelId) ?? "idle" : "idle";
  editor.classList.toggle("terminal-active", active);
  editor.classList.toggle("terminal-collapsed", active && terminalCollapsed);
  editor.classList.toggle("terminal-controller", controller);
  editor.classList.toggle("terminal-host", host && !controller);
  panel.classList.toggle("is-collapsed", active && terminalCollapsed);
  const showAgentActions = active && agentCommandsMiniApp && !terminalCollapsed;
  panel.classList.toggle("has-agent-actions", showAgentActions);
  if (actionStrip) {
    actionStrip.hidden = !showAgentActions;
  }
  if (actionGrid) {
    actionGrid.innerHTML = showAgentActions
      ? quickActions.map((action) => agentActionButtonHtml(action)).join("")
      : "";
    actionGrid.querySelectorAll<HTMLButtonElement>(".agent-action-button").forEach((button) => {
      button.addEventListener("click", () => {
        void runQuickAction(button.dataset.actionId || "");
      });
    });
  }
  panel.dataset.state = state;
  form.hidden = !controller;
  const tunnel = loadTunnels().find((item) => item.id === tunnelId);
  peer.textContent = tunnel ? initials(counterpartyLabel(tunnel)) : ".";
  if (title) {
    title.textContent = tunnel && isAgentTunnel(tunnel) ? "AGENT CONSOLE" : "REMOTE CONSOLE";
  }
  if (status) {
    status.textContent = controller ? "CONTROL" : host ? "HOST" : state.toUpperCase();
  }
  if (collapseButton) {
    collapseButton.innerHTML = icon(terminalCollapsed ? "expand" : "collapse");
    collapseButton.setAttribute("aria-label", terminalCollapsed ? "expand" : "collapse");
    collapseButton.dataset.tooltip = terminalCollapsed ? "Развернуть окно команд" : "Свернуть окно команд";
  }
  output.innerHTML = (terminalLogs.get(tunnelId) ?? [])
    .map((line) => `<pre>${escapeHtml(line || " ")}</pre>`)
    .join("");
  if (active && !terminalCollapsed) {
    output.scrollTop = output.scrollHeight;
    window.setTimeout(() => app.querySelector<HTMLInputElement>(".terminal-form input")?.focus(), 0);
  }
}

function activeTerminalTunnelId(): string {
  if (terminalOpenId && (remoteAccess.has(terminalOpenId) || remoteEnabled.has(terminalOpenId) || terminalLogs.has(terminalOpenId))) {
    return terminalOpenId;
  }
  if (selectedId && isAgentTunnelId(selectedId)) {
    terminalOpenId = selectedId;
    return selectedId;
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
  void tunnelId;
  void terminal;
}

function appendTerminalLine(tunnelId: string, line: string): void {
  const next = [...(terminalLogs.get(tunnelId) ?? []), line].slice(-600);
  terminalLogs.set(tunnelId, next);
}

function appendTerminalExitLine(tunnelId: string, exitCode: number): void {
  if (exitCode !== 0) {
    appendTerminalLine(tunnelId, `! ${exitCode}`);
  }
}

function setTerminalState(tunnelId: string, state: "idle" | "run" | "ok" | "bad" | "off"): void {
  terminalState.set(tunnelId, state);
}

function activeChessTunnelId(): string {
  return chessOpenId && chessOpenId === selectedId ? chessOpenId : "";
}

async function openChessForSelected(): Promise<void> {
  if (!selectedId) {
    return;
  }
  if (miniAppSession) {
    collapseMiniApp();
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
  if (!isAgentSourceCompanionReady(localAgent, device.id)) {
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

function processLocalAgentDataPlaneOutput(tunnelId: string, commandId: string, rawText: string, flush = false): string {
  if (!rawText && !flush) {
    return "";
  }
  const previous = sotyFileLineBuffers.get(commandId) || "";
  const combined = `${previous}${rawText || ""}`;
  const lines = combined.split("\n");
  const tail = flush ? "" : lines.pop() ?? "";
  if (tail) {
    sotyFileLineBuffers.set(commandId, tail.slice(0, 900_000));
  } else {
    sotyFileLineBuffers.delete(commandId);
  }
  const visible: string[] = [];
  for (const rawLine of lines) {
    const line = rawLine.replace(/\r$/u, "");
    if (handleSotyFileProtocolLine(tunnelId, commandId, line)) {
      continue;
    }
    visible.push(line);
  }
  if (flush && tail && !handleSotyFileProtocolLine(tunnelId, commandId, tail.replace(/\r$/u, ""))) {
    visible.push(tail);
  }
  if (visible.length === 0) {
    return "";
  }
  return `${visible.join("\n")}${rawText.endsWith("\n") || flush ? "\n" : ""}`;
}

function handleSotyFileProtocolLine(tunnelId: string, commandId: string, line: string): boolean {
  if (!line.startsWith("SOTY_FILE_")) {
    return false;
  }
  if (line.startsWith("SOTY_FILE_BEGIN ")) {
    const meta = parseSotyFileMetadata(line.slice("SOTY_FILE_BEGIN ".length));
    if (!meta) {
      appendTerminalLine(tunnelId, "! file transfer metadata");
      return true;
    }
    sotyFileStreams.set(meta.fileId, {
      tunnelId,
      commandId,
      fileId: meta.fileId,
      name: meta.name,
      type: meta.type,
      size: meta.size,
      total: meta.total,
      autoDownload: meta.autoDownload,
      delivery: meta.delivery,
      sourceCommandId: commandId,
      sent: 0
    });
    appendTerminalLine(tunnelId, `+ file ${meta.name} ${formatFileSize(meta.size)}`);
    return true;
  }
  if (line.startsWith("SOTY_FILE_CHUNK ")) {
    const match = /^SOTY_FILE_CHUNK\s+([A-Za-z0-9_-]{1,120})\s+(\d{1,8})\s+([+/=0-9A-Za-z]+)$/u.exec(line);
    if (!match) {
      appendTerminalLine(tunnelId, "! file transfer chunk");
      return true;
    }
    const fileId = match[1] || "";
    const index = Number.parseInt(match[2] || "0", 10);
    const state = sotyFileStreams.get(fileId);
    const sync = syncs.get(tunnelId);
    if (!state || !sync || !Number.isSafeInteger(index)) {
      appendTerminalLine(tunnelId, "! file transfer state");
      return true;
    }
    try {
      const chunk = base64ToBytes(match[3] || "");
      void sync.sendFileChunkFromBytes(fileId, {
        name: state.name,
        type: state.type,
        size: state.size,
        autoDownload: state.autoDownload,
        delivery: state.delivery,
        commandId: state.sourceCommandId
      }, chunk, index, state.total).catch(() => {
        appendTerminalLine(tunnelId, "! file transfer send");
        renderTerminal();
      });
      state.sent += 1;
    } catch {
      appendTerminalLine(tunnelId, "! file transfer decode");
    }
    return true;
  }
  if (line.startsWith("SOTY_FILE_END ")) {
    const payload = parseSotyFileEnd(line.slice("SOTY_FILE_END ".length));
    const state = payload?.fileId ? sotyFileStreams.get(payload.fileId) : null;
    if (state) {
      appendTerminalLine(tunnelId, `+ file ready ${state.name}`);
      sotyFileStreams.delete(state.fileId);
    }
    return true;
  }
  return true;
}

function parseSotyFileMetadata(value: string): SotyFileStreamState | null {
  try {
    const parsed = JSON.parse(new TextDecoder().decode(base64ToBytes(value.trim()))) as {
      readonly id?: unknown;
      readonly name?: unknown;
      readonly type?: unknown;
      readonly size?: unknown;
      readonly total?: unknown;
      readonly autoDownload?: unknown;
      readonly delivery?: unknown;
    };
    const fileId = String(parsed.id || "").replace(/[^A-Za-z0-9_-]/gu, "_").slice(0, 120);
    const name = cleanDownloadedFileName(String(parsed.name || "file"));
    const type = String(parsed.type || "application/octet-stream").slice(0, 160);
    const size = Number.isSafeInteger(parsed.size) ? Math.max(0, Number(parsed.size)) : 0;
    const total = Number.isSafeInteger(parsed.total) ? Math.max(1, Math.min(Number(parsed.total), 8192)) : 1;
    if (!fileId || !name) {
      return null;
    }
    const delivery = String(parsed.delivery || "").slice(0, 80);
    return {
      tunnelId: "",
      commandId: "",
      fileId,
      name,
      type,
      size,
      total,
      autoDownload: parsed.autoDownload === true,
      delivery,
      sourceCommandId: "",
      sent: 0
    };
  } catch {
    return null;
  }
}

function parseSotyFileEnd(value: string): { readonly fileId: string; readonly sha256?: string } | null {
  try {
    const parsed = JSON.parse(new TextDecoder().decode(base64ToBytes(value.trim()))) as {
      readonly id?: unknown;
      readonly sha256?: unknown;
    };
    const fileId = String(parsed.id || "").replace(/[^A-Za-z0-9_-]/gu, "_").slice(0, 120);
    if (!fileId) {
      return null;
    }
    const sha256 = typeof parsed.sha256 === "string" && /^[a-f0-9]{64}$/iu.test(parsed.sha256)
      ? parsed.sha256.toLowerCase()
      : undefined;
    return { fileId, ...(sha256 ? { sha256 } : {}) };
  } catch {
    return null;
  }
}

function base64ToBytes(value: string): Uint8Array {
  const binary = atob(value.replace(/-/gu, "+").replace(/_/gu, "/"));
  const bytes = new Uint8Array(binary.length);
  for (let index = 0; index < binary.length; index += 1) {
    bytes[index] = binary.charCodeAt(index);
  }
  return bytes;
}

function cleanDownloadedFileName(value: string): string {
  return value.replace(/[\\/:*?"<>|]/gu, "_").slice(0, 120) || "file";
}

function cleanupSotyFileDataPlane(commandId: string): void {
  sotyFileLineBuffers.delete(commandId);
  for (const [fileId, state] of [...sotyFileStreams.entries()]) {
    if (state.commandId === commandId) {
      sotyFileStreams.delete(fileId);
    }
  }
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
    cleanupSotyFileDataPlane(command.id);
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
      cleanupSotyFileDataPlane(command.id);
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
      cleanupSotyFileDataPlane(command.id);
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
    const rawText = typeof message.text === "string" ? message.text : "";
    const exitCode = typeof message.exitCode === "number" ? message.exitCode : undefined;
    const text = processLocalAgentDataPlaneOutput(tunnelId, command.id, rawText, typeof exitCode === "number");
    if (text.trim()) {
      appendTerminalLine(tunnelId, text);
    }
    if (typeof exitCode === "number") {
      finished = true;
      window.clearTimeout(watchdogTimer);
      cleanupSotyFileDataPlane(command.id);
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
      cleanupSotyFileDataPlane(command.id);
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
    cleanupSotyFileDataPlane(script.id);
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
      cleanupSotyFileDataPlane(script.id);
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
      cleanupSotyFileDataPlane(script.id);
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
    const rawText = typeof message.text === "string" ? message.text : "";
    const exitCode = typeof message.exitCode === "number" ? message.exitCode : undefined;
    const text = processLocalAgentDataPlaneOutput(tunnelId, script.id, rawText, typeof exitCode === "number");
    if (text.trim()) {
      appendTerminalLine(tunnelId, text);
    }
    if (typeof exitCode === "number") {
      finished = true;
      window.clearTimeout(watchdogTimer);
      cleanupSotyFileDataPlane(script.id);
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
      cleanupSotyFileDataPlane(script.id);
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
  if (agentThinking.has(tunnelId)) {
    stopAgentDialogReply(tunnelId);
    return;
  }
  primeAgentDoneSound();
  const draft = composer.value || localDrafts.get(tunnelId) || "";
  const message = normalizeChatMessage(draft);
  const pendingCount = pendingAttachments.get(tunnelId)?.length ?? 0;
  if (!message && pendingCount === 0) {
    if (draft) {
      composer.value = "";
      rememberComposerDraft();
    }
    return;
  }
  const sentFiles = await sendPendingAttachments(tunnelId, sync);
  if (!message && sentFiles.length === 0) {
    renderComposerAttachments();
    return;
  }
  const current = texts.get(tunnelId) ?? textarea.value;
  const separator = current.length > 0 && !current.endsWith("\n") ? "\n" : "";
  const bundleLine = sentFiles.length > 0 ? fileBundleLine(sentFiles) : "";
  const visibleMessage = [message, bundleLine].filter(Boolean).join("\n");
  const next = `${current}${separator}${visibleMessage}\n`;
  textarea.value = next;
  texts.set(tunnelId, next);
  sync.setText(next);
  saveTextSnapshotNow(tunnelId, next);
  const pendingLiveDraftTimer = liveDraftSendTimers.get(tunnelId);
  if (pendingLiveDraftTimer) {
    window.clearTimeout(pendingLiveDraftTimer);
    liveDraftSendTimers.delete(tunnelId);
  }
  void sync.sendLiveDraft("");
  const agentMessage = messageWithAttachmentContext(message, sentFiles);
  if (tunnel && isAgentTunnel(tunnel)) {
    await prepareAgentSourceForDialog(tunnelId, tunnel);
    void sendAgentDialogMessage(tunnelId, agentMessage);
  } else if (tunnel && containsAgentInvocation(message)) {
    void sendAgentDialogMessage(tunnelId, agentMessage, { explicitMention: true });
  }
  localDrafts.delete(tunnelId);
  composer.value = "";
  touchSelected();
  resizeComposer();
  renderTiles();
  renderComposerAttachments();
  renderTextPaint();
  renderWriterPop();
}

function fileBundleLine(sentFiles: readonly ReceivedFile[]): string {
  const bundle: FileBundleMarker = {
    id: `bundle_${crypto.randomUUID()}`,
    files: sentFiles.map(fileBundleAttachment)
  };
  return `${fileBundlePrefix}${JSON.stringify(bundle)}`;
}

function fileBundleAttachment(file: ReceivedFile): FileBundleAttachment {
  return {
    id: file.id,
    name: file.name || "file",
    type: file.type || "application/octet-stream",
    size: Math.max(0, Math.trunc(file.size || file.bytes.byteLength || 0))
  };
}

function messageWithAttachmentContext(message: string, sentFiles: readonly ReceivedFile[]): string {
  if (sentFiles.length === 0) {
    return message;
  }
  const summary = sentFiles
    .map((file) => `- ${file.name || "file"} (${formatFileSize(file.size || file.bytes.byteLength || 0)}, sotyFileId=${file.id})`)
    .join("\n");
  return [
    message || "Files attached.",
    "",
    "Attached files in this Soty message:",
    summary,
    "",
    "Use the room-file-transfer / artifact route to inspect or move these files when needed."
  ].join("\n");
}

function containsAgentInvocation(text: string): boolean {
  return /(^|[^\p{L}\p{N}_])(?:\u043b\u043e\u0440\u0434|lord)(?=$|[^\p{L}\p{N}_])/iu.test(text);
}

function stripAgentInvocation(text: string): string {
  const body = normalizeChatMessage(text);
  const stripped = body.replace(/^\s*(?:\u043b\u043e\u0440\u0434|lord)\s*[,.:;!?-]*\s*/iu, "").trim();
  return stripped || body;
}

function stopAgentDialogReply(tunnelId: string): void {
  const controller = agentReplyControllers.get(tunnelId);
  if (!controller) {
    clearPendingAgentRelayRepliesForTunnel(tunnelId);
    setAgentThinking(tunnelId, false);
    return;
  }
  controller.abort();
  agentReplyControllers.delete(tunnelId);
  clearPendingAgentRelayRepliesForTunnel(tunnelId);
  appendTerminalLine(tunnelId, "! agent stop requested");
  setAgentThinking(tunnelId, false);
}

function primeAgentDoneSound(): void {
  const context = ensureAgentDoneAudio();
  if (!context) {
    return;
  }
  void context.resume().catch(() => undefined);
}

function playAgentDoneSound(): void {
  const context = ensureAgentDoneAudio();
  if (!context) {
    return;
  }
  void context.resume().catch(() => undefined);
  const now = context.currentTime + 0.015;
  playAgentDoneTone(context, now, 740, 0.16);
  playAgentDoneTone(context, now + 0.18, 520, 0.22);
}

function playAgentDoneTone(context: AudioContext, start: number, frequency: number, duration: number): void {
  const oscillator = context.createOscillator();
  const gain = context.createGain();
  oscillator.type = "triangle";
  oscillator.frequency.setValueAtTime(frequency, start);
  oscillator.frequency.exponentialRampToValueAtTime(Math.max(80, frequency * 0.82), start + duration);
  gain.gain.setValueAtTime(0.0001, start);
  gain.gain.exponentialRampToValueAtTime(0.085, start + 0.025);
  gain.gain.exponentialRampToValueAtTime(0.0001, start + duration);
  oscillator.connect(gain);
  gain.connect(context.destination);
  oscillator.start(start);
  oscillator.stop(start + duration + 0.03);
}

function ensureAgentDoneAudio(): AudioContext | null {
  if (agentDoneAudio) {
    return agentDoneAudio;
  }
  const ctor = window.AudioContext
    || (window as unknown as { readonly webkitAudioContext?: typeof AudioContext }).webkitAudioContext;
  if (!ctor) {
    return null;
  }
  try {
    agentDoneAudio = new ctor();
  } catch {
    return null;
  }
  return agentDoneAudio;
}

function restorePendingAgentDialogSelection(): void {
  const pending = loadPendingAgentRelayReplies()
    .find((reply) => loadTunnels().some((tunnel) => tunnel.id === reply.tunnelId && isAgentReplyTunnel(tunnel)));
  if (!pending) {
    return;
  }
  selectedId = pending.tunnelId;
  saveSelectedTunnelId(pending.tunnelId);
}

function resumePendingAgentDialogReplies(): void {
  for (const pending of loadPendingAgentRelayReplies()) {
    const tunnel = loadTunnels().find((item) => item.id === pending.tunnelId);
    if (!tunnel || !isAgentReplyTunnel(tunnel)) {
      clearPendingAgentRelayReply(pending.id);
      continue;
    }
    if (agentReplyQueues.has(pending.tunnelId)) {
      continue;
    }
    const next = resumeAgentDialogReply(pending);
    agentReplyQueues.set(pending.tunnelId, next);
    void next.finally(() => {
      if (agentReplyQueues.get(pending.tunnelId) === next) {
        agentReplyQueues.delete(pending.tunnelId);
      }
    });
  }
}

function isAgentReplyTunnel(tunnel: TunnelRecord): boolean {
  return isAgentTunnel(tunnel) || remoteAccess.has(tunnel.id);
}

async function resumeAgentDialogReply(pending: LocalAgentPendingRelayReply): Promise<void> {
  const tunnelId = pending.tunnelId;
  const controller = new AbortController();
  agentReplyControllers.set(tunnelId, controller);
  setAgentThinking(tunnelId, true);
  appendTerminalLine(tunnelId, "+ agent reply resumed");
  const streamedMessages = pending.messages
    .map((message) => normalizeChatMessage(cleanAgentReplyText(message)))
    .filter(Boolean);
  try {
    const reply = await resumeAgentRelayReply(pending, (message) => {
      const streamed = normalizeChatMessage(cleanAgentReplyText(message));
      if (!streamed || streamedMessages[streamedMessages.length - 1] === streamed) {
        return;
      }
      streamedMessages.push(streamed);
      appendAgentChatMessage(tunnelId, streamed);
    }, undefined, controller.signal);
    if (!controller.signal.aborted) {
      finishAgentDialogReply(tunnelId, reply, streamedMessages);
    }
  } finally {
    if (agentReplyControllers.get(tunnelId) === controller) {
      agentReplyControllers.delete(tunnelId);
    }
    setAgentThinking(tunnelId, false);
  }
}

function sendAgentDialogMessage(
  tunnelId: string,
  text: string,
  options: { readonly explicitMention?: boolean } = {}
): Promise<LocalAgentReply | null> {
  const tunnel = loadTunnels().find((item) => item.id === tunnelId);
  const agentTunnel = tunnel ? isAgentTunnel(tunnel) : false;
  if (!tunnel || (!agentTunnel && options.explicitMention !== true) || !text.trim()) {
    return Promise.resolve(null);
  }
  const taskText = options.explicitMention === true ? stripAgentInvocation(text) : text;
  const context = cleanAgentContext(texts.get(tunnelId) || "").slice(-16_000);
  const previous = agentReplyQueues.get(tunnelId) ?? Promise.resolve();
  const next = previous
    .catch(() => undefined)
    .then(async () => {
      const controller = new AbortController();
      agentReplyControllers.set(tunnelId, controller);
      setAgentThinking(tunnelId, true);
      let reply: LocalAgentReply;
      const streamedMessages: string[] = [];
      try {
        if (agentTunnel) {
          await prepareAgentSourceForDialog(tunnelId, tunnel);
        } else if (options.explicitMention === true) {
          await preparePeerAgentInvocation(tunnelId);
        }
        const source = agentRequestSourceForTunnel(tunnelId, tunnel, agentTunnel);
        reply = await askLocalAgentReply(taskText, context, source, 2 * 60 * 60_000, (message) => {
          const streamed = normalizeChatMessage(cleanAgentReplyText(message));
          if (!streamed || streamedMessages[streamedMessages.length - 1] === streamed) {
            return;
          }
          streamedMessages.push(streamed);
          appendAgentChatMessage(tunnelId, streamed);
        }, undefined, controller.signal);
      } finally {
        if (agentReplyControllers.get(tunnelId) === controller) {
          agentReplyControllers.delete(tunnelId);
        }
        setAgentThinking(tunnelId, false);
      }
      if (controller.signal.aborted) {
        return {
          ok: false,
          text: "! cancelled",
          exitCode: 130
        };
      }
      finishAgentDialogReply(tunnelId, reply, streamedMessages);
      return reply;
    });
  agentReplyQueues.set(tunnelId, next);
  void next.finally(() => {
    if (agentReplyQueues.get(tunnelId) === next) {
      agentReplyQueues.delete(tunnelId);
    }
  });
  return next;
}

function agentRequestSourceForTunnel(tunnelId: string, tunnel: TunnelRecord, agentTunnel: boolean): LocalAgentRequestSource {
  const targets = operatorTargets();
  const deviceNetwork = agentDeviceNetworkContext(tunnelId, tunnel, targets);
  const preferredTarget = agentTunnel
    ? null
    : linkedOperatorTargetForTunnel(tunnelId, targets);
  return {
    tunnelId,
    tunnelLabel: counterpartyLabel(tunnel),
    deviceId: device?.id || "",
    deviceNick: device?.nick || "",
    localAgent,
    appOrigin: window.location.origin,
    preferredTargetId: preferredTarget?.id || "",
    preferredTargetLabel: preferredTarget?.label || "",
    operatorTargets: targets,
    deviceNetwork
  };
}

async function prepareAgentSourceForDialog(tunnelId: string, tunnel: TunnelRecord): Promise<void> {
  if (!device || !isAgentTunnel(tunnel)) {
    return;
  }
  localAgent = await ensureAgentSourceCompanion();
  if (!isAgentSourceCompanionReady(localAgent, device.id)) {
    renderTiles();
    renderTerminal();
    publishOperatorTargets();
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
  await grantAgentSourceAccess(device.id, device.nick, true, agentSourceClientState(), 2500).catch(() => false);
  startAgentSourceControl(tunnelId);
  publishOperatorTargets();
}

async function preparePeerAgentInvocation(tunnelId: string): Promise<void> {
  if (!device) {
    return;
  }
  const tunnel = loadTunnels().find((item) => item.id === tunnelId);
  if (!tunnel || isAgentTunnel(tunnel) || !remoteAccess.has(tunnelId)) {
    return;
  }
  if (!terminalState.has(tunnelId)) {
    setTerminalState(tunnelId, "idle");
  }
  ensureSync(tunnel);
  await ensureOperatorBridge();
  publishOperatorTargets();
}

function finishAgentDialogReply(
  tunnelId: string,
  reply: LocalAgentReply,
  streamedMessages: readonly string[]
): void {
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
  if (shouldOfferAgentInstall(reply)) {
    markAgentDownloadNeeded();
  }
  const appended = appendAgentReplyMessages(tunnelId, finalReply, body);
  if (!appended && !reply.ok && body) {
    appendAgentChatMessage(tunnelId, userVisibleAgentFailureText(body));
    appendTerminalLine(tunnelId, `! codex bridge: ${body}`);
  }
  playAgentDoneSound();
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
  saveTextSnapshotNow(tunnelId, next);
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

function setAgentThinking(tunnelId: string, active: boolean): void {
  if (active) {
    agentThinking.add(tunnelId);
  } else {
    agentThinking.delete(tunnelId);
  }
  if (tunnelId === selectedId) {
    renderDialogChrome();
    renderTextPaint();
    renderWriterPop();
  }
}

function cleanTerminalTranscript(value: string): string {
  return redactVisibleTerminalSecrets(value)
    .replace(/\r\n?/gu, "\n")
    .replace(/[\u0000-\u0008\u000B\u000C\u000E-\u001F\u007F]/gu, "")
    .replace(/\n{5,}/gu, "\n\n\n\n")
    .trim()
    .slice(0, 12_000);
}

function redactVisibleTerminalSecrets(value: string): string {
  return String(value || "")
    .replace(/\b((?:https?|socks5h?|socks5):\/\/)([^:@\s/]+):([^@\s/]+)@/giu, "$1<redacted>@")
    .replace(/\b(SOTY_CODEX_PROXY_URL|SOTY_AGENT_PROXY_URL|HTTPS?_PROXY|ALL_PROXY|https?_proxy|all_proxy)\s*[:=]\s*['"]?[^'"\s]+/gu, "$1=<redacted>")
    .replace(/(api[_-]?key|authorization|bearer|token|secret|password|passwd|cap_sid)\s*[:=]\s*['"]?[^'"\s]+/giu, "$1=<redacted>")
    .replace(/\b(?:sk|sess|cap|pat|ghp|github_pat)_[A-Za-z0-9_-]{16,}\b/gu, "<redacted-token>")
    .replace(/[A-Za-z0-9+/]{80,}={0,2}/gu, "<redacted-long-token>");
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
  if (/(?:уш[её]л[ао]?\s+в\s+таймаут|таймаут.*статус(?:\s+задания)?|сниму\s+статус\s+этого\s+же\s+задания|сниму\s+статус\s+задания|повторяю\s+через\s+(?:shell|script|desktop)|fallback|route\s+timeout)/iu.test(text)) {
    return true;
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
  renderComposerAttachments();
  renderTextPaint();
  renderWriterPop();
  restoreSelectedChatScroll();
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
  scheduleTextSnapshot(tunnelId, next);
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
  scheduleTextSnapshot(tunnelId, next);
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
  const text = textarea.value;
  const lines = text.endsWith("\n") ? text.slice(0, -1).split("\n") : text.split("\n");
  const active = activeActivities.get(selectedId);
  const activeLine = active ? lineFromIndex(text, active.index) : -1;
  const drafts = liveDraftsForSelected();
  const hasAgentThinking = agentThinking.has(selectedId);
  textPaint.style.transform = "";
  if (!text.trim() && drafts.length === 0 && !hasAgentThinking) {
    textPaint.innerHTML = `
      <div class="chat-empty">
        <span>READY</span>
        <b>${escapeHtml(counterpartyLabelForSelected())}</b>
      </div>
    `;
    return;
  }
  let operatorBlock = false;
  let operatorBlockNick = "";
  const bubbles: {
    key: string;
    side: string;
    nick: string;
    color: string;
    time: string;
    className: string;
    lines: string[];
    attachments: FileBundleMarker[];
    live: WriterActivity | null;
  }[] = [];
  for (let index = 0; index < lines.length; index += 1) {
    const line = lines[index] ?? "";
    const fileBundle = parseFileBundleLine(line);
    if (fileBundle) {
      const current = bubbles[bubbles.length - 1];
      if (current) {
        current.attachments.push(fileBundle);
      } else {
        bubbles.push({
          key: `files:${fileBundle.id}`,
          side: "surface",
          nick: counterpartyLabelForSelected(),
          color: safeColor(undefined, `${selectedId}:files`),
          time: clock(),
          className: "is-file-bundle",
          lines: [],
          attachments: [fileBundle],
          live: null
        });
      }
      continue;
    }
    const label = labels.get(index);
    let state = classifyChatLine(line, operatorBlock);
    const operatorNick = operatorNameFromLine(line);
    if (operatorNick) {
      operatorBlockNick = operatorNick;
    }
    if (label && label.deviceId !== "operator" && !isAgentChromeLineClass(state.className)) {
      state = { className: "is-user-line", operatorBlock: false };
    }
    operatorBlock = state.operatorBlock;
    if (isAgentChromeLineClass(state.className)) {
      if (!operatorBlock) {
        operatorBlockNick = "";
      }
      continue;
    }
    if (!line.trim()) {
      operatorBlock = false;
      operatorBlockNick = "";
      if (bubbles.length > 0) {
        bubbles[bubbles.length - 1]?.lines.push("");
      }
      continue;
    }
    const speaker = speakerForLine(line, state.className, label, operatorBlockNick);
    if (!operatorBlock) {
      operatorBlockNick = "";
    }
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
      attachments: [],
      live
    });
  }
  if (hasAgentThinking) {
    bubbles.push({
      key: "agent-thinking",
      side: "remote",
      nick: agentDialogLabel,
      color: colorFor(`agent-thinking:${selectedId}`),
      time: clock(),
      className: "is-agent-thinking",
      lines: ["думаю"],
      attachments: [],
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
      attachments: [],
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
  const referencedFiles = new Set(bubbles.flatMap((bubble) => bubble.attachments.flatMap((bundle) => bundle.files.map((file) => file.id))));
  const looseFiles = (files.get(selectedId) ?? []).filter((file) => !referencedFiles.has(file.id));
  if (looseFiles.length > 0) {
    bubbles.push({
      key: "loose-files",
      side: "surface",
      nick: counterpartyLabelForSelected(),
      color: safeColor(undefined, `${selectedId}:files`),
      time: clock(),
      className: "is-file-bundle",
      lines: [],
      attachments: [{ id: "loose-files", files: looseFiles.map(fileBundleAttachment) }],
      live: null
    });
  }
  const visibleBubbles = bubbles.filter((bubble) =>
    bubble.className === "is-agent-thinking" || bubble.live || bubble.attachments.length > 0 || bubble.lines.some((line) => line.trim())
  );
  textPaint.innerHTML = visibleBubbles.map((bubble) => {
    const body = bubble.className === "is-agent-thinking"
      ? `<span class="thinking-label">${escapeHtml(bubble.lines[0] || "думаю")}</span><span class="thinking-rig" aria-hidden="true"><i></i><i></i><i></i><i></i><i></i></span>`
      : bubble.lines
        .map((line) => line ? `<span>${escapeHtml(line)}</span>` : "<br>")
        .join("");
    const attachmentHtml = bubble.attachments.length > 0 ? renderBubbleAttachments(bubble.attachments) : "";
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
        ${body ? `<p>${body}</p>` : ""}
        ${attachmentHtml}
      </article>
    `;
  }).join("");
  installBubbleAttachmentDownloads();
  if (scroll && stickToBottom) {
    window.setTimeout(() => {
      scroll.scrollTop = scroll.scrollHeight;
    }, 0);
  }
}

function parseFileBundleLine(line: string): FileBundleMarker | null {
  if (!line.startsWith(fileBundlePrefix)) {
    return null;
  }
  try {
    const payload = JSON.parse(line.slice(fileBundlePrefix.length)) as unknown;
    if (!isRecord(payload)) {
      return null;
    }
    const id = recordString(payload, "id").slice(0, 120) || "bundle";
    const rawFiles = Array.isArray(payload.files) ? payload.files : [];
    const bundleFiles = rawFiles
      .map((item) => isRecord(item) ? {
        id: recordString(item, "id").slice(0, 140),
        name: cleanDownloadedFileName(recordString(item, "name") || "file"),
        type: recordString(item, "type").slice(0, 160) || "application/octet-stream",
        size: Math.max(0, Math.trunc(Number(item.size) || 0))
      } : null)
      .filter((item): item is FileBundleAttachment => Boolean(item?.id));
    return bundleFiles.length > 0 ? { id, files: bundleFiles } : null;
  } catch {
    return null;
  }
}

function renderBubbleAttachments(bundles: readonly FileBundleMarker[]): string {
  const known = new Map((files.get(selectedId) ?? []).map((file) => [file.id, file]));
  const items = bundles.flatMap((bundle) => bundle.files);
  if (items.length === 0) {
    return "";
  }
  return `
    <div class="bubble-files">
      ${items.map((item) => {
        const file = known.get(item.id);
        const ready = Boolean(file);
        return `
          <button class="bubble-file" type="button" data-file-id="${escapeHtml(item.id)}" ${ready ? "" : "disabled"} data-tooltip="${ready ? "Download file" : "Waiting for file data"}">
            <span>${icon(ready ? "download" : "clip")}</span>
            <b>${escapeHtml(item.name)}</b>
            <small>${escapeHtml(formatFileSize(item.size))}</small>
          </button>
        `;
      }).join("")}
    </div>
  `;
}

function installBubbleAttachmentDownloads(): void {
  textPaint?.querySelectorAll<HTMLButtonElement>(".bubble-file[data-file-id]").forEach((button) => {
    button.addEventListener("click", (event) => {
      event.preventDefault();
      event.stopPropagation();
      const fileId = button.dataset.fileId || "";
      const file = (files.get(selectedId) ?? []).find((item) => item.id === fileId);
      if (file) {
        downloadReceivedFile(file);
      }
    });
  });
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
  },
  operatorBlockNick = ""
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
    const nick = operator || label?.nick || cleanNick(operatorBlockNick) || (isAgentTunnelId(selectedId) ? agentDialogLabel : "Operator");
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
    .map(agentContextLine)
    .filter((line) => !isAgentContextChromeLine(line))
    .join("\n")
    .replace(/\n{3,}/gu, "\n\n")
    .trim();
}

function agentContextLine(line: string): string {
  const bundle = parseFileBundleLine(line);
  if (!bundle) {
    return line;
  }
  const names = bundle.files
    .map((file) => `${file.name} (${formatFileSize(file.size)}, sotyFileId=${file.id})`)
    .join("; ");
  return `Attached files: ${names}`;
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

function loadTerminalCollapsed(): boolean {
  try {
    return localStorage.getItem(terminalCollapsedKey) === "1";
  } catch {
    return false;
  }
}

function saveTerminalCollapsed(value: boolean): void {
  try {
    localStorage.setItem(terminalCollapsedKey, value ? "1" : "0");
  } catch {
    // Ignore storage failures; the current in-memory setting still applies.
  }
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
