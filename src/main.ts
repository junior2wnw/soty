import QRCode from "qrcode";
import jsQR from "jsqr";
import {
  appSurfaceAllowedOrigin,
  appSurfaceInstallSchema,
  normalizeAppSurfaceInstallRequest,
  stableJson,
  toBase64Url,
  utf8
} from "trustlink-kernel";
import { JoinRequest, LiveDraft, NoticeKnock, PeerInfo, ReceivedFile, RemoteCancel, RemoteCommand, RemoteGrant, RemoteOutput, RemoteRequest, RemoteScript, SyncedChessState, SyncedMiniApp, SyncedWriterLine, TerminalSnapshot, TunnelSync, WriterActivity } from "./sync";
import { icon } from "./icons";
import { fetchFrontendQuickActions, mergeQuickActions, quickActions } from "./features/quick-actions";
import type { QuickAction } from "./features/quick-actions";
import { copyText, showLinkShareSheet } from "./features/share-sheet";
import { dedupeMiniApps, miniAppDefaultHeight, miniAppDefaultWidth, miniAppLayouts, miniAppRecordKey, normalizeMiniAppLayout, normalizeMiniAppScope, safeMiniAppCssSize, sameMiniAppRecord, sanitizeLocalMiniAppDefinition, sanitizeMiniAppDefinition, searchMiniApps } from "./features/mini-apps";
import type { FileBundleAttachment, FileBundleMarker, MiniAppDefinition, MiniAppInstallResult, MiniAppSession, MiniAppVisibility, MiniAppWindowLayout, PendingAttachment } from "./features/mini-apps";
import { commonMessageDialogTarget, createMessageDialogLine, isMessageDialogLine, messageDialogVisibleForTarget, parseMessageDialogLine } from "./features/message-dialogs";
import type { MessageDialogEntry, MessageDialogTarget } from "./features/message-dialogs";
import { colorFor, safeColor } from "./core/color";
import { clock } from "./core/time";
import { adoptAgentRelayFromUrl, askLocalAgentReply, bindLocalAgentRelay, checkAgentSourceMachineAgent, checkAgentSourceWorker, checkLocalAgent, checkLocalCompanionAgent, clearPendingAgentRelayReply, clearPendingAgentRelayRepliesForTunnel, downloadAgentInstallerForDevice, grantAgentSourceAccess, hasAgentRelayId, loadPendingAgentRelayReplies, resumeAgentRelayReply } from "./features/agent";
import type { LocalAgentDeviceNetwork, LocalAgentOperatorTarget, LocalAgentPendingRelayReply, LocalAgentReply, LocalAgentRequestSource, LocalAgentStatus } from "./features/agent";
import { agentSide, applyChessMove, boardSquares, buildGeniusLine, chessFromSnapshot, chooseAgentMove, createChessSnapshot, geniusCoach, isAgentTurn, isSquare, legalMovesForSquare, normalizeChessSnapshot, pieceGlyph, promotionChoices, sideName, statusText, withCoach } from "./features/chess";
import type { ChessCoach, ChessMode, ChessSnapshot } from "./features/chess";
import { downloadReceivedFile, filesFrom, formatFileSize, maxFileBytes, oversizedFilesFrom } from "./features/files";
import { bindLegalPage } from "./features/legal";
import { isLocalAgentUnavailableText, localAgentUnavailableText, localAgentWsUrl } from "./features/local-agent-endpoint";
import { clearAttentionNotices, notifyHiddenOnce, requestNotificationPermission, shouldNotifyTyping, shouldOfferNotifications } from "./features/notifications";
import type { AttentionNotice } from "./features/notifications";
import { clearRemoteSessionState, loadRemoteAccess, loadRemoteEnabled, loadRemoteGrantTargets, setRemoteAccess, setRemoteEnabled, setRemoteGrantTarget } from "./features/remote";
import { makeSpaceEntryLine, normalizeSpaceEntryKind, normalizeSpaceMode, parseSpaceEntryLine, renderSpaceEntryBubble, renderSpaceRail, spaceComposerAccess, spaceEmptyPrompt, spaceEntryKindForMessage, spaceMarkDisplay } from "./features/space";
import type { SpaceComposerAccess, SpaceEntry, SpaceEntryKind, SpaceMode, SpaceModel } from "./features/space";
import { infoPageHtml, paymentPageHtml, showAccessPanelModal, showTrustModal } from "./features/trust-ui";
import type { AccessPanelRow } from "./features/trust-ui";
import { createPaymentIntent, formatPaymentAmount, loadPaymentConfig } from "./features/payments";
import type { PaymentConfig, PaymentPlan } from "./features/payments";
import { cleanPersonalHandle, loadPersonalHandle, loadPersonalSpaceInbox, personalSpaceManifestHref, personalSpaceRouteFromLocation, renderPersonalSpacePage, savePersonalHandle, savePersonalSpaceModule, savePersonalSpacePost, updatePersonalSpaceProfile, uploadPersonalSpacePhoto } from "./features/personal-space";
import type { PersonalOwnerAction, PersonalOwnerProof, PersonalSpaceAgentRequest, PersonalSpaceAgentResult, PersonalSpaceInstallResult, PersonalSpaceModuleDraft, PersonalSpacePostDraft, PersonalSpaceProfile, PersonalSpaceProfileUpdate, PersonalSpaceRoute } from "./features/personal-space";
import { runtimeModuleTargetFromString, runtimeModuleUsesEntity } from "./features/runtime-modules";
import type { RuntimeModuleTarget } from "./features/runtime-modules";
import { installWebController, resolveWebControllerTarget } from "./features/web-controller";
import type { WebControllerPending, WebControllerRunRequest, WebControllerRunResult, WebControllerTargetInfo, WebControllerTargetRef } from "./features/web-controller";
import { agentDialogLabel, isOperatorHeaderText } from "./features/agent-identity";
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

type BeforeInstallPromptEvent = Event & {
  readonly userChoice: Promise<{ readonly outcome: "accepted" | "dismissed"; readonly platform: string }>;
  prompt: () => Promise<void>;
};

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

type PersonalOwnerRecord = {
  readonly handle: string;
  readonly deviceId: string;
  readonly publicJwk?: JsonWebKey;
  readonly createdAt: string;
  readonly updatedAt: string;
};

type MiniAppInstallDraft = {
  readonly id: string;
  readonly title: string;
  readonly url: string;
  readonly summary: string;
  readonly placement: string;
};

type LauncherItem = {
  readonly kind: "action" | "install";
  readonly key: string;
  readonly markHtml: string;
  readonly title: string;
  readonly summary: string;
  readonly meta?: string;
};

const root = document.querySelector<HTMLDivElement>("#app");
if (!root) {
  throw new Error("App root missing");
}
const app: HTMLDivElement = root;
installTooltips();

const selfCellLabel = "Я";
const agentReleaseCheckTtlMs = 60_000;
const infoPagePath = "/info";
const paymentPagePath = "/pay";

type AgentButtonMode = "download" | "update" | "link";

type AgentRelease = {
  readonly version: string;
  readonly sha256?: string;
};

let device: DeviceRecord | null = null;
let tunnels: TunnelRecord[] = [];
let selectedId = "";
let firstSurfacePending = true;
let bareChatMode = requestedBareChatMode();
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
let remoteGrantTargets = loadRemoteGrantTargets();
let terminalOpenId = "";
const terminalLogs = new Map<string, string[]>();
const terminalState = new Map<string, "idle" | "run" | "ok" | "bad" | "off">();
const chessStoreKey = "soty:chess:v1";
const terminalCollapsedKey = "soty:terminal-collapsed:v1";
const textSnapshotsKey = "soty:text-snapshots:v1";
const chatScrollKey = "soty:chat-scroll:v1";
const spaceModeKey = "soty:space-mode:v1";
const agentModeKey = "soty:agent-mode:v1";
const agentPrivateLogKey = "soty:agent-private-log:v1";
const hiveDrawerKey = "soty:hive-drawer:v1";
const autoDownloadedFilesKey = "soty:auto-downloaded-files:v1";
const miniAppsRegistryKey = "soty:mini-apps:v1";
const miniAppProtocol = "soty.mini-app.v1";
const miniAppContextProtocol = "soty.mini-app.context.v1";
const fileBundlePrefix = "SOTY_FILE_BUNDLE:";
const agentAttachmentLimit = 10;
let miniApps: MiniAppDefinition[] = [];
const roomMiniApps = new Map<string, MiniAppDefinition[]>();
let miniAppSession: MiniAppSession | null = null;
let openMessageDialog: OpenMessageDialog | null = null;
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
let remoteHostSourceGrantTimer = 0;
let remoteHostSourceGrantPolling = false;
let remoteHostSourceGrantRefreshAt = 0;
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

type AgentPrivateLine = {
  readonly role: "user" | "agent";
  readonly text: string;
  readonly createdAt: string;
};

type OpenMessageDialog = {
  readonly chatId: string;
  readonly sourceId: string;
  readonly sourceLine: number;
  readonly sourceText: string;
  readonly sourceAuthor: string;
  readonly target: MessageDialogTarget;
};

const writerLines = new Map<string, Map<number, WriterLine>>();
const activeActivities = new Map<string, WriterActivity>();
const activeActivityTicks = new Map<string, number>();
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
let miniAppGalleryOverlay: HTMLDivElement | null = null;
let actionSearchText = "";
let quickActionCatalog: readonly QuickAction[] = quickActions;
let quickActionCatalogProbe: Promise<readonly QuickAction[]> | null = null;
let quickActionCatalogCheckedAt = 0;
const quickActionCatalogTtlMs = 5 * 60_000;
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
let spaceModes = loadSpaceModes();
let agentModes = loadAgentModes();
let agentPrivateLogs = loadAgentPrivateLogs();
let hiveDrawerOpen = loadHiveDrawerOpen();
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
const webControllerPending = new Map<string, WebControllerPending>();
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
const agentReplyStopTokens = new Map<string, number>();
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
let personalManifestObjectUrl = "";
const serviceWorkerUpdateMs = 60_000;
const appBundleWatchVisibleMs = 45_000;
const appBundleWatchHiddenMs = 90_000;
const appBundlePath = currentAppBundlePath();
const reservedLegacySelfHandles = new Set([".", cleanSelfStartHandle(selfCellLabel), "soty", "соты"]);
const personalOwnerPrefix = "soty:personal-owner:v1:";
let pendingInstallPrompt: BeforeInstallPromptEvent | null = null;

window.addEventListener("beforeinstallprompt", (event) => {
  event.preventDefault();
  pendingInstallPrompt = event as BeforeInstallPromptEvent;
  window.dispatchEvent(new CustomEvent("soty-installpromptchange"));
});

window.addEventListener("message", (event) => {
  handleMiniAppMessage(event);
});

window.addEventListener("storage", (event) => {
  if (event.key === miniAppsRegistryKey) {
    handleMiniAppRegistryChange();
  }
});

window.addEventListener("appinstalled", () => {
  pendingInstallPrompt = null;
  rememberAppRuntime();
  window.dispatchEvent(new CustomEvent("soty-installpromptchange"));
  void boot();
});

window.addEventListener("popstate", () => {
  const route = personalSpaceRouteFromLocation();
  if (route) {
    showPersonalSpaceRoute(route);
    return;
  }
  if (document.body.classList.contains("personal-space-mode")) {
    void boot();
  }
});

window.addEventListener("soty-personal-routechange", () => {
  const route = personalSpaceRouteFromLocation();
  if (route) {
    showPersonalSpaceRoute(route);
  }
});

window.addEventListener("soty-installpromptchange", () => {
  const route = personalSpaceRouteFromLocation();
  if (route && document.body.classList.contains("personal-space-mode")) {
    showPersonalSpaceRoute(route);
  }
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

installWebController({
  targets: webControllerTargets,
  status: webControllerStatus,
  select: webControllerSelect,
  send: webControllerSend,
  cancel: webControllerCancel,
  tail: webControllerTail
});
void boot();

async function boot(): Promise<void> {
  bareChatMode = requestedBareChatMode();
  const personalRoute = personalSpaceRouteFromLocation();
  setPersonalSpaceMode(Boolean(personalRoute));
  setSelfStartMode(false);
  applyPersonalSpaceManifest(null);
  adoptAgentRelayFromUrl();
  startSameDeviceWindowSync();
  void refreshMiniApps(true);
  const infoRoute = isInfoRoute();
  const paymentRoute = isPaymentRoute();

  if (shouldResetLocalState()) {
    await resetLocalSotyState();
    clearRemoteSessionState();
    remoteEnabled = loadRemoteEnabled();
    remoteAccess = loadRemoteAccess();
    remoteGrantTargets = loadRemoteGrantTargets();
    spaceModes = loadSpaceModes();
    agentModes = loadAgentModes();
    agentPrivateLogs = loadAgentPrivateLogs();
    hiveDrawerOpen = loadHiveDrawerOpen();
    localDrafts.clear();
    pendingAttachments.clear();
    terminalOpenId = "";
    chessOpenId = "";
    rememberAppRuntime();
    window.history.replaceState({}, "", bareChatPath());
  }

  await registerServiceWorker();
  startAppBundleWatcher(true);

  if (personalRoute) {
    showPersonalSpaceRoute(personalRoute);
    return;
  }

  const capturedJoin = captureJoinInviteFromLocation();
  if (capturedJoin) {
    window.history.replaceState({}, "", bareChatPath());
  }
  clearPendingInvite();

  if (!isAppRuntime()) {
    rememberAppRuntime();
  }

  if (infoRoute) {
    renderInfoPage();
    return;
  }

  if (paymentRoute) {
    renderPaymentPage();
    return;
  }

  if (isSelfStartRoute()) {
    await renderSelfStartPage();
    return;
  }

  device = await loadDevice();
  if (!device) {
    device = await createDevice(selfCellLabel);
    finishDeviceBoot();
    return;
  }

  const pending = loadPendingJoin();
  if (pending) {
    renderJoinWaiting(pending);
    return;
  }

  tunnels = loadTunnels();
  ensurePermanentCells();
  const requestedContactId = ensureRequestedContactTunnel();
  tunnels = loadTunnels();
  selectedId = requestedChatTunnelId(tunnels) || requestedContactId || loadSelectedTunnelId() || tunnels[0]?.id || "";
  if (selectedId) {
    saveSelectedTunnelId(selectedId);
  }
  renderApp();
  openRequestedRuntimeModule();
  void refreshQuickActionCatalog(true).then(() => renderTerminal());
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
    if (!event.key || event.key === tunnelsKey) {
      scheduleSameDeviceWindowState(event.key || "storage", false);
    }
  });
  window.addEventListener("focus", () => scheduleSameDeviceWindowState("focus", false));
  window.addEventListener("pageshow", () => scheduleSameDeviceWindowState("pageshow", false));
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
  const previousSelected = selectedId;
  const previousSignature = tunnelListSignature(tunnels);
  const nextTunnels = loadTunnels();
  const nextSignature = tunnelListSignature(nextTunnels);
  const storedSelected = loadSelectedTunnelId() || "";
  const currentStillExists = nextTunnels.some((tunnel) => tunnel.id === selectedId);
  tunnels = nextTunnels;
  if ((!currentStillExists || followSelected) && storedSelected && nextTunnels.some((tunnel) => tunnel.id === storedSelected)) {
    selectedId = storedSelected;
  }
  ensurePermanentCells();
  normalizeSelectedTunnel();
  const selectionChanged = selectedId !== previousSelected;
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
  if (previousSignature !== nextSignature || selectionChanged || reason === "focus" || reason === "pageshow") {
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

function requestedBareChatMode(): boolean {
  const url = new URL(window.location.href);
  return url.searchParams.get("bare") === "1" || url.searchParams.get("view") === "chat";
}

function bareChatPath(): string {
  return bareChatMode ? "/?pwa=1&bare=1" : "/?pwa=1";
}

function requestedChatTunnelId(items: readonly TunnelRecord[] = loadTunnels()): string {
  try {
    const raw = new URL(window.location.href).searchParams.get("chat") || "";
    return items.some((item) => item.id === raw) ? raw : "";
  } catch {
    return "";
  }
}

function requestedContactHandle(location: Location = window.location): string {
  try {
    const url = new URL(location.href);
    return cleanContactHandle(url.searchParams.get("to") || url.searchParams.get("contact") || "");
  } catch {
    return "";
  }
}

function requestedSenderHandle(location: Location = window.location): string {
  try {
    return cleanContactHandle(new URL(location.href).searchParams.get("from") || "");
  } catch {
    return "";
  }
}

function cleanContactHandle(value: string): string {
  return cleanSelfStartHandle(value).slice(0, 32);
}

function contactTunnelLabel(handle: string): string {
  return handle ? `@${handle}` : "";
}

function contactHandleFromTunnel(tunnel: TunnelRecord): string {
  return cleanContactHandle(peers.get(tunnel.id) || tunnel.label || "");
}

function publicContactUrlForTunnel(tunnel: TunnelRecord): string {
  if (isPermanentCell(tunnel) || isAgentTunnel(tunnel) || !hasCounterparty(tunnel)) {
    return "";
  }
  const handle = contactHandleFromTunnel(tunnel);
  return handle ? new URL(`/@${encodeURIComponent(handle)}`, window.location.origin).href : "";
}

function ensureRequestedContactTunnel(): string {
  const handle = requestedContactHandle();
  if (!device || !handle) {
    return "";
  }

  const senderHandle = requestedSenderHandle();
  if (senderHandle) {
    saveSelfStartHandle(senderHandle);
  }

  const ownHandle = loadSelfStartHandle();
  if (ownHandle && ownHandle === handle) {
    const self = loadTunnels().find((tunnel) => !tunnel.archived && isSelfTunnel(tunnel));
    if (self) {
      selectedId = self.id;
      saveSelectedTunnelId(self.id);
      consumeRequestedContactParams();
      return self.id;
    }
  }

  const current = loadTunnels();
  const existing = current.find((tunnel) => !isPermanentCell(tunnel) && contactHandleFromTunnel(tunnel) === handle);
  if (existing) {
    const now = new Date().toISOString();
    let selected: TunnelRecord | null = null;
    const next = current.map((tunnel) => {
      if (tunnel.id !== existing.id) {
        return tunnel;
      }
      selected = {
        ...tunnel,
        label: contactTunnelLabel(handle),
        counterparty: true,
        archived: false,
        unread: false,
        color: tunnel.color || colorFor(`contact:${handle}`),
        updatedAt: now,
        lastActionAt: now
      };
      return selected;
    });
    saveTunnels(next);
    tunnels = next;
    selectedId = existing.id;
    saveSelectedTunnelId(existing.id);
    if (selected) {
      ensureSync(selected);
    }
    consumeRequestedContactParams();
    return existing.id;
  }

  const fresh = addCell({
    label: contactTunnelLabel(handle),
    counterparty: true,
    colorSeed: `contact:${handle}`
  }, { select: true });
  if (!fresh) {
    return "";
  }
  consumeRequestedContactParams();
  return fresh.id;
}

function consumeRequestedContactParams(): void {
  try {
    const url = new URL(window.location.href);
    if (!url.searchParams.has("to") && !url.searchParams.has("contact") && !url.searchParams.has("from")) {
      return;
    }
    url.searchParams.delete("to");
    url.searchParams.delete("contact");
    url.searchParams.delete("from");
    window.history.replaceState({}, "", `${url.pathname}${url.search}${url.hash}`);
  } catch {
    // Deep links remain usable even when History API state cannot be updated.
  }
}

function setPersonalSpaceMode(active: boolean): void {
  document.body.classList.toggle("personal-space-mode", active);
}

function setSelfStartMode(active: boolean): void {
  document.body.classList.toggle("self-start-mode", active);
}

function applyPersonalSpaceManifest(route: PersonalSpaceRoute | null): void {
  revokePersonalManifestObjectUrl();
  const manifest = document.querySelector<HTMLLinkElement>('link[rel="manifest"]');
  if (!manifest) {
    return;
  }
  manifest.href = route ? personalSpaceManifestHref(route) : "/manifest.webmanifest";
}

function applyPersonalProfileManifest(profile: PersonalSpaceProfile): void {
  const manifest = document.querySelector<HTMLLinkElement>('link[rel="manifest"]');
  if (!manifest) {
    return;
  }
  try {
    const blob = new Blob([JSON.stringify(personalProfileManifest(profile))], {
      type: "application/manifest+json"
    });
    const nextHref = URL.createObjectURL(blob);
    revokePersonalManifestObjectUrl();
    personalManifestObjectUrl = nextHref;
    manifest.href = nextHref;
  } catch {
    manifest.href = personalSpaceManifestHref({ handle: profile.handle, slug: profile.slug });
  }
}

function revokePersonalManifestObjectUrl(): void {
  if (!personalManifestObjectUrl) {
    return;
  }
  URL.revokeObjectURL(personalManifestObjectUrl);
  personalManifestObjectUrl = "";
}

function personalProfileManifest(profile: PersonalSpaceProfile): Record<string, unknown> {
  const name = cleanManifestText(profile.slug ? profile.displayName : profile.accountName || profile.displayName || profile.handle, 96) || "соты";
  const startUrl = profile.url || (profile.slug ? `/@${profile.handle}/${profile.slug}` : `/@${profile.handle}`);
  const absoluteStartUrl = absoluteManifestUrl(startUrl);
  return {
    name,
    short_name: manifestShortName(name),
    description: cleanManifestText(profile.slug ? profile.displayName : profile.headline || profile.about, 180) || name,
    id: absoluteStartUrl,
    start_url: absoluteStartUrl,
    scope: absoluteManifestUrl("/"),
    display: "standalone",
    launch_handler: {
      client_mode: "navigate-existing"
    },
    background_color: "#cacaca",
    theme_color: "#000000",
    icons: personalManifestIcons(profile)
  };
}

function personalManifestIcons(profile: PersonalSpaceProfile): readonly Record<string, string>[] {
  const photo = cleanManifestIconSrc(profile.photoUrl);
  const src = absoluteManifestUrl(photo || fallbackPersonalIconSrc(profile));
  const type = manifestIconType(src);
  const sizes = photo ? ["192x192", "512x512"] : ["any"];
  return sizes.map((size) => ({
    src,
    sizes: size,
    ...(type ? { type } : {}),
    purpose: "any"
  }));
}

function fallbackPersonalIconSrc(profile: PersonalSpaceProfile): string {
  const handle = encodeURIComponent(profile.handle);
  return profile.slug
    ? `/icon/space/${handle}/${encodeURIComponent(profile.slug)}.svg`
    : `/icon/space/${handle}.svg`;
}

function cleanManifestIconSrc(value: string): string {
  const text = value.trim();
  if (!text || /[<>"']/u.test(text)) {
    return "";
  }
  if (/^(?:\/|https?:\/\/)/iu.test(text)) {
    return text.slice(0, 900_000);
  }
  if (/^data:image\/(?:png|jpe?g|webp);base64,[a-z0-9+/=]+$/iu.test(text)) {
    return text.slice(0, 900_000);
  }
  return "";
}

function manifestIconType(src: string): string {
  const dataMatch = src.match(/^data:(image\/(?:png|jpe?g|webp));base64,/iu);
  if (dataMatch?.[1]) {
    return dataMatch[1].toLowerCase();
  }
  if (/\.svg(?:\?|$)/iu.test(src)) {
    return "image/svg+xml";
  }
  if (/\.webp(?:\?|$)/iu.test(src)) {
    return "image/webp";
  }
  if (/\.(?:jpe?g)(?:\?|$)/iu.test(src) || src.startsWith("/photo/space/")) {
    return "image/jpeg";
  }
  if (/\.png(?:\?|$)/iu.test(src)) {
    return "image/png";
  }
  return "";
}

function absoluteManifestUrl(value: string): string {
  if (value.startsWith("data:")) {
    return value;
  }
  try {
    return new URL(value, window.location.origin).href;
  } catch {
    return new URL("/", window.location.origin).href;
  }
}

function manifestShortName(value: string): string {
  const chars = Array.from(cleanManifestText(value, 96));
  return chars.slice(0, 18).join("") || "соты";
}

function cleanManifestText(value: string, max: number): string {
  return value.replace(/\s+/gu, " ").trim().slice(0, max);
}

function showPersonalSpaceRoute(route: PersonalSpaceRoute): void {
  setSelfStartMode(false);
  setPersonalSpaceMode(true);
  applyPersonalSpaceManifest(null);
  void renderPersonalSpacePage(app, {
    route,
    canInstall: shouldShowPersonalSpaceInstallAction,
    canNotify: shouldOfferNotifications,
    install: promptPersonalSpaceInstall,
    enableNotifications: promptPersonalSpaceNotifications,
    updateProfile: updateSignedPersonalSpaceProfile,
    savePost: saveSignedPersonalSpacePost,
    saveModule: saveSignedPersonalSpaceModule,
    loadInbox: loadSignedPersonalSpaceInbox,
    askAgent: askPersonalSpaceAgent,
    uploadPhoto: uploadSignedPersonalSpacePhoto,
    exportBackup: exportSotyBackup,
    importBackup: importSotyBackupFile,
    applyManifest: applyPersonalProfileManifest,
    isOwned: isPersonalProfileOwned,
    openRuntime: openPersonalSpaceRuntime
  });
}

async function updateSignedPersonalSpaceProfile(route: PersonalSpaceRoute, update: PersonalSpaceProfileUpdate): Promise<PersonalSpaceInstallResult> {
  return updatePersonalSpaceProfile(route, update, await createPersonalOwnerProof(route, "profile", update));
}

async function saveSignedPersonalSpacePost(route: PersonalSpaceRoute, draft: PersonalSpacePostDraft): Promise<PersonalSpaceInstallResult> {
  return savePersonalSpacePost(route, draft, await createPersonalOwnerProof(route, "post", draft));
}

async function saveSignedPersonalSpaceModule(route: PersonalSpaceRoute, draft: PersonalSpaceModuleDraft): Promise<PersonalSpaceInstallResult> {
  return savePersonalSpaceModule(route, draft, await createPersonalOwnerProof(route, "module", draft));
}

async function loadSignedPersonalSpaceInbox(route: PersonalSpaceRoute) {
  const request = { limit: 50 };
  return loadPersonalSpaceInbox(route, request, await createPersonalOwnerProof(route, "messages", request));
}

async function askPersonalSpaceAgent(profile: PersonalSpaceProfile, request: PersonalSpaceAgentRequest): Promise<PersonalSpaceAgentResult> {
  const taskText = personalSpaceAgentTask(profile, request);
  const source = await personalSpaceAgentSource(profile);
  const reply = await askLocalAgentReply(taskText, "", source, 2 * 60 * 60_000);
  const body = personalSpaceAgentReplyText(reply);
  if (!reply.ok) {
    const message = personalSpaceAgentFailureText(body || reply.text);
    return {
      ok: false,
      message,
      reply: message
    };
  }
  return {
    ok: true,
    message: "Готово.",
    reply: body || "Готово."
  };
}

async function personalSpaceAgentSource(profile: PersonalSpaceProfile): Promise<LocalAgentRequestSource> {
  const currentDevice = device ?? await loadDevice().catch(() => null);
  if (!device && currentDevice) {
    device = currentDevice;
  }
  const targets = operatorTargets();
  const deviceNetwork = agentDeviceNetworkContext("", null, targets);
  const label = personalSpaceAgentLabel(profile);
  return {
    tunnelId: `card:${profile.handle}${profile.slug ? `/${profile.slug}` : ""}`,
    tunnelLabel: label,
    deviceId: currentDevice?.id || "",
    deviceNick: currentDevice?.nick || "",
    localAgent,
    appOrigin: window.location.origin,
    operatorTargets: targets,
    deviceNetwork: {
      ...deviceNetwork,
      activeTunnelLabel: label,
      capabilities: [
        ...deviceNetwork.capabilities,
        "info-card-context",
        "card-module-install",
        "external-url-mini-app-modules"
      ]
    }
  };
}

function personalSpaceAgentTask(profile: PersonalSpaceProfile, request: PersonalSpaceAgentRequest): string {
  const cardUrl = new URL(profile.url, window.location.origin).toString();
  const moduleLine = request.intent === "page"
    ? 'SOTY_CARD_MODULE:{"kind":"link","title":"...","summary":"...","href":"https://...","visibility":"public"}'
    : 'SOTY_CARD_MODULE:{"kind":"miniapp","title":"...","summary":"...","href":"https://...","visibility":"public","layout":"large"}';
  const runtimeModuleLine = 'SOTY_CARD_MODULE:{"kind":"runtime","title":"Шахматы","summary":"игра","href":"chess","visibility":"public"}';
  const lines = [
    "Ты работаешь внутри инфо-карты Soty, а не в старом интерфейсе сот.",
    "Отвечай кратко по-русски, без лишних кнопок, без маркетинга и без длинных инструкций.",
    "Карточка должна ощущаться как визитка, личное пространство, отзывы, личные сообщения и расширяемые модули.",
    "Если задача требует mini-app, модуль должен быть внешним URL. Нельзя использовать этот origin Soty, /mini-apps или встроенный сервер Soty как хостинг mini-app.",
    "Если готов внешний URL mini-app или полезной страницы, последней отдельной строкой верни строго один JSON-модуль с выбранным kind.",
    "Если подходит встроенная возможность сот, верни kind runtime. Доступные href: apps, actions, access, qr, files, chess.",
    "Формат последней строки:",
    moduleLine,
    runtimeModuleLine,
    "Если URL не готов, не выдумывай его. Ответь, какой один следующий шаг нужен.",
    "",
    "Контекст карточки:",
    `Ссылка: ${cardUrl}`,
    `Ник: @${profile.handle}${profile.slug ? `/${profile.slug}` : ""}`,
    `Название: ${profile.displayName}`,
    `Описание: ${profile.about || "не указано"}`,
    `Тип задачи: ${request.intent === "page" ? "страница/ссылка" : "mini-app"}`,
    "",
    "Задача владельца:",
    request.text
  ];
  return lines.join("\n");
}

function personalSpaceAgentLabel(profile: PersonalSpaceProfile): string {
  return `@${profile.handle}${profile.slug ? `/${profile.slug}` : ""}`;
}

function personalSpaceAgentReplyText(reply: LocalAgentReply): string {
  const seen = new Set<string>();
  const parts = [...(reply.messages ?? []), reply.text]
    .map((message) => normalizeChatMessage(cleanAgentReplyText(message)))
    .filter((message) => {
      if (!message || seen.has(message)) {
        return false;
      }
      seen.add(message);
      return true;
    });
  return parts.join("\n\n").trim();
}

function personalSpaceAgentFailureText(value: string): string {
  const message = userVisibleAgentFailureText(value);
  return /agent-relay|agent bridge|could not reach relay|relay-not-connected/iu.test(message)
    ? "ИИ пока не подключен."
    : message;
}

async function uploadSignedPersonalSpacePhoto(route: PersonalSpaceRoute, file: File): Promise<string> {
  return uploadPersonalSpacePhoto(route, file, (data) => createPersonalOwnerProof(route, "photo", data));
}

async function createPersonalOwnerProof(route: PersonalSpaceRoute, action: PersonalOwnerAction, data: unknown): Promise<PersonalOwnerProof | null> {
  const handle = cleanSelfStartHandle(route.handle);
  if (!handle) {
    return null;
  }
  try {
    const currentDevice = device ?? await loadDevice();
    const owner = loadPersonalOwnerRecord(handle);
    if (!currentDevice || !owner || owner.deviceId !== currentDevice.id) {
      return null;
    }
    if (!device) {
      device = currentDevice;
    }
    const payload = {
      v: 1,
      kind: "soty.personal-space.owner-action",
      action,
      handle,
      slug: cleanSelfStartHandle(route.slug),
      bodyHash: await personalOwnerBodyHash(data),
      deviceId: currentDevice.id,
      publicJwk: currentDevice.publicJwk,
      createdAt: new Date().toISOString(),
      nonce: randomOwnerNonce()
    } as const satisfies PersonalOwnerProof["payload"];
    const signature = await crypto.subtle.sign(
      { name: "ECDSA", hash: "SHA-256" },
      currentDevice.privateKey,
      bytesBuffer(utf8(stableJson(payload)))
    );
    return {
      payload,
      signature: toBase64Url(new Uint8Array(signature))
    };
  } catch {
    return null;
  }
}

async function personalOwnerBodyHash(data: unknown): Promise<string> {
  const digest = await crypto.subtle.digest("SHA-256", bytesBuffer(utf8(stableJson(data))));
  return toBase64Url(new Uint8Array(digest));
}

function randomOwnerNonce(): string {
  const bytes = new Uint8Array(16);
  crypto.getRandomValues(bytes);
  return toBase64Url(bytes);
}

function bytesBuffer(bytes: Uint8Array): ArrayBuffer {
  return bytes.buffer.slice(bytes.byteOffset, bytes.byteOffset + bytes.byteLength) as ArrayBuffer;
}

async function promptPersonalSpaceInstall(): Promise<PersonalSpaceInstallResult> {
  if (!pendingInstallPrompt) {
    return {
      ok: false,
      message: "Меню браузера -> На экран."
    };
  }
  const promptEvent = pendingInstallPrompt;
  pendingInstallPrompt = null;
  await Promise.race([
    promptEvent.prompt(),
    wait(1500)
  ]).catch(() => undefined);
  const choice = await Promise.race([
    promptEvent.userChoice,
    wait(12_000).then(() => ({ outcome: "dismissed" as const, platform: "" }))
  ]).catch(() => ({ outcome: "dismissed" as const, platform: "" }));
  return choice.outcome === "accepted"
    ? { ok: true, message: "Готово." }
    : { ok: false, message: "Можно повторить позже." };
}

function shouldShowPersonalSpaceInstallAction(): boolean {
  return Boolean(pendingInstallPrompt) || !isStandaloneDisplay();
}

function isStandaloneDisplay(): boolean {
  const standaloneNavigator = navigator as Navigator & { readonly standalone?: boolean };
  return standaloneNavigator.standalone === true
    || window.matchMedia?.("(display-mode: standalone)").matches === true
    || window.matchMedia?.("(display-mode: fullscreen)").matches === true;
}

async function promptPersonalSpaceNotifications(): Promise<PersonalSpaceInstallResult> {
  const permission = await requestNotificationPermission();
  if (permission === "granted") {
    return { ok: true, message: "Оповещения включены." };
  }
  if (permission === "denied") {
    return { ok: false, message: "Включите в настройках браузера." };
  }
  if (permission === "unsupported") {
    return { ok: false, message: "Браузер не поддерживает оповещения." };
  }
  return { ok: false, message: "Оповещения не включены." };
}

function openPersonalSpaceRuntime(profile: PersonalSpaceProfile, target = ""): void {
  const url = new URL(profile.actions.runtimeUrl || bareChatPath(), window.location.origin);
  const moduleTarget = runtimeModuleTargetFromString(target);
  if (moduleTarget) {
    url.searchParams.set("module", moduleTarget);
    if (runtimeModuleUsesEntity(moduleTarget) && profile.handle) {
      url.searchParams.set("to", `@${profile.handle}`);
    }
  }
  window.location.assign(`${url.pathname}${url.search}${url.hash}`);
}

function requestedRuntimeModule(location: Location = window.location): RuntimeModuleTarget | "" {
  try {
    return runtimeModuleTargetFromString(new URL(location.href).searchParams.get("module") || "");
  } catch {
    return "";
  }
}

function consumeRequestedRuntimeModule(): RuntimeModuleTarget | "" {
  const target = requestedRuntimeModule();
  if (!target) {
    return "";
  }
  try {
    const url = new URL(window.location.href);
    url.searchParams.delete("module");
    window.history.replaceState({}, "", `${url.pathname}${url.search}${url.hash}`);
  } catch {
    // The module can still open even if the URL cannot be cleaned.
  }
  return target;
}

function openRequestedRuntimeModule(): void {
  const target = consumeRequestedRuntimeModule();
  if (!target) {
    return;
  }
  window.setTimeout(() => {
    void openRuntimeModule(target);
  }, 0);
}

function isSelfStartRoute(location: Location = window.location): boolean {
  const url = new URL(location.href);
  if (url.pathname !== "/" && url.pathname !== "") {
    return false;
  }
  return !shouldResetLocalState()
    && !isInfoRoute()
    && !isPaymentRoute()
    && !url.searchParams.has("j")
    && !url.searchParams.has("to")
    && !url.searchParams.has("room")
    && !url.searchParams.has("chat")
    && !url.searchParams.has("space")
    && !url.searchParams.has("module")
    && !url.searchParams.has("restore-local");
}

async function renderSelfStartPage(): Promise<void> {
  const saved = await loadSelfStartHandleOrLegacyDevice();
  if (saved) {
    window.history.replaceState({}, "", `/@${encodeURIComponent(saved)}`);
    showPersonalSpaceRoute({ handle: saved, slug: "" });
    return;
  }
  const suggestedHandle = loadPersonalHandle();
  setPersonalSpaceMode(false);
  setSelfStartMode(true);
  applyPersonalSpaceManifest(null);
  app.innerHTML = `
    <main class="self-start-shell" aria-label="создать страницу">
      <form class="self-start-form">
        <label class="self-start-field">
          <span aria-hidden="true">@</span>
          <input
            class="self-start-input"
            name="handle"
            autocomplete="nickname"
            autocapitalize="none"
            enterkeyhint="go"
            inputmode="text"
            maxlength="32"
            aria-label="Имя страницы"
            placeholder="имя"
            value="${escapeHtml(suggestedHandle)}"
          />
          <button type="submit" aria-label="Открыть Я" data-tooltip="Открыть Я">${icon("check")}</button>
        </label>
      </form>
    </main>
  `;
  const input = app.querySelector<HTMLInputElement>(".self-start-input");
  app.querySelector<HTMLFormElement>(".self-start-form")?.addEventListener("submit", (event) => {
    event.preventDefault();
    const handle = cleanSelfStartHandle(input?.value || "");
    if (!handle) {
      input?.focus();
      return;
    }
    void openCreatedSelfStartHandle(handle, input);
  });
}

function exportSotyBackup(): void {
  const text = buildOperatorExport();
  const blob = new Blob([text], { type: "application/json" });
  const url = URL.createObjectURL(blob);
  const link = document.createElement("a");
  const stamp = new Date().toISOString().replace(/[:.]/gu, "-");
  link.href = url;
  link.download = `soty-${stamp}.json`;
  document.body.append(link);
  link.click();
  link.remove();
  window.setTimeout(() => URL.revokeObjectURL(url), 1000);
}

async function importSotyBackupFile(file: File, nickInput?: HTMLInputElement | null): Promise<PersonalSpaceInstallResult> {
  const restored = await restoreFromOperatorExportText(file.text(), nickInput);
  if (!restored) {
    return { ok: false, message: "Не получилось импортировать." };
  }
  return {
    ok: true,
    message: restored.count > 0 ? `Импортировано: ${restored.count}.` : "Импортировано."
  };
}

function loadSelfStartHandle(): string {
  return loadOwnedPersonalHandleForDevice(device?.id || "");
}

async function loadSelfStartHandleOrLegacyDevice(): Promise<string> {
  try {
    const currentDevice = device ?? await loadDevice();
    if (currentDevice && !device) {
      device = currentDevice;
    }
    const owned = loadOwnedPersonalHandleForDevice(currentDevice?.id || "");
    if (owned) {
      if (await personalHandleMatchesServerOwner(owned, currentDevice?.id || "")) {
        return owned;
      }
      removePersonalOwnerRecord(owned);
    }
    const legacyHandle = legacySelfStartHandleFromNick(currentDevice?.nick || "");
    if (legacyHandle && !hasAnyPersonalOwnerRecord()) {
      if (await personalHandleMatchesServerOwner(legacyHandle, currentDevice?.id || "")) {
        const migrated = await bindPersonalOwner(legacyHandle, { replace: false });
        if (migrated) {
          saveSelfStartHandle(migrated);
          return migrated;
        }
      }
    }
  } catch {
    // Old profiles without readable IndexedDB should still get the simple start field.
  }
  return "";
}

async function openCreatedSelfStartHandle(handle: string, input?: HTMLInputElement | null): Promise<void> {
  const currentDevice = await loadOrCreatePersonalDevice(handle);
  const serverOwnerDeviceId = await fetchPersonalOwnerDeviceId(handle);
  if (serverOwnerDeviceId && serverOwnerDeviceId !== currentDevice.id) {
    savePersonalHandle(handle);
    window.history.pushState({}, "", `/@${encodeURIComponent(handle)}`);
    showPersonalSpaceRoute({ handle, slug: "" });
    return;
  }
  const owned = await bindPersonalOwner(handle, { replace: true });
  if (!owned) {
    input?.focus();
    return;
  }
  saveSelfStartHandle(owned);
  window.history.pushState({}, "", `/@${encodeURIComponent(owned)}`);
  showPersonalSpaceRoute({ handle: owned, slug: "" });
}

function legacySelfStartHandleFromNick(value: string): string {
  const handle = cleanSelfStartHandle(value);
  return handle && !reservedLegacySelfHandles.has(handle) ? handle : "";
}

function saveSelfStartHandle(handle: string): void {
  savePersonalHandle(handle);
}

function cleanSelfStartHandle(value: string): string {
  return cleanPersonalHandle(value);
}

async function isPersonalProfileOwned(profile: PersonalSpaceProfile): Promise<boolean> {
  const handle = cleanSelfStartHandle(profile.handle);
  if (!handle) {
    return false;
  }
  try {
    const currentDevice = device ?? await loadDevice();
    if (currentDevice && !device) {
      device = currentDevice;
    }
    const owner = loadPersonalOwnerRecord(handle);
    if (!currentDevice || !owner || owner.deviceId !== currentDevice.id) {
      return false;
    }
    if (profile.ownerDeviceId && profile.ownerDeviceId !== currentDevice.id) {
      removePersonalOwnerRecord(handle);
      return false;
    }
    return true;
  } catch {
    return false;
  }
}

async function bindPersonalOwner(handle: string, options: { readonly replace: boolean }): Promise<string> {
  const clean = cleanSelfStartHandle(handle);
  if (!clean) {
    return "";
  }
  const currentDevice = await loadOrCreatePersonalDevice(clean);
  const existing = loadPersonalOwnerRecord(clean);
  if (existing && existing.deviceId !== currentDevice.id && !options.replace) {
    return "";
  }
  const now = new Date().toISOString();
  savePersonalOwnerRecord({
    handle: clean,
    deviceId: currentDevice.id,
    publicJwk: currentDevice.publicJwk,
    createdAt: existing?.createdAt || now,
    updatedAt: now
  });
  return clean;
}

async function loadOrCreatePersonalDevice(handle: string): Promise<DeviceRecord> {
  const currentDevice = device ?? await loadDevice();
  if (currentDevice) {
    device = currentDevice;
    return currentDevice;
  }
  device = await createDevice(cleanNick(handle || selfCellLabel));
  return device;
}

function loadOwnedPersonalHandleForDevice(deviceId: string): string {
  if (!deviceId) {
    return "";
  }
  const records = loadPersonalOwnerRecords()
    .filter((record) => record.deviceId === deviceId)
    .sort((left, right) => right.updatedAt.localeCompare(left.updatedAt));
  return records[0]?.handle || "";
}

function hasAnyPersonalOwnerRecord(): boolean {
  return loadPersonalOwnerRecords().length > 0;
}

function loadPersonalOwnerRecords(): readonly PersonalOwnerRecord[] {
  const records: PersonalOwnerRecord[] = [];
  try {
    for (let index = 0; index < localStorage.length; index += 1) {
      const key = localStorage.key(index) || "";
      if (!key.startsWith(personalOwnerPrefix)) {
        continue;
      }
      const record = loadPersonalOwnerRecord(cleanSelfStartHandle(key.slice(personalOwnerPrefix.length)));
      if (record) {
        records.push(record);
      }
    }
  } catch {
    // Ownership only controls local UI affordances; blocked storage means visitor mode.
  }
  return records;
}

function loadPersonalOwnerRecord(handle: string): PersonalOwnerRecord | null {
  const clean = cleanSelfStartHandle(handle);
  if (!clean) {
    return null;
  }
  try {
    const parsed: unknown = JSON.parse(localStorage.getItem(personalOwnerKey(clean)) || "null");
    return normalizePersonalOwnerRecord(parsed, clean);
  } catch {
    return null;
  }
}

function normalizePersonalOwnerRecord(value: unknown, fallbackHandle: string): PersonalOwnerRecord | null {
  if (!isRecord(value)) {
    return null;
  }
  const handle = cleanSelfStartHandle(recordString(value, "handle") || fallbackHandle);
  const deviceId = recordString(value, "deviceId").slice(0, 120);
  if (!handle || !deviceId) {
    return null;
  }
  const createdAt = recordString(value, "createdAt") || new Date(0).toISOString();
  const updatedAt = recordString(value, "updatedAt") || createdAt;
  return {
    handle,
    deviceId,
    ...(isRecord(value.publicJwk) ? { publicJwk: value.publicJwk as JsonWebKey } : {}),
    createdAt,
    updatedAt
  };
}

function savePersonalOwnerRecord(record: PersonalOwnerRecord): void {
  try {
    localStorage.setItem(personalOwnerKey(record.handle), JSON.stringify(record));
  } catch {
    // Without storage the page still opens, just without owner-only controls.
  }
}

function removePersonalOwnerRecord(handle: string): void {
  try {
    localStorage.removeItem(personalOwnerKey(handle));
  } catch {
    // Storage cleanup is best effort; server ownership still decides rendered controls.
  }
}

function personalOwnerKey(handle: string): string {
  return `${personalOwnerPrefix}${cleanSelfStartHandle(handle)}`;
}

async function personalHandleMatchesServerOwner(handle: string, deviceId: string): Promise<boolean> {
  const serverOwnerDeviceId = await fetchPersonalOwnerDeviceId(handle);
  return !serverOwnerDeviceId || serverOwnerDeviceId === deviceId;
}

async function fetchPersonalOwnerDeviceId(handle: string): Promise<string> {
  const clean = cleanSelfStartHandle(handle);
  if (!clean) {
    return "";
  }
  try {
    const response = await fetch(`/api/spaces/${encodeURIComponent(clean)}`, {
      cache: "no-store",
      headers: { Accept: "application/json" }
    });
    if (!response.ok) {
      return "";
    }
    const payload = await response.json() as unknown;
    return isRecord(payload) ? cleanOwnerDeviceId(payload.ownerDeviceId) : "";
  } catch {
    return "";
  }
}

function cleanOwnerDeviceId(value: unknown): string {
  const text = String(typeof value === "string" || typeof value === "number" ? value : "").trim().slice(0, 120);
  return /^dev_[A-Za-z0-9_-]{16,80}$/u.test(text) ? text : "";
}

function isInfoRoute(): boolean {
  const url = new URL(window.location.href);
  return url.pathname === infoPagePath || url.searchParams.get("info") === "1";
}

function isPaymentRoute(): boolean {
  const url = new URL(window.location.href);
  return url.pathname === paymentPagePath || url.searchParams.get("pay") === "1";
}

function openInfoPage(): void {
  window.location.assign(infoPagePath);
}

function renderInfoPage(): void {
  app.innerHTML = infoPageHtml(bareChatPath(), paymentPagePath);
  void bindLegalPage(app);
}

function renderPaymentPage(): void {
  app.innerHTML = paymentPageHtml(bareChatPath(), infoPagePath);
  bindPaymentPage();
}

function bindPaymentPage(): void {
  const status = app.querySelector<HTMLElement>("[data-payment-status]");
  const plansNode = app.querySelector<HTMLElement>("[data-payment-plans]");
  const actionNode = app.querySelector<HTMLElement>("[data-payment-action]");
  const consent = app.querySelector<HTMLInputElement>("[data-payment-consent]");
  if (!status || !plansNode || !actionNode) {
    return;
  }

  let selectedPlanId = "";
  setPaymentStatus(status, "loading", "Проверяю, подключена ли оплата...");
  void loadPaymentConfig().then((config) => {
    selectedPlanId = config.plans[0]?.id || "";
    const rerender = (nextPlanId = selectedPlanId) => {
      selectedPlanId = nextPlanId;
      renderPaymentConfig(config, selectedPlanId, plansNode, actionNode, status, consent, rerender);
    };
    rerender();
    consent?.addEventListener("change", () => {
      rerender();
    });
  });
}

function renderPaymentConfig(
  config: PaymentConfig,
  selectedPlanId: string,
  plansNode: HTMLElement,
  actionNode: HTMLElement,
  status: HTMLElement,
  consent?: HTMLInputElement | null,
  onPlanSelect?: (planId: string) => void
): void {
  const provider = config.enabled
    ? `Подключено: ${config.providerLabel}`
    : "Оплата после согласования";
  setPaymentStatus(status, config.enabled ? "ready" : "manual", provider);

  plansNode.innerHTML = config.plans.length
    ? config.plans.map((plan) => paymentPlanButton(plan, config.currency, plan.id === selectedPlanId)).join("")
    : `<div class="payment-empty">Варианты оплаты появятся после настройки платежей.</div>`;
  plansNode.querySelectorAll<HTMLButtonElement>(".payment-plan").forEach((button) => {
    button.addEventListener("click", () => {
      const nextPlanId = button.dataset.planId || "";
      if (onPlanSelect) {
        onPlanSelect(nextPlanId);
      } else {
        renderPaymentConfig(config, nextPlanId, plansNode, actionNode, status, consent);
      }
    });
  });

  if (config.enabled) {
    const consentReady = consent?.checked === true;
    actionNode.innerHTML = `
      <button class="payment-start" type="button" ${consentReady ? "" : "disabled"}>${icon("heart")} Открыть оплату</button>
      <small>${escapeHtml(config.policy?.text || "Оплата откроется на внешней странице провайдера.")}</small>
    `;
    actionNode.querySelector<HTMLButtonElement>(".payment-start")?.addEventListener("click", () => {
      void startPayment(selectedPlanId, status, consent);
    });
    return;
  }

  actionNode.innerHTML = config.contactUrl
    ? `
      <a class="payment-start" href="${escapeHtml(config.contactUrl)}" target="_blank" rel="noopener noreferrer">${icon("send")} Написать по оплате</a>
      <small>${escapeHtml(config.policy?.text || "Сначала согласуйте задачу в чате.")}</small>
    `
    : `
      <a class="payment-start" href="${escapeHtml(bareChatPath())}">${icon("send")} Согласовать в чате</a>
      <small>${escapeHtml(config.policy?.text || "Сначала согласуйте задачу в чате.")}</small>
    `;
}

function paymentPlanButton(plan: PaymentPlan, currency: string, selected: boolean): string {
  return `
    <button class="payment-plan${selected ? " is-selected" : ""}" type="button" data-plan-id="${escapeHtml(plan.id)}">
      <span>${escapeHtml(formatPaymentAmount(plan, currency))}</span>
      <b>${escapeHtml(plan.title)}</b>
      ${plan.description ? `<small>${escapeHtml(plan.description)}</small>` : ""}
    </button>
  `;
}

async function startPayment(planId: string, status: HTMLElement, consent?: HTMLInputElement | null): Promise<void> {
  if (consent && !consent.checked) {
    setPaymentStatus(status, "manual", "Сначала примите оферту, политику ПДн и правила доступа.");
    return;
  }
  setPaymentStatus(status, "loading", "Готовлю переход к оплате...");
  const intent = await createPaymentIntent(planId);
  if (intent.ok && intent.paymentUrl) {
    setPaymentStatus(status, "ready", intent.reference ? `Переход к оплате. Номер: ${intent.reference}` : "Переход к оплате.");
    window.location.assign(intent.paymentUrl);
    return;
  }
  setPaymentStatus(status, "manual", intent.message || "Оплата пока недоступна. Согласуйте задачу в чате.");
}

function setPaymentStatus(node: HTMLElement, state: "loading" | "ready" | "manual", text: string): void {
  node.dataset.state = state;
  node.textContent = text;
}

function finishDeviceBoot(restoredTexts = new Map<string, string>()): void {
  operatorBridgeAllowEmpty = false;
  const pending = loadPendingJoin();
  if (pending) {
    renderJoinWaiting(pending);
    return;
  }
  tunnels = loadTunnels();
  ensurePermanentCells();
  const requestedContactId = ensureRequestedContactTunnel();
  tunnels = loadTunnels();
  selectedId = requestedChatTunnelId(tunnels) || requestedContactId || loadSelectedTunnelId() || selectedId || tunnels[0]?.id || "";
  if (selectedId) {
    saveSelectedTunnelId(selectedId);
  }
  const snapshots = loadTextSnapshots();
  const writerSnapshots = loadWriterLineSnapshots();
  for (const [tunnelId, lines] of writerSnapshots) {
    writerLines.set(tunnelId, lines);
  }
  for (const tunnel of tunnels) {
    if (!restoredTexts.has(tunnel.id)) {
      const snapshot = snapshots.get(tunnel.id);
      if (snapshot) {
        restoredTexts.set(tunnel.id, snapshot);
      }
    }
  }
  renderApp();
  openRequestedRuntimeModule();
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
  const restoredHandle = restoredSelfStartHandle(payload);
  restorePortableLocalStorage(payload.localStorage);
  if (restoredHandle) {
    saveSelfStartHandle(restoredHandle);
    await bindPersonalOwner(restoredHandle, { replace: true });
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
  remoteGrantTargets = loadRemoteGrantTargets();
  terminalOpenId = "";
  chessOpenId = "";
  return {
    count: restored.tunnels.length,
    texts: restored.texts
  };
}

function restorePortableLocalStorage(snapshot?: Readonly<Record<string, unknown>>): void {
  if (!snapshot) {
    return;
  }
  for (const [key, value] of Object.entries(snapshot)) {
    if (!isRestorableLocalStorageKey(key) || typeof value !== "string") {
      continue;
    }
    try {
      localStorage.setItem(key, value.slice(0, 1_000_000));
    } catch {
      // Import should restore as much as possible without failing the whole backup.
    }
  }
}

function isRestorableLocalStorageKey(key: string): boolean {
  return key === "soty:personal-handle:v1"
    || key === "soty:self-start-handle:v1"
    || key === "soty:handle:v1"
    || key.startsWith("soty:personal-profile:v1:")
    || key.startsWith(personalOwnerPrefix)
    || key.startsWith("soty:personal-thread:v1:");
}

function restoredSelfStartHandle(payload: OperatorExportPayload): string {
  return cleanSelfStartHandle(recordString(payload.localStorage, "soty:personal-handle:v1")
    || recordString(payload.localStorage, "soty:self-start-handle:v1")
    || recordString(payload.localStorage, "soty:handle:v1"))
    || legacySelfStartHandleFromNick(payload.device?.nick || "");
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
    ...(raw.self === true ? { self: true } : {}),
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
    const syncedWriters = syncedWriterLinesForSnapshot(tunnelId);
    syncs.get(tunnelId)?.setText(text, {}, syncedWriters.length > 0 ? syncedWriters : undefined);
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

function loadWriterLineSnapshots(): Map<string, Map<number, WriterLine>> {
  try {
    const parsed: unknown = JSON.parse(localStorage.getItem(textSnapshotsKey) || "{}");
    if (!isRecord(parsed)) {
      return new Map();
    }
    const result = new Map<string, Map<number, WriterLine>>();
    for (const [tunnelId, record] of Object.entries(parsed)) {
      if (!isRecord(record) || !isRecord(record.writers)) {
        continue;
      }
      const lines = new Map<number, WriterLine>();
      for (const [lineText, value] of Object.entries(record.writers)) {
        const line = Number.parseInt(lineText, 10);
        if (!Number.isSafeInteger(line) || line < 0 || !isRecord(value)) {
          continue;
        }
        const nick = cleanNick(recordString(value, "nick"));
        const deviceId = recordString(value, "deviceId").slice(0, 140);
        const at = Math.max(0, Math.trunc(Number(value.at) || 0));
        const time = recordString(value, "time").slice(0, 8) || clock(at > 0 ? new Date(at) : undefined);
        const action = value.action === "erase" || value.action === "edit" ? value.action : "write";
        lines.set(line, {
          nick,
          deviceId,
          color: recordString(value, "color").slice(0, 32) || colorFor(`${nick}:${deviceId || tunnelId}`),
          time,
          at,
          action,
          preview: recordString(value, "preview").slice(0, 120)
        });
      }
      if (lines.size > 0) {
        result.set(tunnelId, lines);
      }
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
    const next: Record<string, { readonly text: string; readonly at: number; readonly writers?: Record<string, unknown> }> = {};
    next[tunnelId] = { text: text.slice(-200_000), at: now, writers: writerLinesForSnapshot(tunnelId) };
    for (const [id, record] of Object.entries(current)) {
      if (id === tunnelId || !isRecord(record) || typeof record.text !== "string") {
        continue;
      }
      const at = typeof record.at === "number" && Number.isFinite(record.at) ? record.at : 0;
      next[id] = {
        text: record.text.slice(-200_000),
        at,
        ...(isRecord(record.writers) ? { writers: record.writers } : {})
      };
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

function loadSpaceModes(): Map<string, SpaceMode> {
  try {
    const parsed: unknown = JSON.parse(localStorage.getItem(spaceModeKey) || "{}");
    if (!isRecord(parsed)) {
      return new Map();
    }
    return new Map(Object.entries(parsed).map(([id, mode]) => [id, normalizeSpaceMode(String(mode || ""))]));
  } catch {
    return new Map();
  }
}

function saveSpaceModes(): void {
  try {
    localStorage.setItem(spaceModeKey, JSON.stringify(Object.fromEntries(spaceModes)));
  } catch {
    // Space mode is local UI memory.
  }
}

function loadAgentModes(): Map<string, boolean> {
  try {
    const parsed: unknown = JSON.parse(localStorage.getItem(agentModeKey) || "{}");
    if (!isRecord(parsed)) {
      return new Map();
    }
    return new Map(Object.entries(parsed).map(([id, active]) => [id, active === true || active === "1"]));
  } catch {
    return new Map();
  }
}

function saveAgentModes(): void {
  try {
    localStorage.setItem(agentModeKey, JSON.stringify(Object.fromEntries(agentModes)));
  } catch {
    // Agent mode is local UI memory.
  }
}

function selectedAgentMode(tunnelId = selectedId): boolean {
  return Boolean(tunnelId && normalizeSpaceMode(spaceModes.get(tunnelId) || "dialog") === "dialog" && agentModes.get(tunnelId) === true);
}

function setSelectedAgentMode(active: boolean): void {
  if (!selectedId) {
    return;
  }
  if (active) {
    spaceModes.set(selectedId, "dialog");
    agentModes.set(selectedId, true);
  } else {
    agentModes.delete(selectedId);
  }
  saveSpaceModes();
  saveAgentModes();
  void syncs.get(selectedId)?.sendLiveDraft("");
  renderSpace();
  updateComposerSpaceMode();
  renderDialogChrome();
  renderAgentPrivatePanel();
  renderTextPaint();
  composer?.focus();
}

function loadAgentPrivateLogs(): Map<string, AgentPrivateLine[]> {
  try {
    const parsed: unknown = JSON.parse(localStorage.getItem(agentPrivateLogKey) || "{}");
    if (!isRecord(parsed)) {
      return new Map();
    }
    const result = new Map<string, AgentPrivateLine[]>();
    for (const [id, value] of Object.entries(parsed)) {
      const lines = Array.isArray(value)
        ? value.map(sanitizeAgentPrivateLine).filter((line): line is AgentPrivateLine => Boolean(line)).slice(-80)
        : [];
      if (lines.length > 0) {
        result.set(id, lines);
      }
    }
    return result;
  } catch {
    return new Map();
  }
}

function sanitizeAgentPrivateLine(value: unknown): AgentPrivateLine | null {
  if (!isRecord(value)) {
    return null;
  }
  const role = value.role === "user" ? "user" : value.role === "agent" ? "agent" : null;
  const text = normalizeChatMessage(recordString(value, "text")).slice(0, 12_000);
  if (!role || !text) {
    return null;
  }
  return {
    role,
    text,
    createdAt: recordString(value, "createdAt").slice(0, 40) || new Date().toISOString()
  };
}

function saveAgentPrivateLogs(): void {
  try {
    localStorage.setItem(agentPrivateLogKey, JSON.stringify(Object.fromEntries(agentPrivateLogs)));
  } catch {
    // Private agent transcript is local-only memory.
  }
}

function appendAgentPrivateLine(tunnelId: string, role: AgentPrivateLine["role"], rawText: string): boolean {
  const text = role === "agent" ? cleanAgentReplyText(rawText) : normalizeChatMessage(rawText);
  if (!tunnelId || !text) {
    return false;
  }
  const next = [
    ...(agentPrivateLogs.get(tunnelId) ?? []),
    { role, text: text.slice(0, 12_000), createdAt: new Date().toISOString() }
  ].slice(-80);
  agentPrivateLogs.set(tunnelId, next);
  saveAgentPrivateLogs();
  if (tunnelId === selectedId) {
    renderAgentPrivatePanel();
  }
  return true;
}

function loadHiveDrawerOpen(): boolean {
  try {
    return localStorage.getItem(hiveDrawerKey) === "1";
  } catch {
    return false;
  }
}

function saveHiveDrawerOpen(): void {
  try {
    localStorage.setItem(hiveDrawerKey, hiveDrawerOpen ? "1" : "0");
  } catch {
    // Drawer state is cosmetic.
  }
}

function setHiveDrawerOpen(open: boolean): void {
  hiveDrawerOpen = open;
  saveHiveDrawerOpen();
  app.querySelector<HTMLElement>(".shell")?.classList.toggle("hive-open", hiveDrawerOpen);
  const panel = app.querySelector<HTMLElement>(".hive-panel");
  panel?.toggleAttribute("inert", !hiveDrawerOpen);
  panel?.setAttribute("aria-hidden", hiveDrawerOpen ? "false" : "true");
}

function selectedSpaceMode(): SpaceMode {
  return normalizeSpaceMode(spaceModes.get(selectedId) || "dialog");
}

function setSelectedSpaceMode(mode: SpaceMode): void {
  if (!selectedId) {
    return;
  }
  spaceModes.set(selectedId, mode);
  saveSpaceModes();
  renderSpace();
  updateComposerSpaceMode();
  renderTextPaint();
  restoreSelectedChatScroll();
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

function maybeRefreshQuickActionCatalog(): void {
  if (quickActionCatalogProbe || Date.now() - quickActionCatalogCheckedAt < quickActionCatalogTtlMs) {
    return;
  }
  const previous = quickActionCatalogSignature();
  void refreshQuickActionCatalog().then(() => {
    if (actionOverlay && quickActionCatalogSignature() !== previous) {
      openLauncher();
    }
    if (activeTerminalTunnelId()) {
      renderTerminal();
    }
  });
}

async function refreshQuickActionCatalog(force = false): Promise<readonly QuickAction[]> {
  if (!force && Date.now() - quickActionCatalogCheckedAt < quickActionCatalogTtlMs) {
    return quickActionCatalog;
  }
  if (quickActionCatalogProbe) {
    return quickActionCatalogProbe;
  }
  quickActionCatalogProbe = fetchFrontendQuickActions()
    .then((generated) => {
      quickActionCatalog = mergeQuickActions(quickActions, generated);
      quickActionCatalogCheckedAt = Date.now();
      return quickActionCatalog;
    })
    .catch(() => {
      quickActionCatalogCheckedAt = Date.now();
      return quickActionCatalog;
    })
    .finally(() => {
      quickActionCatalogProbe = null;
    });
  return quickActionCatalogProbe;
}

function quickActionCatalogSignature(): string {
  return quickActionCatalog.map((action) => action.id).join("|");
}

function openActionMenu(): void {
  actionSearchText = "";
  closeMiniAppGallery();
  openLauncher();
}

async function openRuntimeModule(target: RuntimeModuleTarget): Promise<void> {
  const handlers: Record<RuntimeModuleTarget, () => void | Promise<void>> = {
    agent: () => {
      setSelectedAgentMode(true);
    },
    apps: () => {
      openMiniAppGallery();
    },
    actions: () => {
      openActionMenu();
    },
    access: () => {
      showAccessPanel();
    },
    qr: () => {
      void showQr();
    },
    files: () => {
      fileInput?.click();
    },
    chess: () => openChessForSelected(true)
  };
  await handlers[target]();
}

function openLauncher(): void {
  maybeRefreshQuickActionCatalog();
  closeActionMenu();
  const query = actionSearchText.trim();
  const items = launcherItems(query);
  const comment = selectedId
    ? normalizeChatMessage(composer?.value || localDrafts.get(selectedId) || "")
    : "";
  const overlay = document.createElement("div");
  overlay.className = "action-modal";
  overlay.innerHTML = `
    <section class="action-sheet" role="dialog" aria-modal="true" aria-label="launcher">
      <header class="action-head">
        <span class="action-mark">${icon("check")}</span>
        <span>
          <b>ACTION</b>
          <small>${escapeHtml(counterpartyLabelForSelected())}</small>
        </span>
        <button class="action-close icon-button" type="button" aria-label="close" data-tooltip="Close">${icon("close")}</button>
      </header>
      <input class="action-search" type="search" value="${escapeHtml(actionSearchText)}" placeholder="action or url" />
      ${comment ? `<div class="action-comment"><b>TEXT</b><span>${escapeHtml(comment.slice(0, 180))}</span></div>` : ""}
      <div class="action-list">
        ${items.map((item) => launcherRowHtml(item)).join("")}
      </div>
      ${items.length === 0 ? `<output class="action-empty">No matches</output>` : ""}
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
    openLauncher();
    actionOverlay?.querySelector<HTMLInputElement>(".action-search")?.focus();
  });
  overlay.querySelectorAll<HTMLButtonElement>(".launcher-open").forEach((button) => {
    button.addEventListener("click", () => {
      runLauncherItem(button.dataset.kind || "", button.dataset.key || "");
    });
  });
  overlay.querySelector<HTMLInputElement>(".action-search")?.focus();
}

function launcherItems(query: string): LauncherItem[] {
  const installDraft = miniAppInstallDraftFromSearch(query, globalMiniApps());
  return [
    ...(installDraft ? [installDraftLauncherItem(installDraft)] : []),
    ...visibleQuickActions(query).map(actionLauncherItem)
  ];
}

function launcherRowHtml(item: LauncherItem): string {
  return `
    <div class="launcher-entry">
      <button class="quick-action-run launcher-row launcher-open" type="button" data-kind="${escapeHtml(item.kind)}" data-key="${escapeHtml(item.key)}">
        <span class="quick-action-label">${item.markHtml}</span>
        <span class="quick-action-copy">
          <b>${escapeHtml(item.title)}</b>
          <small>${escapeHtml(item.summary)}</small>
          ${item.meta ? `<em class="launcher-meta">${escapeHtml(item.meta)}</em>` : ""}
        </span>
      </button>
    </div>
  `;
}

function runLauncherItem(kind: string, key: string): void {
  if (kind === "action") {
    void runQuickAction(key);
    return;
  }
  if (kind === "install") {
    const draft = miniAppInstallDraftFromSearch(actionSearchText, globalMiniApps());
    if (!draft) {
      return;
    }
    installMiniAppFromConnector({
      ...draft,
      icon: "remote",
      scope: "account",
      visibility: "private",
      open: true,
      capabilities: []
    });
  }
}

function closeActionMenu(): void {
  actionOverlay?.remove();
  actionOverlay = null;
}

function closeMiniAppGallery(): void {
  miniAppGalleryOverlay?.remove();
  miniAppGalleryOverlay = null;
}

async function refreshMiniApps(_force = false): Promise<readonly MiniAppDefinition[]> {
  miniApps = currentMiniApps();
  renderDialogChrome();
  return miniApps;
}

function currentMiniApps(): MiniAppDefinition[] {
  miniApps = dedupeMiniApps(scopedMiniAppsForSelected());
  return miniApps;
}

function globalMiniApps(): MiniAppDefinition[] {
  return dedupeMiniApps([
    ...scopedMiniAppsForSelected(),
    ...loadLocalMiniApps(),
    ...Array.from(roomMiniApps.values()).flat()
  ]);
}

function loadLocalMiniApps(): MiniAppDefinition[] {
  try {
    const parsed: unknown = JSON.parse(localStorage.getItem(miniAppsRegistryKey) || "{}");
    const rawItems = Array.isArray((parsed as { readonly apps?: unknown })?.apps)
      ? (parsed as { readonly apps: readonly unknown[] }).apps
      : [];
    return rawItems
      .map((item) => sanitizeLocalMiniAppDefinition(item))
      .filter((item): item is MiniAppDefinition => Boolean(item));
  } catch {
    return [];
  }
}

function writerLinesForSnapshot(tunnelId: string): Record<string, unknown> {
  const lines = writerLines.get(tunnelId);
  if (!lines || lines.size === 0) {
    return {};
  }
  const entries = [...lines.entries()]
    .filter(([line]) => Number.isSafeInteger(line) && line >= 0)
    .sort((left, right) => left[0] - right[0])
    .slice(-5000)
    .map(([line, writer]) => [String(line), {
      nick: writer.nick,
      deviceId: writer.deviceId,
      color: writer.color,
      time: writer.time,
      at: writer.at,
      action: writer.action,
      preview: writer.preview
    }]);
  return Object.fromEntries(entries);
}

function syncedWriterLinesForSnapshot(tunnelId: string): SyncedWriterLine[] {
  const lines = writerLines.get(tunnelId);
  if (!lines || lines.size === 0) {
    return [];
  }
  return [...lines.entries()]
    .filter(([line]) => Number.isSafeInteger(line) && line >= 0)
    .sort((left, right) => left[0] - right[0])
    .slice(-5000)
    .map(([line, writer]) => ({
      line,
      deviceId: writer.deviceId,
      nick: writer.nick,
      createdAt: new Date(writer.at > 0 ? writer.at : Date.now()).toISOString(),
      action: writer.action,
      preview: writer.preview
    }));
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
      .sort((left, right) => String(right.updatedAt || right.installedAt || "").localeCompare(String(left.updatedAt || left.installedAt || "")));
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

function miniAppFromSynced(appItem: SyncedMiniApp, tunnelId = ""): MiniAppDefinition | null {
  const definition = sanitizeMiniAppDefinition(appItem);
  if (!definition) {
    return null;
  }
  return {
    ...definition,
    source: "room",
    scope: appItem.scope,
    ...(tunnelId ? { tunnelId } : {}),
    ...(appItem.targetDeviceId ? { targetDeviceId: appItem.targetDeviceId } : {}),
    ...(appItem.visibility ? { visibility: appItem.visibility } : {}),
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
    ...(appItem.tags?.length ? { tags: appItem.tags } : {}),
    ...(appItem.profileId ? { profileId: appItem.profileId } : {}),
    ...(appItem.profileTitle ? { profileTitle: appItem.profileTitle } : {}),
    ...(appItem.placement ? { placement: appItem.placement } : {}),
    ...(appItem.layout && appItem.layout !== "half" ? { layout: appItem.layout } : {}),
    ...(appItem.height ? { height: appItem.height } : {}),
    ...(appItem.width ? { width: appItem.width } : {}),
    capabilities: appItem.capabilities,
    scope,
    ...(appItem.visibility ? { visibility: appItem.visibility } : {}),
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
    const sourceDisplay = isRecord(source.display) ? source.display : source;
    const planDisplay = isRecord(plan.definition.display) ? plan.definition.display : {};
    const layout = normalizeMiniAppLayout(recordString(planDisplay, "layout") || recordString(sourceDisplay, "layout"));
    const height = safeMiniAppCssSize(recordString(planDisplay, "height") || recordString(sourceDisplay, "height"));
    const width = safeMiniAppCssSize(recordString(planDisplay, "width") || recordString(sourceDisplay, "width"));
    const scope = normalizeMiniAppScope(plan.scope);
    const now = new Date().toISOString();
    const normalized = sanitizeMiniAppDefinition({
      ...source,
      id: plan.definition.id,
      title: plan.definition.title,
      url: plan.definition.url,
      ...(plan.definition.inlineHtml ? { inlineHtml: plan.definition.inlineHtml } : {}),
      summary: plan.definition.summary || recordString(source, "summary"),
      capabilities: plan.definition.capabilities
    }, { baseUrl: window.location.origin });
    if (!normalized) {
      return { ok: false, error: "invalid-mini-app-definition" };
    }
    const appItem: MiniAppDefinition = {
      id: plan.definition.id,
      title: plan.definition.title,
      url: plan.definition.url,
      ...(plan.definition.inlineHtml ? { inlineHtml: plan.definition.inlineHtml } : {}),
      summary: plan.definition.summary || plan.definition.id,
      icon: normalized.icon,
      ...(normalized.tags?.length ? { tags: normalized.tags } : {}),
      ...(normalized.profileId ? { profileId: normalized.profileId } : {}),
      ...(normalized.profileTitle ? { profileTitle: normalized.profileTitle } : {}),
      placement: normalized.placement || plan.mode,
      ...(layout !== "half" ? { layout } : {}),
      ...(height ? { height } : {}),
      ...(width ? { width } : {}),
      ...(normalized.visibility ? { visibility: normalized.visibility } : {}),
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

function connectorMiniAppRecord(value: unknown): Record<string, unknown> {
  if (!isRecord(value)) {
    return {};
  }
  return isRecord(value.app) ? value.app : value;
}

function handleMiniAppRegistryChange(): void {
  miniApps = currentMiniApps();
  if (miniAppSession) {
    const activeMiniAppKey = miniAppRecordKey(miniAppSession.app);
    if (!globalMiniApps().some((item) => miniAppRecordKey(item) === activeMiniAppKey)) {
      miniAppSession = null;
      renderMiniAppPanel();
    }
  }
  renderDialogChrome();
}

function renderCellAppShelf(): void {
  const shelf = app.querySelector<HTMLElement>(".cell-app-shelf");
  if (!shelf) {
    return;
  }
  const cellApps = cellShelfMiniApps();
  const allCount = globalMiniApps().length;
  shelf.dataset.empty = cellApps.length > 0 ? "0" : "1";
  shelf.innerHTML = `
    <button class="cell-app-all" type="button" aria-label="все мини-аппы" data-tooltip="Все мини-аппы">
      ${icon("apps")}
      <small>${allCount}</small>
    </button>
    <div class="cell-app-strip" role="list">
      ${cellApps.length > 0
        ? cellApps.map((item) => cellAppTileHtml(item)).join("")
        : `<button class="cell-app-empty" type="button" aria-label="добавить мини-апп" data-tooltip="Добавить мини-апп">${icon("install")}</button>`}
    </div>
  `;
  shelf.querySelector<HTMLButtonElement>(".cell-app-all")?.addEventListener("click", openMiniAppGallery);
  shelf.querySelector<HTMLButtonElement>(".cell-app-empty")?.addEventListener("click", openActionMenu);
  shelf.querySelectorAll<HTMLButtonElement>("[data-mini-app-key]").forEach((button) => {
    button.addEventListener("click", () => {
      openMiniAppByKey(button.dataset.miniAppKey || "");
    });
  });
}

function cellAppTileHtml(appItem: MiniAppDefinition): string {
  const active = miniAppSession && sameMiniAppRecord(miniAppSession.app, appItem);
  return `
    <button class="cell-app-tile${active ? " is-active" : ""}" type="button" role="listitem" data-mini-app-key="${escapeHtml(miniAppRecordKey(appItem))}" aria-label="${escapeHtml(appItem.title)}" data-tooltip="${escapeHtml(appItem.title)}">
      <span>${icon(appItem.icon)}</span>
      <b>${escapeHtml(appItem.title)}</b>
    </button>
  `;
}

function openMiniAppGallery(): void {
  closeActionMenu();
  closeMiniAppGallery();
  const items = sortedMiniApps(globalMiniApps());
  const overlay = document.createElement("div");
  overlay.className = "action-modal mini-app-gallery-modal";
  overlay.innerHTML = `
    <section class="action-sheet mini-app-gallery-sheet" role="dialog" aria-modal="true" aria-label="мини-аппы">
      <header class="action-head">
        <span class="action-mark">${icon("apps")}</span>
        <span>
          <b>APPS</b>
          <small>${items.length}</small>
        </span>
        <button class="mini-app-gallery-close action-close icon-button" type="button" aria-label="close" data-tooltip="Close">${icon("close")}</button>
      </header>
      <div class="mini-app-gallery-list">
        ${items.map((item) => miniAppGalleryItemHtml(item)).join("")}
      </div>
      ${items.length === 0 ? `<button class="mini-app-gallery-empty" type="button" aria-label="добавить мини-апп" data-tooltip="Добавить мини-апп">${icon("install")}</button>` : ""}
    </section>
  `;
  document.body.append(overlay);
  miniAppGalleryOverlay = overlay;
  overlay.addEventListener("click", (event) => {
    if (event.target === overlay) {
      closeMiniAppGallery();
    }
  });
  overlay.querySelector<HTMLButtonElement>(".mini-app-gallery-close")?.addEventListener("click", closeMiniAppGallery);
  overlay.querySelector<HTMLButtonElement>(".mini-app-gallery-empty")?.addEventListener("click", openActionMenu);
  overlay.querySelectorAll<HTMLButtonElement>("[data-mini-app-key]").forEach((button) => {
    button.addEventListener("click", () => {
      openMiniAppByKey(button.dataset.miniAppKey || "");
    });
  });
}

function miniAppGalleryItemHtml(appItem: MiniAppDefinition): string {
  return `
    <button class="mini-app-gallery-item" type="button" data-mini-app-key="${escapeHtml(miniAppRecordKey(appItem))}" aria-label="${escapeHtml(appItem.title)}">
      <span>${icon(appItem.icon)}</span>
      <b>${escapeHtml(appItem.title)}</b>
    </button>
  `;
}

function sortedMiniApps(apps: readonly MiniAppDefinition[]): MiniAppDefinition[] {
  return searchMiniApps(apps, "");
}

function cellShelfMiniApps(): MiniAppDefinition[] {
  const scoped = [
    ...sortedMiniApps(roomMiniAppsForSelected()),
    ...sortedMiniApps(localMiniAppsForSelected())
  ];
  return dedupeMiniApps([
    ...scoped,
    ...sortedMiniApps(globalMiniApps()).filter((item) =>
      !scoped.some((scopedItem) => sameMiniAppRecord(scopedItem, item))
    )
  ]);
}

function installDraftLauncherItem(draft: MiniAppInstallDraft): LauncherItem {
  return {
    kind: "install",
    key: draft.url,
    markHtml: icon("install"),
    title: draft.title,
    summary: draft.summary,
    meta: `account / ${draft.placement}`
  };
}

function miniAppInstallDraftFromSearch(query: string, existingApps: readonly MiniAppDefinition[]): MiniAppInstallDraft | null {
  const raw = query.trim();
  if (!raw || /\s/u.test(raw)) {
    return null;
  }
  let url: URL;
  try {
    url = new URL(raw, window.location.origin);
  } catch {
    return null;
  }
  const sameOrigin = url.origin === window.location.origin;
  const loopback = url.protocol === "http:" && /^(?:localhost|127\.|0\.0\.0\.0|\[::1\])$/iu.test(url.hostname);
  if (url.protocol !== "https:" && !sameOrigin && !loopback) {
    return null;
  }
  if (existingApps.some((item) => item.url === url.href)) {
    return null;
  }
  const host = url.hostname.replace(/^www\./iu, "") || "app";
  return {
    id: miniAppDraftId(url),
    title: host.slice(0, 80),
    url: url.href,
    summary: url.origin,
    placement: sameOrigin ? "same-origin" : loopback ? "device-local" : "remote-origin"
  };
}

function miniAppDraftId(url: URL): string {
  const raw = `${url.hostname}-${url.pathname}`.toLowerCase();
  return raw
    .replace(/[^a-z0-9._-]+/gu, "-")
    .replace(/^-+|-+$/gu, "")
    .slice(0, 80) || `app-${crypto.randomUUID().slice(0, 8)}`;
}

function openMiniApp(appId: string): void {
  const appItem = currentMiniApps().find((item) => item.id === appId);
  if (!appItem) {
    return;
  }
  openMiniAppRecord(appItem);
}

function openMiniAppByKey(appKey: string): void {
  const appItem = globalMiniApps().find((item) => miniAppRecordKey(item) === appKey);
  if (!appItem) {
    return;
  }
  if (appItem.tunnelId && appItem.tunnelId !== selectedId) {
    selectTunnel(appItem.tunnelId);
    renderTiles();
    applySelectedText(true);
    renderComposerAttachments();
    renderTerminal();
    renderChess();
  }
  openMiniAppRecord(appItem);
}

function openMiniAppRecord(appItem: MiniAppDefinition): void {
  closeActionMenu();
  closeMiniAppGallery();
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

function miniAppEffectiveVisibility(appItem: MiniAppDefinition): MiniAppVisibility {
  return appItem.visibility || (appItem.scope === "account" ? "private" : "granted-cells");
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
      ? "на весь экран"
      : session.layout === "floating"
        ? "окно"
        : "сота";
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
  if (!miniAppFrameCanReceive(frame, miniAppSession)) {
    return;
  }
  const tunnel = loadTunnels().find((item) => item.id === selectedId) || null;
  const label = tunnel ? counterpartyLabel(tunnel) : "";
  const message = {
    schema: miniAppContextProtocol,
    nonce: miniAppSession.nonce,
    appId: miniAppSession.app.id,
    app: {
      id: miniAppSession.app.id,
      title: miniAppSession.app.title,
      tags: miniAppSession.app.tags || [],
      profileId: miniAppSession.app.profileId || "",
      profileTitle: miniAppSession.app.profileTitle || "",
      visibility: miniAppEffectiveVisibility(miniAppSession.app),
      placement: miniAppSession.app.placement || ""
    },
    device: device ? { id: device.id, nick: device.nick } : null,
    selected: tunnel ? {
      tunnelId: tunnel.id,
      label,
      color: safeColor(tunnel.color, label + tunnel.id),
      agent: isAgentTunnel(tunnel),
      agentMode: selectedAgentMode(tunnel.id),
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
    capabilities: miniAppGrantedCapabilities(miniAppSession.app)
  };
  frame.contentWindow.postMessage(message, miniAppTargetOrigin(miniAppSession));
}

function miniAppTargetOrigin(session: MiniAppSession): string {
  return session.app.inlineHtml ? "*" : new URL(session.app.url, window.location.href).origin;
}

function miniAppFrameCanReceive(frame: HTMLIFrameElement, session: MiniAppSession): boolean {
  if (session.app.inlineHtml) {
    return true;
  }
  try {
    return frame.contentWindow?.location.origin === miniAppTargetOrigin(session);
  } catch {
    return true;
  }
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
    privateMode: !isAgentTunnel(tunnel)
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
  if (!miniAppFrameCanReceive(frame, miniAppSession)) {
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
  const available = quickActionCatalog.filter((action) => !action.hidden);
  const needle = actionSearchNeedle(query);
  if (!needle) {
    return available;
  }
  return available
    .map((action) => ({ action, score: quickActionMatchScore(action, needle) }))
    .filter((item) => item.score > 0)
    .sort((left, right) => right.score - left.score || left.action.title.localeCompare(right.action.title))
    .map((item) => item.action);
}

function quickActionMatchScore(action: QuickAction, needle: string): number {
  const runtimeText = action.runtime ? Object.values(action.runtime).flat().join(" ") : "";
  const haystack = actionSearchNeedle(`${action.title} ${action.summary} ${action.kind || ""} ${action.source || ""} ${action.tags.join(" ")} ${runtimeText}`);
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

function actionLauncherItem(action: QuickAction): LauncherItem {
  return {
    kind: "action",
    key: action.id,
    markHtml: escapeHtml(action.label),
    title: action.title,
    summary: action.summary
  };
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
  const action = quickActionCatalog.find((item) => item.id === actionId);
  const tunnelId = selectedId;
  const tunnel = loadTunnels().find((item) => item.id === tunnelId);
  if (!action || action.hidden || !tunnelId || !tunnel) {
    return;
  }
  const appId = typeof action.runtime?.appId === "string" ? action.runtime.appId : "";
  if (action.kind === "mini-app" && appId && currentMiniApps().some((item) => item.id === appId)) {
    closeActionMenu();
    openMiniApp(appId);
    return;
  }
  const comment = normalizeChatMessage(composer?.value || localDrafts.get(tunnelId) || "");
  const visible = quickActionVisibleMessage(action, comment);
  const agentTask = quickActionAgentMessage(action, comment, tunnel);
  closeActionMenu();
  appendUserMessageToDialog(tunnelId, visible);
  clearComposerDraftForTunnel(tunnelId);
  void sendAgentDialogMessage(tunnelId, agentTask, {
    privateMode: !isAgentTunnel(tunnel)
  });
}

function quickActionVisibleMessage(action: QuickAction, comment: string): string {
  return [
    `Действие: ${action.title}`,
    `Комментарий: ${comment}`
  ].join("\n").trimEnd();
}

function quickActionAgentMessage(action: QuickAction, comment: string, tunnel: TunnelRecord): string {
  const hint = {
    schema: "soty.action-hint.v1",
    id: action.id,
    title: action.title,
    source: action.source || "curated",
    kind: action.kind || "curated",
    runtime: action.runtime || {}
  };
  return [
    `Действие: ${action.title}`,
    `Комментарий пользователя: ${comment || "(нет)"}`,
    `Текущая сота: ${counterpartyLabel(tunnel)}`,
    "",
    "ACTION_HINT:",
    JSON.stringify(hint),
    "",
    "Treat this as a lightweight action label, not a route or plan. Use current chat context, available tools, fresh proof, and on-demand capability discovery."
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
  window.history.replaceState({}, "", bareChatPath());
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
            window.history.replaceState({}, "", bareChatPath());
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
  ensurePermanentCells();
  selectFirstSurface();
  normalizeSelectedTunnel();
  for (const tunnel of tunnels) {
    ensureSync(tunnel);
  }

  const hasVisibleTunnels = sortedVisibleTunnels().length > 0;
  app.innerHTML = `
    <section class="shell retro-shell${bareChatMode ? " bare-chat-shell" : ""}${hiveDrawerOpen ? " hive-open" : ""}">
      <button class="hive-toggle retro-icon-button" type="button" aria-label="cells" data-tooltip="Cells">${icon("hexagon")}</button>
      <aside class="tiles hive-panel${hasVisibleTunnels ? "" : " empty"}" aria-hidden="${hiveDrawerOpen ? "false" : "true"}"${hiveDrawerOpen ? "" : " inert"}>
        <div class="retro-brand">
          <span class="retro-brand-mark">S</span>
          <span>
            <b>Соты</b>
            <small>мое место</small>
          </span>
        </div>
        <button class="info-open retro-icon-button" type="button" aria-label="инфа" data-tooltip="Инфа и безопасность">${icon("shield")}</button>
        <button class="qr-open retro-icon-button" type="button" aria-label="подключить" data-tooltip="Подключить контакт или устройство">${icon("qr")}</button>
        <div class="hex-field"></div>
      </aside>
      <main class="dialog-shell">
        <header class="dialog-head">
          <span class="dialog-avatar"></span>
          <span class="dialog-copy">
            <b class="dialog-name"></b>
            <small class="dialog-state">OFFLINE</small>
          </span>
          <span class="dialog-live" aria-live="polite">
            <span class="writer-pop"></span>
          </span>
          <button class="agent-mode-button retro-icon-button" type="button" aria-label="agent mode" data-tooltip="Agent">${icon("agent")}</button>
          <button class="clear-dialog-button retro-icon-button" type="button" aria-label="очистить" data-tooltip="Очистить диалог">${icon("refresh")}</button>
          <button class="access-open retro-icon-button" type="button" aria-label="доступы" data-tooltip="Доступы и устройства">${icon("shield")}</button>
          <button class="dialog-id" type="button" aria-label="поделиться" data-tooltip="Поделиться">${icon("copy")}</button>
          <button class="dialog-notify retro-icon-button" type="button" aria-label="включить оповещения" data-tooltip="Оповещения" hidden>${icon("bell")}</button>
        </header>
        <section class="cell-surface" aria-label="пространство соты">
          <div class="cell-app-shelf" aria-label="мини-аппы"></div>
          <section class="space-rail" aria-label="режимы соты"></section>
        </section>
        <section class="editor retro-screen">
          <div class="chat-scroll">
            <div class="text-paint" aria-live="polite"><div class="text-paint-inner chat-stream"></div></div>
          </div>
          <div class="agent-private-panel" hidden></div>
          <div class="line-gutter" aria-hidden="true"></div>
          <div class="line-meta" aria-hidden="true"></div>
          <textarea class="dialog-buffer" spellcheck="false" autocapitalize="sentences" aria-hidden="true" tabindex="-1"></textarea>
          <form class="composer-bar">
            <div class="composer-attachments" hidden></div>
            <button class="composer-attach retro-icon-button" type="button" aria-label="прикрепить" data-tooltip="Прикрепить файл">${icon("clip")}</button>
            <textarea class="chat-composer" rows="1" spellcheck="false" autocapitalize="sentences" aria-label="сообщение"></textarea>
            <button class="send-button retro-icon-button" type="submit" aria-label="send" data-tooltip="Отправить сообщение">${icon("send")}</button>
          </form>
        <div class="terminal-panel mini-app-panel" data-mini-app="commands" data-tooltip="Окно удаленных команд" data-tooltip-side="top">
          <div class="terminal-head">
            <span class="terminal-led"></span>
            <span class="terminal-peer"></span>
            <span class="terminal-title">Инструменты</span>
            <span class="terminal-status">готово</span>
            <button class="terminal-collapse" type="button" aria-label="collapse" data-tooltip="Свернуть окно команд">${icon("collapse")}</button>
          </div>
          <div class="agent-action-strip" hidden>
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
            <small class="chess-status">готово</small>
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
            <small class="mini-frame-status">готово</small>
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
  if (textPaint) {
    bindTextPaintInteractions(textPaint);
  }
  lineGutter = app.querySelector(".line-gutter");
  lineMeta = app.querySelector(".line-meta");
  fileInput = app.querySelector(".file-input");
  app.querySelector<HTMLDivElement>(".chat-scroll")?.addEventListener("scroll", () => {
    rememberCurrentChatScroll();
  }, { passive: true });
  app.querySelector<HTMLButtonElement>(".hive-toggle")?.addEventListener("click", () => {
    setHiveDrawerOpen(!hiveDrawerOpen);
  });
  app.querySelector<HTMLButtonElement>(".qr-open")?.addEventListener("click", () => {
    void showQr();
  });
  app.querySelector<HTMLButtonElement>(".agent-mode-button")?.addEventListener("click", () => {
    setSelectedAgentMode(!selectedAgentMode());
  });
  app.querySelector<HTMLButtonElement>(".info-open")?.addEventListener("click", openInfoPage);
  app.querySelector<HTMLButtonElement>(".access-open")?.addEventListener("click", () => {
    showAccessPanel();
  });
  app.querySelector<HTMLButtonElement>(".clear-dialog-button")?.addEventListener("click", () => {
    startFreshDialog();
  });
  app.querySelector<HTMLButtonElement>(".dialog-id")?.addEventListener("click", () => {
    void shareSelectedDialogLink();
  });
  app.querySelector<HTMLButtonElement>(".dialog-notify")?.addEventListener("click", () => {
    void enableSelectedNotifications();
  });
  renderTiles();
  composer?.addEventListener("input", () => rememberComposerDraft());
  composer?.addEventListener("keydown", (event) => {
    if (isMessageSendEnter(event)) {
      event.preventDefault();
      void finalizeComposerDraft();
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
  app.querySelector<HTMLIFrameElement>(".mini-frame")?.addEventListener("load", () => {
    publishMiniAppContext();
  });
  setupSplitter();
  applySelectedText();
  renderComposerAttachments();
  renderTerminal();
  renderChess();
  renderMiniAppPanel();
  void ensureOperatorBridge(true);
  resumeAgentSourceControl();
  resumeRemoteHostSourceGrantControl();
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
      if (window.matchMedia("(max-width: 980px)").matches) {
        setHiveDrawerOpen(false);
      }
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
      const canClose = !tunnel || !isPermanentCell(tunnel);
      const availableMiniApps = globalMiniApps();
      openCounterpartyMenu(x, y, {
        attach: () => {
          selectTunnel(id);
          fileInput?.click();
        },
        knock: () => {
          selectTunnel(id);
          void requestNotificationPermission();
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
          openMiniAppGallery();
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
              requestAgentDownload(isAgentTunnelId(id) ? device || undefined : undefined, "Клава ставится один раз и потом дает управляемые инструменты для выбранных устройств.");
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

async function enableRemoteGrant(id: string, requestedTargetDeviceId = ""): Promise<boolean> {
  const mode = await refreshAgentButtonState(true);
  if (mode !== "link") {
    markAgentDownloadNeeded();
    requestAgentDownload(undefined, "Команды на устройстве идут через Клаву. Сначала поставьте локальную программу.");
    return false;
  }

  const targetDeviceId = resolveRemoteGrantTarget(id, requestedTargetDeviceId);
  if (!targetDeviceId) {
    await showTrustModal({
      title: "Нужен точный получатель",
      lead: "В этой соте нет одного понятного устройства для доступа. Пусть нужное устройство запросит доступ само.",
      facts: ["Так право не попадет другому телефону или компьютеру.", "Запрос придет отдельным окном с именем устройства."],
      primaryLabel: "Понятно",
      cancelLabel: "",
      icon: "shield"
    });
    return false;
  }
  if (!(await confirmRemoteGrant(id, targetDeviceId))) {
    return false;
  }

  remoteEnabled = setRemoteEnabled(id, true);
  remoteGrantTargets = setRemoteGrantTarget(id, targetDeviceId, true);
  syncs.get(id)?.grantRemote(true, targetDeviceId);
  startRemoteHostSourceGrantControl();
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
  const mode = await refreshAgentButtonState(true);
  if (mode !== "link") {
    markAgentDownloadNeeded();
    requestAgentDownload(device, "Чтобы Клава могла работать с этого устройства, нужен локальный machine-агент.");
    return;
  }
  const companion = await ensureAgentSourceCompanion();
  if (!companion.ok) {
    markAgentDownloadNeeded();
    requestAgentDownload(device, "Клава не отвечает на локальном канале. Установщик проверит версию, автозапуск и связь.");
    return;
  }
  if (!isAgentSourceCompanionReady(companion, device.id)) {
    markAgentDownloadNeeded();
    requestAgentDownload(device, "Клава установлена не для этого устройства. Нужна привязка к текущей соте.");
    return;
  }
  if (!(await confirmRemoteGrant(agentTunnelId, device.id, true))) {
    return;
  }
  const granted = await grantAgentSourceAccess(device.id, device.nick, true, agentSourceClientState());
  if (!granted) {
    await typeOperatorChat(
      agentTunnelId,
      formatOperatorChat("Клава не смогла подключить командный канал. Проверь интернет: чат сам повторит подключение.", "sysadmin"),
      "fast"
    );
    return;
  }
  remoteEnabled = setRemoteEnabled(agentTunnelId, true);
  remoteGrantTargets = setRemoteGrantTarget(agentTunnelId, device.id, true);
  agentSourceGrantRefreshAt = Date.now() + agentSourceGrantRefreshMs;
  terminalOpenId = agentTunnelId;
  setTerminalState(agentTunnelId, "idle");
  appendTerminalLine(agentTunnelId, "+ инструменты");
  renderTerminal();
  renderTiles();
  startAgentSourceControl(agentTunnelId);
  startRemoteHostSourceGrantControl();
}

function resolveRemoteGrantTarget(tunnelId: string, requestedTargetDeviceId = ""): string {
  const requested = requestedTargetDeviceId.trim();
  if (requested && requested !== "*") {
    return requested;
  }
  const stored = remoteGrantTargets.get(tunnelId);
  if (stored && stored !== "*") {
    return stored;
  }
  const peerIds = uniquePeerDeviceIds(tunnelId);
  return peerIds.length === 1 ? peerIds[0] || "" : "";
}

function uniquePeerDeviceIds(tunnelId: string): string[] {
  return [...new Set((peerDevices.get(tunnelId) ?? [])
    .map((peer) => peer.id.trim())
    .filter(Boolean))];
}

function remoteGrantTargetLabel(tunnelId: string, targetDeviceId: string): string {
  const peer = (peerDevices.get(tunnelId) ?? []).find((item) => item.id === targetDeviceId);
  return cleanNick(peer?.nick || "") || targetDeviceId.slice(0, 12) || "устройство";
}

async function confirmRemoteGrant(tunnelId: string, targetDeviceId: string, agentSource = false): Promise<boolean> {
  const tunnel = loadTunnels().find((item) => item.id === tunnelId);
  const label = tunnel ? counterpartyLabel(tunnel) : "сота";
  const target = agentSource
    ? cleanNick(device?.nick || "") || "это устройство"
    : remoteGrantTargetLabel(tunnelId, targetDeviceId);
  return showTrustModal({
    title: agentSource ? "Дать Клаве инструменты" : "Открыть доступ",
    lead: agentSource
      ? "Клава сможет выполнять задачи на этом устройстве, пока доступ включен."
      : `${label} сможет отправлять команды только на выбранное устройство.`,
    facts: [
      `Устройство: ${target}`,
      "Разрешение можно отключить повторным нажатием или кнопкой щита.",
      agentSource ? "Файлы и команды остаются в выбранном контексте Сот." : "Если в соте появится другое устройство, оно не получит это право автоматически."
    ],
    primaryLabel: agentSource ? "Разрешить Клаву" : "Открыть доступ",
    cancelLabel: "Не сейчас",
    icon: "shield"
  });
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

function announceRemoteGrant(tunnelId: string, targetDeviceId = ""): void {
  if (!remoteEnabled.has(tunnelId)) {
    return;
  }
  const target = resolveRemoteGrantTarget(tunnelId, targetDeviceId);
  if (!target) {
    return;
  }
  remoteGrantTargets = setRemoteGrantTarget(tunnelId, target, true);
  syncs.get(tunnelId)?.grantRemote(true, target);
  startRemoteHostSourceGrantControl();
}

function resumeRemoteHostSourceGrantControl(): void {
  if (remoteHostSourceGrantTunnels().length === 0) {
    stopRemoteHostSourceGrantControl();
    return;
  }
  startRemoteHostSourceGrantControl();
}

function startRemoteHostSourceGrantControl(): void {
  window.clearTimeout(remoteHostSourceGrantTimer);
  remoteHostSourceGrantTimer = window.setTimeout(() => void pollRemoteHostSourceGrant(), 0);
}

function stopRemoteHostSourceGrantControl(): void {
  window.clearTimeout(remoteHostSourceGrantTimer);
  remoteHostSourceGrantTimer = 0;
  remoteHostSourceGrantPolling = false;
  remoteHostSourceGrantRefreshAt = 0;
}

function remoteHostSourceGrantTunnels(): TunnelRecord[] {
  return loadTunnels().filter((tunnel) => !tunnel.archived && remoteEnabled.has(tunnel.id));
}

async function pollRemoteHostSourceGrant(): Promise<void> {
  if (remoteHostSourceGrantPolling || !device) {
    return;
  }
  if (remoteHostSourceGrantTunnels().length === 0) {
    stopRemoteHostSourceGrantControl();
    return;
  }
  remoteHostSourceGrantPolling = true;
  try {
    const now = Date.now();
    if (remoteHostSourceGrantRefreshAt <= now) {
      remoteHostSourceGrantRefreshAt = now + agentSourceGrantRefreshMs;
      const ok = await refreshCurrentDeviceSourceGrant();
      if (!ok) {
        remoteHostSourceGrantRefreshAt = now + 5000;
      }
    }
  } finally {
    remoteHostSourceGrantPolling = false;
    if (device && remoteHostSourceGrantTunnels().length > 0) {
      remoteHostSourceGrantTimer = window.setTimeout(() => void pollRemoteHostSourceGrant(), agentSourceGrantRefreshMs);
    }
  }
}

async function refreshCurrentDeviceSourceGrant(): Promise<boolean> {
  if (!device) {
    return false;
  }
  localAgent = await ensureAgentSourceCompanion();
  if (!isAgentSourceCompanionReady(localAgent, device.id)) {
    return false;
  }
  return await grantAgentSourceAccess(device.id, device.nick, true, agentSourceClientState(), 2500);
}

function maybeRevokeCurrentDeviceSourceGrant(): void {
  if (!device || remoteHostSourceGrantTunnels().length > 0) {
    return;
  }
  stopRemoteHostSourceGrantControl();
  void grantAgentSourceAccess(device.id, device.nick, false);
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

function requestAgentDownload(sourceDeviceForInstaller?: { readonly id?: string; readonly nick?: string }, reason = ""): void {
  void showAgentInstallPassport(sourceDeviceForInstaller, reason);
}

function startAgentDownload(sourceDeviceForInstaller?: { readonly id?: string; readonly nick?: string }): void {
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

async function showAgentInstallPassport(sourceDeviceForInstaller?: { readonly id?: string; readonly nick?: string }, reason = ""): Promise<void> {
  const release = await refreshAgentRelease(true);
  const mode = agentButtonMode();
  const version = release?.version || agentRelease?.version || "последняя";
  const sha = release?.sha256 || agentRelease?.sha256 || "";
  const shortSha = sha ? `${sha.slice(0, 12)}…${sha.slice(-8)}` : "в manifest.json";
  const ok = await showTrustModal({
    title: mode === "update" ? "Обновить Клаву" : "Установить Клаву",
    lead: reason || "Клава нужна только для действий на устройстве: команды, файлы, проверка состояния.",
    facts: [
      `Версия: ${version}`,
      `SHA-256: ${shortSha}`,
      "Установщик попросит права администратора только для machine-агента.",
      "Сам доступ к устройствам включается отдельно."
    ],
    primaryLabel: mode === "update" ? "Скачать обновление" : "Скачать",
    cancelLabel: "Не сейчас",
    icon: "download",
    footerHtml: `<a href="/agent/manifest.json" target="_blank" rel="noopener noreferrer">manifest.json</a>`
  });
  if (ok) {
    startAgentDownload(sourceDeviceForInstaller);
  }
}

function showAccessPanel(): void {
  showAccessPanelModal(accessPanelRows(), () => {
    for (const tunnelId of [...new Set([...remoteEnabled, ...remoteAccess.keys()])]) {
      closeRemoteMode(tunnelId);
    }
  }, infoPagePath);
}

function accessPanelRows(): AccessPanelRow[] {
  const visible = sortedVisibleTunnels();
  const rows: AccessPanelRow[] = [];
  for (const tunnel of visible) {
    if (remoteEnabled.has(tunnel.id)) {
      const target = remoteGrantTargets.get(tunnel.id);
      rows.push({
        kind: "открыто",
        label: counterpartyLabel(tunnel),
        detail: target && target !== "*" ? `получатель: ${remoteGrantTargetLabel(tunnel.id, target)}` : "ожидает точный запрос устройства"
      });
    }
    const hostDeviceId = remoteAccess.get(tunnel.id);
    if (hostDeviceId) {
      rows.push({
        kind: "управляю",
        label: counterpartyLabel(tunnel),
        detail: `удаленное устройство: ${remoteGrantTargetLabel(tunnel.id, hostDeviceId)}`
      });
    }
  }
  return rows;
}

function closeRemoteMode(tunnelId: string): void {
  const sync = syncs.get(tunnelId);
  const hostDeviceId = remoteAccess.get(tunnelId);
  if (remoteEnabled.has(tunnelId)) {
    const grantTarget = remoteGrantTargets.get(tunnelId) || "*";
    remoteEnabled = setRemoteEnabled(tunnelId, false);
    remoteGrantTargets = setRemoteGrantTarget(tunnelId, "", false);
    sync?.grantRemote(false, grantTarget);
    if (isAgentTunnelId(tunnelId) && device) {
      stopAgentSourceControl(tunnelId);
    }
    maybeRevokeCurrentDeviceSourceGrant();
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
    .map((tunnel, index) => ({ tunnel, index }))
    .filter((item) => isSelectableCell(item.tunnel))
    .sort((a, b) => {
      const rank = permanentCellRank(b.tunnel) - permanentCellRank(a.tunnel);
      if (rank !== 0) {
        return rank;
      }
      return a.index - b.index;
    })
    .map((item) => item.tunnel);
}

function permanentCellRank(tunnel: TunnelRecord): number {
  if (isSelfTunnel(tunnel)) {
    return 2;
  }
  return 0;
}

function isSelectableCell(tunnel: TunnelRecord): boolean {
  return !tunnel.archived && !isAgentTunnel(tunnel) && hasCounterparty(tunnel);
}

function isSimpleContactSurface(tunnel: TunnelRecord): boolean {
  return bareChatMode
    && !isPermanentCell(tunnel)
    && !isAgentTunnel(tunnel)
    && hasCounterparty(tunnel)
    && !selectedAgentMode(tunnel.id)
    && !remoteAccess.has(tunnel.id)
    && !remoteEnabled.has(tunnel.id);
}

function normalizeSelectedTunnel(): void {
  const all = loadTunnels();
  if (all.length === 0) {
    selectedId = "";
    return;
  }
  const visible = all.filter(isSelectableCell);
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

function selectFirstSurface(): void {
  if (!firstSurfacePending) {
    return;
  }
  firstSurfacePending = false;
  if (hasVisibleSelection(selectedId)) {
    return;
  }
  const self = loadTunnels().find((tunnel) => !tunnel.archived && isSelfTunnel(tunnel));
  if (!self) {
    return;
  }
  selectedId = self.id;
  saveSelectedTunnelId(self.id);
}

function selectTunnel(id: string): void {
  rememberCurrentChatScroll();
  openMessageDialog = null;
  selectedId = id;
  saveSelectedTunnelId(id);
  publishMiniAppContext();
}

function hasVisibleSelection(id = selectedId): boolean {
  return Boolean(id && loadTunnels().some((tunnel) => tunnel.id === id && isSelectableCell(tunnel)));
}

function hasCounterparty(tunnel: TunnelRecord): boolean {
  if (isPermanentCell(tunnel)) {
    return true;
  }
  const label = cleanNick(peers.get(tunnel.id) || tunnel.label || "");
  return Boolean(tunnel.counterparty || peers.has(tunnel.id) || (label !== "." && label !== device?.nick));
}

function isOwnSpace(tunnel: TunnelRecord | null | undefined): boolean {
  return Boolean(tunnel && isSelfTunnel(tunnel));
}

function counterpartyLabel(tunnel: TunnelRecord): string {
  if (isSelfTunnel(tunnel)) {
    return selfCellLabel;
  }
  const label = rawCounterpartyLabel(tunnel);
  if (tunnel.agent === true) {
    return agentDialogLabel;
  }
  return label;
}

type PermanentCellKind = "self" | "agent";

type CellRecordOptions = {
  readonly label?: string;
  readonly counterparty?: boolean;
  readonly agent?: boolean;
  readonly self?: boolean;
  readonly score?: number;
  readonly color?: string;
  readonly colorSeed?: string;
};

type PermanentCellSpec = {
  readonly kind: PermanentCellKind;
  readonly label: string;
  readonly score: number;
  readonly colorSeed: string;
  readonly match: (tunnel: TunnelRecord) => boolean;
};

function createCellRecord(options: CellRecordOptions = {}): TunnelRecord {
  const label = cleanNick(options.label || "");
  const counterparty = options.counterparty ?? Boolean(label);
  const cell = createTunnel(label, counterparty);
  const colorSeed = options.colorSeed || `${label || "cell"}:${cell.createdAt}`;
  return {
    ...cell,
    label,
    counterparty,
    ...(options.agent === true ? { agent: true } : {}),
    ...(options.self === true ? { self: true } : {}),
    score: typeof options.score === "number" ? options.score : (cell.score ?? 0),
    color: options.color || colorFor(colorSeed)
  };
}

function addCell(
  options: CellRecordOptions = {},
  behavior: { readonly archiveSelected?: boolean; readonly select?: boolean } = {}
): TunnelRecord | null {
  if (!device) {
    return null;
  }
  const current = loadTunnels();
  const active = current.find((tunnel) => tunnel.id === selectedId);
  const archiveSelected = behavior.archiveSelected === true;
  const now = new Date().toISOString();
  const fresh = createCellRecord(options);
  const next = [
    fresh,
    ...current.map((tunnel) => tunnel.id === selectedId && archiveSelected
      ? { ...tunnel, archived: true, unread: false, updatedAt: now, lastActionAt: now }
      : tunnel)
  ];
  saveTunnels(next);
  tunnels = next;
  if (behavior.select !== false) {
    selectedId = fresh.id;
    saveSelectedTunnelId(fresh.id);
  }
  localDrafts.delete(active?.id || "");
  texts.set(fresh.id, "");
  saveTextSnapshotNow(fresh.id, "");
  ensureSync(fresh);
  return fresh;
}

function permanentCellSpecs(now = new Date().toISOString()): readonly PermanentCellSpec[] {
  return [
    {
      kind: "self",
      label: selfCellLabel,
      score: 2_000_000,
      colorSeed: `self:${device?.id || now}`,
      match: isSelfTunnel
    }
  ];
}

function ensurePermanentCells(): void {
  let current = loadTunnels();
  const now = new Date().toISOString();
  let changed = false;
  const duplicateIds: string[] = [];

  for (const spec of permanentCellSpecs(now)) {
    const matches = current.filter(spec.match);
    if (matches.length === 0) {
      const cell = createCellRecord({
        label: spec.label,
        counterparty: true,
        self: spec.kind === "self",
        agent: spec.kind === "agent",
        score: spec.score,
        colorSeed: spec.colorSeed
      });
      current = [cell, ...current];
      changed = true;
      if (!selectedId || !current.some((tunnel) => tunnel.id === selectedId && !tunnel.archived)) {
        selectedId = cell.id;
        saveSelectedTunnelId(cell.id);
      }
      continue;
    }

    const canonical = chooseCanonicalPermanentCell(matches);
    const duplicateMatches = matches.filter((tunnel) => tunnel.id !== canonical.id);
    duplicateIds.push(...duplicateMatches.map((tunnel) => tunnel.id));
    const enabledIds = matches.filter((tunnel) => remoteEnabled.has(tunnel.id)).map((tunnel) => tunnel.id);
    if (enabledIds.some((id) => id !== canonical.id)) {
      remoteEnabled = setRemoteEnabled(canonical.id, true);
      const target = remoteGrantTargets.get(canonical.id)
        || enabledIds.map((id) => remoteGrantTargets.get(id)).find(Boolean)
        || "";
      if (target) {
        remoteGrantTargets = setRemoteGrantTarget(canonical.id, target, true);
      }
      for (const id of enabledIds) {
        if (id !== canonical.id) {
          remoteEnabled = setRemoteEnabled(id, false);
          remoteGrantTargets = setRemoteGrantTarget(id, "", false);
        }
      }
    }

    current = current.map((tunnel) => {
      if (!spec.match(tunnel)) {
        return tunnel;
      }
      if (tunnel.id !== canonical.id) {
        changed = true;
        return null;
      }
      const normalized = normalizePermanentCell(tunnel, spec, now);
      if (!samePermanentCell(tunnel, normalized)) {
        changed = true;
      }
      return normalized;
    }).filter((tunnel): tunnel is TunnelRecord => tunnel !== null);
  }

  if (!changed) {
    tunnels = current;
  } else {
    saveTunnels(current);
    tunnels = current;
    forgetDuplicateCells(duplicateIds);
  }
  if (!selectedId || !current.some((tunnel) => tunnel.id === selectedId && !tunnel.archived)) {
    selectedId = current.find(isSelfTunnel)?.id || current.find(isSelectableCell)?.id || current.find((tunnel) => !tunnel.archived && !isAgentTunnel(tunnel))?.id || "";
  }
  if (selectedId) {
    saveSelectedTunnelId(selectedId);
  }
}

function normalizePermanentCell(tunnel: TunnelRecord, spec: PermanentCellSpec, now: string): TunnelRecord {
  const base: TunnelRecord & { agent?: boolean; self?: boolean } = { ...tunnel };
  delete base.agent;
  delete base.self;
  return {
    ...base,
    ...(spec.kind === "agent" ? { agent: true } : {}),
    ...(spec.kind === "self" ? { self: true } : {}),
    label: spec.label,
    counterparty: true,
    archived: false,
    unread: tunnel.unread,
    score: Math.max(tunnel.score ?? 0, spec.score),
    color: tunnel.color || colorFor(spec.colorSeed),
    updatedAt: tunnel.updatedAt || now,
    lastActionAt: tunnel.lastActionAt || tunnel.updatedAt || now
  };
}

function samePermanentCell(a: TunnelRecord, b: TunnelRecord): boolean {
  return a.agent === b.agent
    && a.self === b.self
    && a.label === b.label
    && a.counterparty === b.counterparty
    && a.archived === b.archived
    && a.unread === b.unread
    && a.score === b.score
    && a.color === b.color
    && a.updatedAt === b.updatedAt
    && a.lastActionAt === b.lastActionAt;
}

function chooseCanonicalPermanentCell(items: readonly TunnelRecord[]): TunnelRecord {
  const selected = items.find((tunnel) => tunnel.id === selectedId);
  return mostRecentTunnel(items.filter((tunnel) => remoteEnabled.has(tunnel.id)))
    || selected
    || mostRecentTunnel(items.filter((tunnel) => !tunnel.archived))
    || mostRecentTunnel(items)
    || items[0]!;
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

function forgetDuplicateCells(ids: readonly string[]): void {
  if (ids.length === 0) {
    return;
  }
  for (const id of ids) {
    syncs.get(id)?.destroy();
    syncs.delete(id);
    syncStates.delete(id);
    peerDevices.delete(id);
    remoteEnabled = setRemoteEnabled(id, false);
    remoteGrantTargets = setRemoteGrantTarget(id, "", false);
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
    agentModes.delete(id);
    agentPrivateLogs.delete(id);
    localDrafts.delete(id);
    pendingAttachments.delete(id);
    files.delete(id);
    fileNotices.delete(id);
    texts.delete(id);
  }
  saveAgentModes();
  saveAgentPrivateLogs();
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
  openMessageDialog = null;
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

function clearCurrentDialog(tunnelId: string): void {
  if (openMessageDialog?.chatId === tunnelId) {
    openMessageDialog = null;
  }
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
  agentPrivateLogs.delete(tunnelId);
  saveAgentPrivateLogs();
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

function findActiveAgentDialog(): TunnelRecord | null {
  return sortedVisibleTunnels().find((tunnel) => isAgentTunnel(tunnel))
    || loadTunnels().find((tunnel) => !tunnel.archived && isAgentTunnel(tunnel))
    || null;
}

function isAgentTunnel(tunnel: TunnelRecord): boolean {
  return tunnel.agent === true;
}

function isSelfTunnel(tunnel: TunnelRecord): boolean {
  return tunnel.self === true;
}

function isPermanentCell(tunnel: TunnelRecord): boolean {
  return isSelfTunnel(tunnel) || isAgentTunnel(tunnel);
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
  return addCell({
    label,
    counterparty: true,
    agent: isAgent,
    score: isAgent ? 1_900_000 : 0,
    color: (archiveCurrent ? active?.color : "") || colorFor(`${label}:${now}`)
  }, {
    archiveSelected: archiveCurrent,
    select: true
  });
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
  const label = tunnel ? counterpartyLabel(tunnel) : "";
  const color = tunnel ? safeColor(tunnel.color, label + tunnel.id) : "#67e8f9";
  const head = app.querySelector<HTMLElement>(".dialog-head");
  const avatar = app.querySelector<HTMLElement>(".dialog-avatar");
  const name = app.querySelector<HTMLElement>(".dialog-name");
  const state = app.querySelector<HTMLElement>(".dialog-state");
  const id = app.querySelector<HTMLButtonElement>(".dialog-id");
  const notifyButton = app.querySelector<HTMLButtonElement>(".dialog-notify");
  const shell = app.querySelector<HTMLElement>(".dialog-shell");
  const appShell = app.querySelector<HTMLElement>(".shell");
  const editor = app.querySelector<HTMLElement>(".editor");
  const remoteButton = app.querySelector<HTMLButtonElement>(".remote-action");
  const agentButton = app.querySelector<HTMLButtonElement>(".agent-mode-button");
  const sendButton = app.querySelector<HTMLButtonElement>(".send-button");
  const mode = agentButtonMode();
  const agentTunnel = tunnel ? isAgentTunnel(tunnel) : false;
  const agentMode = Boolean(tunnel && selectedAgentMode(tunnel.id));
  const simpleContactSurface = Boolean(tunnel && isSimpleContactSurface(tunnel));
  const publicContactUrl = tunnel ? publicContactUrlForTunnel(tunnel) : "";
  const availableMiniApps = globalMiniApps();
  if (miniAppSession) {
    const activeMiniAppKey = miniAppRecordKey(miniAppSession.app);
    if (!availableMiniApps.some((item) => miniAppRecordKey(item) === activeMiniAppKey)) {
      miniAppSession = null;
      renderMiniAppPanel();
    }
  }
  if (shell) {
    shell.style.setProperty("--peer-color", color);
    shell.classList.toggle("agent-mode-active", agentMode);
    shell.classList.toggle("simple-contact-surface", simpleContactSurface);
  }
  appShell?.classList.toggle("agent-mode-active", agentMode);
  appShell?.classList.toggle("simple-contact-shell", simpleContactSurface);
  editor?.classList.toggle("agent-mode-active", agentMode);
  editor?.classList.toggle("simple-contact-surface", simpleContactSurface);
  const offerNotifications = simpleContactSurface && shouldOfferNotifications();
  head?.classList.toggle("has-notification-offer", offerNotifications);
  if (notifyButton) {
    notifyButton.hidden = !offerNotifications;
    notifyButton.setAttribute("aria-label", "включить оповещения");
    notifyButton.dataset.tooltip = "Оповещения";
  }
  if (avatar) {
    avatar.textContent = label ? initials(label) : "";
  }
  if (name) {
    name.textContent = label;
  }
  if (state) {
    let remote = tunnel && isSelfTunnel(tunnel) ? "мое место" : "на связи";
    if (agentTunnel && mode === "download") {
      remote = "подключить Клаву";
    } else if (agentTunnel && mode === "update") {
      remote = "обновить Клаву";
    } else if (agentTunnel) {
      remote = remoteEnabled.has(selectedId) ? "Клава рядом" : "Клава";
    } else if (remoteAccess.has(selectedId)) {
      remote = "доступ открыт";
    } else if (remoteEnabled.has(selectedId)) {
      remote = "можно подключиться";
    }
    const syncState = selectedId ? syncStates.get(selectedId) : "";
    const syncSuffix = syncState === "connecting" ? " · соединяем" : syncState === "closed" ? " · не в сети" : "";
    state.textContent = `${remote}${syncSuffix}`;
  }
  if (id) {
    const code = selectedDialogCode();
    const canShare = Boolean(publicContactUrl || code);
    id.innerHTML = canShare ? icon(publicContactUrl ? "qr" : "copy") : "";
    id.disabled = !canShare;
    id.setAttribute("aria-label", publicContactUrl ? "поделиться контактом" : code ? "скопировать ссылку" : "сота не выбрана");
    if (id.dataset.copied !== "1") {
      id.dataset.tooltip = publicContactUrl ? "Поделиться контактом" : code ? `Скопировать ссылку · ${code}` : "Сота не выбрана";
    }
  }
  if (sendButton) {
    const stopping = selectedSpaceMode() === "dialog" && agentThinking.has(selectedId);
    sendButton.classList.toggle("is-stop", stopping);
    sendButton.setAttribute("aria-label", stopping ? "остановить" : "отправить");
    sendButton.dataset.tooltip = stopping ? "Остановить" : agentMode ? "Agent" : "Отправить";
    sendButton.innerHTML = icon(stopping ? "stop" : agentMode ? "agent" : "send");
  }
  if (agentButton) {
    agentButton.hidden = !tunnel || selectedSpaceMode() !== "dialog";
    agentButton.classList.toggle("is-on", agentMode);
    agentButton.setAttribute("aria-pressed", agentMode ? "true" : "false");
    agentButton.dataset.tooltip = agentMode ? "Agent on" : "Agent";
  }
  if (remoteButton) {
    const needsAgent = mode !== "link";
    remoteButton.hidden = !needsAgent;
    remoteButton.classList.toggle("is-on", false);
    remoteButton.classList.toggle("has-access", false);
    remoteButton.classList.toggle("needs-agent", needsAgent);
    if (needsAgent) {
      remoteButton.setAttribute("aria-label", mode === "update" ? "update" : "download");
      remoteButton.innerHTML = `${icon("download")}<span>${mode === "update" ? "Обновить" : "Подключить"}</span>`;
      remoteButton.dataset.tooltip = mode === "update" ? "Обновить Клаву" : "Подключить Клаву";
    }
  }
  renderCellAppShelf();
  publishMiniAppContext();
  renderSpace();
  updateComposerSpaceMode();
  renderAgentPrivatePanel();
}

async function enableSelectedNotifications(): Promise<void> {
  await requestNotificationPermission();
  renderDialogChrome();
}

function renderSpace(): void {
  const rail = app.querySelector<HTMLDivElement>(".space-rail");
  if (!rail) {
    return;
  }
  const model = selectedSpaceModel();
  if (!model) {
    rail.innerHTML = "";
    return;
  }
  rail.innerHTML = renderSpaceRail(model);
  rail.querySelectorAll<HTMLButtonElement>("[data-space-mode]").forEach((button) => {
    button.addEventListener("click", () => {
      setSelectedSpaceMode(normalizeSpaceMode(button.dataset.spaceMode || "dialog"));
    });
  });
}

function renderAgentPrivatePanel(): void {
  const panel = app.querySelector<HTMLDivElement>(".agent-private-panel");
  if (!panel) {
    return;
  }
  const active = selectedAgentMode();
  panel.hidden = !active;
  if (!active) {
    panel.innerHTML = "";
    return;
  }
  const lines = agentPrivateLogs.get(selectedId) ?? [];
  panel.innerHTML = lines.length > 0
    ? lines.map((line) => `
      <article class="agent-private-line ${line.role}" title="${escapeHtml(clock(new Date(line.createdAt)))}">
        <span>${icon(line.role === "agent" ? "agent" : "person")}</span>
        <p>${line.text.split("\n").map((item) => item ? `<span>${linkifyChatLine(item)}</span>` : "<br>").join("")}</p>
      </article>
    `).join("")
    : `<div class="agent-private-empty">${icon("agent")}</div>`;
  panel.scrollTop = panel.scrollHeight;
}

function selectedSpaceModel(): SpaceModel | null {
  const tunnel = loadTunnels().find((item) => item.id === selectedId);
  if (!tunnel) {
    return null;
  }
  const label = counterpartyLabel(tunnel);
  const color = safeColor(tunnel.color, label + tunnel.id);
  return {
    id: tunnel.id,
    color,
    mode: selectedSpaceMode(),
    ownSpace: isOwnSpace(tunnel)
  };
}

function updateComposerSpaceMode(): void {
  if (!composer) {
    return;
  }
  const mode = selectedSpaceMode();
  const tunnel = loadTunnels().find((item) => item.id === selectedId);
  const access = composerAccessFor(tunnel, mode);
  const agentMode = Boolean(tunnel && selectedAgentMode(tunnel.id));
  composer.placeholder = agentMode ? "" : access.placeholder;
  composer.disabled = !access.canCompose;
  const bar = app.querySelector<HTMLFormElement>(".composer-bar");
  if (bar) {
    bar.hidden = !access.canCompose;
    bar.dataset.spaceMode = mode;
    bar.classList.toggle("is-wall-mode", mode === "wall");
    bar.classList.toggle("is-reputation-mode", mode === "reputation");
    bar.classList.toggle("is-agent-mode", agentMode);
    bar.classList.toggle("is-readonly-space", !access.canCompose);
  }
  app.querySelector<HTMLButtonElement>(".composer-attach")?.toggleAttribute("disabled", !access.canCompose || agentMode);
  app.querySelector<HTMLButtonElement>(".send-button")?.toggleAttribute("disabled", !access.canCompose);
}

function composerAccessFor(tunnel: TunnelRecord | null | undefined, mode = selectedSpaceMode()): SpaceComposerAccess {
  if (!tunnel) {
    return { canCompose: false, entryKind: null, placeholder: "" };
  }
  return spaceComposerAccess(mode, counterpartyLabel(tunnel), isOwnSpace(tunnel));
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
    onWriterLines: (lines) => {
      applySyncedWriterLines(tunnel.id, lines);
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
      const visibleChange = remoteActivityTouchesVisibleChat(tunnel.id, activity);
      if (visibleChange) {
        maybeKnockForTyping(tunnel.id, activity, hadNotice);
      }
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
      if (visibleChange && (tunnel.id !== selectedId || document.visibilityState === "hidden")) {
        tunnels = markTunnel(tunnel.id, true);
        renderTiles();
      } else {
        tunnels = touchTunnel(tunnel.id);
        if (tunnel.id === selectedId) {
          renderLineTags();
          renderTextPaint();
          renderWriterPop();
        }
      }
    },
    onFile: (file) => {
      const next = [file, ...(files.get(tunnel.id) ?? []).filter((item) => item.id !== file.id)];
      files.set(tunnel.id, next);
      if (file.historical !== true) {
        const hadNotice = tunnelHasNotice(tunnel.id);
        maybeAutoDownloadReceivedFile(tunnel.id, file);
        vibrateHiddenOnce(`file:${file.id}`, tunnel.id, hadNotice, {
          title: cleanNick(file.nick) || counterpartyLabel(tunnel),
          body: file.name ? `Файл: ${file.name}` : "Файл"
        });
        tunnels = tunnel.id === selectedId ? touchTunnel(tunnel.id) : markTunnel(tunnel.id, true);
      }
      if (tunnel.id === selectedId) {
        renderTextPaint();
        renderComposerAttachments();
      }
      if (file.historical !== true) {
        renderTiles();
      }
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
        .map((appItem) => miniAppFromSynced(appItem, tunnel.id))
        .filter((item): item is MiniAppDefinition => Boolean(item)));
      if (tunnel.id === selectedId) {
        miniApps = currentMiniApps();
        if (miniAppSession) {
          const activeMiniAppKey = miniAppRecordKey(miniAppSession.app);
          if (!miniApps.some((item) => miniAppRecordKey(item) === activeMiniAppKey)) {
            miniAppSession = null;
            renderMiniAppPanel();
          }
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
  if (tunnel && isPermanentCell(tunnel)) {
    selectTunnel(id);
    renderTiles();
    return;
  }
  const sync = syncs.get(id);
  sync?.closeForEveryone();
  syncs.delete(id);
  syncStates.delete(id);
  remoteEnabled = setRemoteEnabled(id, false);
  remoteGrantTargets = setRemoteGrantTarget(id, "", false);
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

function stageFiles(list?: FileList | null): void {
  if (!selectedId) {
    return;
  }
  const tunnelId = selectedId;
  const tunnel = loadTunnels().find((item) => item.id === tunnelId);
  if (!composerAccessFor(tunnel).canCompose) {
    return;
  }
  if (selectedAgentMode(tunnelId)) {
    return;
  }
  const accepted = filesFrom(list);
  const oversized = oversizedFilesFrom(list);
  const current = pendingAttachments.get(tunnelId) ?? [];
  const limit = attachmentLimitForComposer(tunnelId);
  const openSlots = Math.max(0, limit - current.length);
  const nextFiles = accepted.slice(0, openSlots).map(pendingAttachmentFromFile);
  const rejectedByCount = accepted.length - nextFiles.length;
  if (nextFiles.length > 0) {
    pendingAttachments.set(tunnelId, [...current, ...nextFiles]);
  }
  if (oversized.length > 0 || rejectedByCount > 0) {
    const parts = [
      oversized.length > 0 ? `File is too large: current browser transfer limit is ${formatFileSize(maxFileBytes)}` : "",
      rejectedByCount > 0 ? `Диалог с агентом принимает до ${agentAttachmentLimit} файлов за раз` : ""
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
  const limit = attachmentLimitForComposer(tunnelId);
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

function attachmentLimitForComposer(tunnelId: string): number {
  if (selectedAgentMode(tunnelId)) {
    return 0;
  }
  return selectedSpaceMode() === "dialog" && isAgentTunnelId(tunnelId)
    ? agentAttachmentLimit
    : Number.POSITIVE_INFINITY;
}

function renderComposerAttachments(): void {
  const root = app.querySelector<HTMLDivElement>(".composer-attachments");
  if (!root) {
    return;
  }
  const tunnel = loadTunnels().find((item) => item.id === selectedId);
  if (selectedAgentMode()) {
    root.hidden = true;
    root.innerHTML = "";
    return;
  }
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

function remoteActivityTouchesVisibleChat(tunnelId: string, activity: WriterActivity): boolean {
  if (activity.local) {
    return false;
  }
  if ((activity.deleteCount ?? 0) > 0) {
    return true;
  }
  const inserted = activity.insertText || "";
  const lines = inserted
    .split(/\r?\n/u)
    .map((line) => line.trim())
    .filter(Boolean);
  if (lines.length === 0) {
    return Boolean(activity.preview.trim());
  }
  return lines.some((line) => !isSilentProtocolNoticeLine(tunnelId, line));
}

function isSilentProtocolNoticeLine(tunnelId: string, line: string): boolean {
  if (!isMessageDialogLine(line)) {
    return false;
  }
  const entry = parseMessageDialogLine(line);
  if (!entry || entry.chatId !== tunnelId) {
    return true;
  }
  const viewer = {
    deviceId: device?.id || "",
    cellIds: loadTunnels().filter((tunnel) => !tunnel.archived).map((tunnel) => tunnel.id)
  };
  return !messageDialogVisibleForTarget(entry, viewer) || !messageDialogSourceExists(tunnelId, entry.sourceId);
}

function messageDialogSourceExists(tunnelId: string, sourceId: string): boolean {
  if (!sourceId) {
    return false;
  }
  const text = texts.get(tunnelId) || "";
  const lines = text.endsWith("\n") ? text.slice(0, -1).split("\n") : text.split("\n");
  for (let index = 0; index < lines.length; index += 1) {
    const line = lines[index] ?? "";
    if (!line.trim() || isMessageDialogLine(line) || parseSpaceEntryLine(line) || parseFileBundleLine(line)) {
      continue;
    }
    if (spaceMessageSourceId(tunnelId, index, line) === sourceId) {
      return true;
    }
  }
  return false;
}

function applyKnock(tunnelId: string, knock: NoticeKnock): void {
  if (!device || knock.deviceId === device.id || !grantTargetsThisDevice(knock.targetDeviceId)) {
    return;
  }
  const hadNotice = tunnelHasNotice(tunnelId);
  const title = cleanNick(knock.nick) || counterpartyLabelForTunnelId(tunnelId) || "соты";
  vibrateHiddenOnce(`knock:${knock.deviceId || knock.nick}`, tunnelId, hadNotice, {
    title,
    body: "Позвали в чат"
  });
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
    const currentTarget = remoteGrantTargets.get(tunnelId);
    if (!currentTarget || currentTarget === request.deviceId || currentTarget === "*") {
      remoteGrantTargets = setRemoteGrantTarget(tunnelId, request.deviceId, true);
      sync?.grantRemote(true, request.deviceId);
      startRemoteHostSourceGrantControl();
      return;
    }
  }
  tunnels = tunnelId === selectedId ? touchTunnel(tunnelId) : markTunnel(tunnelId, true);
  renderTiles();
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
      <p>${escapeHtml(requester || "Оператор")} просит доступ к этому устройству.</p>
      <ul class="trust-facts">
        <li>Разрешение только для устройства, которое отправило запрос.</li>
        <li>Отключается повторным нажатием или кнопкой щита.</li>
      </ul>
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
        requestAgentDownload(undefined, "Чтобы принять удаленные команды, этому устройству нужна Клава.");
        return;
      }
      remoteEnabled = setRemoteEnabled(tunnelId, true);
      remoteGrantTargets = setRemoteGrantTarget(tunnelId, request.deviceId, true);
      syncs.get(tunnelId)?.grantRemote(true, request.deviceId);
      startRemoteHostSourceGrantControl();
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
    remoteGrantTargets = setRemoteGrantTarget(tunnelId, "", false);
    syncs.get(tunnelId)?.grantRemote(false, "*");
    maybeRevokeCurrentDeviceSourceGrant();
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
  const wasSelected = tunnelId === selectedId;
  terminalOpenId = tunnelId;
  setTerminalState(tunnelId, "run");
  appendTerminalLine(tunnelId, `< ${command.command}`);
  if (wasSelected) {
    clearTunnelNotices(tunnelId);
    tunnels = markTunnel(tunnelId, false);
  } else {
    tunnels = markTunnel(tunnelId, true);
  }
  renderTiles();
  if (wasSelected) {
    applySelectedText(true);
    renderComposerAttachments();
  }
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
  const wasSelected = tunnelId === selectedId;
  terminalOpenId = tunnelId;
  setTerminalState(tunnelId, "run");
  appendTerminalLine(tunnelId, `< ${script.name || "script"}`);
  if (wasSelected) {
    clearTunnelNotices(tunnelId);
    tunnels = markTunnel(tunnelId, false);
  } else {
    tunnels = markTunnel(tunnelId, true);
  }
  renderTiles();
  if (wasSelected) {
    applySelectedText(true);
    renderComposerAttachments();
  }
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
  resolveWebControllerOutput(tunnelId, output);
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

function webControllerTargets(): WebControllerTargetInfo[] {
  remoteAccess = loadRemoteAccess();
  return sortedVisibleTunnels()
    .filter((tunnel) => !isAgentTunnel(tunnel) && remoteAccess.has(tunnel.id))
    .map(webControllerTargetInfo);
}

function webControllerStatus(): {
  readonly deviceId: string;
  readonly deviceNick: string;
  readonly localAgentOk: boolean;
  readonly pending: readonly string[];
} {
  return {
    deviceId: device?.id || "",
    deviceNick: device?.nick || "",
    localAgentOk: localAgent.ok === true,
    pending: [...webControllerPending.keys()]
  };
}

function webControllerTargetInfo(tunnel: TunnelRecord): WebControllerTargetInfo {
  const hostDeviceId = remoteAccess.get(tunnel.id) || "";
  return {
    tunnelId: tunnel.id,
    label: counterpartyLabel(tunnel),
    hostDeviceId,
    deviceIds: [...new Set((peerDevices.get(tunnel.id) ?? []).map((peer) => peer.id).filter(Boolean))],
    selected: tunnel.id === selectedId,
    syncState: syncStates.get(tunnel.id) || (syncs.has(tunnel.id) ? "connecting" : "closed"),
    terminalState: terminalState.get(tunnel.id) || "idle"
  };
}

function webControllerSelect(target?: WebControllerTargetRef): WebControllerTargetInfo {
  const resolved = webControllerTarget(target);
  selectedId = resolved.tunnel.id;
  saveSelectedTunnelId(selectedId);
  terminalOpenId = resolved.tunnel.id;
  ensureSync(resolved.tunnel);
  renderApp();
  return resolved.info;
}

async function webControllerSend(request: WebControllerRunRequest): Promise<WebControllerRunResult> {
  const command = request.body.trim();
  const options = request.options;
  const kind = request.kind;
  if (!command) {
    throw new Error("SOTY.remote: empty command");
  }
  if (!device) {
    throw new Error("SOTY.remote: local Soty device is not ready");
  }
  const resolved = webControllerTarget(options.target);
  const hostDeviceId = resolved.info.hostDeviceId;
  if (!hostDeviceId) {
    throw new Error("SOTY.remote: selected target has no trusted host device");
  }
  ensureSync(resolved.tunnel);
  const sync = syncs.get(resolved.tunnel.id);
  if (!sync) {
    throw new Error("SOTY.remote: tunnel sync is not available");
  }
  const timeoutMs = safeOperatorTimeoutMs(options.timeoutMs) || 30_000;
  const wasSelected = selectedId === resolved.tunnel.id;
  terminalOpenId = resolved.tunnel.id;
  setTerminalState(resolved.tunnel.id, "run");
  appendTerminalLine(resolved.tunnel.id, kind === "run" ? `$ ${command}` : `$ ${options.name || "script"}`);
  tunnels = wasSelected ? touchTunnel(resolved.tunnel.id) : markTunnel(resolved.tunnel.id, true);
  if (wasSelected) {
    renderTiles();
    renderTerminal();
  } else {
    renderTiles();
  }
  const startedAt = new Date().toISOString();
  let commandId = "";
  try {
    commandId = kind === "run"
      ? await sync.sendRemoteCommand(hostDeviceId, command, timeoutMs, options.runAs || "")
      : await sync.sendRemoteScript(hostDeviceId, {
        name: options.name || "script",
        shell: options.shell || "",
        script: command,
        runAs: options.runAs || "",
        timeoutMs
      });
  } catch (error) {
    setTerminalState(resolved.tunnel.id, "bad");
    appendTerminalLine(resolved.tunnel.id, `! ${error instanceof Error ? error.message : String(error)}`);
    renderTerminal();
    throw error;
  }
  return await new Promise<WebControllerRunResult>((resolve) => {
    const timer = window.setTimeout(() => {
      const pending = webControllerPending.get(commandId);
      if (!pending) {
        return;
      }
      webControllerPending.delete(commandId);
      void sync.sendRemoteCancel(hostDeviceId, commandId).catch(() => undefined);
      setTerminalState(resolved.tunnel.id, "bad");
      appendTerminalLine(resolved.tunnel.id, "! timeout");
      renderTerminal();
      resolve({
        ok: false,
        tunnelId: pending.tunnelId,
        label: pending.label,
        hostDeviceId: pending.hostDeviceId,
        commandId,
        text: `${pending.chunks.join("") || ""}! timeout\n`,
        exitCode: 124,
        startedAt: pending.startedAt,
        finishedAt: new Date().toISOString(),
        timedOut: true
      });
    }, timeoutMs + 2000);
    webControllerPending.set(commandId, {
      tunnelId: resolved.tunnel.id,
      label: resolved.info.label,
      hostDeviceId,
      commandId,
      startedAt,
      timer,
      chunks: [],
      resolve
    });
  });
}

function webControllerTarget(target?: WebControllerTargetRef): { readonly tunnel: TunnelRecord; readonly info: WebControllerTargetInfo } {
  const targets = webControllerTargets();
  if (targets.length === 0) {
    throw new Error("SOTY.remote: no trusted remote targets");
  }
  const info = resolveWebControllerTarget(targets, selectedId, target);
  const tunnel = loadTunnels().find((item) => item.id === info.tunnelId);
  if (!tunnel) {
    throw new Error("SOTY.remote: tunnel record is missing");
  }
  return { tunnel, info };
}

function resolveWebControllerOutput(tunnelId: string, output: RemoteOutput): void {
  const pending = webControllerPending.get(output.commandId);
  if (!pending || pending.tunnelId !== tunnelId) {
    return;
  }
  if (output.text) {
    pending.chunks.push(output.text);
  }
  if (typeof output.exitCode !== "number") {
    return;
  }
  window.clearTimeout(pending.timer);
  webControllerPending.delete(output.commandId);
  pending.resolve({
    ok: output.exitCode === 0,
    tunnelId: pending.tunnelId,
    label: pending.label,
    hostDeviceId: pending.hostDeviceId,
    commandId: pending.commandId,
    text: pending.chunks.join(""),
    exitCode: output.exitCode,
    startedAt: pending.startedAt,
    finishedAt: new Date().toISOString()
  });
}

function webControllerCancel(commandId: string): boolean {
  const pending = webControllerPending.get(commandId);
  if (!pending) {
    return false;
  }
  const sync = syncs.get(pending.tunnelId);
  window.clearTimeout(pending.timer);
  webControllerPending.delete(commandId);
  if (sync) {
    void sync.sendRemoteCancel(pending.hostDeviceId, commandId).catch(() => undefined);
  }
  pending.resolve({
    ok: false,
    tunnelId: pending.tunnelId,
    label: pending.label,
    hostDeviceId: pending.hostDeviceId,
    commandId,
    text: `${pending.chunks.join("") || ""}! cancelled\n`,
    exitCode: 130,
    startedAt: pending.startedAt,
    finishedAt: new Date().toISOString()
  });
  return true;
}

function webControllerTail(target?: WebControllerTargetRef, lines = 80): string[] {
  const resolved = webControllerTarget(target);
  const count = Math.max(1, Math.min(Math.trunc(lines) || 80, 600));
  return (terminalLogs.get(resolved.tunnel.id) || []).slice(-count);
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
  const ws = new WebSocket(localAgentWsUrl);
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
      readonly visibility?: string;
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
    activeTunnelKind: tunnel && (isAgentTunnel(tunnel) || selectedAgentMode(tunnel.id)) ? "agent" : "peer",
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
  const wasSelected = tunnel.id === selectedId;
  terminalOpenId = tunnel.id;
  setTerminalState(tunnel.id, "run");
  appendTerminalLine(tunnel.id, `$ ${command}`);
  if (wasSelected) {
    clearTunnelNotices(tunnel.id);
    tunnels = markTunnel(tunnel.id, false);
  } else {
    tunnels = markTunnel(tunnel.id, true);
  }
  renderTiles();
  if (wasSelected) {
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
  const wasSelected = tunnel.id === selectedId;
  terminalOpenId = tunnel.id;
  setTerminalState(tunnel.id, "run");
  appendTerminalLine(tunnel.id, `$ ${name}`);
  if (wasSelected) {
    clearTunnelNotices(tunnel.id);
    tunnels = markTunnel(tunnel.id, false);
  } else {
    tunnels = markTunnel(tunnel.id, true);
  }
  renderTiles();
  if (wasSelected) {
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
  const wasSelected = tunnel.id === selectedId;
  tunnels = wasSelected ? touchTunnel(tunnel.id) : markTunnel(tunnel.id, true);
  renderTiles();
  if (wasSelected) {
    applySelectedText();
  }
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
  const shouldFocusAgentMessage = !hasVisibleSelection();
  if (shouldFocusAgentMessage) {
    selectTunnel(tunnel.id);
  }
  const current = texts.get(tunnel.id) || "";
  const separator = current.length > 0 && !current.endsWith("\n") ? "\n" : "";
  const next = `${current}${separator}${body}\n`;
  texts.set(tunnel.id, next);
  sync.setText(next);
  saveTextSnapshotNow(tunnel.id, next);
  localDrafts.delete(tunnel.id);
  clearLiveDraftState(tunnel.id);
  void sync.sendLiveDraft("");
  tunnels = touchTunnel(tunnel.id);
  renderTiles();
  if (tunnel.id === selectedId) {
    applySelectedText();
    renderTextPaint();
    renderWriterPop();
  }
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
  readonly tags?: unknown;
  readonly profileId?: string;
  readonly profileTitle?: string;
  readonly profile?: unknown;
  readonly layout?: string;
  readonly height?: string;
  readonly width?: string;
  readonly display?: unknown;
  readonly scope?: string;
  readonly visibility?: string;
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
  readonly tags?: unknown;
  readonly profileId?: string;
  readonly profileTitle?: string;
  readonly profile?: unknown;
  readonly layout?: string;
  readonly height?: string;
  readonly width?: string;
  readonly display?: unknown;
  readonly scope?: string;
  readonly visibility?: string;
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
    tags: Array.isArray(message.tags) || typeof message.tags === "string" ? message.tags : (Array.isArray(appRecord.tags) || typeof appRecord.tags === "string" ? appRecord.tags : []),
    profileId: recordString(appRecord, "profileId") || message.profileId || "",
    profileTitle: recordString(appRecord, "profileTitle") || message.profileTitle || "",
    profile: isRecord(message.profile) || typeof message.profile === "string" ? message.profile : (isRecord(appRecord.profile) || typeof appRecord.profile === "string" ? appRecord.profile : undefined),
    layout: recordString(appRecord, "layout") || message.layout || "",
    height: recordString(appRecord, "height") || message.height || "",
    width: recordString(appRecord, "width") || message.width || "",
    display: isRecord(message.display) ? message.display : (isRecord(appRecord.display) ? appRecord.display : undefined),
    scope: message.scope || recordString(appRecord, "scope") || "chat",
    visibility: message.visibility || recordString(appRecord, "visibility") || "",
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
      ? visibleQuickActions("").map((action) => agentActionButtonHtml(action)).join("")
      : "";
    bindAgentActionGridScroll(actionGrid);
    actionGrid.querySelectorAll<HTMLButtonElement>(".agent-action-button").forEach((button) => {
      button.addEventListener("click", () => {
        if (actionGrid.dataset.justDragged === "1") {
          return;
        }
        void runQuickAction(button.dataset.actionId || "");
      });
    });
  }
  panel.dataset.state = state;
  form.hidden = !controller;
  const tunnel = loadTunnels().find((item) => item.id === tunnelId);
  peer.textContent = tunnel ? initials(counterpartyLabel(tunnel)) : ".";
  if (title) {
    title.textContent = "Инструменты";
  }
  if (status) {
    status.textContent = controller ? "доступ" : host ? "открыто" : terminalStateLabel(state);
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

function bindAgentActionGridScroll(scroller: HTMLDivElement): void {
  if (scroller.dataset.dragScrollBound === "1") {
    return;
  }
  scroller.dataset.dragScrollBound = "1";
  let pointerId = -1;
  let startX = 0;
  let startScrollLeft = 0;
  let dragged = false;
  const stop = () => {
    if (pointerId < 0) {
      return;
    }
    pointerId = -1;
    scroller.classList.remove("is-dragging");
    if (dragged) {
      scroller.dataset.justDragged = "1";
      window.setTimeout(() => {
        if (scroller.dataset.justDragged === "1") {
          delete scroller.dataset.justDragged;
        }
      }, 120);
    }
  };
  scroller.addEventListener("pointerdown", (event) => {
    if (event.button !== 0 || scroller.scrollWidth <= scroller.clientWidth + 2) {
      return;
    }
    pointerId = event.pointerId;
    startX = event.clientX;
    startScrollLeft = scroller.scrollLeft;
    dragged = false;
    scroller.classList.add("is-dragging");
    scroller.setPointerCapture(event.pointerId);
  });
  scroller.addEventListener("pointermove", (event) => {
    if (event.pointerId !== pointerId) {
      return;
    }
    const delta = event.clientX - startX;
    if (Math.abs(delta) > 3) {
      dragged = true;
      event.preventDefault();
    }
    scroller.scrollLeft = startScrollLeft - delta;
  });
  scroller.addEventListener("pointerup", stop);
  scroller.addEventListener("pointercancel", stop);
  scroller.addEventListener("wheel", (event) => {
    if (Math.abs(event.deltaY) <= Math.abs(event.deltaX) || scroller.scrollWidth <= scroller.clientWidth + 2) {
      return;
    }
    event.preventDefault();
    scroller.scrollLeft += event.deltaY;
  }, { passive: false });
}

function activeTerminalTunnelId(): string {
  if (
    terminalOpenId
    && terminalOpenId === selectedId
    && (remoteAccess.has(terminalOpenId) || remoteEnabled.has(terminalOpenId) || terminalLogs.has(terminalOpenId))
  ) {
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

function terminalStateLabel(state: "idle" | "run" | "ok" | "bad" | "off"): string {
  if (state === "run") {
    return "работает";
  }
  if (state === "bad") {
    return "ошибка";
  }
  if (state === "off") {
    return "не в сети";
  }
  return "готово";
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

async function openChessForSelected(forceOpen = false): Promise<void> {
  if (!selectedId) {
    return;
  }
  if (miniAppSession) {
    collapseMiniApp();
  }
  if (activeChessTunnelId() === selectedId) {
    if (forceOpen) {
      return;
    }
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
    status.textContent = snapshot.result ? "готово" : game.isCheck() ? "шах" : `${game.moveNumber()}`;
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

function grantTargetsThisDevice(targetDeviceId?: string): boolean {
  return !targetDeviceId || targetDeviceId === "*" || targetDeviceId === device?.id;
}

function tunnelHasNotice(tunnelId: string): boolean {
  return loadTunnels().some((tunnel) => tunnel.id === tunnelId && tunnel.unread);
}

function notificationUrlForTunnel(tunnelId: string): string {
  const base = bareChatMode ? "/?pwa=1&bare=1" : "/?pwa=1";
  return `${base}&chat=${encodeURIComponent(tunnelId)}`;
}

function vibrateHiddenOnce(reason: string, tunnelId: string, hadNotice: boolean, notice?: AttentionNotice): void {
  notifyHiddenOnce({
    reason,
    tunnelId,
    hadNotice,
    hidden: document.visibilityState === "hidden",
    url: notificationUrlForTunnel(tunnelId),
    notice
  });
}

function clearTunnelNotices(tunnelId: string): void {
  clearAttentionNotices(tunnelId);
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
  if (!shouldNotifyTyping(key)) {
    return;
  }
  vibrateHiddenOnce(`typing:${writer}`, tunnelId, hadNotice, {
    title: cleanNick(activity.nick) || counterpartyLabelForTunnelId(tunnelId) || "соты",
    body: "Пишет сообщение"
  });
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
  const ws = new WebSocket(localAgentWsUrl);
  const fail = () => {
    if (finished || opened) {
      return;
    }
    finished = true;
    cleanupSotyFileDataPlane(command.id);
    setTerminalState(tunnelId, "off");
    appendTerminalLine(tunnelId, localAgentUnavailableText);
    renderTerminal();
    window.clearTimeout(timer);
    window.clearTimeout(watchdogTimer);
    void sync.sendRemoteOutput(command.deviceId, command.id, localAgentUnavailableText, 127);
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
  const ws = new WebSocket(localAgentWsUrl);
  const fail = () => {
    if (finished || opened) {
      return;
    }
    finished = true;
    cleanupSotyFileDataPlane(script.id);
    setTerminalState(tunnelId, "off");
    appendTerminalLine(tunnelId, localAgentUnavailableText);
    renderTerminal();
    window.clearTimeout(timer);
    window.clearTimeout(watchdogTimer);
    void sync.sendRemoteOutput(script.deviceId, script.id, localAgentUnavailableText, 127);
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
  const tunnel = loadTunnels().find((item) => item.id === selectedId);
  if (!composerAccessFor(tunnel).canCompose) {
    return;
  }
  // Drafts stay local; Enter or the send button is the only publish path.
  const draft = composer.value;
  if (draft) {
    localDrafts.set(selectedId, draft);
  } else {
    localDrafts.delete(selectedId);
  }
  if (selectedAgentMode(selectedId)) {
    const pendingLiveDraftTimer = liveDraftSendTimers.get(selectedId);
    if (pendingLiveDraftTimer) {
      window.clearTimeout(pendingLiveDraftTimer);
      liveDraftSendTimers.delete(selectedId);
    }
    void syncs.get(selectedId)?.sendLiveDraft("");
    resizeComposer();
    return;
  }
  scheduleLiveDraft(selectedId, draft);
  resizeComposer();
}

function isMessageSendEnter(event: KeyboardEvent): boolean {
  return event.key === "Enter"
    && (event.ctrlKey || event.metaKey)
    && !event.shiftKey
    && !event.altKey
    && !event.isComposing;
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
  primeAgentDoneSound();
  const draft = composer.value || localDrafts.get(tunnelId) || "";
  const message = normalizeChatMessage(draft);
  const pendingCount = pendingAttachments.get(tunnelId)?.length ?? 0;
  const spaceMode = selectedSpaceMode();
  if (!message && pendingCount === 0) {
    if (draft) {
      composer.value = "";
      rememberComposerDraft();
    }
    return;
  }
  const sync = syncs.get(tunnelId);
  const tunnel = loadTunnels().find((item) => item.id === tunnelId);
  if (!sync) {
    return;
  }
  const access = composerAccessFor(tunnel, spaceMode);
  if (!access.canCompose) {
    renderDialogChrome();
    return;
  }
  if (spaceMode === "dialog" && agentThinking.has(tunnelId)) {
    stopAgentDialogReply(tunnelId);
    return;
  }
  if (spaceMode === "dialog" && tunnel && selectedAgentMode(tunnelId)) {
    if (!message) {
      renderComposerAttachments();
      return;
    }
    appendAgentPrivateLine(tunnelId, "user", message);
    const pendingLiveDraftTimer = liveDraftSendTimers.get(tunnelId);
    if (pendingLiveDraftTimer) {
      window.clearTimeout(pendingLiveDraftTimer);
      liveDraftSendTimers.delete(tunnelId);
    }
    void sync.sendLiveDraft("");
    localDrafts.delete(tunnelId);
    composer.value = "";
    resizeComposer();
    renderAgentPrivatePanel();
    renderDialogChrome();
    void sendAgentDialogMessage(tunnelId, message, { privateMode: true });
    return;
  }
  const sentFiles = await sendPendingAttachments(tunnelId, sync);
  if (!message && sentFiles.length === 0) {
    renderComposerAttachments();
    return;
  }
  const author = cleanNick(device?.nick || "") || "Я";
  const markerText = message || (sentFiles.length > 0 ? "Медиа" : "");
  const entryKind = access.entryKind;
  const visibleText = entryKind && markerText
    ? makeSpaceEntryLine(entryKind, { author, authorId: device?.id || "", text: markerText })
    : message;
  const current = texts.get(tunnelId) ?? textarea.value;
  const separator = current.length > 0 && !current.endsWith("\n") ? "\n" : "";
  const bundleLine = sentFiles.length > 0 ? fileBundleLine(sentFiles) : "";
  const visibleMessage = [visibleText, bundleLine].filter(Boolean).join("\n");
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
  if (spaceMode === "dialog" && tunnel && isAgentTunnel(tunnel)) {
    await prepareAgentSourceForDialog(tunnelId, tunnel);
    void sendAgentDialogMessage(tunnelId, agentMessage);
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

function agentReplyStopToken(tunnelId: string): number {
  return agentReplyStopTokens.get(tunnelId) ?? 0;
}

function bumpAgentReplyStopToken(tunnelId: string): number {
  const next = agentReplyStopToken(tunnelId) + 1;
  agentReplyStopTokens.set(tunnelId, next);
  return next;
}

function cancelledAgentDialogReply(): LocalAgentReply {
  return {
    ok: false,
    text: "! cancelled",
    exitCode: 130
  };
}

function stopAgentDialogReply(tunnelId: string): void {
  bumpAgentReplyStopToken(tunnelId);
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
  return isAgentTunnel(tunnel) || remoteAccess.has(tunnel.id) || selectedAgentMode(tunnel.id);
}

async function resumeAgentDialogReply(pending: LocalAgentPendingRelayReply): Promise<void> {
  const tunnelId = pending.tunnelId;
  const tunnel = loadTunnels().find((item) => item.id === tunnelId);
  const privateMode = Boolean(tunnel && !isAgentTunnel(tunnel) && selectedAgentMode(tunnel.id));
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
      appendAgentOutput(tunnelId, streamed, privateMode);
    }, undefined, controller.signal);
    if (!controller.signal.aborted) {
      finishAgentDialogReply(tunnelId, reply, streamedMessages, privateMode);
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
  options: { readonly privateMode?: boolean } = {}
): Promise<LocalAgentReply | null> {
  const tunnel = loadTunnels().find((item) => item.id === tunnelId);
  const agentTunnel = tunnel ? isAgentTunnel(tunnel) : false;
  const privateMode = options.privateMode === true;
  if (!tunnel || (!agentTunnel && !privateMode) || !text.trim()) {
    return Promise.resolve(null);
  }
  const taskText = text;
  const context = cleanAgentContext(texts.get(tunnelId) || "").slice(-16_000);
  const previous = agentReplyQueues.get(tunnelId) ?? Promise.resolve();
  const replyToken = agentReplyStopToken(tunnelId);
  const next = previous
    .catch(() => undefined)
    .then(async () => {
      if (agentReplyStopToken(tunnelId) !== replyToken) {
        return cancelledAgentDialogReply();
      }
      const controller = new AbortController();
      agentReplyControllers.set(tunnelId, controller);
      setAgentThinking(tunnelId, true);
      let reply: LocalAgentReply | null = null;
      const streamedMessages: string[] = [];
      try {
        if (agentTunnel) {
          await prepareAgentSourceForDialog(tunnelId, tunnel);
        } else if (privateMode) {
          await preparePeerAgentInvocation(tunnelId);
        }
        if (controller.signal.aborted || agentReplyStopToken(tunnelId) !== replyToken) {
          return cancelledAgentDialogReply();
        }
        const source = agentRequestSourceForTunnel(tunnelId, tunnel, agentTunnel);
        reply = await askLocalAgentReply(taskText, context, source, 2 * 60 * 60_000, (message) => {
          const streamed = normalizeChatMessage(cleanAgentReplyText(message));
          if (!streamed || streamedMessages[streamedMessages.length - 1] === streamed) {
            return;
          }
          streamedMessages.push(streamed);
          appendAgentOutput(tunnelId, streamed, privateMode);
        }, undefined, controller.signal);
      } finally {
        if (agentReplyControllers.get(tunnelId) === controller) {
          agentReplyControllers.delete(tunnelId);
        }
        setAgentThinking(tunnelId, false);
      }
      if (!reply || controller.signal.aborted || agentReplyStopToken(tunnelId) !== replyToken) {
        return cancelledAgentDialogReply();
      }
      finishAgentDialogReply(tunnelId, reply, streamedMessages, privateMode);
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
  remoteGrantTargets = setRemoteGrantTarget(tunnelId, device.id, true);
  terminalOpenId = tunnelId;
  if (!terminalState.has(tunnelId)) {
    setTerminalState(tunnelId, "idle");
  }
  renderTiles();
  renderTerminal();
  publishOperatorTargets();
  await grantAgentSourceAccess(device.id, device.nick, true, agentSourceClientState(), 2500).catch(() => false);
  startAgentSourceControl(tunnelId);
  startRemoteHostSourceGrantControl();
  publishOperatorTargets();
}

async function preparePeerAgentInvocation(tunnelId: string): Promise<void> {
  if (!device) {
    return;
  }
  const tunnel = loadTunnels().find((item) => item.id === tunnelId);
  if (!tunnel || isAgentTunnel(tunnel)) {
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
  streamedMessages: readonly string[],
  privateMode = false
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
  const appended = appendAgentReplyMessages(tunnelId, finalReply, body, privateMode);
  if (!appended && !reply.ok && body) {
    appendAgentOutput(tunnelId, userVisibleAgentFailureText(body), privateMode);
    appendTerminalLine(tunnelId, `! codex bridge: ${body}`);
  }
  playAgentDoneSound();
}

function appendAgentReplyMessages(tunnelId: string, reply: LocalAgentReply, fallback: string, privateMode = false): boolean {
  const messages = (reply.messages ?? [])
    .map((message) => cleanAgentReplyText(message))
    .filter(Boolean);
  if (messages.length > 0) {
    return appendAgentOutput(tunnelId, messages.join("\n\n"), privateMode);
  }
  if (reply.ok && fallback) {
    return appendAgentOutput(tunnelId, fallback, privateMode);
  }
  return false;
}

function userVisibleAgentFailureText(value: string): string {
  return cleanAgentReplyText(value) || "! agent: no reply";
}

function appendAgentOutput(tunnelId: string, rawText: string, privateMode: boolean): boolean {
  return privateMode ? appendAgentPrivateLine(tunnelId, "agent", rawText) : appendAgentChatMessage(tunnelId, rawText);
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
  sync.setText(next, { deviceId: "codex", nick: agentDialogLabel, local: false });
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
    .trim();
}

function shouldOfferAgentInstall(reply: LocalAgentReply): boolean {
  return !reply.ok
    && reply.exitCode === 127
    && isLocalAgentUnavailableText(reply.text)
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
  if (focus && composer && !composer.disabled) {
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
  sync.setText(next, { deviceId: "operator", nick: isAgentTunnelId(tunnelId) ? agentDialogLabel : "Operator", local: false });
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
  sync.setText(next, { deviceId: "operator", nick: isAgentTunnelId(tunnelId) ? agentDialogLabel : "Operator", local: false });
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

function applySyncedWriterLines(tunnelId: string, synced: readonly SyncedWriterLine[]): void {
  if (synced.length === 0) {
    return;
  }
  const lines = new Map<number, WriterLine>();
  for (const item of synced) {
    if (!Number.isSafeInteger(item.line) || item.line < 0) {
      continue;
    }
    const nick = cleanNick(item.nick) || (item.deviceId === device?.id ? cleanNick(device?.nick || "") : counterpartyLabelForTunnelId(tunnelId));
    const at = Date.parse(item.createdAt);
    const safeAt = Number.isFinite(at) ? at : Date.now();
    lines.set(item.line, {
      nick,
      deviceId: item.deviceId,
      color: colorFor(`${nick}:${item.deviceId || tunnelId}`),
      time: clock(new Date(safeAt)),
      at: safeAt,
      action: item.action,
      preview: item.preview
    });
  }
  if (lines.size === 0) {
    return;
  }
  writerLines.set(tunnelId, lines);
  saveTextSnapshotNow(tunnelId, texts.get(tunnelId) || "");
  if (tunnelId === selectedId) {
    renderTextPaint();
    renderWriterPop();
  }
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

function bindTextPaintInteractions(root: HTMLDivElement): void {
  root.addEventListener("click", handleTextPaintClick);
  root.addEventListener("keydown", handleTextPaintKeydown);
  root.addEventListener("submit", handleTextPaintSubmit);
}

function handleTextPaintClick(event: MouseEvent): void {
  const target = event.target instanceof Element ? event.target : null;
  const root = event.currentTarget instanceof HTMLElement ? event.currentTarget : textPaint;
  if (!target || !root || !root.contains(target)) {
    return;
  }

  const fileButton = target.closest<HTMLButtonElement>(".bubble-file[data-file-id]");
  if (fileButton && root.contains(fileButton)) {
    event.preventDefault();
    event.stopPropagation();
    const fileId = fileButton.dataset.fileId || "";
    const file = (files.get(selectedId) ?? []).find((item) => item.id === fileId);
    if (file) {
      downloadReceivedFile(file);
    }
    return;
  }

  const markButton = target.closest<HTMLButtonElement>(".bubble-mark");
  if (markButton && root.contains(markButton)) {
    event.preventDefault();
    event.stopPropagation();
    markDialogMessage(markButton);
    return;
  }

  const peekButton = target.closest<HTMLElement>(".message-dialog-peek");
  if (peekButton && root.contains(peekButton)) {
    event.preventDefault();
    event.stopPropagation();
    openMessageDialogFromElement(peekButton);
    return;
  }

  const closeButton = target.closest<HTMLButtonElement>(".message-dialog-close");
  if (closeButton && root.contains(closeButton)) {
    event.preventDefault();
    event.stopPropagation();
    openMessageDialog = null;
    renderTextPaint();
    return;
  }

  const dialogForm = target.closest(".message-dialog-form");
  if (dialogForm && root.contains(dialogForm)) {
    event.stopPropagation();
    return;
  }

  if (target.closest("a, button, textarea, input, .message-dialog-panel, .bubble-file")) {
    return;
  }
  const bubble = target.closest<HTMLElement>(".chat-bubble[data-dialog-source-id]");
  if (bubble && root.contains(bubble)) {
    openMessageDialogFromElement(bubble);
  }
}

function handleTextPaintKeydown(event: KeyboardEvent): void {
  const target = event.target instanceof Element ? event.target : null;
  const root = event.currentTarget instanceof HTMLElement ? event.currentTarget : textPaint;
  if (!target || !root || !root.contains(target)) {
    return;
  }

  const replyInput = target.closest<HTMLTextAreaElement>(".message-dialog-form textarea");
  const replyForm = replyInput?.closest<HTMLFormElement>(".message-dialog-form") ?? null;
  if (replyInput && replyForm && root.contains(replyForm) && isMessageSendEnter(event)) {
    event.preventDefault();
    void submitMessageDialogReply(replyForm);
    return;
  }

  if (target.closest("a, button, textarea, input, .message-dialog-panel, .bubble-file")) {
    return;
  }
  const bubble = target.closest<HTMLElement>(".chat-bubble[data-dialog-source-id]");
  if (bubble && root.contains(bubble) && (event.key === "Enter" || event.key === " ")) {
    event.preventDefault();
    openMessageDialogFromElement(bubble);
  }
}

function handleTextPaintSubmit(event: SubmitEvent): void {
  const form = event.target instanceof HTMLFormElement ? event.target : null;
  const root = event.currentTarget instanceof HTMLElement ? event.currentTarget : textPaint;
  if (!form || !root || !root.contains(form) || !form.classList.contains("message-dialog-form")) {
    return;
  }
  event.preventDefault();
  void submitMessageDialogReply(form);
}

function messageBubbleGroupKey(label: WriterLine | undefined): string {
  if (!label || !(label.deviceId || label.nick) || !Number.isFinite(label.at)) {
    return "";
  }
  return [
    label.deviceId || "",
    label.nick || "",
    Math.trunc(label.at),
    label.action,
    label.preview || ""
  ].join("\u001f");
}

function renderTextPaint(): void {
  if (!textarea || !textPaint) {
    return;
  }
  renderDialogChrome();
  const scroll = app.querySelector<HTMLDivElement>(".chat-scroll");
  const stickToBottom = scroll ? scroll.scrollHeight - scroll.scrollTop - scroll.clientHeight < 180 : false;
  const labels = writerLines.get(selectedId) ?? new Map();
  const tunnel = loadTunnels().find((item) => item.id === selectedId);
  const text = textarea.value;
  const lines = text.endsWith("\n") ? text.slice(0, -1).split("\n") : text.split("\n");
  const spaceMode = selectedSpaceMode();
  const showDialog = spaceMode === "dialog";
  const markedSources = markedSourceIds(lines);
  const messageDialogs = messageDialogEntriesBySource(lines);
  const active = activeActivities.get(selectedId);
  const activeLine = active ? lineFromIndex(text, active.index) : -1;
  const drafts = showDialog ? liveDraftsForSelected() : [];
  const hasAgentThinking = showDialog && agentThinking.has(selectedId);
  textPaint.style.transform = "";
  if (!text.trim() && drafts.length === 0 && !hasAgentThinking) {
    textPaint.innerHTML = renderEmptySpacePrompt(spaceMode, tunnel);
    return;
  }
  let operatorBlock = false;
  let operatorBlockNick = "";
  let operatorBlockTime = "";
  const bubbles: {
    key: string;
    side: string;
    nick: string;
    color: string;
    time: string;
    className: string;
    lines: string[];
    attachments: FileBundleMarker[];
    entry: SpaceEntry | null;
    live: WriterActivity | null;
    groupKey: string;
    sourceLine: number;
    sourceText: string;
    sourceId: string;
    markKind: SpaceEntryKind | null;
    marked: boolean;
  }[] = [];
  let lastHiddenEntry = false;
  for (let index = 0; index < lines.length; index += 1) {
    const line = lines[index] ?? "";
    if (isMessageDialogLine(line)) {
      lastHiddenEntry = false;
      continue;
    }
    const spaceEntry = parseSpaceEntryLine(line);
    if (spaceEntry) {
      if (!spaceEntryVisibleInMode(spaceEntry, spaceMode)) {
        lastHiddenEntry = true;
        continue;
      }
      const mine = Boolean(spaceEntry.authorId && spaceEntry.authorId === device?.id);
      bubbles.push({
        key: `space-entry:${spaceEntry.id}`,
        side: mine ? "local" : "remote",
        nick: spaceEntry.author,
        color: mine ? colorFor(`local:${device?.id || selectedId}`) : safeColor(undefined, `${selectedId}:${spaceEntry.author}`),
        time: clock(new Date(spaceEntry.createdAt)),
        className: `is-space-entry is-space-${spaceEntry.kind}`,
        lines: [],
        attachments: [],
        entry: spaceEntry,
        live: null,
        groupKey: "",
        sourceLine: -1,
        sourceText: "",
        sourceId: "",
        markKind: null,
        marked: true
      });
      lastHiddenEntry = false;
      continue;
    }
    const fileBundle = parseFileBundleLine(line);
    if (fileBundle) {
      if (lastHiddenEntry) {
        continue;
      }
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
          entry: null,
          live: null,
          groupKey: "",
          sourceLine: -1,
          sourceText: "",
          sourceId: "",
          markKind: null,
          marked: false
        });
      }
      continue;
    }
    if (!showDialog) {
      lastHiddenEntry = Boolean(line.trim());
      continue;
    }
    const label = labels.get(index);
    const groupKey = messageBubbleGroupKey(label);
    let state = classifyChatLine(line, operatorBlock);
    const operatorNick = operatorNameFromLine(line);
    if (operatorNick) {
      operatorBlockNick = operatorNick;
      operatorBlockTime = operatorTimeFromLine(line);
    }
    if (label && label.deviceId !== "operator" && !isAgentChromeLineClass(state.className)) {
      state = { className: "is-user-line", operatorBlock: false };
    }
    operatorBlock = state.operatorBlock;
    if (isAgentChromeLineClass(state.className)) {
      if (!operatorBlock) {
        operatorBlockNick = "";
        operatorBlockTime = "";
      }
      lastHiddenEntry = true;
      continue;
    }
    if (!line.trim()) {
      const current = bubbles[bubbles.length - 1];
      if (groupKey && current?.groupKey === groupKey && !current.entry && !current.live) {
        current.lines.push("");
        lastHiddenEntry = false;
        continue;
      }
      operatorBlock = false;
      operatorBlockNick = "";
      operatorBlockTime = "";
      lastHiddenEntry = false;
      continue;
    }
    const speaker = speakerForLine(line, state.className, label, operatorBlockNick);
    const time = label?.time || operatorBlockTime || clock();
    if (!operatorBlock) {
      operatorBlockNick = "";
      operatorBlockTime = "";
    }
    const live = active && index === activeLine ? active : null;
    const sourceId = spaceMessageSourceId(selectedId, index, line);
    const markKind = spaceEntryKindForMessage(speaker.side === "local", isOwnSpace(tunnel));
    const current = bubbles[bubbles.length - 1];
    if (
      groupKey
      && current?.groupKey === groupKey
      && !current.entry
      && !current.live
      && current.side === speaker.side
      && current.nick === speaker.nick
      && current.className === state.className
    ) {
      current.lines.push(line);
      lastHiddenEntry = false;
      continue;
    }
    bubbles.push({
      key: `${speaker.side}:${speaker.nick}:${speaker.deviceId}:${state.className}:${index}`,
      side: speaker.side,
      nick: speaker.nick,
      color: speaker.color,
      time,
      className: state.className,
      lines: [line],
      attachments: [],
      entry: null,
      live,
      groupKey,
      sourceLine: index,
      sourceText: line,
      sourceId,
      markKind,
      marked: markedSources.has(sourceId) || markedSources.has(spaceFallbackMarkerId(markKind, line))
    });
    lastHiddenEntry = false;
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
      entry: null,
      live: null,
      groupKey: "",
      sourceLine: -1,
      sourceText: "",
      sourceId: "",
      markKind: null,
      marked: false
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
      entry: null,
      live: {
        deviceId: draft.deviceId,
        nick,
        index: draft.index,
        local: draft.deviceId === device?.id,
        action: "write",
        preview: draft.text
      },
      groupKey: "",
      sourceLine: -1,
      sourceText: "",
      sourceId: "",
      markKind: null,
      marked: false
    });
  }
  const visibleBubbles = bubbles.filter((bubble) =>
    bubble.entry || bubble.className === "is-agent-thinking" || bubble.live || bubble.attachments.length > 0 || bubble.lines.some((line) => line.trim())
  );
  if (visibleBubbles.length === 0) {
    textPaint.innerHTML = renderEmptySpacePrompt(spaceMode, tunnel);
    return;
  }
  const ownSpace = isOwnSpace(tunnel);
  textPaint.innerHTML = visibleBubbles.map((bubble) => {
    const body = bubble.entry
      ? renderSpaceEntryBubble(bubble.entry, ownSpace)
      : bubble.className === "is-agent-thinking"
      ? `<span class="thinking-label">${escapeHtml(bubble.lines[0] || "думаю")}</span><span class="thinking-rig" aria-hidden="true"><i></i><i></i><i></i><i></i><i></i></span>`
      : bubble.lines
        .map((line) => line ? `<span>${linkifyChatLine(line)}</span>` : "<br>")
        .join("");
    const attachmentHtml = bubble.attachments.length > 0 ? renderBubbleAttachments(bubble.attachments) : "";
    const live = bubble.live
      ? `<em class="live-chip">${escapeHtml(activityCode(bubble.live.action))}${bubble.live.preview ? ` ${escapeHtml(compactPreview(bubble.live.preview))}` : ""}</em>`
      : "";
    const time = bubble.time ? `<time class="bubble-time">${escapeHtml(bubble.time)}</time>` : "";
    const avatar = bubble.side === "local" || bubble.side === "remote"
      ? `<span class="bubble-avatar" aria-hidden="true">${escapeHtml(initials(bubble.nick))}</span>`
      : "";
    const action = bubble.markKind && bubble.sourceLine >= 0 && !bubble.entry
      ? renderBubbleMarkButton(bubble.markKind, bubble.sourceLine, bubble.sourceId, bubble.nick, bubble.sourceText, bubble.marked, ownSpace)
      : "";
    const entries = bubble.sourceId ? messageDialogs.get(bubble.sourceId) ?? [] : [];
    const messageDialog = bubble.sourceId && !bubble.entry
      ? renderMessageDialogPanel(bubble, entries, openMessageDialog?.chatId === selectedId && openMessageDialog.sourceId === bubble.sourceId)
      : "";
    const dialogAttrs = bubble.sourceId && !bubble.entry
      ? ` data-dialog-source-id="${escapeHtml(bubble.sourceId)}" data-source-line="${bubble.sourceLine}" data-source-author="${escapeHtml(bubble.nick)}" data-source-text="${escapeHtml(bubble.sourceText)}" tabindex="0"`
      : "";
    return `
      <article class="chat-bubble ${bubble.side} ${bubble.className}" style="--bubble-color:${bubble.color}"${dialogAttrs}>
        ${avatar}
        ${action}
        ${time}
        ${live}
        ${body ? bubble.entry ? body : `<p>${body}</p>` : ""}
        ${attachmentHtml}
        ${messageDialog}
      </article>
    `;
  }).join("");
  if (scroll && stickToBottom) {
    window.setTimeout(() => {
      scroll.scrollTop = scroll.scrollHeight;
    }, 0);
  }
}

function messageDialogEntriesBySource(lines: readonly string[]): Map<string, MessageDialogEntry[]> {
  const result = new Map<string, MessageDialogEntry[]>();
  const viewer = {
    deviceId: device?.id || "",
    cellIds: loadTunnels().filter((tunnel) => !tunnel.archived).map((tunnel) => tunnel.id)
  };
  for (const line of lines) {
    const entry = parseMessageDialogLine(line);
    if (!entry || entry.chatId !== selectedId || !messageDialogVisibleForTarget(entry, viewer)) {
      continue;
    }
    const current = result.get(entry.sourceId) ?? [];
    current.push(entry);
    result.set(entry.sourceId, current);
  }
  for (const entries of result.values()) {
    entries.sort((left, right) => Date.parse(left.createdAt) - Date.parse(right.createdAt));
  }
  return result;
}

function renderMessageDialogPanel(
  bubble: {
    readonly sourceId: string;
    readonly sourceLine: number;
    readonly sourceText: string;
    readonly nick: string;
  },
  entries: readonly MessageDialogEntry[],
  open: boolean
): string {
  const sourceAttrs = `data-dialog-source-id="${escapeHtml(bubble.sourceId)}" data-source-line="${bubble.sourceLine}" data-source-author="${escapeHtml(bubble.nick)}" data-source-text="${escapeHtml(bubble.sourceText)}"`;
  if (!open) {
    if (entries.length === 0) {
      return "";
    }
    const latest = entries[entries.length - 1];
    return `
      <button class="message-dialog-peek" type="button" ${sourceAttrs} aria-label="открыть диалог" data-tooltip="Открыть диалог">
        <span>${escapeHtml(latest?.text || "")}</span>
        <b>${entries.length}</b>
      </button>
    `;
  }
  const thread = entries.length > 0
    ? entries.map((entry) => `
      <div class="message-dialog-reply">
        <b>${escapeHtml(entry.author)}</b>
        <span>${linkifyChatLine(entry.text)}</span>
        <time>${escapeHtml(clock(new Date(entry.createdAt)))}</time>
      </div>
    `).join("")
    : `<div class="message-dialog-empty"></div>`;
  return `
    <section class="message-dialog-panel" ${sourceAttrs} aria-label="диалог сообщения">
      <header>
        <span>общий</span>
        <button class="message-dialog-close" type="button" aria-label="закрыть" data-tooltip="Закрыть">${icon("close")}</button>
      </header>
      <div class="message-dialog-thread">${thread}</div>
      <form class="message-dialog-form">
        <textarea rows="1" spellcheck="false" autocapitalize="sentences" aria-label="ответ"></textarea>
        <button type="submit" aria-label="отправить" data-tooltip="Отправить">${icon("send")}</button>
      </form>
    </section>
  `;
}

function openMessageDialogFromElement(element: HTMLElement): void {
  const sourceId = element.dataset.dialogSourceId || "";
  if (!selectedId || !sourceId) {
    return;
  }
  openMessageDialog = {
    chatId: selectedId,
    sourceId,
    sourceLine: Number(element.dataset.sourceLine || "-1"),
    sourceText: normalizeChatMessage(element.dataset.sourceText || ""),
    sourceAuthor: cleanNick(element.dataset.sourceAuthor || "") || counterpartyLabelForSelected(),
    target: commonMessageDialogTarget()
  };
  renderTextPaint();
  window.setTimeout(() => {
    textPaint?.querySelector<HTMLTextAreaElement>(`.message-dialog-panel[data-dialog-source-id="${cssEscape(sourceId)}"] textarea`)?.focus();
  }, 0);
}

async function submitMessageDialogReply(form: HTMLFormElement): Promise<void> {
  if (!selectedId || !textarea || !openMessageDialog || openMessageDialog.chatId !== selectedId) {
    return;
  }
  const input = form.querySelector<HTMLTextAreaElement>("textarea");
  const reply = normalizeChatMessage(input?.value || "");
  if (!reply) {
    return;
  }
  const sync = syncs.get(selectedId);
  if (!sync) {
    return;
  }
  const author = cleanNick(device?.nick || "") || "Я";
  const line = createMessageDialogLine({
    chatId: selectedId,
    sourceId: openMessageDialog.sourceId,
    sourceText: openMessageDialog.sourceText,
    sourceAuthor: openMessageDialog.sourceAuthor,
    author,
    authorId: device?.id || "",
    text: reply,
    target: openMessageDialog.target
  });
  if (!line) {
    return;
  }
  const current = texts.get(selectedId) ?? textarea.value;
  const separator = current.length > 0 && !current.endsWith("\n") ? "\n" : "";
  const next = `${current}${separator}${line}\n`;
  textarea.value = next;
  texts.set(selectedId, next);
  sync.setText(next);
  saveTextSnapshotNow(selectedId, next);
  input && (input.value = "");
  touchSelected();
  renderTiles();
  renderTextPaint();
}

function spaceEntryVisibleInMode(entry: SpaceEntry, mode: SpaceMode): boolean {
  if (mode === "wall") {
    return entry.kind === "wall" || entry.kind === "wall-comment";
  }
  if (mode === "reputation") {
    return entry.kind === "reputation" || entry.kind === "reputation-comment";
  }
  return false;
}

function markedSourceIds(lines: readonly string[]): Set<string> {
  const result = new Set<string>();
  for (const line of lines) {
    const entry = parseSpaceEntryLine(line);
    if (!entry) {
      continue;
    }
    if (entry.sourceId) {
      result.add(entry.sourceId);
    }
    result.add(spaceFallbackMarkerId(entry.kind, entry.text));
  }
  return result;
}

function renderBubbleMarkButton(
  kind: SpaceEntryKind,
  lineIndex: number,
  sourceId: string,
  author: string,
  text: string,
  marked: boolean,
  ownSpace: boolean
): string {
  const display = spaceMarkDisplay(kind, ownSpace);
  const label = marked ? display.activeLabel : display.actionLabel;
  return `
    <button class="bubble-mark ${display.className}${marked ? " is-marked" : ""}" type="button" data-mark-kind="${kind}" data-mark-role="${display.role}" data-owner-space="${ownSpace ? "1" : "0"}" data-line-index="${lineIndex}" data-source-id="${escapeHtml(sourceId)}" data-author="${escapeHtml(author)}" data-text="${escapeHtml(text)}" aria-label="${escapeHtml(label)}" data-tooltip="${escapeHtml(label)}">
      ${icon(display.icon)}
    </button>
  `;
}

function markDialogMessage(button: HTMLButtonElement): void {
  if (!selectedId || !textarea) {
    return;
  }
  const kind = normalizeSpaceEntryKind(button.dataset.markKind || "");
  if (!kind) {
    return;
  }
  const ownSpace = button.dataset.ownerSpace === "1";
  const current = texts.get(selectedId) ?? textarea.value;
  const lines = current.endsWith("\n") ? current.slice(0, -1).split("\n") : current.split("\n");
  const lineIndex = Number(button.dataset.lineIndex || "-1");
  const line = lines[lineIndex] ?? "";
  const text = normalizeChatMessage(line || button.dataset.text || "");
  if (!text) {
    return;
  }
  const sourceId = button.dataset.sourceId || spaceMessageSourceId(selectedId, lineIndex, text);
  const existing = markedSourceIds(lines);
  if (existing.has(sourceId) || existing.has(spaceFallbackMarkerId(kind, text))) {
    const display = spaceMarkDisplay(kind, ownSpace);
    button.classList.add("is-marked");
    button.setAttribute("aria-label", display.activeLabel);
    button.dataset.tooltip = display.activeLabel;
    return;
  }
  const sync = syncs.get(selectedId);
  if (!sync) {
    return;
  }
  const author = cleanNick(button.dataset.author || "") || cleanNick(device?.nick || "") || "Я";
  const marker = makeSpaceEntryLine(kind, {
    author,
    authorId: kind === "wall" ? device?.id || "" : "",
    sourceId,
    text
  });
  const separator = current.length > 0 && !current.endsWith("\n") ? "\n" : "";
  const next = `${current}${separator}${marker}\n`;
  textarea.value = next;
  texts.set(selectedId, next);
  sync.setText(next);
  saveTextSnapshotNow(selectedId, next);
  syncMarkedSpaceEntryToProfile(kind, text, author);
  button.classList.add("is-marked");
  const display = spaceMarkDisplay(kind, ownSpace);
  button.setAttribute("aria-label", display.activeLabel);
  button.dataset.tooltip = display.activeLabel;
  renderTextPaint();
}

function syncMarkedSpaceEntryToProfile(kind: SpaceEntryKind, text: string, author: string): void {
  const handle = selectedSpaceOwnerHandle();
  if (!handle) {
    return;
  }
  const job = spaceMarkDisplay(kind).role === "self"
    ? savePersonalSpacePost({ handle, slug: "" }, { text })
    : savePersonalSpaceReviewFromMark(handle, author, text);
  void job.catch(() => undefined);
}

async function savePersonalSpaceReviewFromMark(handle: string, author: string, text: string): Promise<void> {
  const response = await fetch(`/api/spaces/${encodeURIComponent(handle)}/reviews`, {
    method: "POST",
    headers: {
      "Content-Type": "application/json",
      Accept: "application/json"
    },
    body: JSON.stringify({
      author: cleanNick(author) || loadSelfStartHandle() || cleanNick(device?.nick || "") || "Гость",
      text,
      rating: 5
    })
  });
  if (!response.ok) {
    throw new Error("personal review mark rejected");
  }
}

function selectedSpaceOwnerHandle(): string {
  const tunnel = loadTunnels().find((item) => item.id === selectedId);
  if (!tunnel) {
    return "";
  }
  if (isSelfTunnel(tunnel)) {
    return loadSelfStartHandle();
  }
  if (isAgentTunnel(tunnel)) {
    return "";
  }
  return contactHandleFromTunnel(tunnel);
}

function spaceMessageSourceId(tunnelId: string, lineIndex: number, text: string): string {
  return `msg:${tunnelId}:${lineIndex}:${hashShort(text)}`;
}

function spaceFallbackMarkerId(kind: SpaceEntryKind, text: string): string {
  return `fallback:${kind}:${hashShort(text)}`;
}

function hashShort(value: string): string {
  let hash = 2166136261;
  for (let index = 0; index < value.length; index += 1) {
    hash ^= value.charCodeAt(index);
    hash = Math.imul(hash, 16777619);
  }
  return (hash >>> 0).toString(36);
}

function renderEmptySpacePrompt(mode: SpaceMode, tunnel: TunnelRecord | null | undefined): string {
  const text = spaceEmptyPrompt(mode, {
    ownSpace: isOwnSpace(tunnel),
    agentSpace: Boolean(tunnel && isAgentTunnel(tunnel))
  });
  return `<div class="space-empty">${escapeHtml(text)}</div>`;
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
    side: "remote"
  };
}

function operatorNameFromLine(line: string): string {
  const match = line.trim().match(/^(.+?)\s+·\s+\d{1,2}:\d{2}$/u);
  return cleanNick(match?.[1] || "");
}

function operatorTimeFromLine(line: string): string {
  return line.trim().match(/\s+·\s+(\d{1,2}:\d{2})$/u)?.[1] || "";
}

function counterpartyLabelForSelected(): string {
  return counterpartyLabelForTunnelId(selectedId);
}

function counterpartyLabelForTunnelId(tunnelId: string): string {
  const tunnel = loadTunnels().find((item) => item.id === tunnelId);
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
  return isOperatorHeaderText(line);
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
    || isMessageDialogLine(trimmed)
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
    pop.innerHTML = "";
    pop.dataset.state = "quiet";
    return;
  }
  const nick = cleanNick(activity.nick) || counterpartyLabelForSelected();
  pop.dataset.state = "active";
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

function selectedDialogCode(): string {
  return selectedId ? selectedId.slice(0, 8).toUpperCase() : "";
}

async function selectedDialogLink(): Promise<string> {
  if (!device || !selectedId) {
    return "";
  }
  const tunnel = loadTunnels().find((item) => item.id === selectedId);
  if (!tunnel) {
    return "";
  }
  const publicUrl = publicContactUrlForTunnel(tunnel);
  if (publicUrl) {
    return publicUrl;
  }
  const url = new URL(await inviteUrl(tunnel, device));
  url.searchParams.set("bare", "1");
  return url.href;
}

async function shareSelectedDialogLink(): Promise<void> {
  const tunnel = loadTunnels().find((item) => item.id === selectedId);
  const publicUrl = tunnel ? publicContactUrlForTunnel(tunnel) : "";
  if (publicUrl) {
    closeQrOverlay();
    const overlay = await showLinkShareSheet({
      title: tunnel ? counterpartyLabel(tunnel) : "Контакт",
      url: publicUrl,
      onClose: (closed) => {
        if (qrOverlay === closed) {
          qrOverlay = null;
          qrMode = null;
        }
      }
    });
    qrOverlay = overlay;
    qrMode = "manual";
    return;
  }
  const link = await selectedDialogLink();
  if (!link) {
    return;
  }
  let copied = false;
  await copyText(link);
  copied = true;
  const id = app.querySelector<HTMLButtonElement>(".dialog-id");
  if (!id) {
    return;
  }
  id.dataset.copied = "1";
  id.dataset.tooltip = copied ? "Ссылка скопирована" : "Готово";
  window.setTimeout(() => {
    if (id.dataset.copied === "1") {
      delete id.dataset.copied;
      const code = selectedDialogCode();
      const contactUrl = tunnel ? publicContactUrlForTunnel(tunnel) : "";
      id.dataset.tooltip = contactUrl ? "Поделиться контактом" : code ? `Скопировать ссылку · ${code}` : "Диалог не выбран";
    }
  }, 1200);
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

function linkifyChatLine(line: string): string {
  const urlPattern = /\b(?:https?:\/\/|www\.)[^\s<>"']+/giu;
  let html = "";
  let lastIndex = 0;
  for (const match of line.matchAll(urlPattern)) {
    const start = match.index ?? 0;
    const raw = match[0] || "";
    html += escapeHtml(line.slice(lastIndex, start));
    const [urlText, suffix] = splitTrailingUrlPunctuation(raw);
    const href = safeChatHref(urlText);
    if (href) {
      html += `<a href="${escapeHtml(href)}" target="_blank" rel="noopener noreferrer">${escapeHtml(urlText)}</a>${escapeHtml(suffix)}`;
    } else {
      html += escapeHtml(raw);
    }
    lastIndex = start + raw.length;
  }
  return `${html}${escapeHtml(line.slice(lastIndex))}`;
}

function splitTrailingUrlPunctuation(value: string): readonly [string, string] {
  let urlText = value;
  let suffix = "";
  while (/[.,!?;:)\]}]$/u.test(urlText)) {
    suffix = `${urlText.slice(-1)}${suffix}`;
    urlText = urlText.slice(0, -1);
  }
  return [urlText, suffix] as const;
}

function safeChatHref(value: string): string {
  const normalized = value.toLowerCase().startsWith("www.") ? `https://${value}` : value;
  try {
    const url = new URL(normalized);
    return url.protocol === "http:" || url.protocol === "https:" ? url.href : "";
  } catch {
    return "";
  }
}

function cssEscape(value: string): string {
  const css = (window as Window & { CSS?: { escape?: (input: string) => string } }).CSS;
  return css?.escape ? css.escape(value) : value.replace(/["\\]/gu, "\\$&");
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
  const clean = cleanNick(value).replace(/^@/u, "").trim();
  const parts = clean.split(" ").filter(Boolean);
  const letters = parts.length > 1
    ? `${parts[0]?.[0] ?? ""}${parts[1]?.[0] ?? ""}`
    : clean.slice(0, 2);
  return (letters || ".").toUpperCase();
}
