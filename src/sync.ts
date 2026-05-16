import * as Y from "yjs";
import {
  DeviceRecord,
  JoinAcceptPayload,
  TunnelRecord,
  decryptFromTunnel,
  decode,
  encode,
  encryptRoomKeyForJoin,
  encryptForTunnel,
  roomAuth
} from "./trustlink";

export interface PeerInfo {
  readonly id: string;
  readonly nick: string;
}

export interface SyncCallbacks {
  readonly onText: (text: string) => void;
  readonly onTerminal: (terminal: TerminalSnapshot) => void;
  readonly onChess: (chess: SyncedChessState | null) => void;
  readonly onActivity: (activity: WriterActivity) => void;
  readonly onRemoteChange: (activity: WriterActivity) => void;
  readonly onLiveDraft: (draft: LiveDraft) => void;
  readonly onFile: (file: ReceivedFile) => void;
  readonly onFileDeleted: (fileId: string) => void;
  readonly onKnock: (knock: NoticeKnock) => void;
  readonly onRemoteRequest: (request: RemoteRequest) => void;
  readonly onRemoteGrant: (grant: RemoteGrant) => void;
  readonly onRemoteCommand: (command: RemoteCommand) => void;
  readonly onRemoteScript: (script: RemoteScript) => void;
  readonly onRemoteCancel: (cancel: RemoteCancel) => void;
  readonly onRemoteOutput: (output: RemoteOutput) => void;
  readonly onPeers: (peers: readonly PeerInfo[]) => void;
  readonly onJoinRequest: (request: JoinRequest) => void;
  readonly onClosed: () => void;
  readonly onState: (state: "open" | "closed" | "connecting") => void;
}

export interface JoinRequest {
  readonly requestId: string;
  readonly deviceId: string;
  readonly nick: string;
  readonly publicJwk: JsonWebKey;
}

export interface WriterActivity {
  readonly deviceId: string;
  readonly nick: string;
  readonly index: number;
  readonly local: boolean;
  readonly action: "write" | "erase" | "edit";
  readonly preview: string;
  readonly insertText?: string;
  readonly deleteCount?: number;
  readonly startLine?: number;
  readonly startColumn?: number;
  readonly lineDelta?: number;
}

export interface LiveDraft {
  readonly deviceId: string;
  readonly nick: string;
  readonly text: string;
  readonly index: number;
  readonly active: boolean;
  readonly seq: number;
  readonly createdAt: string;
}

export interface ReceivedFile {
  readonly id: string;
  readonly name: string;
  readonly type: string;
  readonly size: number;
  readonly bytes: Uint8Array;
  readonly url: string;
  readonly nick: string;
  readonly deviceId: string;
  readonly createdAt: string;
  readonly autoDownload?: boolean;
  readonly delivery?: string;
  readonly commandId?: string;
}

export interface NoticeKnock {
  readonly id: string;
  readonly targetDeviceId: string;
  readonly deviceId: string;
  readonly nick: string;
  readonly createdAt: string;
}

export interface RemoteGrant {
  readonly id: string;
  readonly enabled: boolean;
  readonly targetDeviceId: string;
  readonly deviceId: string;
  readonly nick: string;
  readonly createdAt: string;
}

export interface RemoteRequest {
  readonly id: string;
  readonly targetDeviceId: string;
  readonly deviceId: string;
  readonly nick: string;
  readonly createdAt: string;
}

export interface RemoteCommand {
  readonly id: string;
  readonly command: string;
  readonly timeoutMs?: number;
  readonly runAs?: string;
  readonly targetDeviceId: string;
  readonly deviceId: string;
  readonly nick: string;
  readonly createdAt: string;
}

export interface RemoteScript {
  readonly id: string;
  readonly name: string;
  readonly shell: string;
  readonly script: string;
  readonly timeoutMs?: number;
  readonly runAs?: string;
  readonly targetDeviceId: string;
  readonly deviceId: string;
  readonly nick: string;
  readonly createdAt: string;
}

export interface RemoteCancel {
  readonly id: string;
  readonly commandId: string;
  readonly targetDeviceId: string;
  readonly deviceId: string;
  readonly nick: string;
  readonly createdAt: string;
}

export interface RemoteOutput {
  readonly id: string;
  readonly commandId: string;
  readonly text: string;
  readonly targetDeviceId: string;
  readonly exitCode?: number;
  readonly deviceId: string;
  readonly nick: string;
  readonly createdAt: string;
}

export interface TerminalSnapshot {
  readonly lines: readonly string[];
  readonly state: "idle" | "run" | "ok" | "bad" | "off";
}

export type SyncedChessState = Readonly<Record<string, unknown>>;

interface EncryptedUpdate {
  readonly id: string;
  readonly kind: "update" | "snapshot";
  readonly nonce: string;
  readonly ciphertext: string;
  readonly deviceId?: string;
  readonly deviceNick?: string;
  readonly createdAt?: string;
}

interface EncryptedFileComplete {
  readonly kind?: "complete";
  readonly id: string;
  readonly nonce: string;
  readonly ciphertext: string;
  readonly metaNonce: string;
  readonly metaCiphertext: string;
  readonly bytes: number;
  readonly deviceId?: string;
  readonly deviceNick?: string;
  readonly createdAt?: string;
}

interface EncryptedFileChunk {
  readonly kind: "chunk";
  readonly id: string;
  readonly fileId: string;
  readonly index: number;
  readonly total: number;
  readonly totalBytes: number;
  readonly nonce: string;
  readonly ciphertext: string;
  readonly metaNonce?: string;
  readonly metaCiphertext?: string;
  readonly bytes: number;
  readonly deviceId?: string;
  readonly deviceNick?: string;
  readonly createdAt?: string;
}

interface EncryptedFileDelete {
  readonly kind: "delete";
  readonly id: string;
  readonly fileId: string;
  readonly deviceId?: string;
  readonly deviceNick?: string;
  readonly createdAt?: string;
}

type EncryptedFile = EncryptedFileComplete | EncryptedFileChunk | EncryptedFileDelete;
type OutboundEncryptedFile =
  | Omit<EncryptedFileComplete, "deviceId" | "deviceNick" | "createdAt">
  | Omit<EncryptedFileChunk, "deviceId" | "deviceNick" | "createdAt">
  | Omit<EncryptedFileDelete, "deviceId" | "deviceNick" | "createdAt">;

interface EncryptedLiveDraft {
  readonly id: string;
  readonly nonce: string;
  readonly ciphertext: string;
  readonly deviceId?: string;
  readonly deviceNick?: string;
  readonly nick?: string;
  readonly createdAt?: string;
}

interface EncryptedRemoteCommand {
  readonly id: string;
  readonly targetDeviceId: string;
  readonly nonce: string;
  readonly ciphertext: string;
  readonly deviceId?: string;
  readonly deviceNick?: string;
  readonly nick?: string;
  readonly createdAt?: string;
}

interface EncryptedRemoteScript {
  readonly id: string;
  readonly targetDeviceId: string;
  readonly nonce: string;
  readonly ciphertext: string;
  readonly deviceId?: string;
  readonly deviceNick?: string;
  readonly nick?: string;
  readonly createdAt?: string;
}

interface EncryptedRemoteOutput {
  readonly id: string;
  readonly commandId: string;
  readonly targetDeviceId: string;
  readonly nonce: string;
  readonly ciphertext: string;
  readonly exitCode?: number;
  readonly deviceId?: string;
  readonly deviceNick?: string;
  readonly nick?: string;
  readonly createdAt?: string;
}

interface RemoteCancelMessage {
  readonly id: string;
  readonly commandId: string;
  readonly targetDeviceId: string;
  readonly deviceId?: string;
  readonly deviceNick?: string;
  readonly nick?: string;
  readonly createdAt?: string;
}

interface P2pDescriptionSignal {
  readonly id: string;
  readonly kind: "offer" | "answer";
  readonly targetDeviceId: string;
  readonly sdp: string;
  readonly deviceId?: string;
  readonly deviceNick?: string;
  readonly nick?: string;
  readonly createdAt?: string;
}

interface P2pCandidateSignal {
  readonly id: string;
  readonly targetDeviceId: string;
  readonly candidate: string;
  readonly sdpMid?: string | null;
  readonly sdpMLineIndex?: number | null;
  readonly deviceId?: string;
  readonly deviceNick?: string;
  readonly nick?: string;
  readonly createdAt?: string;
}

interface P2pPeer {
  readonly id: string;
  pc: RTCPeerConnection;
  channel: RTCDataChannel | null;
  pendingCandidates: RTCIceCandidateInit[];
  retryTimer: number;
}

interface FileTransfer {
  readonly chunks: Uint8Array[];
  readonly seen: Set<number>;
  total: number;
  totalBytes: number;
  meta?: {
    readonly name?: string;
    readonly type?: string;
    readonly size?: number;
    readonly autoDownload?: boolean;
    readonly delivery?: string;
    readonly commandId?: string;
  };
  createdAt?: string;
  deviceId?: string;
  deviceNick?: string;
}

type ServerMessage =
  | { readonly type: "hello"; readonly snapshot: EncryptedUpdate | null; readonly updates: readonly EncryptedUpdate[]; readonly files?: readonly EncryptedFile[]; readonly peers: readonly PeerInfo[] }
  | { readonly type: "ack"; readonly id: string }
  | { readonly type: "pong" }
  | { readonly type: "update"; readonly update: EncryptedUpdate }
  | { readonly type: "file"; readonly file: EncryptedFile }
  | { readonly type: "presence"; readonly peers: readonly PeerInfo[] }
  | { readonly type: "join.request"; readonly request: JoinRequest }
  | { readonly type: "notice.knock"; readonly knock: NoticeKnock }
  | { readonly type: "live.draft"; readonly draft: EncryptedLiveDraft }
  | { readonly type: "remote.request"; readonly request: RemoteRequest }
  | { readonly type: "remote.grant"; readonly grant: RemoteGrant }
  | { readonly type: "remote.command"; readonly command: EncryptedRemoteCommand }
  | { readonly type: "remote.script"; readonly script: EncryptedRemoteScript }
  | { readonly type: "remote.cancel"; readonly cancel: RemoteCancelMessage }
  | { readonly type: "remote.output"; readonly output: EncryptedRemoteOutput }
  | { readonly type: "p2p.offer"; readonly signal: P2pDescriptionSignal }
  | { readonly type: "p2p.answer"; readonly signal: P2pDescriptionSignal }
  | { readonly type: "p2p.candidate"; readonly signal: P2pCandidateSignal }
  | { readonly type: "closed" };

type UpdateKind = "update" | "snapshot";
interface OutboundUpdate {
  readonly id: string;
  readonly kind: UpdateKind;
  readonly update: Uint8Array;
}

type ControlMessage =
  | { readonly type: "join.accept"; readonly requestId: string; readonly accept: JoinAcceptPayload }
  | { readonly type: "join.deny"; readonly requestId: string }
  | { readonly type: "file"; readonly file: OutboundEncryptedFile }
  | { readonly type: "notice.knock"; readonly knock: { readonly id: string; readonly targetDeviceId: string } }
  | { readonly type: "live.draft"; readonly draft: { readonly id: string; readonly nonce: string; readonly ciphertext: string } }
  | { readonly type: "remote.request"; readonly request: { readonly id: string; readonly targetDeviceId: string } }
  | { readonly type: "remote.grant"; readonly grant: { readonly id: string; readonly enabled: boolean; readonly targetDeviceId: string } }
  | { readonly type: "remote.command"; readonly command: { readonly id: string; readonly targetDeviceId: string; readonly nonce: string; readonly ciphertext: string } }
  | { readonly type: "remote.script"; readonly script: { readonly id: string; readonly targetDeviceId: string; readonly nonce: string; readonly ciphertext: string } }
  | { readonly type: "remote.cancel"; readonly cancel: { readonly id: string; readonly commandId: string; readonly targetDeviceId: string } }
  | { readonly type: "remote.output"; readonly output: { readonly id: string; readonly commandId: string; readonly targetDeviceId: string; readonly nonce: string; readonly ciphertext: string; readonly exitCode?: number } };
type FileControlMessage = Extract<ControlMessage, { readonly type: "file" }>;

type DirectMessage =
  | { readonly type: "update"; readonly update: EncryptedUpdate }
  | { readonly type: "file"; readonly file: EncryptedFile }
  | { readonly type: "notice.knock"; readonly knock: NoticeKnock }
  | { readonly type: "live.draft"; readonly draft: EncryptedLiveDraft }
  | { readonly type: "remote.request"; readonly request: RemoteRequest }
  | { readonly type: "remote.grant"; readonly grant: RemoteGrant }
  | { readonly type: "remote.command"; readonly command: EncryptedRemoteCommand }
  | { readonly type: "remote.script"; readonly script: EncryptedRemoteScript }
  | { readonly type: "remote.cancel"; readonly cancel: RemoteCancelMessage }
  | { readonly type: "remote.output"; readonly output: EncryptedRemoteOutput };

const heartbeatIntervalMs = 12_000;
const staleConnectionMs = 42_000;
const reconnectJitterMs = 750;
const minReconnectDelayMs = 500;
const maxReconnectDelayMs = 30_000;
const p2pRetryMs = 6000;
const p2pIceServers: RTCIceServer[] = [
  { urls: "stun:stun.l.google.com:19302" },
  { urls: "stun:stun.cloudflare.com:3478" }
];

function safeRemoteTimeoutMs(value: unknown): number {
  const timeoutMs = typeof value === "number" ? value : 0;
  if (!Number.isFinite(timeoutMs) || timeoutMs <= 0) {
    return 0;
  }
  return Math.max(1000, Math.min(Math.trunc(timeoutMs), 2 * 60 * 60_000));
}

export class TunnelSync {
  private readonly doc = new Y.Doc();
  private readonly text = this.doc.getText("body");
  private readonly chessMeta = this.doc.getMap<string>("chessMeta");
  private ws: WebSocket | null = null;
  private destroyed = false;
  private ready = false;
  private localUpdates = 0;
  private reconnectTimer = 0;
  private reconnectDelay = minReconnectDelayMs;
  private snapshotTimer = 0;
  private heartbeatTimer = 0;
  private lastSeenAt = 0;
  private needsSnapshot = false;
  private readonly auth: Promise<string>;
  private readonly offlineQueue: OutboundUpdate[] = [];
  private readonly pendingAcks = new Map<string, OutboundUpdate>();
  private readonly pendingFileControls = new Map<string, FileControlMessage>();
  private readonly controlQueue: ControlMessage[] = [];
  private readonly fileTransfers = new Map<string, FileTransfer>();
  private readonly p2pPeers = new Map<string, P2pPeer>();
  private readonly seenUpdateIds = new Set<string>();
  private readonly seenControlIds = new Set<string>();
  private readonly completedFileIds = new Set<string>();
  private readonly deletedFileIds = new Set<string>();
  private readonly directSentUpdateIds = new Set<string>();
  private liveDraftSeq = 0;
  private sendQueue = Promise.resolve();
  private readonly wakeReconnect = () => {
    this.recoverConnection();
  };
  private readonly visibleReconnect = () => {
    if (document.visibilityState === "visible") {
      this.recoverConnection(true);
      return;
    }
    this.queueSnapshotNow();
  };
  private readonly offlineState = () => {
    this.ready = false;
    this.callbacks.onState("closed");
  };
  private readonly networkConnection = (navigator as Navigator & { connection?: EventTarget }).connection ?? null;

  constructor(
    private readonly tunnel: TunnelRecord,
    private readonly device: DeviceRecord,
    private readonly callbacks: SyncCallbacks
  ) {
    this.auth = roomAuth(tunnel);
    this.doc.on("update", (update: Uint8Array, origin: unknown) => {
      if (origin === "remote") {
        return;
      }
      this.queueUpdate(update, "update");
      this.scheduleSnapshot();
    });
    this.text.observe(() => {
      this.callbacks.onText(this.text.toString());
    });
    this.chessMeta.observe(() => {
      this.callbacks.onChess(this.chessSnapshot());
    });
    window.addEventListener("online", this.wakeReconnect);
    window.addEventListener("offline", this.offlineState);
    window.addEventListener("focus", this.wakeReconnect);
    window.addEventListener("pageshow", this.wakeReconnect);
    document.addEventListener("visibilitychange", this.visibleReconnect);
    this.networkConnection?.addEventListener("change", this.wakeReconnect);
    this.heartbeatTimer = window.setInterval(() => this.checkConnection(), heartbeatIntervalMs);
    this.connect();
  }

  setText(next: string): void {
    const current = this.text.toString();
    if (current === next) {
      return;
    }
    const [start, deleteCount, insertText] = diffText(current, next);
    const activity = describeActivity(current, start, deleteCount, insertText);
    this.callbacks.onActivity({
      deviceId: this.device.id,
      nick: this.device.nick,
      index: start,
      local: true,
      action: activity.action,
      preview: activity.preview,
      insertText,
      deleteCount,
      startLine: lineFromIndex(current, start),
      startColumn: columnFromIndex(current, start),
      lineDelta: lineBreakCount(insertText) - lineBreakCount(current.slice(start, start + deleteCount))
    });
    this.doc.transact(() => {
      if (deleteCount > 0) {
        this.text.delete(start, deleteCount);
      }
      if (insertText.length > 0) {
        this.text.insert(start, insertText);
      }
    }, "local");
  }

  appendTerminalLine(line: string): void {
    // Terminal output is a local capability surface, never shared room state.
    void line;
  }

  setTerminalState(state: TerminalSnapshot["state"]): void {
    void state;
  }

  terminalSnapshot(): TerminalSnapshot {
    return {
      lines: [],
      state: "off"
    };
  }

  setChessSnapshot(snapshot: SyncedChessState): void {
    this.chessMeta.set("state", JSON.stringify(snapshot));
  }

  chessSnapshot(): SyncedChessState | null {
    const raw = this.chessMeta.get("state");
    if (!raw) {
      return null;
    }
    try {
      const parsed = JSON.parse(raw) as unknown;
      if (parsed && typeof parsed === "object" && !Array.isArray(parsed)) {
        return parsed as SyncedChessState;
      }
    } catch {
      // Ignore corrupt chess state; the room can create a fresh game.
    }
    return null;
  }

  async sendLiveDraft(text: string): Promise<void> {
    const body = String(text || "").slice(0, 4000);
    const payload = {
      text: body,
      index: this.text.toString().length,
      active: body.length > 0,
      seq: ++this.liveDraftSeq
    };
    const encrypted = await encryptForTunnel(this.tunnel, encode(JSON.stringify(payload)));
    this.sendControl({
      type: "live.draft",
      draft: {
        id: `live_${crypto.randomUUID()}`,
        nonce: encrypted.nonce,
        ciphertext: encrypted.ciphertext
      }
    });
  }

  closeForEveryone(): void {
    if (this.ws?.readyState === WebSocket.OPEN) {
      this.ws.send(JSON.stringify({ type: "close" }));
    }
    this.destroy();
  }

  async acceptJoin(request: JoinRequest, hostNick: string): Promise<void> {
    const accept = await encryptRoomKeyForJoin(this.tunnel.key, request.publicJwk, hostNick);
    this.sendControl({ type: "join.accept", requestId: request.requestId, accept });
  }

  denyJoin(request: JoinRequest): void {
    this.sendControl({ type: "join.deny", requestId: request.requestId });
  }

  async sendFile(file: File): Promise<ReceivedFile> {
    const fileId = `file_${crypto.randomUUID()}`;
    const bytes = new Uint8Array(await file.arrayBuffer());
    const meta = encode(JSON.stringify({
      name: file.name,
      type: file.type || "application/octet-stream",
      size: file.size
    }));
    const encryptedMeta = await encryptForTunnel(this.tunnel, meta);
    const chunkSize = 256_000;
    const total = Math.max(1, Math.ceil(bytes.length / chunkSize));
    for (let index = 0; index < total; index += 1) {
      const chunk = bytes.slice(index * chunkSize, Math.min(bytes.length, (index + 1) * chunkSize));
      const encrypted = await encryptForTunnel(this.tunnel, chunk);
      this.sendControl({
        type: "file",
        file: {
          kind: "chunk",
          id: `${fileId}_${index}`,
          fileId,
          index,
          total,
          totalBytes: file.size,
          bytes: chunk.byteLength,
          nonce: encrypted.nonce,
          ciphertext: encrypted.ciphertext,
          ...(index === 0 ? {
            metaNonce: encryptedMeta.nonce,
            metaCiphertext: encryptedMeta.ciphertext
          } : {})
        }
      });
      if (total > 1) {
        await wait(60);
      }
    }
    return {
      id: fileId,
      name: file.name || "file",
      type: file.type || "application/octet-stream",
      size: file.size,
      bytes: new Uint8Array(),
      url: URL.createObjectURL(file),
      nick: this.device.nick,
      deviceId: this.device.id,
      createdAt: new Date().toISOString()
    };
  }

  async sendFileChunkFromBytes(
    fileId: string,
    meta: {
      readonly name: string;
      readonly type?: string;
      readonly size: number;
      readonly autoDownload?: boolean;
      readonly delivery?: string;
      readonly commandId?: string;
    },
    chunk: Uint8Array,
    index: number,
    total: number
  ): Promise<void> {
    const safeFileId = cleanFileId(fileId);
    const safeIndex = Math.max(0, Math.trunc(index));
    const safeTotal = Math.max(1, Math.trunc(total));
    if (!safeFileId || safeIndex >= safeTotal) {
      throw new Error("bad file chunk");
    }
    const fileName = cleanFileName(meta.name || "file");
    const fileType = (meta.type || "application/octet-stream").slice(0, 160);
    const fileSize = Math.max(0, Math.trunc(meta.size || 0));
    const encrypted = await encryptForTunnel(this.tunnel, chunk);
    const encryptedMeta = safeIndex === 0
      ? await encryptForTunnel(this.tunnel, encode(JSON.stringify({
        name: fileName,
        type: fileType,
        size: fileSize,
        ...(meta.autoDownload === true ? { autoDownload: true } : {}),
        ...(meta.delivery ? { delivery: String(meta.delivery).slice(0, 80) } : {}),
        ...(meta.commandId ? { commandId: String(meta.commandId).slice(0, 120) } : {})
      })))
      : null;
    this.sendControl({
      type: "file",
      file: {
        kind: "chunk",
        id: `${safeFileId}_${safeIndex}`,
        fileId: safeFileId,
        index: safeIndex,
        total: safeTotal,
        totalBytes: fileSize,
        bytes: chunk.byteLength,
        nonce: encrypted.nonce,
        ciphertext: encrypted.ciphertext,
        ...(encryptedMeta ? {
          metaNonce: encryptedMeta.nonce,
          metaCiphertext: encryptedMeta.ciphertext
        } : {})
      }
    });
  }

  deleteFile(fileId: string): void {
    this.fileTransfers.delete(fileId);
    this.sendControl({
      type: "file",
      file: {
        kind: "delete",
        id: `file_delete_${crypto.randomUUID()}`,
        fileId
      }
    });
  }

  sendKnock(targetDeviceId = "*"): void {
    this.sendControl({
      type: "notice.knock",
      knock: {
        id: `knock_${crypto.randomUUID()}`,
        targetDeviceId
      }
    });
  }

  requestRemote(targetDeviceId = "*"): void {
    this.sendControl({
      type: "remote.request",
      request: {
        id: `remote_request_${crypto.randomUUID()}`,
        targetDeviceId
      }
    });
  }

  grantRemote(enabled: boolean, targetDeviceId = "*"): void {
    this.sendControl({
      type: "remote.grant",
      grant: {
        id: `remote_grant_${crypto.randomUUID()}`,
        enabled,
        targetDeviceId
      }
    });
  }

  async sendRemoteCommand(targetDeviceId: string, command: string, timeoutMs = 0, runAs = ""): Promise<string> {
    const id = `remote_cmd_${crypto.randomUUID()}`;
    const encrypted = await encryptForTunnel(this.tunnel, encode(JSON.stringify({
      command,
      ...(runAs ? { runAs } : {}),
      ...(safeRemoteTimeoutMs(timeoutMs) ? { timeoutMs: safeRemoteTimeoutMs(timeoutMs) } : {})
    })));
    this.sendControl({
      type: "remote.command",
      command: {
        id,
        targetDeviceId,
        nonce: encrypted.nonce,
        ciphertext: encrypted.ciphertext
      }
    });
    return id;
  }

  async sendRemoteScript(targetDeviceId: string, script: { readonly name?: string; readonly shell?: string; readonly script: string; readonly timeoutMs?: number; readonly runAs?: string }): Promise<string> {
    const id = `remote_script_${crypto.randomUUID()}`;
    const encrypted = await encryptForTunnel(this.tunnel, encode(JSON.stringify({
      name: script.name || "script",
      shell: script.shell || "",
      script: script.script.slice(0, 1_000_000),
      ...(script.runAs ? { runAs: script.runAs } : {}),
      ...(safeRemoteTimeoutMs(script.timeoutMs) ? { timeoutMs: safeRemoteTimeoutMs(script.timeoutMs) } : {})
    })));
    this.sendControl({
      type: "remote.script",
      script: {
        id,
        targetDeviceId,
        nonce: encrypted.nonce,
        ciphertext: encrypted.ciphertext
      }
    });
    return id;
  }

  async sendRemoteCancel(targetDeviceId: string, commandId: string): Promise<string> {
    const id = `remote_cancel_${crypto.randomUUID()}`;
    this.sendControl({
      type: "remote.cancel",
      cancel: {
        id,
        commandId,
        targetDeviceId
      }
    });
    return id;
  }

  async sendRemoteOutput(targetDeviceId: string, commandId: string, text: string, exitCode?: number): Promise<string> {
    const id = `remote_out_${crypto.randomUUID()}`;
    const encrypted = await encryptForTunnel(this.tunnel, encode(JSON.stringify({ text })));
    this.sendControl({
      type: "remote.output",
      output: {
        id,
        commandId,
        targetDeviceId,
        nonce: encrypted.nonce,
        ciphertext: encrypted.ciphertext,
        ...(typeof exitCode === "number" ? { exitCode } : {})
      }
    });
    return id;
  }

  destroy(): void {
    this.destroyed = true;
    window.clearTimeout(this.reconnectTimer);
    window.clearTimeout(this.snapshotTimer);
    window.clearInterval(this.heartbeatTimer);
    window.removeEventListener("online", this.wakeReconnect);
    window.removeEventListener("offline", this.offlineState);
    window.removeEventListener("focus", this.wakeReconnect);
    window.removeEventListener("pageshow", this.wakeReconnect);
    document.removeEventListener("visibilitychange", this.visibleReconnect);
    this.networkConnection?.removeEventListener("change", this.wakeReconnect);
    const ws = this.ws;
    this.ws = null;
    if (ws?.readyState === WebSocket.OPEN) {
      ws.close();
    }
    this.closeP2pPeers();
    this.doc.destroy();
  }

  private connect(): void {
    if (this.destroyed) {
      return;
    }
    if (this.ws && this.ws.readyState < WebSocket.CLOSING) {
      return;
    }
    this.callbacks.onState("connecting");
    const protocol = window.location.protocol === "https:" ? "wss:" : "ws:";
    const ws = new WebSocket(`${protocol}//${window.location.host}/ws/${this.tunnel.id}`);
    this.ws = ws;

    ws.onopen = () => {
      if (this.destroyed) {
        ws.close();
        return;
      }
      this.lastSeenAt = Date.now();
      void this.auth.then((auth) => {
        if (!this.destroyed && this.ws === ws && ws.readyState === WebSocket.OPEN) {
          ws.send(JSON.stringify({
            type: "hello",
            deviceId: this.device.id,
            nick: this.device.nick,
            roomAuth: auth
          }));
        }
      }).catch(() => ws.close());
    };

    ws.onmessage = (event) => {
      if (!this.destroyed) {
        void this.handleRawMessage(event.data as string);
      }
    };

    ws.onerror = () => {
      if (this.ws === ws && ws.readyState < WebSocket.CLOSING) {
        ws.close();
      }
    };

    ws.onclose = () => {
      if (this.ws !== ws) {
        return;
      }
      this.ready = false;
      this.callbacks.onState("closed");
      if (!this.destroyed) {
        const delay = this.reconnectDelay + Math.round(Math.random() * reconnectJitterMs);
        this.reconnectDelay = Math.min(maxReconnectDelayMs, Math.round(this.reconnectDelay * 1.7));
        this.reconnectTimer = window.setTimeout(() => this.connect(), delay);
      }
    };
  }

  private async handleRawMessage(raw: string): Promise<void> {
    try {
      await this.handleMessage(JSON.parse(raw) as ServerMessage);
    } catch {
      this.callbacks.onState("connecting");
    }
  }

  private async handleMessage(message: ServerMessage): Promise<void> {
    this.lastSeenAt = Date.now();
    if (message.type === "closed") {
      this.callbacks.onClosed();
      this.destroy();
      return;
    }

    if (message.type === "pong") {
      return;
    }

    if (message.type === "presence") {
      const peers = message.peers.filter((peer) => peer.id !== this.device.id);
      this.callbacks.onPeers(peers);
      this.syncP2pPeers(peers);
      return;
    }

    if (message.type === "ack") {
      this.pendingAcks.delete(message.id);
      this.pendingFileControls.delete(message.id);
      return;
    }

    if (message.type === "join.request") {
      if (message.request.deviceId !== this.device.id) {
        this.callbacks.onJoinRequest(message.request);
      }
      return;
    }

    if (message.type === "hello") {
      if (message.snapshot) {
        await this.applyIncomingUpdate(message.snapshot);
      }
      for (const update of message.updates) {
        await this.applyIncomingUpdate(update);
      }
      for (const file of message.files ?? []) {
        await this.applyFile(file);
      }
      const peers = message.peers.filter((peer) => peer.id !== this.device.id);
      this.callbacks.onPeers(peers);
      this.syncP2pPeers(peers);
      this.ready = true;
      this.reconnectDelay = minReconnectDelayMs;
      this.callbacks.onState("open");
      this.callbacks.onText(this.text.toString());
      this.callbacks.onTerminal(this.terminalSnapshot());
      this.flushOfflineQueue();
      this.flushControls();
      this.scheduleSnapshot();
      return;
    }

    if (message.type === "update") {
      const before = this.text.toString();
      const applied = await this.applyIncomingUpdate(message.update);
      if (!applied) {
        return;
      }
      const after = this.text.toString();
      if (message.update.deviceId !== this.device.id) {
        const [index, deleteCount, insertText] = diffText(before, after);
        const activity = describeActivity(before, index, deleteCount, insertText);
        this.callbacks.onRemoteChange({
          deviceId: message.update.deviceId ?? "",
          nick: message.update.deviceNick ?? "",
          index,
          local: false,
          action: activity.action,
          preview: activity.preview,
          insertText,
          deleteCount,
          startLine: lineFromIndex(before, index),
          startColumn: columnFromIndex(before, index),
          lineDelta: lineBreakCount(insertText) - lineBreakCount(before.slice(index, index + deleteCount))
        });
      }
    }

    if (message.type === "file") {
      await this.applyFile(message.file);
      return;
    }

    if (message.type === "notice.knock") {
      if (this.rememberControl(message.knock.id) && message.knock.deviceId !== this.device.id) {
        this.callbacks.onKnock(message.knock);
      }
      return;
    }

    if (message.type === "live.draft") {
      if (this.rememberControl(message.draft.id) && message.draft.deviceId !== this.device.id) {
        await this.applyLiveDraft(message.draft);
      }
      return;
    }

    if (message.type === "remote.request") {
      if (this.rememberControl(message.request.id) && message.request.deviceId !== this.device.id) {
        this.callbacks.onRemoteRequest(message.request);
      }
      return;
    }

    if (message.type === "p2p.offer" || message.type === "p2p.answer") {
      await this.handleP2pDescription(message.signal);
      return;
    }

    if (message.type === "p2p.candidate") {
      await this.handleP2pCandidate(message.signal);
      return;
    }

    if (message.type === "remote.grant") {
      if (this.rememberControl(message.grant.id) && message.grant.deviceId !== this.device.id) {
        this.callbacks.onRemoteGrant(message.grant);
      }
      return;
    }

    if (message.type === "remote.command") {
      if (this.rememberControl(message.command.id) && message.command.deviceId !== this.device.id) {
        await this.applyRemoteCommand(message.command);
      }
      return;
    }

    if (message.type === "remote.script") {
      if (this.rememberControl(message.script.id) && message.script.deviceId !== this.device.id) {
        await this.applyRemoteScript(message.script);
      }
      return;
    }

    if (message.type === "remote.cancel") {
      if (this.rememberControl(message.cancel.id) && message.cancel.deviceId !== this.device.id) {
        this.applyRemoteCancel(message.cancel);
      }
      return;
    }

    if (message.type === "remote.output") {
      if (this.rememberControl(message.output.id) && message.output.deviceId !== this.device.id) {
        await this.applyRemoteOutput(message.output);
      }
      return;
    }

  }

  private async applyIncomingUpdate(update: EncryptedUpdate): Promise<boolean> {
    if (!this.rememberUpdate(update.id)) {
      return false;
    }
    await this.applyEncrypted(update);
    return true;
  }

  private rememberUpdate(id: string): boolean {
    return rememberBounded(this.seenUpdateIds, id);
  }

  private rememberControl(id: string): boolean {
    return rememberBounded(this.seenControlIds, id);
  }

  private async applyEncrypted(update: EncryptedUpdate): Promise<void> {
    const bytes = await decryptFromTunnel(this.tunnel, update.nonce, update.ciphertext);
    Y.applyUpdate(this.doc, bytes, "remote");
  }

  private async applyFile(file: EncryptedFile): Promise<void> {
    if (file.kind === "delete") {
      this.fileTransfers.delete(file.fileId);
      this.completedFileIds.delete(file.fileId);
      rememberBounded(this.deletedFileIds, file.fileId);
      this.callbacks.onFileDeleted(file.fileId);
      return;
    }
    if (file.deviceId === this.device.id) {
      return;
    }
    if (file.kind === "chunk") {
      await this.applyFileChunk(file);
      return;
    }
    if (this.completedFileIds.has(file.id) || this.deletedFileIds.has(file.id)) {
      return;
    }
    const [metaBytes, bodyBytes] = await Promise.all([
      decryptFromTunnel(this.tunnel, file.metaNonce, file.metaCiphertext),
      decryptFromTunnel(this.tunnel, file.nonce, file.ciphertext)
    ]);
    const meta = JSON.parse(decode(metaBytes)) as {
      readonly name?: string;
      readonly type?: string;
      readonly size?: number;
      readonly autoDownload?: boolean;
      readonly delivery?: string;
      readonly commandId?: string;
    };
    const type = meta.type || "application/octet-stream";
    const body = bodyBytes.buffer.slice(bodyBytes.byteOffset, bodyBytes.byteOffset + bodyBytes.byteLength) as ArrayBuffer;
    const blob = new Blob([body], { type });
    this.callbacks.onFile({
      id: file.id,
      name: cleanFileName(meta.name || "file"),
      type,
      size: meta.size || file.bytes,
      bytes: bodyBytes,
      url: URL.createObjectURL(blob),
      nick: file.deviceNick || "",
      deviceId: file.deviceId || "",
      createdAt: file.createdAt || new Date().toISOString(),
      ...(meta.autoDownload === true ? { autoDownload: true } : {}),
      ...(meta.delivery ? { delivery: String(meta.delivery).slice(0, 80) } : {}),
      ...(meta.commandId ? { commandId: String(meta.commandId).slice(0, 120) } : {})
    });
    rememberBounded(this.completedFileIds, file.id);
  }

  private async applyFileChunk(file: EncryptedFileChunk): Promise<void> {
    if (this.completedFileIds.has(file.fileId) || this.deletedFileIds.has(file.fileId)) {
      return;
    }
    if (file.index < 0 || file.index >= file.total || file.total < 1 || file.total > 8192) {
      return;
    }
    const transfer: FileTransfer = this.fileTransfers.get(file.fileId) ?? {
      chunks: new Array<Uint8Array>(file.total),
      seen: new Set<number>(),
      total: file.total,
      totalBytes: file.totalBytes
    };
    if (!transfer.createdAt && file.createdAt) {
      transfer.createdAt = file.createdAt;
    }
    if (!transfer.deviceId && file.deviceId) {
      transfer.deviceId = file.deviceId;
    }
    if (!transfer.deviceNick && file.deviceNick) {
      transfer.deviceNick = file.deviceNick;
    }
    if (file.metaNonce && file.metaCiphertext) {
      const metaBytes = await decryptFromTunnel(this.tunnel, file.metaNonce, file.metaCiphertext);
      transfer.meta = JSON.parse(decode(metaBytes)) as {
        readonly name?: string;
        readonly type?: string;
        readonly size?: number;
        readonly autoDownload?: boolean;
        readonly delivery?: string;
        readonly commandId?: string;
      };
    }
    if (!transfer.seen.has(file.index)) {
      transfer.chunks[file.index] = await decryptFromTunnel(this.tunnel, file.nonce, file.ciphertext);
      transfer.seen.add(file.index);
    }
    transfer.total = file.total;
    transfer.totalBytes = file.totalBytes;
    this.fileTransfers.set(file.fileId, transfer);
    if (transfer.seen.size !== transfer.total || !transfer.meta) {
      return;
    }
    const bodyBytes = concatChunks(transfer.chunks);
    const type = transfer.meta.type || "application/octet-stream";
    const body = bodyBytes.buffer.slice(bodyBytes.byteOffset, bodyBytes.byteOffset + bodyBytes.byteLength) as ArrayBuffer;
    const blob = new Blob([body], { type });
    this.callbacks.onFile({
      id: file.fileId,
      name: cleanFileName(transfer.meta.name || "file"),
      type,
      size: transfer.meta.size || transfer.totalBytes,
      bytes: bodyBytes,
      url: URL.createObjectURL(blob),
      nick: transfer.deviceNick || "",
      deviceId: transfer.deviceId || "",
      createdAt: transfer.createdAt || new Date().toISOString(),
      ...(transfer.meta.autoDownload === true ? { autoDownload: true } : {}),
      ...(transfer.meta.delivery ? { delivery: String(transfer.meta.delivery).slice(0, 80) } : {}),
      ...(transfer.meta.commandId ? { commandId: String(transfer.meta.commandId).slice(0, 120) } : {})
    });
    this.fileTransfers.delete(file.fileId);
    rememberBounded(this.completedFileIds, file.fileId);
  }

  private async applyLiveDraft(draft: EncryptedLiveDraft): Promise<void> {
    const bytes = await decryptFromTunnel(this.tunnel, draft.nonce, draft.ciphertext);
    const payload = JSON.parse(decode(bytes)) as {
      readonly text?: string;
      readonly index?: number;
      readonly active?: boolean;
      readonly seq?: number;
    };
    const text = typeof payload.text === "string" ? payload.text.slice(0, 4000) : "";
    const active = payload.active === true && text.length > 0;
    const index = Number.isSafeInteger(payload.index) ? Math.max(0, Number(payload.index)) : this.text.toString().length;
    const seq = Number.isSafeInteger(payload.seq) ? Math.max(0, Number(payload.seq)) : 0;
    this.callbacks.onLiveDraft({
      deviceId: draft.deviceId || "",
      nick: draft.deviceNick || draft.nick || "",
      text,
      index,
      active,
      seq,
      createdAt: draft.createdAt || new Date().toISOString()
    });
  }

  private async applyRemoteCommand(command: EncryptedRemoteCommand): Promise<void> {
    const bytes = await decryptFromTunnel(this.tunnel, command.nonce, command.ciphertext);
    const payload = JSON.parse(decode(bytes)) as { readonly command?: string; readonly timeoutMs?: number; readonly runAs?: string };
    const text = typeof payload.command === "string" ? payload.command.slice(0, 8000) : "";
    if (!text.trim()) {
      return;
    }
    this.callbacks.onRemoteCommand({
      id: command.id,
      command: text,
      ...(safeRemoteTimeoutMs(payload.timeoutMs) ? { timeoutMs: safeRemoteTimeoutMs(payload.timeoutMs) } : {}),
      ...(typeof payload.runAs === "string" ? { runAs: payload.runAs.slice(0, 20) } : {}),
      targetDeviceId: command.targetDeviceId,
      deviceId: command.deviceId || "",
      nick: command.deviceNick || command.nick || "",
      createdAt: command.createdAt || new Date().toISOString()
    });
  }

  private async applyRemoteScript(script: EncryptedRemoteScript): Promise<void> {
    const bytes = await decryptFromTunnel(this.tunnel, script.nonce, script.ciphertext);
    const payload = JSON.parse(decode(bytes)) as { readonly name?: string; readonly shell?: string; readonly script?: string; readonly timeoutMs?: number; readonly runAs?: string };
    const text = typeof payload.script === "string" ? payload.script.slice(0, 1_000_000) : "";
    if (!text.trim()) {
      return;
    }
    this.callbacks.onRemoteScript({
      id: script.id,
      name: cleanFileName(payload.name || "script"),
      shell: typeof payload.shell === "string" ? payload.shell.slice(0, 40) : "",
      script: text,
      ...(safeRemoteTimeoutMs(payload.timeoutMs) ? { timeoutMs: safeRemoteTimeoutMs(payload.timeoutMs) } : {}),
      ...(typeof payload.runAs === "string" ? { runAs: payload.runAs.slice(0, 20) } : {}),
      targetDeviceId: script.targetDeviceId,
      deviceId: script.deviceId || "",
      nick: script.deviceNick || script.nick || "",
      createdAt: script.createdAt || new Date().toISOString()
    });
  }

  private applyRemoteCancel(cancel: RemoteCancelMessage): void {
    if (!cancel.commandId.trim()) {
      return;
    }
    this.callbacks.onRemoteCancel({
      id: cancel.id,
      commandId: cancel.commandId,
      targetDeviceId: cancel.targetDeviceId,
      deviceId: cancel.deviceId || "",
      nick: cancel.deviceNick || cancel.nick || "",
      createdAt: cancel.createdAt || new Date().toISOString()
    });
  }

  private async applyRemoteOutput(output: EncryptedRemoteOutput): Promise<void> {
    const bytes = await decryptFromTunnel(this.tunnel, output.nonce, output.ciphertext);
    const payload = JSON.parse(decode(bytes)) as { readonly text?: string };
    const text = typeof payload.text === "string" ? payload.text.slice(0, 40_000) : "";
    this.callbacks.onRemoteOutput({
      id: output.id,
      commandId: output.commandId,
      text,
      targetDeviceId: output.targetDeviceId,
      ...(typeof output.exitCode === "number" ? { exitCode: output.exitCode } : {}),
      deviceId: output.deviceId || "",
      nick: output.deviceNick || output.nick || "",
      createdAt: output.createdAt || new Date().toISOString()
    });
  }

  private queueUpdate(update: Uint8Array, kind: UpdateKind): void {
    const item: OutboundUpdate = {
      id: `${kind}_${crypto.randomUUID()}`,
      kind,
      update
    };
    this.queueOutbound(item);
  }

  private queueOutbound(item: OutboundUpdate): void {
    void this.sendDirectUpdate(item);
    if (!this.ready || !this.ws || this.ws.readyState !== WebSocket.OPEN) {
      if (item.kind === "update") {
        this.offlineQueue.push(item);
      } else {
        this.needsSnapshot = true;
      }
      return;
    }
    this.sendQueue = this.sendQueue
      .then(() => this.sendUpdate(item))
      .catch(() => undefined);
  }

  private async sendUpdate(item: OutboundUpdate): Promise<void> {
    const ws = this.ws;
    if (!ws || ws.readyState !== WebSocket.OPEN) {
      if (item.kind === "update") {
        this.offlineQueue.push(item);
      } else {
        this.needsSnapshot = true;
      }
      return;
    }
    const encrypted = await encryptForTunnel(this.tunnel, item.update);
    const payload = {
      id: item.id,
      kind: item.kind,
      deviceId: this.device.id,
      deviceNick: this.device.nick,
      createdAt: new Date().toISOString(),
      ...encrypted
    };
    this.sendDirectUpdatePayload(payload);
    this.pendingAcks.set(item.id, item);
    ws.send(JSON.stringify({
      type: "update",
      update: payload
    }));

    if (item.kind === "update") {
      this.localUpdates += 1;
      if (this.localUpdates % 64 === 0) {
        this.queueUpdate(Y.encodeStateAsUpdate(this.doc), "snapshot");
      }
    }
  }

  private async sendDirectUpdate(item: OutboundUpdate): Promise<void> {
    if (this.directSentUpdateIds.has(item.id) || !this.hasOpenDirectChannels()) {
      return;
    }
    const encrypted = await encryptForTunnel(this.tunnel, item.update);
    this.sendDirectUpdatePayload({
      id: item.id,
      kind: item.kind,
      deviceId: this.device.id,
      deviceNick: this.device.nick,
      createdAt: new Date().toISOString(),
      ...encrypted
    });
  }

  private sendDirectUpdatePayload(update: EncryptedUpdate): void {
    if (this.directSentUpdateIds.has(update.id)) {
      return;
    }
    if (this.broadcastDirect({ type: "update", update })) {
      rememberBounded(this.directSentUpdateIds, update.id);
    }
  }

  private flushOfflineQueue(): void {
    const unacked = [...this.pendingAcks.values()];
    if (unacked.length > 0 || this.offlineQueue.length > 0) {
      const pending = [...unacked, ...this.offlineQueue.splice(0)];
      this.sendQueue = this.sendQueue
        .then(async () => {
          for (const item of pending) {
            await this.sendUpdate(item);
          }
        })
        .then(() => {
          if (this.needsSnapshot) {
            this.scheduleSnapshot();
          }
        })
        .catch(() => undefined);
      return;
    }
    if (this.needsSnapshot) {
      this.scheduleSnapshot();
    }
  }

  private scheduleSnapshot(): void {
    this.needsSnapshot = true;
    if (!this.ready) {
      return;
    }
    window.clearTimeout(this.snapshotTimer);
    this.snapshotTimer = window.setTimeout(() => {
      this.needsSnapshot = false;
      this.queueUpdate(Y.encodeStateAsUpdate(this.doc), "snapshot");
    }, 350);
  }

  private queueSnapshotNow(): void {
    if (!this.ready) {
      this.needsSnapshot = true;
      return;
    }
    window.clearTimeout(this.snapshotTimer);
    this.needsSnapshot = false;
    this.queueUpdate(Y.encodeStateAsUpdate(this.doc), "snapshot");
  }

  private sendControl(message: ControlMessage): void {
    this.sendDirectControl(message);
    if (message.type === "file") {
      this.pendingFileControls.set(message.file.id, message);
    }
    const ws = this.ws;
    if (this.ready && ws?.readyState === WebSocket.OPEN) {
      ws.send(JSON.stringify(message));
      return;
    }
    this.controlQueue.push(message);
  }

  private sendDirectControl(message: ControlMessage): void {
    const createdAt = new Date().toISOString();
    if (message.type === "file") {
      this.broadcastDirect({
        type: "file",
        file: {
          ...message.file,
          deviceId: this.device.id,
          deviceNick: this.device.nick,
          createdAt
        }
      });
      return;
    }
    if (message.type === "notice.knock") {
      this.broadcastDirect({
        type: "notice.knock",
        knock: {
          ...message.knock,
          deviceId: this.device.id,
          nick: this.device.nick,
          createdAt
        }
      });
      return;
    }
    if (message.type === "live.draft") {
      this.broadcastDirect({
        type: "live.draft",
        draft: {
          ...message.draft,
          deviceId: this.device.id,
          deviceNick: this.device.nick,
          nick: this.device.nick,
          createdAt
        }
      });
      return;
    }
    if (message.type === "remote.request") {
      this.broadcastDirect({
        type: "remote.request",
        request: {
          ...message.request,
          deviceId: this.device.id,
          nick: this.device.nick,
          createdAt
        }
      });
      return;
    }
    if (message.type === "remote.grant") {
      this.broadcastDirect({
        type: "remote.grant",
        grant: {
          ...message.grant,
          deviceId: this.device.id,
          nick: this.device.nick,
          createdAt
        }
      });
      return;
    }
    if (message.type === "remote.command") {
      this.broadcastDirect({
        type: "remote.command",
        command: {
          ...message.command,
          deviceId: this.device.id,
          deviceNick: this.device.nick,
          nick: this.device.nick,
          createdAt
        }
      });
      return;
    }
    if (message.type === "remote.script") {
      this.broadcastDirect({
        type: "remote.script",
        script: {
          ...message.script,
          deviceId: this.device.id,
          deviceNick: this.device.nick,
          nick: this.device.nick,
          createdAt
        }
      });
      return;
    }
    if (message.type === "remote.cancel") {
      this.broadcastDirect({
        type: "remote.cancel",
        cancel: {
          ...message.cancel,
          deviceId: this.device.id,
          deviceNick: this.device.nick,
          nick: this.device.nick,
          createdAt
        }
      });
      return;
    }
    if (message.type === "remote.output") {
      this.broadcastDirect({
        type: "remote.output",
        output: {
          ...message.output,
          deviceId: this.device.id,
          deviceNick: this.device.nick,
          nick: this.device.nick,
          createdAt
        }
      });
    }
  }

  private flushControls(): void {
    const ws = this.ws;
    if (ws?.readyState !== WebSocket.OPEN) {
      return;
    }
    const sent = new Set<string>();
    const pendingFiles = [...this.pendingFileControls.values()];
    const queued = this.controlQueue.splice(0);
    for (const message of [...pendingFiles, ...queued]) {
      const id = controlMessageId(message);
      if (id && sent.has(id)) {
        continue;
      }
      if (id) {
        sent.add(id);
      }
      ws.send(JSON.stringify(message));
    }
  }

  private syncP2pPeers(peers: readonly PeerInfo[]): void {
    if (!("RTCPeerConnection" in window) || !("RTCDataChannel" in window)) {
      return;
    }
    for (const peer of peers) {
      if (!peer.id || peer.id === this.device.id) {
        continue;
      }
      const link = this.ensureP2pPeer(peer.id);
      if (this.device.id < peer.id && link.pc.signalingState === "stable" && link.channel?.readyState !== "open") {
        void this.startP2pOffer(link);
      }
    }
  }

  private ensureP2pPeer(peerId: string): P2pPeer {
    const existing = this.p2pPeers.get(peerId);
    if (existing) {
      return existing;
    }
    const pc = new RTCPeerConnection({ iceServers: p2pIceServers });
    const link: P2pPeer = {
      id: peerId,
      pc,
      channel: null,
      pendingCandidates: [],
      retryTimer: 0
    };
    pc.onicecandidate = (event) => {
      if (event.candidate) {
        this.sendP2pCandidate(peerId, event.candidate.toJSON());
      }
    };
    pc.ondatachannel = (event) => {
      this.attachP2pChannel(link, event.channel);
    };
    pc.onconnectionstatechange = () => {
      if (pc.connectionState === "failed" || pc.connectionState === "closed") {
        this.restartP2pLater(peerId);
      }
    };
    pc.oniceconnectionstatechange = () => {
      if (pc.iceConnectionState === "failed") {
        this.restartP2pLater(peerId);
      }
    };
    if (this.device.id < peerId) {
      this.attachP2pChannel(link, pc.createDataChannel("soty", { ordered: true }));
    }
    this.p2pPeers.set(peerId, link);
    return link;
  }

  private attachP2pChannel(link: P2pPeer, channel: RTCDataChannel): void {
    link.channel = channel;
    channel.onopen = () => {
      this.sendDirectSnapshot();
    };
    channel.onmessage = (event) => {
      if (typeof event.data === "string") {
        void this.handleDirectMessage(event.data);
      }
    };
    channel.onclose = () => {
      if (link.channel === channel) {
        link.channel = null;
      }
      this.restartP2pLater(link.id);
    };
    channel.onerror = () => {
      this.restartP2pLater(link.id);
    };
  }

  private async startP2pOffer(link: P2pPeer, iceRestart = false): Promise<void> {
    if (this.destroyed || link.pc.signalingState !== "stable") {
      return;
    }
    try {
      const offer = await link.pc.createOffer({ iceRestart });
      await link.pc.setLocalDescription(offer);
      this.sendP2pDescription("p2p.offer", link.id, "offer", offer.sdp || "");
    } catch {
      this.restartP2pLater(link.id);
    }
  }

  private async handleP2pDescription(signal: P2pDescriptionSignal): Promise<void> {
    if (signal.targetDeviceId !== this.device.id || signal.deviceId === this.device.id || !signal.deviceId) {
      return;
    }
    const link = this.ensureP2pPeer(signal.deviceId);
    try {
      if (signal.kind === "offer") {
        await link.pc.setRemoteDescription({ type: "offer", sdp: signal.sdp });
        await this.flushP2pCandidates(link);
        const answer = await link.pc.createAnswer();
        await link.pc.setLocalDescription(answer);
        this.sendP2pDescription("p2p.answer", signal.deviceId, "answer", answer.sdp || "");
        return;
      }
      if (link.pc.signalingState === "have-local-offer") {
        await link.pc.setRemoteDescription({ type: "answer", sdp: signal.sdp });
        await this.flushP2pCandidates(link);
      }
    } catch {
      this.restartP2pLater(signal.deviceId);
    }
  }

  private async handleP2pCandidate(signal: P2pCandidateSignal): Promise<void> {
    if (signal.targetDeviceId !== this.device.id || signal.deviceId === this.device.id || !signal.deviceId) {
      return;
    }
    const link = this.ensureP2pPeer(signal.deviceId);
    const candidate: RTCIceCandidateInit = { candidate: signal.candidate };
    if (signal.sdpMid !== undefined) {
      candidate.sdpMid = signal.sdpMid;
    }
    if (signal.sdpMLineIndex !== undefined) {
      candidate.sdpMLineIndex = signal.sdpMLineIndex;
    }
    if (!link.pc.remoteDescription) {
      link.pendingCandidates.push(candidate);
      return;
    }
    try {
      await link.pc.addIceCandidate(candidate);
    } catch {
      this.restartP2pLater(signal.deviceId);
    }
  }

  private async flushP2pCandidates(link: P2pPeer): Promise<void> {
    for (const candidate of link.pendingCandidates.splice(0)) {
      await link.pc.addIceCandidate(candidate).catch(() => undefined);
    }
  }

  private sendP2pDescription(type: "p2p.offer" | "p2p.answer", targetDeviceId: string, kind: "offer" | "answer", sdp: string): void {
    this.sendSignal(type, {
      id: `p2p_${kind}_${crypto.randomUUID()}`,
      kind,
      targetDeviceId,
      sdp
    });
  }

  private sendP2pCandidate(targetDeviceId: string, candidate: RTCIceCandidateInit): void {
    if (!candidate.candidate) {
      return;
    }
    this.sendSignal("p2p.candidate", {
      id: `p2p_candidate_${crypto.randomUUID()}`,
      targetDeviceId,
      candidate: candidate.candidate,
      ...(candidate.sdpMid !== undefined ? { sdpMid: candidate.sdpMid } : {}),
      ...(candidate.sdpMLineIndex !== undefined ? { sdpMLineIndex: candidate.sdpMLineIndex } : {})
    });
  }

  private sendSignal(type: "p2p.offer" | "p2p.answer", signal: Omit<P2pDescriptionSignal, "deviceId" | "deviceNick" | "nick" | "createdAt">): void;
  private sendSignal(type: "p2p.candidate", signal: Omit<P2pCandidateSignal, "deviceId" | "deviceNick" | "nick" | "createdAt">): void;
  private sendSignal(
    type: "p2p.offer" | "p2p.answer" | "p2p.candidate",
    signal: Omit<P2pDescriptionSignal | P2pCandidateSignal, "deviceId" | "deviceNick" | "nick" | "createdAt">
  ): void {
    const ws = this.ws;
    if (ws?.readyState === WebSocket.OPEN) {
      ws.send(JSON.stringify({ type, signal }));
    }
  }

  private restartP2pLater(peerId: string): void {
    if (this.destroyed) {
      return;
    }
    const link = this.p2pPeers.get(peerId);
    if (link) {
      window.clearTimeout(link.retryTimer);
      link.retryTimer = window.setTimeout(() => {
        this.closeP2pPeer(peerId);
        const next = this.ensureP2pPeer(peerId);
        if (this.device.id < peerId) {
          void this.startP2pOffer(next, true);
        }
      }, p2pRetryMs + Math.round(Math.random() * reconnectJitterMs));
    }
  }

  private closeP2pPeer(peerId: string): void {
    const link = this.p2pPeers.get(peerId);
    if (!link) {
      return;
    }
    window.clearTimeout(link.retryTimer);
    link.channel?.close();
    link.pc.close();
    this.p2pPeers.delete(peerId);
  }

  private closeP2pPeers(): void {
    for (const peerId of [...this.p2pPeers.keys()]) {
      this.closeP2pPeer(peerId);
    }
  }

  private hasOpenDirectChannels(): boolean {
    for (const link of this.p2pPeers.values()) {
      if (link.channel?.readyState === "open") {
        return true;
      }
    }
    return false;
  }

  private broadcastDirect(message: DirectMessage): boolean {
    const json = JSON.stringify(message);
    let sent = false;
    for (const link of this.p2pPeers.values()) {
      if (link.channel?.readyState === "open") {
        try {
          link.channel.send(json);
          sent = true;
        } catch {
          this.restartP2pLater(link.id);
        }
      }
    }
    return sent;
  }

  private async handleDirectMessage(raw: string): Promise<void> {
    let message: DirectMessage;
    try {
      message = JSON.parse(raw) as DirectMessage;
    } catch {
      return;
    }
    if (message.type === "update") {
      const before = this.text.toString();
      const applied = await this.applyIncomingUpdate(message.update);
      if (!applied || message.update.deviceId === this.device.id) {
        return;
      }
      const after = this.text.toString();
      const [index, deleteCount, insertText] = diffText(before, after);
      const activity = describeActivity(before, index, deleteCount, insertText);
      this.callbacks.onRemoteChange({
        deviceId: message.update.deviceId ?? "",
        nick: message.update.deviceNick ?? "",
        index,
        local: false,
        action: activity.action,
        preview: activity.preview,
        insertText,
        deleteCount,
        startLine: lineFromIndex(before, index),
        startColumn: columnFromIndex(before, index),
        lineDelta: lineBreakCount(insertText) - lineBreakCount(before.slice(index, index + deleteCount))
      });
      return;
    }
    if (message.type === "file") {
      await this.applyFile(message.file);
      return;
    }
    if (message.type === "notice.knock" && this.rememberControl(message.knock.id) && message.knock.deviceId !== this.device.id) {
      this.callbacks.onKnock(message.knock);
      return;
    }
    if (message.type === "live.draft" && this.rememberControl(message.draft.id) && message.draft.deviceId !== this.device.id) {
      await this.applyLiveDraft(message.draft);
      return;
    }
    if (message.type === "remote.request" && this.rememberControl(message.request.id) && message.request.deviceId !== this.device.id) {
      this.callbacks.onRemoteRequest(message.request);
      return;
    }
    if (message.type === "remote.grant" && this.rememberControl(message.grant.id) && message.grant.deviceId !== this.device.id) {
      this.callbacks.onRemoteGrant(message.grant);
      return;
    }
    if (message.type === "remote.command" && this.rememberControl(message.command.id) && message.command.deviceId !== this.device.id) {
      await this.applyRemoteCommand(message.command);
      return;
    }
    if (message.type === "remote.script" && this.rememberControl(message.script.id) && message.script.deviceId !== this.device.id) {
      await this.applyRemoteScript(message.script);
      return;
    }
    if (message.type === "remote.cancel" && this.rememberControl(message.cancel.id) && message.cancel.deviceId !== this.device.id) {
      this.applyRemoteCancel(message.cancel);
      return;
    }
    if (message.type === "remote.output" && this.rememberControl(message.output.id) && message.output.deviceId !== this.device.id) {
      await this.applyRemoteOutput(message.output);
    }
  }

  private sendDirectSnapshot(): void {
    void (async () => {
      if (!this.hasOpenDirectChannels()) {
        return;
      }
      const encrypted = await encryptForTunnel(this.tunnel, Y.encodeStateAsUpdate(this.doc));
      this.sendDirectUpdatePayload({
        id: `direct_snapshot_${crypto.randomUUID()}`,
        kind: "snapshot",
        deviceId: this.device.id,
        deviceNick: this.device.nick,
        createdAt: new Date().toISOString(),
        ...encrypted
      });
    })();
  }

  private recoverConnection(forcePing = false): void {
    if (this.destroyed) {
      return;
    }
    window.clearTimeout(this.reconnectTimer);
    const ws = this.ws;
    if (!ws || ws.readyState >= WebSocket.CLOSING) {
      this.ready = false;
      this.connect();
      return;
    }
    if (ws.readyState === WebSocket.CONNECTING) {
      return;
    }
    if (ws.readyState !== WebSocket.OPEN) {
      this.closeAndReconnect(ws);
      return;
    }
    if (forcePing || Date.now() - this.lastSeenAt > staleConnectionMs) {
      this.safePing(ws);
    }
  }

  private checkConnection(): void {
    if (this.destroyed) {
      return;
    }
    const ws = this.ws;
    if (!ws || ws.readyState >= WebSocket.CLOSING) {
      this.wakeReconnect();
      return;
    }
    if (ws.readyState !== WebSocket.OPEN) {
      return;
    }
    if (Date.now() - this.lastSeenAt > staleConnectionMs) {
      this.closeAndReconnect(ws);
      return;
    }
    this.safePing(ws);
  }

  private closeAndReconnect(ws: WebSocket): void {
    if (this.ws !== ws) {
      return;
    }
    this.ready = false;
    this.ws = null;
    this.callbacks.onState("connecting");
    try {
      ws.close();
    } catch {
      // The browser can throw when a network switch leaves the socket half-dead.
    }
    this.connect();
  }

  private safePing(ws: WebSocket): void {
    try {
      ws.send(JSON.stringify({ type: "ping" }));
    } catch {
      this.closeAndReconnect(ws);
    }
  }
}

function controlMessageId(message: ControlMessage): string {
  if (message.type === "file") {
    return message.file.id;
  }
  return "";
}

function cleanFileName(value: string): string {
  return value.replace(/[\\/:*?"<>|]/gu, "_").slice(0, 120) || "file";
}

function cleanFileId(value: string): string {
  return value.replace(/[^A-Za-z0-9_-]/gu, "_").slice(0, 120);
}

function wait(ms: number): Promise<void> {
  return new Promise((resolve) => window.setTimeout(resolve, ms));
}

function cleanTerminalLine(value: string): string {
  return value
    .replace(/\r\n?/gu, "\n")
    .replace(/[\u0000-\u0008\u000B\u000C\u000E-\u001F\u007F]/gu, "")
    .replace(/\n{5,}/gu, "\n\n\n\n")
    .trim()
    .slice(0, 12_000);
}

function terminalStateFrom(value: unknown): TerminalSnapshot["state"] {
  return value === "run" || value === "ok" || value === "bad" || value === "off" ? value : "idle";
}

function concatChunks(chunks: readonly Uint8Array[]): Uint8Array {
  const total = chunks.reduce((sum, chunk) => sum + (chunk?.byteLength ?? 0), 0);
  const result = new Uint8Array(total);
  let offset = 0;
  for (const chunk of chunks) {
    if (!chunk) {
      continue;
    }
    result.set(chunk, offset);
    offset += chunk.byteLength;
  }
  return result;
}

function rememberBounded(items: Set<string>, id: string, max = 4096): boolean {
  if (items.has(id)) {
    return false;
  }
  items.add(id);
  while (items.size > max) {
    const first = items.values().next().value;
    if (typeof first !== "string") {
      break;
    }
    items.delete(first);
  }
  return true;
}

function diffText(before: string, after: string): [number, number, string] {
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

function describeActivity(
  before: string,
  start: number,
  deleteCount: number,
  insertText: string
): { readonly action: WriterActivity["action"]; readonly preview: string } {
  const action = deleteCount > 0 && !insertText ? "erase" : deleteCount > 0 && insertText ? "edit" : "write";
  const raw = insertText || before.slice(start, start + deleteCount);
  return {
    action,
    preview: raw.replace(/\s+/gu, " ").trim().slice(0, 48)
  };
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

function lineBreakCount(text: string): number {
  return (text.match(/\n/gu) ?? []).length;
}
