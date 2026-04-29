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
  readonly onActivity: (activity: WriterActivity) => void;
  readonly onRemoteChange: (activity: WriterActivity) => void;
  readonly onFile: (file: ReceivedFile) => void;
  readonly onFileDeleted: (fileId: string) => void;
  readonly onRemoteGrant: (grant: RemoteGrant) => void;
  readonly onRemoteCommand: (command: RemoteCommand) => void;
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
}

export interface RemoteGrant {
  readonly id: string;
  readonly enabled: boolean;
  readonly targetDeviceId: string;
  readonly deviceId: string;
  readonly nick: string;
  readonly createdAt: string;
}

export interface RemoteCommand {
  readonly id: string;
  readonly command: string;
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

interface FileTransfer {
  readonly chunks: Uint8Array[];
  readonly seen: Set<number>;
  total: number;
  totalBytes: number;
  meta?: { readonly name?: string; readonly type?: string; readonly size?: number };
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
  | { readonly type: "remote.grant"; readonly grant: RemoteGrant }
  | { readonly type: "remote.command"; readonly command: EncryptedRemoteCommand }
  | { readonly type: "remote.output"; readonly output: EncryptedRemoteOutput }
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
  | { readonly type: "remote.grant"; readonly grant: { readonly id: string; readonly enabled: boolean; readonly targetDeviceId: string } }
  | { readonly type: "remote.command"; readonly command: { readonly id: string; readonly targetDeviceId: string; readonly nonce: string; readonly ciphertext: string } }
  | { readonly type: "remote.output"; readonly output: { readonly id: string; readonly commandId: string; readonly targetDeviceId: string; readonly nonce: string; readonly ciphertext: string; readonly exitCode?: number } };

export class TunnelSync {
  private readonly doc = new Y.Doc();
  private readonly text = this.doc.getText("body");
  private ws: WebSocket | null = null;
  private destroyed = false;
  private ready = false;
  private localUpdates = 0;
  private reconnectTimer = 0;
  private reconnectDelay = 500;
  private snapshotTimer = 0;
  private heartbeatTimer = 0;
  private lastSeenAt = 0;
  private needsSnapshot = false;
  private readonly auth: Promise<string>;
  private readonly offlineQueue: OutboundUpdate[] = [];
  private readonly pendingAcks = new Map<string, OutboundUpdate>();
  private readonly controlQueue: ControlMessage[] = [];
  private readonly fileTransfers = new Map<string, FileTransfer>();
  private sendQueue = Promise.resolve();
  private readonly wakeReconnect = () => {
    if (!this.destroyed && (!this.ws || this.ws.readyState >= WebSocket.CLOSING)) {
      window.clearTimeout(this.reconnectTimer);
      this.connect();
    }
  };
  private readonly visibleReconnect = () => {
    if (document.visibilityState === "visible") {
      this.wakeReconnect();
    }
  };

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
    window.addEventListener("online", this.wakeReconnect);
    window.addEventListener("focus", this.wakeReconnect);
    window.addEventListener("pageshow", this.wakeReconnect);
    document.addEventListener("visibilitychange", this.visibleReconnect);
    this.heartbeatTimer = window.setInterval(() => this.checkConnection(), 2500);
    this.connect();
  }

  setText(next: string): void {
    const current = this.text.toString();
    if (current === next) {
      return;
    }
    const [start, deleteCount, insertText] = diffText(current, next);
    this.callbacks.onActivity({
      deviceId: this.device.id,
      nick: this.device.nick,
      index: start,
      local: true
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

  async sendRemoteCommand(targetDeviceId: string, command: string): Promise<string> {
    const id = `remote_cmd_${crypto.randomUUID()}`;
    const encrypted = await encryptForTunnel(this.tunnel, encode(JSON.stringify({ command })));
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
    window.removeEventListener("focus", this.wakeReconnect);
    window.removeEventListener("pageshow", this.wakeReconnect);
    document.removeEventListener("visibilitychange", this.visibleReconnect);
    const ws = this.ws;
    this.ws = null;
    if (ws?.readyState === WebSocket.OPEN) {
      ws.close();
    }
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
        if (!this.destroyed && ws.readyState === WebSocket.OPEN) {
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
      if (ws.readyState === WebSocket.OPEN) {
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
        const delay = this.reconnectDelay + Math.round(Math.random() * 250);
        this.reconnectDelay = Math.min(8000, Math.round(this.reconnectDelay * 1.7));
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
      this.callbacks.onPeers(message.peers.filter((peer) => peer.id !== this.device.id));
      return;
    }

    if (message.type === "ack") {
      this.pendingAcks.delete(message.id);
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
        await this.applyEncrypted(message.snapshot);
      }
      for (const update of message.updates) {
        await this.applyEncrypted(update);
      }
      for (const file of message.files ?? []) {
        await this.applyFile(file);
      }
      this.callbacks.onPeers(message.peers.filter((peer) => peer.id !== this.device.id));
      this.ready = true;
      this.reconnectDelay = 500;
      this.callbacks.onState("open");
      this.callbacks.onText(this.text.toString());
      this.flushOfflineQueue();
      this.flushControls();
      return;
    }

    if (message.type === "update") {
      const before = this.text.toString();
      await this.applyEncrypted(message.update);
      const after = this.text.toString();
      if (message.update.deviceId !== this.device.id) {
        this.callbacks.onRemoteChange({
          deviceId: message.update.deviceId ?? "",
          nick: message.update.deviceNick ?? "",
          index: diffIndex(before, after),
          local: false
        });
      }
    }

    if (message.type === "file") {
      await this.applyFile(message.file);
      return;
    }

    if (message.type === "remote.grant") {
      if (message.grant.deviceId !== this.device.id) {
        this.callbacks.onRemoteGrant(message.grant);
      }
      return;
    }

    if (message.type === "remote.command") {
      if (message.command.deviceId !== this.device.id) {
        await this.applyRemoteCommand(message.command);
      }
      return;
    }

    if (message.type === "remote.output") {
      if (message.output.deviceId !== this.device.id) {
        await this.applyRemoteOutput(message.output);
      }
      return;
    }

  }

  private async applyEncrypted(update: EncryptedUpdate): Promise<void> {
    const bytes = await decryptFromTunnel(this.tunnel, update.nonce, update.ciphertext);
    Y.applyUpdate(this.doc, bytes, "remote");
  }

  private async applyFile(file: EncryptedFile): Promise<void> {
    if (file.kind === "delete") {
      this.fileTransfers.delete(file.fileId);
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
    const [metaBytes, bodyBytes] = await Promise.all([
      decryptFromTunnel(this.tunnel, file.metaNonce, file.metaCiphertext),
      decryptFromTunnel(this.tunnel, file.nonce, file.ciphertext)
    ]);
    const meta = JSON.parse(decode(metaBytes)) as { readonly name?: string; readonly type?: string; readonly size?: number };
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
      createdAt: file.createdAt || new Date().toISOString()
    });
  }

  private async applyFileChunk(file: EncryptedFileChunk): Promise<void> {
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
      transfer.meta = JSON.parse(decode(metaBytes)) as { readonly name?: string; readonly type?: string; readonly size?: number };
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
      createdAt: transfer.createdAt || new Date().toISOString()
    });
    this.fileTransfers.delete(file.fileId);
  }

  private async applyRemoteCommand(command: EncryptedRemoteCommand): Promise<void> {
    const bytes = await decryptFromTunnel(this.tunnel, command.nonce, command.ciphertext);
    const payload = JSON.parse(decode(bytes)) as { readonly command?: string };
    const text = typeof payload.command === "string" ? payload.command.slice(0, 8000) : "";
    if (!text.trim()) {
      return;
    }
    this.callbacks.onRemoteCommand({
      id: command.id,
      command: text,
      targetDeviceId: command.targetDeviceId,
      deviceId: command.deviceId || "",
      nick: command.deviceNick || command.nick || "",
      createdAt: command.createdAt || new Date().toISOString()
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
    this.pendingAcks.set(item.id, item);
    ws.send(JSON.stringify({
      type: "update",
      update: {
        id: item.id,
        kind: item.kind,
        deviceNick: this.device.nick,
        ...encrypted
      }
    }));

    if (item.kind === "update") {
      this.localUpdates += 1;
      if (this.localUpdates % 64 === 0) {
        this.queueUpdate(Y.encodeStateAsUpdate(this.doc), "snapshot");
      }
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

  private sendControl(message: ControlMessage): void {
    const ws = this.ws;
    if (this.ready && ws?.readyState === WebSocket.OPEN) {
      ws.send(JSON.stringify(message));
      return;
    }
    this.controlQueue.push(message);
  }

  private flushControls(): void {
    const ws = this.ws;
    if (ws?.readyState !== WebSocket.OPEN) {
      return;
    }
    for (const message of this.controlQueue.splice(0)) {
      ws.send(JSON.stringify(message));
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
    if (Date.now() - this.lastSeenAt > 5500) {
      this.ready = false;
      this.ws = null;
      ws.close();
      this.connect();
      return;
    }
    ws.send(JSON.stringify({ type: "ping" }));
  }
}

function cleanFileName(value: string): string {
  return value.replace(/[\\/:*?"<>|]/gu, "_").slice(0, 120) || "file";
}

function wait(ms: number): Promise<void> {
  return new Promise((resolve) => window.setTimeout(resolve, ms));
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

function diffIndex(before: string, after: string): number {
  let index = 0;
  while (index < before.length && index < after.length && before[index] === after[index]) {
    index += 1;
  }
  return index;
}
