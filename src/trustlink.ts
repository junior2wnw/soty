export interface DeviceRecord {
  readonly id: string;
  readonly nick: string;
  readonly publicJwk: JsonWebKey;
  readonly privateKey: CryptoKey;
  readonly createdAt: string;
}

export interface TunnelRecord {
  readonly id: string;
  readonly key: string;
  readonly label: string;
  readonly color?: string;
  readonly counterparty?: boolean;
  readonly score?: number;
  readonly lastActionAt?: string;
  readonly createdAt: string;
  readonly updatedAt: string;
  readonly unread: boolean;
}

export interface InvitePayload {
  readonly v: 1;
  readonly kind: "soty.trustlink.invite";
  readonly roomId: string;
  readonly roomKey: string;
  readonly from: {
    readonly deviceId: string;
    readonly nick: string;
    readonly publicJwk: JsonWebKey;
  };
  readonly createdAt: string;
}

export interface SignedInvite {
  readonly payload: InvitePayload;
  readonly signature?: string;
}

export interface JoinInvite {
  readonly roomId: string;
  readonly fromNick: string;
}

export interface JoinAcceptPayload {
  readonly roomKeyCiphertext: string;
  readonly nonce: string;
  readonly hostPublicJwk: JsonWebKey;
  readonly hostNick: string;
}

const dbName = "soty-online";
const deviceKey = "device";
const tunnelsKey = "soty:tunnels:v1";
const selectedKey = "soty:selected:v1";
const appModeKey = "soty:pwa-enabled:v1";
const pendingInviteKey = "soty:pending-invite:v1";
const pendingJoinKey = "soty:pending-join:v1";
const encoder = new TextEncoder();
const decoder = new TextDecoder();

export function isStandalone(): boolean {
  return window.matchMedia("(display-mode: standalone)").matches
    || window.matchMedia("(display-mode: fullscreen)").matches
    || window.matchMedia("(display-mode: minimal-ui)").matches
    || window.matchMedia("(display-mode: window-controls-overlay)").matches
    || (navigator as Navigator & { standalone?: boolean }).standalone === true;
}

export function isAppRuntime(): boolean {
  const url = new URL(window.location.href);
  if (url.searchParams.get("pwa") === "1") {
    rememberAppRuntime();
    return true;
  }
  return isStandalone() || localStorage.getItem(appModeKey) === "1";
}

export function rememberAppRuntime(): void {
  localStorage.setItem(appModeKey, "1");
}

export async function loadDevice(): Promise<DeviceRecord | null> {
  return (await idbGet<DeviceRecord>(deviceKey)) ?? null;
}

export async function createDevice(nick: string): Promise<DeviceRecord> {
  const keys = await crypto.subtle.generateKey(
    { name: "ECDSA", namedCurve: "P-256" },
    false,
    ["sign", "verify"]
  ) as CryptoKeyPair;
  const publicJwk = await crypto.subtle.exportKey("jwk", keys.publicKey);
  const id = `dev_${(await sha256Base64Url(stableJson(publicJwk))).slice(0, 32)}`;
  const record: DeviceRecord = {
    id,
    nick: cleanNick(nick),
    publicJwk,
    privateKey: keys.privateKey,
    createdAt: new Date().toISOString()
  };
  await idbSet(deviceKey, record);
  return record;
}

export function loadTunnels(): TunnelRecord[] {
  const raw = localStorage.getItem(tunnelsKey);
  if (!raw) {
    return [];
  }
  try {
    const parsed = JSON.parse(raw) as TunnelRecord[];
    return parsed.filter((item) => item.id && item.key);
  } catch {
    return [];
  }
}

export function saveTunnels(tunnels: readonly TunnelRecord[]): void {
  localStorage.setItem(tunnelsKey, JSON.stringify(tunnels));
}

export function loadSelectedTunnelId(): string | null {
  return localStorage.getItem(selectedKey);
}

export function saveSelectedTunnelId(id: string): void {
  localStorage.setItem(selectedKey, id);
}

export function createTunnel(label = "", counterparty = false): TunnelRecord {
  const now = new Date().toISOString();
  return {
    id: randomToken(24),
    key: randomToken(32),
    label,
    score: 0,
    lastActionAt: now,
    counterparty,
    createdAt: now,
    updatedAt: now,
    unread: false
  };
}

export function upsertTunnel(tunnel: TunnelRecord): TunnelRecord[] {
  const tunnels = loadTunnels();
  const now = new Date().toISOString();
  const next = [{ ...tunnel, updatedAt: now, lastActionAt: now }, ...tunnels.filter((item) => item.id !== tunnel.id)];
  saveTunnels(next);
  saveSelectedTunnelId(tunnel.id);
  return next;
}

export function removeTunnel(id: string): TunnelRecord[] {
  const next = loadTunnels().filter((item) => item.id !== id);
  saveTunnels(next);
  if (loadSelectedTunnelId() === id) {
    localStorage.removeItem(selectedKey);
  }
  return next;
}

export function markTunnel(id: string, unread: boolean): TunnelRecord[] {
  const now = new Date().toISOString();
  const next = loadTunnels().map((item) => item.id === id ? { ...item, unread, updatedAt: now, lastActionAt: now } : item);
  saveTunnels(next);
  return next;
}

export function touchTunnel(id: string): TunnelRecord[] {
  const now = new Date().toISOString();
  const next = loadTunnels().map((item) => item.id === id
    ? { ...item, score: (item.score ?? 0) + 1, updatedAt: now, lastActionAt: now }
    : item);
  saveTunnels(next);
  return next;
}

export async function inviteUrl(tunnel: TunnelRecord, device: DeviceRecord): Promise<string> {
  const url = new URL(window.location.origin);
  url.searchParams.set("j", [
    tunnel.id,
    toBase64Url(encode(device.nick))
  ].join("."));
  return url.toString();
}

export function joinInviteFromLocation(): JoinInvite | null {
  const compact = new URL(window.location.href).searchParams.get("j");
  if (!compact) {
    return null;
  }
  const [roomId, nick] = compact.split(".");
  if (!roomId || !nick) {
    return null;
  }
  return {
    roomId,
    fromNick: decode(fromBase64Url(nick))
  };
}

export function captureJoinInviteFromLocation(): JoinInvite | null {
  const invite = joinInviteFromLocation();
  if (invite) {
    savePendingJoin(invite);
  }
  return invite;
}

export function loadPendingJoin(): JoinInvite | null {
  const raw = localStorage.getItem(pendingJoinKey);
  if (!raw) {
    return null;
  }
  try {
    return JSON.parse(raw) as JoinInvite;
  } catch {
    return null;
  }
}

export function savePendingJoin(invite: JoinInvite): void {
  localStorage.setItem(pendingJoinKey, JSON.stringify(invite));
}

export function clearPendingJoin(): void {
  localStorage.removeItem(pendingJoinKey);
}

export async function inviteFromLocation(): Promise<SignedInvite | null> {
  const compact = new URL(window.location.href).searchParams.get("t");
  if (compact) {
    return parseCompactInvite(compact);
  }

  const encoded = new URL(window.location.href).searchParams.get("i");
  if (!encoded) {
    return null;
  }
  try {
    const invite = JSON.parse(decode(fromBase64Url(encoded))) as SignedInvite;
    return await verifyInvite(invite) ? invite : null;
  } catch {
    return null;
  }
}

export async function captureInviteFromLocation(): Promise<SignedInvite | null> {
  const invite = await inviteFromLocation();
  if (invite) {
    savePendingInvite(invite);
  }
  return invite;
}

export function loadPendingInvite(): SignedInvite | null {
  const raw = localStorage.getItem(pendingInviteKey);
  if (!raw) {
    return null;
  }
  try {
    return JSON.parse(raw) as SignedInvite;
  } catch {
    return null;
  }
}

export function savePendingInvite(invite: SignedInvite): void {
  localStorage.setItem(pendingInviteKey, JSON.stringify(invite));
}

export function clearPendingInvite(): void {
  localStorage.removeItem(pendingInviteKey);
}

export function tunnelFromInvite(invite: SignedInvite): TunnelRecord {
  const now = new Date().toISOString();
  return {
    id: invite.payload.roomId,
    key: invite.payload.roomKey,
    label: invite.payload.from.nick,
    counterparty: true,
    score: 1,
    lastActionAt: now,
    createdAt: now,
    updatedAt: now,
    unread: false
  };
}

export function tunnelFromAcceptedJoin(invite: JoinInvite, roomKey: string): TunnelRecord {
  const now = new Date().toISOString();
  return {
    id: invite.roomId,
    key: roomKey,
    label: invite.fromNick,
    counterparty: true,
    score: 1,
    lastActionAt: now,
    createdAt: now,
    updatedAt: now,
    unread: false
  };
}

export async function createJoinKeyPair(): Promise<CryptoKeyPair> {
  return crypto.subtle.generateKey(
    { name: "ECDH", namedCurve: "P-256" },
    false,
    ["deriveKey"]
  ) as Promise<CryptoKeyPair>;
}

export async function publicJoinJwk(pair: CryptoKeyPair): Promise<JsonWebKey> {
  return crypto.subtle.exportKey("jwk", pair.publicKey);
}

export async function encryptRoomKeyForJoin(
  roomKey: string,
  requesterPublicJwk: JsonWebKey,
  hostNick: string
): Promise<JoinAcceptPayload> {
  const hostPair = await createJoinKeyPair();
  const requesterPublic = await crypto.subtle.importKey(
    "jwk",
    requesterPublicJwk,
    { name: "ECDH", namedCurve: "P-256" },
    false,
    []
  );
  const key = await deriveJoinAesKey(hostPair.privateKey, requesterPublic);
  const nonce = crypto.getRandomValues(new Uint8Array(12));
  const encrypted = await crypto.subtle.encrypt(
    { name: "AES-GCM", iv: bufferSource(nonce) },
    key,
    bufferSource(encode(roomKey))
  );
  return {
    roomKeyCiphertext: toBase64Url(new Uint8Array(encrypted)),
    nonce: toBase64Url(nonce),
    hostPublicJwk: await publicJoinJwk(hostPair),
    hostNick
  };
}

export async function decryptAcceptedJoin(
  privateKey: CryptoKey,
  payload: JoinAcceptPayload
): Promise<string> {
  const hostPublic = await crypto.subtle.importKey(
    "jwk",
    payload.hostPublicJwk,
    { name: "ECDH", namedCurve: "P-256" },
    false,
    []
  );
  const key = await deriveJoinAesKey(privateKey, hostPublic);
  const decrypted = await crypto.subtle.decrypt(
    { name: "AES-GCM", iv: bufferSource(fromBase64Url(payload.nonce)) },
    key,
    bufferSource(fromBase64Url(payload.roomKeyCiphertext))
  );
  return decode(new Uint8Array(decrypted));
}

export async function encryptForTunnel(tunnel: TunnelRecord, bytes: Uint8Array): Promise<{ nonce: string; ciphertext: string }> {
  const nonce = crypto.getRandomValues(new Uint8Array(12));
  const key = await importAesKey(tunnel.key);
  const encrypted = await crypto.subtle.encrypt(
    { name: "AES-GCM", iv: bufferSource(nonce) },
    key,
    bufferSource(bytes)
  );
  return {
    nonce: toBase64Url(nonce),
    ciphertext: toBase64Url(new Uint8Array(encrypted))
  };
}

export async function decryptFromTunnel(tunnel: TunnelRecord, nonce: string, ciphertext: string): Promise<Uint8Array> {
  const key = await importAesKey(tunnel.key);
  const decrypted = await crypto.subtle.decrypt(
    { name: "AES-GCM", iv: bufferSource(fromBase64Url(nonce)) },
    key,
    bufferSource(fromBase64Url(ciphertext))
  );
  return new Uint8Array(decrypted);
}

export function encode(value: string): Uint8Array {
  return encoder.encode(value);
}

export function decode(value: Uint8Array): string {
  return decoder.decode(value);
}

export function cleanNick(value: string): string {
  return value.trim().replace(/\s+/gu, " ").slice(0, 32) || ".";
}

async function verifyInvite(invite: SignedInvite): Promise<boolean> {
  if (invite.payload.v !== 1 || invite.payload.kind !== "soty.trustlink.invite") {
    return false;
  }
  if (!invite.signature) {
    return true;
  }
  const key = await crypto.subtle.importKey(
    "jwk",
    invite.payload.from.publicJwk,
    { name: "ECDSA", namedCurve: "P-256" },
    false,
    ["verify"]
  );
  return crypto.subtle.verify(
    { name: "ECDSA", hash: "SHA-256" },
    key,
    bufferSource(fromBase64Url(invite.signature)),
    bufferSource(encode(stableJson(invite.payload)))
  );
}

function parseCompactInvite(value: string): SignedInvite | null {
  const [roomId, roomKey, nick] = value.split(".");
  if (!roomId || !roomKey || !nick) {
    return null;
  }
  return {
    payload: {
      v: 1,
      kind: "soty.trustlink.invite",
      roomId,
      roomKey,
      from: {
        deviceId: "dev_invite",
        nick: decode(fromBase64Url(nick)),
        publicJwk: {}
      },
      createdAt: new Date().toISOString()
    }
  };
}

async function importAesKey(secret: string): Promise<CryptoKey> {
  return crypto.subtle.importKey(
    "raw",
    await crypto.subtle.digest("SHA-256", bufferSource(fromBase64Url(secret))),
    { name: "AES-GCM" },
    false,
    ["encrypt", "decrypt"]
  );
}

function deriveJoinAesKey(privateKey: CryptoKey, publicKey: CryptoKey): Promise<CryptoKey> {
  return crypto.subtle.deriveKey(
    { name: "ECDH", public: publicKey },
    privateKey,
    { name: "AES-GCM", length: 256 },
    false,
    ["encrypt", "decrypt"]
  );
}

function randomToken(byteLength: number): string {
  const bytes = crypto.getRandomValues(new Uint8Array(byteLength));
  return toBase64Url(bytes);
}

async function sha256Base64Url(value: string): Promise<string> {
  const digest = await crypto.subtle.digest("SHA-256", bufferSource(encode(value)));
  return toBase64Url(new Uint8Array(digest));
}

function bufferSource(bytes: Uint8Array): ArrayBuffer {
  return bytes.buffer.slice(bytes.byteOffset, bytes.byteOffset + bytes.byteLength) as ArrayBuffer;
}

function stableJson(value: unknown): string {
  return JSON.stringify(sortJson(value));
}

function sortJson(value: unknown): unknown {
  if (Array.isArray(value)) {
    return value.map(sortJson);
  }
  if (value && typeof value === "object") {
    const sorted: Record<string, unknown> = {};
    for (const key of Object.keys(value as Record<string, unknown>).sort()) {
      const item = (value as Record<string, unknown>)[key];
      if (item !== undefined) {
        sorted[key] = sortJson(item);
      }
    }
    return sorted;
  }
  return value;
}

function toBase64Url(value: Uint8Array): string {
  let binary = "";
  for (const byte of value) {
    binary += String.fromCharCode(byte);
  }
  return btoa(binary).replaceAll("+", "-").replaceAll("/", "_").replace(/=+$/u, "");
}

function fromBase64Url(value: string): Uint8Array {
  const normalized = value.replaceAll("-", "+").replaceAll("_", "/");
  const padded = normalized + "=".repeat((4 - normalized.length % 4) % 4);
  const binary = atob(padded);
  const bytes = new Uint8Array(binary.length);
  for (let index = 0; index < binary.length; index += 1) {
    bytes[index] = binary.charCodeAt(index);
  }
  return bytes;
}

function openDb(): Promise<IDBDatabase> {
  return new Promise((resolve, reject) => {
    const request = indexedDB.open(dbName, 1);
    request.onupgradeneeded = () => {
      request.result.createObjectStore("kv");
    };
    request.onerror = () => reject(request.error);
    request.onsuccess = () => resolve(request.result);
  });
}

async function idbGet<T>(key: string): Promise<T | undefined> {
  const db = await openDb();
  return new Promise((resolve, reject) => {
    const tx = db.transaction("kv", "readonly");
    const request = tx.objectStore("kv").get(key);
    request.onerror = () => reject(request.error);
    request.onsuccess = () => resolve(request.result as T | undefined);
    tx.oncomplete = () => db.close();
  });
}

async function idbSet<T>(key: string, value: T): Promise<void> {
  const db = await openDb();
  await new Promise<void>((resolve, reject) => {
    const tx = db.transaction("kv", "readwrite");
    tx.objectStore("kv").put(value, key);
    tx.onerror = () => reject(tx.error);
    tx.oncomplete = () => {
      db.close();
      resolve();
    };
  });
}
