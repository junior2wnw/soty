import {
  createCompactJoinCode,
  fromBase64Url,
  parseCompactJoinCode,
  readUtf8,
  stableJson,
  utf8
} from "trustlink-kernel";
import {
  createWebJoinKeyPair,
  exportWebJoinPublicKey,
  openRoomSecretFromWebJoin,
  sealRoomSecretForWebJoin
} from "trustlink-kernel/platform/web";
import { cleanNick, decode } from "./codec";
import {
  loadJson,
  pendingInviteKey,
  pendingJoinKey,
  removeStored,
  saveJson
} from "./storage";
import { DeviceRecord, JoinAcceptPayload, JoinInvite, SignedInvite, TunnelRecord } from "./types";

export async function inviteUrl(tunnel: TunnelRecord, device: DeviceRecord): Promise<string> {
  return `${publicOrigin()}/?j=${createCompactJoinCode(tunnel.id, device.nick)}`;
}

export function joinInviteFromLocation(): JoinInvite | null {
  const compact = new URL(window.location.href).searchParams.get("j");
  if (!compact) {
    return null;
  }
  try {
    const invite = parseCompactJoinCode(compact);
    return {
      roomId: invite.roomId,
      fromNick: invite.label
    };
  } catch {
    return null;
  }
}

export function captureJoinInviteFromLocation(): JoinInvite | null {
  const invite = joinInviteFromLocation();
  if (invite) {
    savePendingJoin(invite);
  }
  return invite;
}

export function loadPendingJoin(): JoinInvite | null {
  return loadJson<JoinInvite>(pendingJoinKey);
}

export function savePendingJoin(invite: JoinInvite): void {
  saveJson(pendingJoinKey, invite);
}

export function clearPendingJoin(): void {
  removeStored(pendingJoinKey);
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
    const invite = JSON.parse(readUtf8(fromBase64Url(encoded))) as SignedInvite;
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
  return loadJson<SignedInvite>(pendingInviteKey);
}

export function savePendingInvite(invite: SignedInvite): void {
  saveJson(pendingInviteKey, invite);
}

export function clearPendingInvite(): void {
  removeStored(pendingInviteKey);
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

export function createJoinKeyPair(): Promise<CryptoKeyPair> {
  return createWebJoinKeyPair();
}

export function publicJoinJwk(pair: CryptoKeyPair): Promise<JsonWebKey> {
  return exportWebJoinPublicKey(pair);
}

export async function encryptRoomKeyForJoin(
  roomKey: string,
  requesterPublicJwk: JsonWebKey,
  hostNick: string
): Promise<JoinAcceptPayload> {
  const accepted = await sealRoomSecretForWebJoin(roomKey, requesterPublicJwk, hostNick);
  return {
    roomKeyCiphertext: accepted.secretCiphertext,
    nonce: accepted.nonce,
    hostPublicJwk: accepted.responderPublicJwk,
    hostNick: accepted.responderLabel
  };
}

export function decryptAcceptedJoin(privateKey: CryptoKey, payload: JoinAcceptPayload): Promise<string> {
  return openRoomSecretFromWebJoin(privateKey, {
    secretCiphertext: payload.roomKeyCiphertext,
    nonce: payload.nonce,
    responderPublicJwk: payload.hostPublicJwk,
    responderLabel: payload.hostNick
  });
}

function publicOrigin(): string {
  return window.location.hostname === "xn--n1afe0b.online"
    ? "https://\u0441\u043e\u0442\u044b.online"
    : window.location.origin;
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
    bufferSource(utf8(stableJson(invite.payload)))
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
        nick: cleanNick(decode(fromBase64Url(nick))),
        publicJwk: {}
      },
      createdAt: new Date().toISOString()
    }
  };
}

function bufferSource(bytes: Uint8Array): ArrayBuffer {
  return bytes.buffer.slice(bytes.byteOffset, bytes.byteOffset + bytes.byteLength) as ArrayBuffer;
}
