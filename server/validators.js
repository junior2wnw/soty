export function isShortText(value, max) {
  return typeof value === "string" && value.length > 0 && value.length <= max;
}

export function isEncryptedUpdate(value) {
  return value
    && (value.kind === "update" || value.kind === "snapshot")
    && isShortText(value.id, 120)
    && isShortText(value.nonce, 64)
    && isShortText(value.ciphertext, 2_000_000);
}

export function isEncryptedFile(value) {
  if (!value || !isShortText(value.id, 140)) {
    return false;
  }
  if (value.kind === "delete") {
    return isShortText(value.fileId, 120);
  }
  if (value.kind === "chunk") {
    return isShortText(value.fileId, 120)
      && isSafeRange(value.index, 0, 8191)
      && isSafeRange(value.total, 1, 8192)
      && isSafeRange(value.totalBytes, 0, 2_000_000_000)
      && isSafeRange(value.bytes, 0, 512_000)
      && isShortText(value.nonce, 64)
      && isShortText(value.ciphertext, 800_000)
      && optionalShortText(value.metaNonce, 64)
      && optionalShortText(value.metaCiphertext, 20_000);
  }
  return (value.kind === undefined || value.kind === "complete")
    && isSafeRange(value.bytes, 0, 200_000_000)
    && isShortText(value.nonce, 64)
    && isShortText(value.metaNonce, 64)
    && isShortText(value.ciphertext, 320_000_000)
    && isShortText(value.metaCiphertext, 20_000);
}

export function isRemoteGrant(value) {
  return value
    && isShortText(value.id, 140)
    && typeof value.enabled === "boolean"
    && optionalTargetDeviceId(value.targetDeviceId);
}

export function isRemoteRequest(value) {
  return value
    && isShortText(value.id, 140)
    && optionalTargetDeviceId(value.targetDeviceId);
}

export function isNoticeKnock(value) {
  return value
    && isShortText(value.id, 140)
    && optionalTargetDeviceId(value.targetDeviceId);
}

export function isLiveDraft(value) {
  return value
    && isShortText(value.id, 140)
    && isShortText(value.nonce, 64)
    && isShortText(value.ciphertext, 20_000);
}

export function isRemoteCommand(value) {
  return value
    && isShortText(value.id, 140)
    && optionalTargetDeviceId(value.targetDeviceId)
    && isShortText(value.nonce, 64)
    && isShortText(value.ciphertext, 20_000);
}

export function isRemoteScript(value) {
  return value
    && isShortText(value.id, 140)
    && optionalTargetDeviceId(value.targetDeviceId)
    && isShortText(value.nonce, 64)
    && isShortText(value.ciphertext, 2_000_000);
}

export function isRemoteOutput(value) {
  return value
    && isShortText(value.id, 140)
    && isShortText(value.commandId, 140)
    && optionalTargetDeviceId(value.targetDeviceId)
    && isShortText(value.nonce, 64)
    && isShortText(value.ciphertext, 80_000)
    && (value.exitCode === undefined || isSafeRange(value.exitCode, -32768, 32767));
}

export function isRemoteCancel(value) {
  return value
    && isShortText(value.id, 140)
    && isShortText(value.commandId, 140)
    && optionalTargetDeviceId(value.targetDeviceId);
}

export function isP2pDescription(value, kind) {
  return value
    && isShortText(value.id, 140)
    && isShortText(value.targetDeviceId, 140)
    && value.kind === kind
    && isShortText(value.sdp, 40_000);
}

export function isP2pCandidate(value) {
  return value
    && isShortText(value.id, 140)
    && isShortText(value.targetDeviceId, 140)
    && isShortText(value.candidate, 8_000)
    && optionalShortText(value.sdpMid, 120)
    && (value.sdpMLineIndex === null || value.sdpMLineIndex === undefined || isSafeRange(value.sdpMLineIndex, 0, 128));
}

export function isJoinRequest(value) {
  return value
    && isShortText(value.requestId, 120)
    && isJoinPublicJwk(value.publicJwk);
}

export function isJoinAccept(value) {
  return value
    && isShortText(value.roomKeyCiphertext, 512)
    && isShortText(value.nonce, 64)
    && isShortText(value.hostNick, 80)
    && isJoinPublicJwk(value.hostPublicJwk);
}

export function isPlainObject(value) {
  return value && typeof value === "object" && !Array.isArray(value);
}

function isJoinPublicJwk(value) {
  if (!isPlainObject(value) || !isShortText(value.kty, 32)) {
    return false;
  }
  const entries = Object.entries(value);
  if (entries.length > 16) {
    return false;
  }
  let jsonLength = 2;
  for (const [key, item] of entries) {
    if (!isShortText(key, 64)) {
      return false;
    }
    if (typeof item === "string") {
      if (item.length > 1024) {
        return false;
      }
      jsonLength += key.length + item.length + 6;
      continue;
    }
    if (typeof item === "number" || typeof item === "boolean" || item === null) {
      jsonLength += key.length + String(item).length + 4;
      continue;
    }
    if (Array.isArray(item) && item.length <= 8 && item.every((entry) => typeof entry === "string" && entry.length <= 64)) {
      jsonLength += key.length + item.reduce((sum, entry) => sum + entry.length + 3, 4);
      continue;
    }
    return false;
  }
  return jsonLength <= 4096;
}

function optionalShortText(value, max) {
  return value === undefined || value === null || isShortText(value, max);
}

function optionalTargetDeviceId(value) {
  return value === undefined || isShortText(value, 140);
}

function isSafeRange(value, min, max) {
  return Number.isSafeInteger(value) && value >= min && value <= max;
}
