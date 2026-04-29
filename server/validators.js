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
    && isShortText(value.targetDeviceId, 140);
}

export function isRemoteCommand(value) {
  return value
    && isShortText(value.id, 140)
    && isShortText(value.targetDeviceId, 140)
    && isShortText(value.nonce, 64)
    && isShortText(value.ciphertext, 20_000);
}

export function isRemoteOutput(value) {
  return value
    && isShortText(value.id, 140)
    && isShortText(value.commandId, 140)
    && isShortText(value.targetDeviceId, 140)
    && isShortText(value.nonce, 64)
    && isShortText(value.ciphertext, 80_000)
    && (value.exitCode === undefined || isSafeRange(value.exitCode, -32768, 32767));
}

export function isJoinRequest(value) {
  return value
    && isShortText(value.requestId, 120)
    && isPlainObject(value.publicJwk);
}

export function isJoinAccept(value) {
  return value
    && isShortText(value.roomKeyCiphertext, 512)
    && isShortText(value.nonce, 64)
    && isShortText(value.hostNick, 80)
    && isPlainObject(value.hostPublicJwk);
}

export function isPlainObject(value) {
  return value && typeof value === "object" && !Array.isArray(value);
}

function optionalShortText(value, max) {
  return value === undefined || isShortText(value, max);
}

function isSafeRange(value, min, max) {
  return Number.isSafeInteger(value) && value >= min && value <= max;
}
