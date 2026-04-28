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
  return value
    && isShortText(value.id, 120)
    && Number.isSafeInteger(value.bytes)
    && value.bytes >= 0
    && value.bytes <= 20_000_000
    && isShortText(value.nonce, 64)
    && isShortText(value.metaNonce, 64)
    && isShortText(value.ciphertext, 32_000_000)
    && isShortText(value.metaCiphertext, 20_000);
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
