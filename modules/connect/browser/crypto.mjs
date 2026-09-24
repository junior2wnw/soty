/** Browser-only cryptographic primitives. No credential is logged or put in a URL. */
export class ConnectError extends Error {
  constructor(code, message, options = {}) {
    super(message, options.cause ? { cause: options.cause } : undefined);
    this.name = 'ConnectError';
    this.code = code;
    if (options.status !== undefined) this.status = options.status;
  }
}

const encoder = new TextEncoder();
const decoder = new TextDecoder('utf-8', { fatal: true });
export const utf8 = value => encoder.encode(value);

export function cryptoApi() {
  if (!globalThis.crypto?.subtle || !globalThis.crypto?.getRandomValues) {
    throw new ConnectError('CRYPTO_UNAVAILABLE', 'Secure cryptography is unavailable in this browser.');
  }
  return globalThis.crypto;
}

export function canonicalJson(value) {
  const seen = new Set();
  const encode = (item, depth) => {
    if (depth > 32) throw new ConnectError('INVALID_JSON', 'The data is too deeply nested.');
    if (item === null || typeof item === 'boolean' || typeof item === 'string') return JSON.stringify(item);
    if (typeof item === 'number' && Number.isFinite(item)) return JSON.stringify(item);
    if (typeof item !== 'object' || seen.has(item)) throw new ConnectError('INVALID_JSON', 'Only finite JSON data is supported.');
    const prototype = Object.getPrototypeOf(item);
    if (!Array.isArray(item) && prototype !== Object.prototype && prototype !== null) {
      throw new ConnectError('INVALID_JSON', 'Only plain JSON objects are supported.');
    }
    seen.add(item);
    let result;
    if (Array.isArray(item)) {
      // Reject sparse arrays instead of silently changing the signed arguments.
      result = '[' + Array.from({ length: item.length }, (_, index) => encode(item[index], depth + 1)).join(',') + ']';
    } else {
      result = '{' + Object.keys(item).sort().map(key => JSON.stringify(key) + ':' + encode(item[key], depth + 1)).join(',') + '}';
    }
    seen.delete(item);
    return result;
  };
  return encode(value, 0);
}

export function base64url(bytes) {
  const data = bytes instanceof Uint8Array ? bytes : new Uint8Array(bytes);
  let binary = '';
  for (let index = 0; index < data.length; index += 0x8000) binary += String.fromCharCode(...data.subarray(index, index + 0x8000));
  return btoa(binary).replaceAll('+', '-').replaceAll('/', '_').replace(/=+$/, '');
}

export function unbase64url(value, expectedLength, maximum = 3 * 1024 * 1024) {
  if (typeof value !== 'string' || !/^[A-Za-z0-9_-]+$/.test(value) || value.length > maximum * 2) {
    throw new ConnectError('INVALID_ENCODING', 'The encoded data is invalid.');
  }
  let bytes;
  try { bytes = Uint8Array.from(atob(value.replaceAll('-', '+').replaceAll('_', '/') + '='.repeat((4 - value.length % 4) % 4)), char => char.charCodeAt(0)); }
  catch { throw new ConnectError('INVALID_ENCODING', 'The encoded data is invalid.'); }
  if (bytes.length > maximum || expectedLength !== undefined && bytes.length !== expectedLength || base64url(bytes) !== value) {
    throw new ConnectError('INVALID_ENCODING', 'The encoded data has an invalid length or representation.');
  }
  return bytes;
}

export async function sha256(value) {
  return base64url(await cryptoApi().subtle.digest('SHA-256', typeof value === 'string' ? utf8(value) : value));
}

export function publicJwk(value) {
  if (!value || value.kty !== 'EC' || value.crv !== 'P-256' || 'd' in value) {
    throw new ConnectError('INVALID_PUBLIC_KEY', 'A public P-256 key is required.');
  }
  unbase64url(value.x, 32); unbase64url(value.y, 32);
  return { crv: 'P-256', kty: 'EC', x: value.x, y: value.y };
}

export const keyFingerprint = jwk => sha256(canonicalJson(publicJwk(jwk)));
export const signingDeviceId = async jwk => 'dev_' + await keyFingerprint(jwk);

function assertCryptoKey(key, algorithm, usage, type = 'private') {
  if (Object.prototype.toString.call(key) !== '[object CryptoKey]' || key.type !== type || key.extractable !== false ||
      key.algorithm?.name !== algorithm || !key.usages.includes(usage) ||
      (type === 'private' && key.algorithm.namedCurve !== 'P-256')) {
    throw new ConnectError('CORRUPT_LOCAL_STATE', 'The saved device key is invalid. Existing data was not replaced.');
  }
}

export function validateInstallationKeys(installation) {
  publicJwk(installation.signingPublicJwk); publicJwk(installation.encryptionPublicJwk);
  assertCryptoKey(installation.signingPrivateKey, 'ECDSA', 'sign');
  assertCryptoKey(installation.encryptionPrivateKey, 'ECDH', 'deriveBits');
  assertCryptoKey(installation.storageKey, 'AES-GCM', 'encrypt', 'secret');
  if (!installation.storageKey.usages.includes('decrypt') || installation.storageKey.algorithm.length !== 256) {
    throw new ConnectError('CORRUPT_LOCAL_STATE', 'The saved local vault key is invalid.');
  }
}

export async function createInstallation(label) {
  const api = cryptoApi();
  const [signing, encryption, storageKey] = await Promise.all([
    api.subtle.generateKey({ name: 'ECDSA', namedCurve: 'P-256' }, false, ['sign', 'verify']),
    api.subtle.generateKey({ name: 'ECDH', namedCurve: 'P-256' }, false, ['deriveBits']),
    api.subtle.generateKey({ name: 'AES-GCM', length: 256 }, false, ['encrypt', 'decrypt']),
  ]);
  const signingPublicJwk = publicJwk(await api.subtle.exportKey('jwk', signing.publicKey));
  const encryptionPublicJwk = publicJwk(await api.subtle.exportKey('jwk', encryption.publicKey));
  return {
    deviceId: await signingDeviceId(signingPublicJwk), label,
    createdAt: new Date().toISOString(), signingPrivateKey: signing.privateKey, signingPublicJwk,
    encryptionPrivateKey: encryption.privateKey, encryptionPublicJwk, storageKey,
    accountId: null, rootEnvelope: null, vaultRevision: 0, revoked: false,
    enrollmentRequestId: null, recoveryFingerprint: null,
  };
}

/** Verify both saved key pairs before using a durable identity. */
export async function verifyInstallation(installation) {
  validateInstallationKeys(installation);
  const api = cryptoApi();
  if (await signingDeviceId(installation.signingPublicJwk) !== installation.deviceId) throw new ConnectError('CORRUPT_LOCAL_STATE', 'The saved device identity does not match its key.');
  const challenge = api.getRandomValues(new Uint8Array(32));
  const signature = await api.subtle.sign({ name: 'ECDSA', hash: 'SHA-256' }, installation.signingPrivateKey, challenge);
  const verifyKey = await api.subtle.importKey('jwk', publicJwk(installation.signingPublicJwk), { name: 'ECDSA', namedCurve: 'P-256' }, false, ['verify']);
  if (!await api.subtle.verify({ name: 'ECDSA', hash: 'SHA-256' }, verifyKey, signature, challenge)) throw new ConnectError('CORRUPT_LOCAL_STATE', 'The saved signing key pair does not match.');
  const peer = await api.subtle.generateKey({ name: 'ECDH', namedCurve: 'P-256' }, false, ['deriveBits']);
  const savedPublic = await api.subtle.importKey('jwk', publicJwk(installation.encryptionPublicJwk), { name: 'ECDH', namedCurve: 'P-256' }, false, []);
  const [a, b] = await Promise.all([
    api.subtle.deriveBits({ name: 'ECDH', public: peer.publicKey }, installation.encryptionPrivateKey, 256),
    api.subtle.deriveBits({ name: 'ECDH', public: savedPublic }, peer.privateKey, 256),
  ]);
  const left = new Uint8Array(a), right = new Uint8Array(b);
  const matches = left.every((byte, index) => byte === right[index]); left.fill(0); right.fill(0);
  if (!matches) throw new ConnectError('CORRUPT_LOCAL_STATE', 'The saved encryption key pair does not match.');
}

export async function signMessage(installation, message) {
  const bytes = new Uint8Array(await cryptoApi().subtle.sign({ name: 'ECDSA', hash: 'SHA-256' }, installation.signingPrivateKey, utf8(message)));
  if (bytes.length !== 64) throw new ConnectError('SIGNATURE_FORMAT', 'This browser did not produce a P-256 P1363 signature.');
  return base64url(bytes);
}

async function aesKey(raw) {
  if (!(raw instanceof Uint8Array) || raw.length !== 32) throw new ConnectError('INVALID_ROOT_KEY', 'A 256-bit data key is required.');
  return cryptoApi().subtle.importKey('raw', raw, { name: 'AES-GCM' }, false, ['encrypt', 'decrypt']);
}

async function sealBytes(key, bytes, binding) {
  const iv = cryptoApi().getRandomValues(new Uint8Array(12));
  const ciphertext = await cryptoApi().subtle.encrypt({ name: 'AES-GCM', iv, additionalData: utf8(canonicalJson(binding)), tagLength: 128 }, key, bytes);
  return { ...binding, iv: base64url(iv), ciphertext: base64url(ciphertext) };
}

async function openBytes(key, envelope, binding) {
  if (!envelope || Object.entries(binding).some(([key, value]) => envelope[key] !== value)) throw new ConnectError('ENVELOPE_MISMATCH', 'The encrypted data belongs to another account or request.');
  try {
    return new Uint8Array(await cryptoApi().subtle.decrypt({ name: 'AES-GCM', iv: unbase64url(envelope.iv, 12), additionalData: utf8(canonicalJson(binding)), tagLength: 128 }, key, unbase64url(envelope.ciphertext)));
  } catch (error) {
    throw new ConnectError('DECRYPT_FAILED', 'The encrypted data could not be verified. Existing data was not replaced.', { cause: error });
  }
}

export function sealLocalRoot(installation, projectId, raw) {
  return sealBytes(installation.storageKey, raw, { schema: 'connect.local-root.v1', projectId, deviceId: installation.deviceId });
}

export async function openLocalRoot(installation, projectId) {
  const raw = await openBytes(installation.storageKey, installation.rootEnvelope, { schema: 'connect.local-root.v1', projectId, deviceId: installation.deviceId });
  if (raw.length !== 32) { raw.fill(0); throw new ConnectError('INVALID_ROOT_KEY', 'The saved account key has an invalid length.'); }
  return raw;
}

async function hkdf(raw, binding, info) {
  const material = await cryptoApi().subtle.importKey('raw', raw, 'HKDF', false, ['deriveKey']);
  const salt = await cryptoApi().subtle.digest('SHA-256', utf8(canonicalJson(binding)));
  return cryptoApi().subtle.deriveKey({ name: 'HKDF', hash: 'SHA-256', salt, info: utf8(info) }, material, { name: 'AES-GCM', length: 256 }, false, ['encrypt', 'decrypt']);
}

export async function wrapEnrollmentRoot(raw, { projectId, accountId, requestId, encryptionPublicJwk }) {
  const api = cryptoApi(), recipient = await keyFingerprint(encryptionPublicJwk);
  const binding = { schema: 'connect.wrapped-key.v1', projectId, accountId, requestId, recipient };
  const ephemeral = await api.subtle.generateKey({ name: 'ECDH', namedCurve: 'P-256' }, false, ['deriveBits']);
  const recipientKey = await api.subtle.importKey('jwk', publicJwk(encryptionPublicJwk), { name: 'ECDH', namedCurve: 'P-256' }, false, []);
  const shared = new Uint8Array(await api.subtle.deriveBits({ name: 'ECDH', public: recipientKey }, ephemeral.privateKey, 256));
  try {
    const key = await hkdf(shared, binding, 'connect.root-key.v1');
    return { ...await sealBytes(key, raw, binding), senderPublicJwk: publicJwk(await api.subtle.exportKey('jwk', ephemeral.publicKey)) };
  } finally { shared.fill(0); }
}

export async function unwrapEnrollmentRoot(envelope, installation, { projectId, accountId, requestId }) {
  const binding = { schema: 'connect.wrapped-key.v1', projectId, accountId, requestId, recipient: await keyFingerprint(installation.encryptionPublicJwk) };
  const sender = await cryptoApi().subtle.importKey('jwk', publicJwk(envelope?.senderPublicJwk), { name: 'ECDH', namedCurve: 'P-256' }, false, []);
  const shared = new Uint8Array(await cryptoApi().subtle.deriveBits({ name: 'ECDH', public: sender }, installation.encryptionPrivateKey, 256));
  try {
    const raw = await openBytes(await hkdf(shared, binding, 'connect.root-key.v1'), envelope, binding);
    if (raw.length !== 32) { raw.fill(0); throw new ConnectError('INVALID_ROOT_KEY', 'The transferred key has an invalid length.'); }
    return raw;
  } finally { shared.fill(0); }
}

export async function wrapRecoveryRoot(raw, secret, { projectId, accountId }) {
  const binding = { schema: 'connect.recovery-key.v1', projectId, accountId };
  return sealBytes(await hkdf(secret, binding, 'connect.recovery-root.v1'), raw, binding);
}

export async function unwrapRecoveryRoot(envelope, secret, { projectId, accountId }) {
  const binding = { schema: 'connect.recovery-key.v1', projectId, accountId };
  const raw = await openBytes(await hkdf(secret, binding, 'connect.recovery-root.v1'), envelope, binding);
  if (raw.length !== 32) { raw.fill(0); throw new ConnectError('INVALID_ROOT_KEY', 'The recovered key has an invalid length.'); }
  return raw;
}

export async function sealVault(raw, payload, { projectId, accountId, revision }) {
  const plaintext = utf8(canonicalJson(payload));
  if (plaintext.length > 1024 * 1024) throw new ConnectError('VAULT_TOO_LARGE', 'The backup is larger than 1 MiB.');
  try { return await sealBytes(await aesKey(raw), plaintext, { schema: 'connect.vault.v1', projectId, accountId, revision }); }
  finally { plaintext.fill(0); }
}

export async function openVault(raw, envelope, { projectId, accountId, revision }) {
  const plaintext = await openBytes(await aesKey(raw), envelope, { schema: 'connect.vault.v1', projectId, accountId, revision });
  try { return JSON.parse(decoder.decode(plaintext)); }
  catch (error) { throw new ConnectError('INVALID_VAULT', 'The verified backup does not contain valid JSON.', { cause: error }); }
  finally { plaintext.fill(0); }
}
