import { fromBase64Url, readUtf8, stableJson, toBase64Url, utf8 } from "trustlink-kernel";
import { webSha256Base64Url } from "trustlink-kernel/platform/web";
import { cleanNick } from "../trustlink/codec";
import { deviceKey, idbSet } from "../trustlink/storage";
import type { DeviceRecord } from "../trustlink/types";

export const accountTransferSchema = "soty.account-transfer.v1";
export const phraseBackupSchema = "soty.account-phrase-backup.v1";
export const phraseMinimumWords = 6;
export const phraseRecommendedWords = 12;

const deviceKeySchema = "soty.device-key.v1";
const phraseLookupIterations = 120_000;
const phraseCipherIterations = 240_000;
const phraseLookupSalt = utf8("soty.account-transfer.lookup.v1");

export type AccountTransferStage = "root" | "export" | "import";
export type AccountTransferRootAction = "export" | "import";
export type AccountTransferChannel = "file" | "phrase";
export type AccountTransferOperation = "export-file" | "export-phrase" | "import-file" | "import-phrase";
export type AccountTransferButtonRole = "standard" | "transfer";

export interface AccountTransferPayload {
  readonly schema: typeof accountTransferSchema;
  readonly exportedAt: string;
  readonly deviceKey: PortableDeviceKey;
  readonly operatorExport: unknown;
}

export interface PortableDeviceKey {
  readonly schema: typeof deviceKeySchema;
  readonly id: string;
  readonly nick: string;
  readonly publicJwk: JsonWebKey;
  readonly privateJwk: JsonWebKey;
  readonly createdAt: string;
}

export interface PhraseValidation {
  readonly ok: boolean;
  readonly words: number;
  readonly normalized: string;
  readonly message: string;
}

export interface AccountTransferAlgorithmNode {
  readonly id: AccountTransferStage;
  readonly label: string;
  readonly choices: readonly {
    readonly channel: AccountTransferChannel;
    readonly operation: AccountTransferOperation;
    readonly label: string;
  }[];
}

export const accountTransferAlgorithm: readonly AccountTransferAlgorithmNode[] = [
  {
    id: "export",
    label: "Экспорт",
    choices: [
      { channel: "file", operation: "export-file", label: "Сохранить файлом" },
      { channel: "phrase", operation: "export-phrase", label: "Сохранить фразой" }
    ]
  },
  {
    id: "import",
    label: "Импорт",
    choices: [
      { channel: "file", operation: "import-file", label: "Импорт файлом" },
      { channel: "phrase", operation: "import-phrase", label: "Импорт фразой" }
    ]
  }
];

export function accountTransferButtonWidth(role: AccountTransferButtonRole): string {
  return role === "transfer" ? "100%" : "200%";
}

export function accountTransferChoices(stage: AccountTransferStage): AccountTransferAlgorithmNode["choices"] {
  return accountTransferAlgorithm.find((node) => node.id === stage)?.choices ?? [];
}

export function accountTransferOperation(action: AccountTransferRootAction, channel: AccountTransferChannel): AccountTransferOperation {
  return `${action}-${channel}` as AccountTransferOperation;
}

export async function createAccountTransferPayload(operatorExport: unknown, record: DeviceRecord): Promise<AccountTransferPayload> {
  return {
    schema: accountTransferSchema,
    exportedAt: new Date().toISOString(),
    deviceKey: await exportPortableDeviceKey(record),
    operatorExport
  };
}

export function isAccountTransferPayload(value: unknown): value is AccountTransferPayload {
  return isRecord(value)
    && value.schema === accountTransferSchema
    && isRecord(value.deviceKey)
    && isRecord(value.operatorExport);
}

export async function importDeviceFromAccountTransferPayload(payload: AccountTransferPayload): Promise<DeviceRecord> {
  return importPortableDeviceKey(payload.deviceKey);
}

export function normalizeAccountPhrase(value: string): string {
  return value
    .normalize("NFKC")
    .trim()
    .replace(/\s+/gu, " ");
}

export function validateAccountPhrase(value: string): PhraseValidation {
  const normalized = normalizeAccountPhrase(value);
  const words = countWords(normalized);
  if (words < phraseMinimumWords) {
    return {
      ok: false,
      words,
      normalized,
      message: `Минимум ${phraseMinimumWords} слов. Лучше ${phraseRecommendedWords}+ уникальных слов.`
    };
  }
  return {
    ok: true,
    words,
    normalized,
    message: words >= phraseRecommendedWords
      ? "Хорошая длина. Чем уникальнее фраза, тем надежнее восстановление."
      : `Работает. Для запаса лучше ${phraseRecommendedWords}+ слов: чем больше, тем лучше.`
  };
}

export async function saveAccountPhraseBackup(phrase: string, payloadText: string): Promise<void> {
  const validation = validateAccountPhrase(phrase);
  if (!validation.ok) {
    throw new Error("phrase-too-short");
  }
  const lookup = await derivePhraseLookup(validation.normalized);
  const salt = crypto.getRandomValues(new Uint8Array(16));
  const nonce = crypto.getRandomValues(new Uint8Array(12));
  const key = await derivePhraseCipherKey(validation.normalized, salt);
  const ciphertext = new Uint8Array(await crypto.subtle.encrypt(
    { name: "AES-GCM", iv: bufferSource(nonce) },
    key,
    bufferSource(utf8(payloadText))
  ));
  const envelope = {
    schema: phraseBackupSchema,
    createdAt: new Date().toISOString(),
    kdf: {
      name: "PBKDF2",
      hash: "SHA-256",
      iterations: phraseCipherIterations,
      salt: toBase64Url(salt)
    },
    cipher: {
      name: "AES-GCM",
      nonce: toBase64Url(nonce),
      ciphertext: toBase64Url(ciphertext)
    }
  };
  const response = await fetch(`/api/account-transfer/${encodeURIComponent(lookup)}`, {
    method: "PUT",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify(envelope)
  });
  if (!response.ok) {
    throw new Error("phrase-backup-save-failed");
  }
  try {
    localStorage.setItem(phraseCacheKey(lookup), JSON.stringify(envelope));
  } catch {
    // Server copy is the durable phrase backup; local cache is only a convenience.
  }
}

export async function loadAccountPhraseBackup(phrase: string): Promise<string> {
  const validation = validateAccountPhrase(phrase);
  if (!validation.ok) {
    throw new Error("phrase-too-short");
  }
  const lookup = await derivePhraseLookup(validation.normalized);
  const envelope = await fetchPhraseEnvelope(lookup);
  if (!envelope) {
    throw new Error("phrase-backup-not-found");
  }
  const kdf = isRecord(envelope.kdf) ? envelope.kdf : {};
  const cipher = isRecord(envelope.cipher) ? envelope.cipher : {};
  if (
    envelope.schema !== phraseBackupSchema
    || kdf.name !== "PBKDF2"
    || kdf.hash !== "SHA-256"
    || typeof kdf.salt !== "string"
    || typeof cipher.nonce !== "string"
    || typeof cipher.ciphertext !== "string"
  ) {
    throw new Error("phrase-backup-invalid");
  }
  const iterations = typeof kdf.iterations === "number" && Number.isSafeInteger(kdf.iterations)
    ? Math.max(100_000, Math.min(kdf.iterations, 1_000_000))
    : phraseCipherIterations;
  const salt = fromBase64Url(kdf.salt);
  const nonce = fromBase64Url(cipher.nonce);
  const ciphertext = fromBase64Url(cipher.ciphertext);
  const key = await derivePhraseCipherKey(validation.normalized, salt, iterations);
  const plaintext = new Uint8Array(await crypto.subtle.decrypt(
    { name: "AES-GCM", iv: bufferSource(nonce) },
    key,
    bufferSource(ciphertext)
  ));
  return readUtf8(plaintext);
}

async function exportPortableDeviceKey(record: DeviceRecord): Promise<PortableDeviceKey> {
  if (!record.privateKey.extractable) {
    throw new Error("device-key-not-exportable");
  }
  const privateJwk = await crypto.subtle.exportKey("jwk", record.privateKey);
  return {
    schema: deviceKeySchema,
    id: record.id,
    nick: record.nick,
    publicJwk: compactPublicJwk(record.publicJwk),
    privateJwk: compactPrivateJwk(privateJwk),
    createdAt: record.createdAt
  };
}

async function importPortableDeviceKey(value: unknown): Promise<DeviceRecord> {
  if (!isRecord(value) || value.schema !== deviceKeySchema) {
    throw new Error("device-key-invalid");
  }
  const publicJwk = compactPublicJwk(value.publicJwk);
  const privateJwk = compactPrivateJwk(value.privateJwk);
  const privateKey = await crypto.subtle.importKey(
    "jwk",
    privateJwk,
    { name: "ECDSA", namedCurve: "P-256" },
    true,
    ["sign"]
  );
  await verifyPortableDeviceKey(publicJwk, privateKey);
  const id = `dev_${(await webSha256Base64Url(stableJson(publicJwk))).slice(0, 32)}`;
  const record: DeviceRecord = {
    id,
    nick: cleanNick(typeof value.nick === "string" ? value.nick : "") || "Soty",
    publicJwk,
    privateKey,
    createdAt: typeof value.createdAt === "string" ? value.createdAt : new Date().toISOString()
  };
  await idbSet(deviceKey, record);
  return record;
}

async function verifyPortableDeviceKey(publicJwk: JsonWebKey, privateKey: CryptoKey): Promise<void> {
  const publicKey = await crypto.subtle.importKey(
    "jwk",
    publicJwk,
    { name: "ECDSA", namedCurve: "P-256" },
    false,
    ["verify"]
  );
  const challenge = crypto.getRandomValues(new Uint8Array(32));
  const signature = await crypto.subtle.sign(
    { name: "ECDSA", hash: "SHA-256" },
    privateKey,
    challenge
  );
  const ok = await crypto.subtle.verify(
    { name: "ECDSA", hash: "SHA-256" },
    publicKey,
    signature,
    challenge
  );
  if (!ok) {
    throw new Error("device-key-mismatch");
  }
}

function compactPublicJwk(value: unknown): JsonWebKey {
  if (!isRecord(value)) {
    throw new Error("public-jwk-invalid");
  }
  const jwk = value as JsonWebKey;
  if (jwk.kty !== "EC" || jwk.crv !== "P-256" || typeof jwk.x !== "string" || typeof jwk.y !== "string") {
    throw new Error("public-jwk-invalid");
  }
  return {
    kty: "EC",
    crv: "P-256",
    x: jwk.x,
    y: jwk.y,
    ext: true,
    key_ops: ["verify"]
  };
}

function compactPrivateJwk(value: unknown): JsonWebKey {
  if (!isRecord(value)) {
    throw new Error("private-jwk-invalid");
  }
  const jwk = value as JsonWebKey;
  if (
    jwk.kty !== "EC"
    || jwk.crv !== "P-256"
    || typeof jwk.x !== "string"
    || typeof jwk.y !== "string"
    || typeof jwk.d !== "string"
  ) {
    throw new Error("private-jwk-invalid");
  }
  return {
    kty: "EC",
    crv: "P-256",
    x: jwk.x,
    y: jwk.y,
    d: jwk.d,
    ext: true,
    key_ops: ["sign"]
  };
}

function countWords(value: string): number {
  if (!value) {
    return 0;
  }
  const Segmenter = (Intl as unknown as {
    readonly Segmenter?: new (locale?: string, options?: { readonly granularity: "word" }) => {
      segment(input: string): Iterable<{ readonly segment: string; readonly isWordLike?: boolean }>;
    };
  }).Segmenter;
  if (Segmenter) {
    const segmenter = new Segmenter(undefined, { granularity: "word" });
    let words = 0;
    for (const item of segmenter.segment(value)) {
      if (item.isWordLike === true) {
        words += 1;
      }
    }
    if (words > 0) {
      return words;
    }
  }
  return value.match(/[\p{L}\p{N}]+/gu)?.length ?? 0;
}

async function derivePhraseLookup(phrase: string): Promise<string> {
  const bits = await derivePhraseBits(phrase, phraseLookupSalt, phraseLookupIterations);
  return toBase64Url(new Uint8Array(bits));
}

async function derivePhraseCipherKey(phrase: string, salt: Uint8Array, iterations = phraseCipherIterations): Promise<CryptoKey> {
  const baseKey = await crypto.subtle.importKey("raw", bufferSource(utf8(phrase)), "PBKDF2", false, ["deriveKey"]);
  return crypto.subtle.deriveKey(
    { name: "PBKDF2", hash: "SHA-256", salt: bufferSource(salt), iterations },
    baseKey,
    { name: "AES-GCM", length: 256 },
    false,
    ["encrypt", "decrypt"]
  );
}

async function derivePhraseBits(phrase: string, salt: Uint8Array, iterations: number): Promise<ArrayBuffer> {
  const baseKey = await crypto.subtle.importKey("raw", bufferSource(utf8(phrase)), "PBKDF2", false, ["deriveBits"]);
  return crypto.subtle.deriveBits(
    { name: "PBKDF2", hash: "SHA-256", salt: bufferSource(salt), iterations },
    baseKey,
    256
  );
}

async function fetchPhraseEnvelope(lookup: string): Promise<Record<string, unknown> | null> {
  try {
    const response = await fetch(`/api/account-transfer/${encodeURIComponent(lookup)}`);
    if (response.ok) {
      const parsed: unknown = await response.json();
      return isRecord(parsed) ? parsed : null;
    }
  } catch {
    // Fall back to the local cache below.
  }
  try {
    const cached = localStorage.getItem(phraseCacheKey(lookup));
    const parsed: unknown = cached ? JSON.parse(cached) : null;
    return isRecord(parsed) ? parsed : null;
  } catch {
    return null;
  }
}

function phraseCacheKey(lookup: string): string {
  return `soty:account-phrase-cache:${lookup}`;
}

function bufferSource(bytes: Uint8Array): Uint8Array<ArrayBuffer> {
  const copy = new Uint8Array(bytes.byteLength) as Uint8Array<ArrayBuffer>;
  copy.set(bytes);
  return copy;
}

function isRecord(value: unknown): value is Record<string, unknown> {
  return typeof value === "object" && value !== null && !Array.isArray(value);
}
