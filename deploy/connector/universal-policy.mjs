import { open, lstat, realpath } from 'node:fs/promises';
import { constants } from 'node:fs';
import { createHash, timingSafeEqual, randomBytes, createCipheriv, createDecipheriv } from 'node:crypto';
import { tmpdir } from 'node:os';
import path from 'node:path';
import { TextDecoder } from 'node:util';
import { SafeError } from './docker-api.mjs';
import { storageReaders, storageReaderLabel } from './storage-guard.mjs';
import { canonicalJson } from '../../modules/capabilities/server/validation.mjs';
import { createHumanIdentityHostProfile } from '../../modules/human-identity/profile.mjs';
import { createReviewsService } from '../../modules/reviews/server/index.mjs';
import { captureHumanPreparedness, captureUniversalPreparedness as captureRuntimePreparedness,
  captureSelectedPreparedness, UNIVERSAL_RUNTIME_SCHEMA } from '../../modules/app-contract/universal-preparedness.mjs';

export const UNIVERSAL_POLICY_SCHEMA = 'soty.universal-rollout-policy.v1';
export { UNIVERSAL_RUNTIME_SCHEMA };
export const universalModeLabel = 'io.soty.universal.legacy';
export const humanPrivateTarget = '/run/secrets/soty-human-identity.json';
export const reviewsTarget = '/run/config/soty-reviews-bindings.json';
export const selectedTarget = '/run/config/soty-selected-embed.json';
const settings = Object.freeze({ universal: 'SOTY_UNIVERSAL_APPS_ENABLED', human: 'SOTY_HUMAN_IDENTITY_ENABLED',
  issuer: 'SOTY_HUMAN_IDENTITY_ISSUER', keys: 'SOTY_HUMAN_IDENTITY_KEYS_FILE', reviews: 'SOTY_REVIEWS_BINDINGS_FILE',
  operator: 'SOTY_UNIVERSAL_OPERATOR_ENABLED', selected: 'SOTY_SELECTED_EMBED_REGISTRY_FILE',
  selectedMigration: 'SOTY_SELECTED_EMBED_MIGRATION' });
const handles = new WeakMap(), HEX = /^[a-f0-9]{64}$/u, MAX_FILE = 65536;
const fail = (code = 'universal_policy_invalid') => { throw new SafeError(code); };
const check = (value, code) => { if (!value) fail(code); };
// Unlike public descriptors, this local private channel permits cryptographic
// byte strings that happen to contain a token-looking prefix. Shapes are closed
// separately; no value reaches a receipt merely because it passed this copy.
function safe(value, code = 'universal_policy_invalid', maximum = MAX_FILE) {
  let nodes = 0, bytes = 0;
  const budget = count => { bytes += count; check(bytes <= maximum, code); };
  function copy(item, depth) {
    check(++nodes <= 4096 && depth <= 18, code);
    if (item === null || typeof item === 'boolean') { budget(5); return item; }
    if (typeof item === 'string') { check(item.isWellFormed() && !/[\u0000-\u0008\u000b\u000c\u000e-\u001f\u007f]/u.test(item), code); budget(Buffer.byteLength(item) + 2); return item; }
    if (typeof item === 'number') { check(Number.isSafeInteger(item) && !Object.is(item, -0), code); budget(17); return item; }
    check(item && typeof item === 'object' && [Object.prototype, null, Array.prototype].includes(Object.getPrototypeOf(item)), code);
    const fields = Object.getOwnPropertyDescriptors(item); check(Object.getOwnPropertySymbols(item).length === 0, code);
    if (Array.isArray(item)) {
      check(item.length <= 128 && Reflect.ownKeys(fields).length === item.length + 1, code); budget(item.length + 2);
      return Array.from({ length: item.length }, (_, i) => { check(fields[i]?.enumerable && 'value' in fields[i], code); return copy(fields[i].value, depth + 1); });
    }
    const result = {}; budget(2);
    for (const name of Object.keys(fields).sort()) {
      check(/^[A-Za-z_$][A-Za-z0-9_.$-]{0,95}$/u.test(name) && !['__proto__', 'constructor', 'prototype'].includes(name)
        && fields[name].enumerable && 'value' in fields[name], code); budget(name.length + 3); result[name] = copy(fields[name].value, depth + 1);
    }
    return result;
  }
  const captured = copy(value, 0); check(Buffer.byteLength(canonicalJson(captured)) <= maximum, code); return captured;
}
const closed = (value, keys, optional = []) => {
  check(value && typeof value === 'object' && !Array.isArray(value)
    && keys.every(key => Object.hasOwn(value, key)) && Object.keys(value).every(key => keys.includes(key) || optional.includes(key)));
};
const freeze = value => { if (value && typeof value === 'object') { Object.values(value).forEach(freeze); Object.freeze(value); } return value; };
const digest = value => createHash('sha256').update(canonicalJson(value)).digest('hex');
const exactPath = value => {
  check(typeof value === 'string' && value.isWellFormed() && value.length > 0 && value.length <= 4096
    && !/[\u0000-\u001f\u007f]/u.test(value) && path.isAbsolute(value) && path.resolve(value) === value, 'universal_policy_path_invalid'); return value;
};
const under = (root, value) => { const relative = path.relative(root, value); return relative !== '' && !relative.startsWith('..' + path.sep)
  && relative !== '..' && !path.isAbsolute(relative); };
const identity = stat => Object.fromEntries(['dev', 'ino', 'mode', 'nlink', 'uid', 'gid', 'size', 'mtimeNs', 'ctimeNs'].map(key => [key, String(stat[key])]));
const sameIdentity = (a, b) => JSON.stringify(a) === JSON.stringify(b);
function stateFor(handle) {
  const state = handles.get(handle); check(state?.active === true, 'universal_policy_handle_invalid'); return state;
}
function forget(state) { state.active = false; for (const file of state.files) file.bytes.fill(0); }
function parse(bytes, maximum = MAX_FILE) {
  try {
    check(bytes.byteLength <= maximum, 'universal_policy_file_invalid');
    const text = new TextDecoder('utf-8', { fatal: true }).decode(bytes); let pos = 0, nodes = 0;
    const require = value => check(value, 'universal_policy_file_invalid');
    const whitespace = () => { while (pos < text.length && /[ \r\n\t]/u.test(text[pos])) pos++; };
    function string() {
      const start = pos++; while (pos < text.length) {
        const next = text[pos++]; if (next === '\\') { pos++; continue; }
        if (next === '"') return JSON.parse(text.slice(start, pos));
      } require(false);
    }
    function value(depth) {
      require(++nodes <= 4096 && depth <= 18); whitespace();
      if (text[pos] === '"') return string();
      if (text[pos] === '{') {
        pos++; whitespace(); const result = Object.create(null), keys = new Set(); if (text[pos] === '}') { pos++; return result; }
        while (true) { require(text[pos] === '"'); const key = string(); require(!keys.has(key)); keys.add(key); whitespace(); require(text[pos++] === ':');
          result[key] = value(depth + 1); whitespace(); const next = text[pos++]; if (next === '}') return result; require(next === ','); whitespace(); }
      }
      if (text[pos] === '[') {
        pos++; whitespace(); const result = []; if (text[pos] === ']') { pos++; return result; }
        while (true) { require(result.length < 128); result.push(value(depth + 1)); whitespace(); const next = text[pos++]; if (next === ']') return result; require(next === ','); }
      }
      for (const [literal, result] of [['true', true], ['false', false], ['null', null]]) if (text.startsWith(literal, pos)) { pos += literal.length; return result; }
      const number = /^-?(?:0|[1-9][0-9]*)/u.exec(text.slice(pos)); require(number); pos += number[0].length; return Number(number[0]);
    }
    const result = value(0); whitespace(); require(pos === text.length); return safe(result, 'universal_policy_file_invalid', maximum);
  } catch { fail('universal_policy_file_invalid'); }
}
async function filesystemContext(options) {
  const input = safe(options); closed(input, ['shellOrigins'], ['ownerUid', 'fixtureRoot']);
  check(Array.isArray(input.shellOrigins) && input.shellOrigins.length <= 16 && new Set(input.shellOrigins).size === input.shellOrigins.length);
  for (const origin of input.shellOrigins) {
    let parsed; try { parsed = new URL(origin); } catch { fail(); }
    check(typeof origin === 'string' && origin.length <= 512 && parsed.protocol === 'https:' && parsed.origin === origin && !parsed.username && !parsed.password);
  }
  const fixtureRoot = input.fixtureRoot === undefined ? null : exactPath(input.fixtureRoot);
  if (fixtureRoot) {
    check(path.dirname(fixtureRoot) === await realpath(tmpdir()) && /^soty-universal-policy-/u.test(path.basename(fixtureRoot)), 'universal_policy_fixture_invalid');
    const stat = await lstat(fixtureRoot); check(stat.isDirectory() && !stat.isSymbolicLink() && await realpath(fixtureRoot) === fixtureRoot, 'universal_policy_fixture_invalid');
  }
  const ownerUid = input.ownerUid ?? (typeof process.getuid === 'function' ? process.getuid() : 0);
  check(Number.isSafeInteger(ownerUid) && ownerUid >= 0 && ownerUid <= 4294967294);
  check(process.platform === 'linux' || fixtureRoot !== null, 'universal_policy_posix_required');
  return { shellOrigins: input.shellOrigins, ownerUid, fixtureRoot, posix: process.platform === 'linux' };
}
async function inspectChain(source, context, { missingFinal = false } = {}) {
  if (context.fixtureRoot) check(under(context.fixtureRoot, source), 'universal_policy_fixture_invalid');
  let directory = path.dirname(source), chain = [];
  while (true) {
    chain.push(directory); check(chain.length <= 64, 'universal_policy_path_invalid');
    if (context.fixtureRoot ? directory === context.fixtureRoot : directory === path.parse(directory).root) break;
    const parent = path.dirname(directory); check(parent !== directory, 'universal_policy_path_invalid'); directory = parent;
  }
  const evidence = [];
  for (const entry of chain.reverse()) {
    const stat = await lstat(entry, { bigint: true });
    check(stat.isDirectory() && !stat.isSymbolicLink(), 'universal_policy_file_invalid');
    if (context.posix) check((Number(stat.mode) & 0o022) === 0 && [0, context.ownerUid].includes(Number(stat.uid)), 'universal_policy_file_permissions');
    evidence.push([entry, String(stat.dev), String(stat.ino), String(stat.mode), String(stat.uid), String(stat.gid)]);
  }
  check(await realpath(missingFinal ? path.dirname(source) : source) === (missingFinal ? path.dirname(source) : source), 'universal_policy_file_invalid'); return evidence;
}
function inspectStat(stat, visibility, context, maximum = MAX_FILE) {
  check(stat.isFile() && !stat.isSymbolicLink() && stat.nlink === 1n && stat.size > 0n && stat.size <= BigInt(maximum), 'universal_policy_file_invalid');
  if (context.posix) {
    const mode = Number(stat.mode);
    check(Number(stat.uid) === context.ownerUid && (mode & 0o400) !== 0 && (mode & 0o111) === 0
      && (mode & (visibility === 'private' ? 0o077 : 0o022)) === 0, 'universal_policy_file_permissions');
  }
}
async function readBounded(source, visibility, context, maximum = MAX_FILE) {
  let descriptor, bytes;
  try {
    const chain = await inspectChain(source, context), before = await lstat(source, { bigint: true }); inspectStat(before, visibility, context, maximum);
    descriptor = await open(source, constants.O_RDONLY | (constants.O_NOFOLLOW || 0) | (constants.O_NONBLOCK || 0));
    const opened = await descriptor.stat({ bigint: true }); inspectStat(opened, visibility, context, maximum);
    check(sameIdentity(identity(before), identity(opened)), 'universal_policy_file_changed');
    bytes = Buffer.alloc(maximum + 1); let length = 0;
    while (length < bytes.length) {
      const result = await descriptor.read(bytes, length, bytes.length - length, length);
      if (!result.bytesRead) break; length += result.bytesRead;
    }
    check(length > 0 && length <= maximum && BigInt(length) === opened.size, 'universal_policy_file_invalid');
    const after = await descriptor.stat({ bigint: true }), named = await lstat(source, { bigint: true });
    check(sameIdentity(identity(opened), identity(after)) && sameIdentity(identity(opened), identity(named))
      && JSON.stringify(chain) === JSON.stringify(await inspectChain(source, context)), 'universal_policy_file_changed');
    const captured = Buffer.from(bytes.subarray(0, length)); return { source, visibility, maximum, bytes: captured, identity: identity(opened), chain };
  } catch (error) { if (error instanceof SafeError) throw error; fail('universal_policy_file_invalid'); }
  finally { bytes?.fill(0); if (descriptor) await descriptor.close(); }
}
/** Local operator file transport only. Bytes stay private and disposable. */
export async function readUniversalLocalFile(source, options, { privateFile = false } = {}) {
  const context = await filesystemContext(options), file = await readBounded(source, privateFile ? 'private' : 'nonsecret', context);
  return { bytes: file.bytes, dispose() { file.bytes.fill(0); } };
}
export function parseUniversalLocalJson(bytes) { return parse(bytes); }
/** Exclusive atomic publication of an encrypted witness, never a plaintext backup. */
export async function writeUniversalWitnessExclusive(target, packet, options) {
  const context = await filesystemContext(options); exactPath(target);
  check(packet instanceof Uint8Array && packet.byteLength > 0 && packet.byteLength <= MAX_FILE, 'universal_policy_witness_invalid');
  const chain = await inspectChain(target, context, { missingFinal: true }), temporary = path.join(path.dirname(target), '.soty-witness-' + randomBytes(16).toString('hex') + '.tmp');
  let descriptor, published = false;
  try {
    descriptor = await open(temporary, constants.O_WRONLY | constants.O_CREAT | constants.O_EXCL | (constants.O_NOFOLLOW || 0), 0o600);
    await descriptor.writeFile(packet); await descriptor.sync(); await descriptor.close(); descriptor = null;
    check(JSON.stringify(chain) === JSON.stringify(await inspectChain(target, context, { missingFinal: true })), 'universal_policy_file_changed');
    const { link, unlink } = await import('node:fs/promises');
    await link(temporary, target); published = true; await unlink(temporary);
    const file = await readBounded(target, 'private', context);
    try { check(file.bytes.length === packet.byteLength && timingSafeEqual(file.bytes, packet), 'universal_policy_witness_invalid'); }
    finally { file.bytes.fill(0); }
    if (process.platform !== 'win32') { const parent = await open(path.dirname(target), constants.O_RDONLY); try { await parent.sync(); } finally { await parent.close(); } }
  } catch { fail(published ? 'universal_policy_witness_write_uncertain' : 'universal_policy_witness_exists_or_invalid'); }
  finally {
    if (descriptor) await descriptor.close();
    const { unlink } = await import('node:fs/promises'); await unlink(temporary).catch(() => {});
  }
}

function humanPreparation(profile) {
  return captureHumanPreparedness(profile);
}
function validateHuman(value, issuer, context) {
  closed(value, ['clients', 'jwks', 'cookieKeys', 'artifactKey', 'artifactKeyId'], ['renewal']);
  check(typeof value.artifactKey === 'string' && /^[A-Za-z0-9_-]{43}$/u.test(value.artifactKey), 'universal_policy_human_invalid');
  const artifactKey = Buffer.from(value.artifactKey, 'base64url');
  try {
    check(artifactKey.length === 32 && artifactKey.toString('base64url') === value.artifactKey, 'universal_policy_human_invalid');
    const profile = createHumanIdentityHostProfile({ enabled: true, issuer, registryId: 'soty', environmentId: 'production', ...value, artifactKey }, context);
    check(profile.secure && profile.publicClients.every(client => client.redirectUri.startsWith('https:')), 'universal_policy_human_invalid');
    return humanPreparation(profile);
  } catch { fail('universal_policy_human_invalid'); } finally { artifactKey.fill(0); }
}
function validateReviews(value) {
  let service;
  try {
    service = createReviewsService({ configuration: value, actorActive: () => false, withAppAuthority: () => null });
    return { configurationDigest: digest(value), providerCount: value.providers.length, bindingCount: value.bindings.length };
  } catch { fail('universal_policy_reviews_invalid'); } finally { service?.close(); }
}
function approvedNativeTransport(value) {
  const url = new URL(value);
  return url.protocol === 'https:' || url.protocol === 'http:'
    && ['localhost', '127.0.0.1', '[::1]'].includes(url.hostname)
    && /^\d{4,5}$/u.test(url.port) && Number(url.port) >= 1024 && Number(url.port) <= 65535;
}

/** Trusted local operator input only. Secret witnesses remain private, process-local
 * and disposable; a JSON receipt cannot reconstruct or authorize this handle. */
export async function prepareUniversalPolicy(input, options = { shellOrigins: [] }) {
  const files = []; let context;
  try {
    const plan = safe(input); closed(plan, ['schema', 'phase', 'human', 'reviews'], ['selected']);
    check(plan.schema === UNIVERSAL_POLICY_SCHEMA && ['legacy-baseline', 'features'].includes(plan.phase));
    check(plan.phase === 'features' || plan.human === null && plan.reviews === null && !plan.selected);
    context = await filesystemContext(options);
    let human = { configured: false }, reviews = validateReviews({ providers: [], bindings: [] });
    if (plan.human !== null) {
      closed(plan.human, ['issuer', 'source']); exactPath(plan.human.source);
      check(typeof plan.human.issuer === 'string' && plan.human.issuer.length <= 1024);
      const file = await readBounded(plan.human.source, 'private', context); file.target = humanPrivateTarget; files.push(file);
      human = validateHuman(parse(file.bytes), plan.human.issuer, context);
    }
    if (plan.reviews !== null) {
      closed(plan.reviews, ['source', 'sha256']); exactPath(plan.reviews.source); check(typeof plan.reviews.sha256 === 'string' && HEX.test(plan.reviews.sha256));
      check(plan.reviews.source !== plan.human?.source, 'universal_policy_file_invalid');
      const file = await readBounded(plan.reviews.source, 'nonsecret', context); file.target = reviewsTarget; files.push(file);
      reviews = validateReviews(parse(file.bytes));
      check(createHash('sha256').update(file.bytes).digest('hex') === plan.reviews.sha256, 'universal_policy_reviews_hash_mismatch');
    }
    let selected;
    if (plan.selected !== undefined && plan.selected !== null) {
      closed(plan.selected, ['source', 'migrationConfigured']); exactPath(plan.selected.source);
      check(typeof plan.selected.migrationConfigured === 'boolean' && !files.some(file => file.source === plan.selected.source));
      const file = await readBounded(plan.selected.source, 'private', context, 131072); file.target = selectedTarget; files.push(file);
      const registry = parse(file.bytes, 131072); closed(registry, ['schema', 'profiles']);
      check(registry.schema === 'soty.selected-embed-registry.v1');
      selected = captureSelectedPreparedness({ profiles: registry.profiles, migrationConfigured: plan.selected.migrationConfigured });
      if (selected.configured) {
        check(human.configured, 'universal_policy_selected_human_required');
        const clients = parse(files.find(value => value.target === humanPrivateTarget).bytes).clients;
        for (const profile of registry.profiles) check(profile.issuer === human.issuer && context.shellOrigins.includes(profile.parentOrigin)
          && profile.embedOrigin.startsWith('https:') && approvedNativeTransport(profile.nativeOrigin)
          && clients.some(client => client.id === profile.clientId && client.redirectUri === profile.embedOrigin + '/api/embed/callback'),
        'universal_policy_selected_human_required');
      }
    }
    const environment = [{ name: settings.operator, value: '1' }, ...(plan.phase === 'legacy-baseline' ? [] : [
      { name: settings.universal, value: 'true' }, { name: settings.human, value: human.configured ? '1' : '0' },
      ...(human.configured ? [{ name: settings.issuer, value: human.issuer }, { name: settings.keys, value: humanPrivateTarget }] : []),
      ...(plan.reviews ? [{ name: settings.reviews, value: reviewsTarget }] : []),
      ...(selected ? [{ name: settings.selected, value: selectedTarget },
        { name: settings.selectedMigration, value: selected.migrationConfigured ? '1' : '0' }] : []),
    ])];
    const mounts = files.map(file => ({ source: file.source, target: file.target, readOnly: true, visibility: file.visibility }));
    const publicValue = { schema: UNIVERSAL_POLICY_SCHEMA, phase: plan.phase, fixtureOnly: context.fixtureRoot !== null, environment, mounts,
      human, reviews: { ...reviews, ...(plan.reviews ? { fileSha256: plan.reviews.sha256 } : {}) }, ...(selected ? { selected } : {}) };
    const receipt = freeze({ ...publicValue, policyDigest: digest(publicValue) }), handle = Object.freeze(Object.create(null));
    handles.set(handle, { active: true, context, files, receipt }); return handle;
  } catch (error) { files.forEach(file => file.bytes.fill(0)); if (error instanceof SafeError) throw error; fail(); }
}
export function publicUniversalPolicy(handle) { return stateFor(handle).receipt; }
export function disposeUniversalPolicy(handle) { const state = stateFor(handle); forget(state); }
function rolloutBinding(input) {
  const value = safe(input); closed(value, ['preservationFingerprint', 'originalId', 'originalImage', 'candidateImage', 'revision', 'transaction']);
  check(HEX.test(value.preservationFingerprint || '') && HEX.test(value.originalId || '')
    && /^sha256:[a-f0-9]{64}$/u.test(value.originalImage || '') && /^sha256:[a-f0-9]{64}$/u.test(value.candidateImage || '')
    && /^[a-f0-9]{40}$/u.test(value.revision || '') && /^[a-f0-9]{16,40}$/u.test(value.transaction || ''), 'universal_policy_rollout_mismatch');
  return freeze(value);
}
/** Private one-time binding. Never emit this fingerprint in a receipt/journal. */
export function bindUniversalRollout(handle, input) {
  const state = stateFor(handle), captured = rolloutBinding(input);
  if (state.rolloutBinding) check(canonicalJson(state.rolloutBinding) === canonicalJson(captured), 'universal_policy_rollout_mismatch');
  else { check(!state.restored, 'universal_policy_rollout_mismatch'); state.rolloutBinding = captured; }
}
export async function assertUniversalPolicyCurrent(handle) {
  const state = stateFor(handle);
  try {
    for (const file of state.files) {
      const current = await readBounded(file.source, file.visibility, state.context, file.maximum);
      try {
        check(state.active && sameIdentity(file.identity, current.identity) && JSON.stringify(file.chain) === JSON.stringify(current.chain)
          && file.bytes.length === current.bytes.length && timingSafeEqual(file.bytes, current.bytes), 'universal_policy_file_changed');
      } finally { current.bytes.fill(0); }
    }
    check(state.active, 'universal_policy_handle_invalid'); return state.receipt;
  } catch (error) { forget(state); if (error instanceof SafeError) throw error; fail('universal_policy_file_invalid'); }
}

function witnessKey(input, restore = false) {
  check(input && typeof input === 'object' && [Object.prototype, null].includes(Object.getPrototypeOf(input)), 'universal_policy_witness_invalid');
  const fields = Object.getOwnPropertyDescriptors(input), required = ['key', 'keyId', ...(restore ? ['expectedWitnessId'] : [])];
  check(Reflect.ownKeys(fields).length === required.length && required.every(name => fields[name]?.enumerable && 'value' in fields[name]), 'universal_policy_witness_invalid');
  const key = fields.key.value, keyId = fields.keyId.value, expectedWitnessId = fields.expectedWitnessId?.value;
  check(key instanceof Uint8Array && key.byteLength === 32 && typeof keyId === 'string' && /^[A-Za-z0-9_-]{1,64}$/u.test(keyId)
    && (!restore || typeof expectedWitnessId === 'string' && /^[A-Za-z0-9_-]{32}$/u.test(expectedWitnessId)), 'universal_policy_witness_invalid');
  return { key: Buffer.from(key), keyId, expectedWitnessId };
}
function privateWitness(state) {
  return { schema: 'soty.universal-policy-private-witness.v1', contextDigest: digest(state.context),
    ...(state.rolloutBinding ? { rolloutBinding: state.rolloutBinding } : {}),
    files: state.files.map(file => ({ source: file.source, visibility: file.visibility, identity: file.identity,
      chainDigest: digest(file.chain), bytesDigest: createHash('sha256').update(file.bytes).digest('hex') })) };
}
const witnessMeta = packet => ({ schema: packet.schema, witnessId: packet.witnessId, keyId: packet.keyId, policyDigest: packet.policyDigest });
const witnessAad = packet => Buffer.from(canonicalJson(witnessMeta(packet)));
function encoded(value, length, maximum = length) {
  check(typeof value === 'string' && /^[A-Za-z0-9_-]+$/u.test(value), 'universal_policy_witness_invalid');
  const bytes = Buffer.from(value, 'base64url');
  check(bytes.length >= length && bytes.length <= maximum && bytes.toString('base64url') === value, 'universal_policy_witness_invalid'); return bytes;
}
/** Private encrypted operator artifact, never a public receipt or an HTTP input.
 * The operator supplies a separate custody key; this module generates no key. */
export async function sealUniversalPolicy(handle, input) {
  const custody = witnessKey(input); let plaintext;
  try {
    await assertUniversalPolicyCurrent(handle); const state = stateFor(handle);
    plaintext = Buffer.from(canonicalJson(privateWitness(state))); check(plaintext.length <= 32768, 'universal_policy_witness_invalid');
    const packet = { schema: 'soty.universal-policy-witness.v1', witnessId: randomBytes(24).toString('base64url'),
      keyId: custody.keyId, policyDigest: state.receipt.policyDigest };
    const nonce = randomBytes(12), cipher = createCipheriv('aes-256-gcm', custody.key, nonce); cipher.setAAD(witnessAad(packet));
    const encrypted = Buffer.concat([cipher.update(plaintext), cipher.final()]);
    const bytes = Buffer.from(JSON.stringify({ ...packet, nonce: nonce.toString('base64url'), ciphertext: encrypted.toString('base64url'), tag: cipher.getAuthTag().toString('base64url') }));
    check(bytes.length <= MAX_FILE, 'universal_policy_witness_invalid');
    return Object.freeze({ witnessId: packet.witnessId, policyDigest: packet.policyDigest, packet: bytes });
  } catch (error) { if (error instanceof SafeError) throw error; fail('universal_policy_witness_invalid'); }
  finally { custody.key.fill(0); plaintext?.fill(0); }
}
/** Reopens only an exact approved plan/file witness after a process restart.
 * expectedWitnessId must come from the reviewed receipt, not from the packet. */
export async function restoreUniversalPolicy(plan, input, options, keyInput) {
  const custody = witnessKey(keyInput, true); let plaintext, handle;
  try {
    check(input instanceof Uint8Array && input.byteLength > 0 && input.byteLength <= MAX_FILE, 'universal_policy_witness_invalid');
    const packet = parse(input); closed(packet, ['schema', 'witnessId', 'keyId', 'policyDigest', 'nonce', 'ciphertext', 'tag']);
    check(packet.schema === 'soty.universal-policy-witness.v1' && packet.witnessId === custody.expectedWitnessId && packet.keyId === custody.keyId
      && typeof packet.policyDigest === 'string' && HEX.test(packet.policyDigest), 'universal_policy_witness_invalid');
    const decipher = createDecipheriv('aes-256-gcm', custody.key, encoded(packet.nonce, 12));
    decipher.setAAD(witnessAad(packet)); decipher.setAuthTag(encoded(packet.tag, 16));
    plaintext = Buffer.concat([decipher.update(encoded(packet.ciphertext, 1, 32768)), decipher.final()]);
    const restored = parse(plaintext);
    handle = await prepareUniversalPolicy(plan, options); const state = stateFor(handle);
    check(packet.policyDigest === state.receipt.policyDigest, 'universal_policy_witness_mismatch');
    if (restored.rolloutBinding !== undefined) state.rolloutBinding = rolloutBinding(restored.rolloutBinding);
    state.restored = true;
    const actual = Buffer.from(canonicalJson(privateWitness(state))), expected = Buffer.from(canonicalJson(restored));
    try { check(actual.length === expected.length && timingSafeEqual(actual, expected), 'universal_policy_witness_mismatch'); }
    finally { actual.fill(0); expected.fill(0); }
    return handle;
  } catch (error) { if (handle && handles.get(handle)?.active) forget(handles.get(handle));
    if (error instanceof SafeError && error.code === 'universal_policy_witness_mismatch') throw error;
    fail('universal_policy_witness_invalid'); }
  finally { custody.key.fill(0); plaintext?.fill(0); }
}

const overlaps = (a, b) => a === b || a.startsWith(b.replace(/\/$/u, '') + '/') || b.startsWith(a.replace(/\/$/u, '') + '/');
/** Returns a clone; no engine calls, inherited secret output or original mutation. */
export function applyUniversalPolicy(config, handle) {
  const receipt = stateFor(handle).receipt; let next;
  try { next = structuredClone(config); } catch { fail('universal_policy_configuration_invalid'); }
  check(next && typeof next === 'object' && next.HostConfig && typeof next.HostConfig === 'object', 'universal_policy_configuration_invalid');
  check(next.Env === undefined || Array.isArray(next.Env) && next.Env.every(value => typeof value === 'string'), 'universal_policy_configuration_invalid');
  const expected = new Map(receipt.environment.map(({ name, value }) => [name, value]));
  for (const name of receipt.phase === 'legacy-baseline' ? [settings.operator] : Object.values(settings)) {
    check(!(next.Env || []).includes(name), 'universal_policy_preexisting_configuration');
    const matches = (next.Env || []).filter(value => value.startsWith(name + '='));
    check(matches.length <= 1 && (matches.length === 0 || expected.has(name) && matches[0] === name + '=' + expected.get(name)), 'universal_policy_preexisting_configuration');
  }
  const existing = [...(next.HostConfig.Mounts || [])];
  for (const bind of next.HostConfig.Binds || []) {
    const parsed = typeof bind === 'string' && bind.match(/^[^:]+:(\/[^:]*)(?::[^:]*)?$/u);
    check(parsed, 'universal_policy_configuration_invalid'); existing.push({ Target: parsed[1], legacyBind: true });
  }
  for (const target of Object.keys(next.Volumes || {})) existing.push({ Target: target, imageVolume: true });
  for (const mount of existing) check(typeof mount?.Target === 'string' && mount.Target.length <= 4096 && path.posix.isAbsolute(mount.Target)
    && !mount.Target.includes('\0'), 'universal_policy_configuration_invalid');
  for (const mount of receipt.mounts) {
    const approved = { Type: 'bind', Source: mount.source, Target: mount.target, ReadOnly: true };
    const conflicting = existing.filter(value => overlaps(path.posix.normalize(value.Target), mount.target));
    check(conflicting.length === 0 || conflicting.length === 1 && canonicalJson(conflicting[0]) === canonicalJson(approved), 'universal_policy_preexisting_configuration');
    if (conflicting.length === 0) { (next.HostConfig.Mounts ||= []).push(approved); existing.push(approved); }
  }
  next.Env ||= [];
  for (const { name, value } of receipt.environment) if (!next.Env.some(item => item.startsWith(name + '='))) next.Env.push(name + '=' + value);
  return next;
}

/** Image inspect objects are supplied by the existing exact-ID rollout fence.
 * Labels declare compatibility; actual cold boots remain a separate gate. */
export function assertUniversalImagePrerequisites(handle, { candidateImage, originalImage }) {
  const receipt = stateFor(handle).receipt;
  try {
    const candidate = storageReaders(candidateImage), original = storageReaders(originalImage);
    check(JSON.parse(candidateImage.Config.Labels[storageReaderLabel]).version === 5 && candidate.appRegistration.includes(1)
      && candidate.feedback.includes(1) && candidate.humanIdentity.includes(1), 'universal_policy_reader_baseline_required');
    check(candidateImage.Config.Labels[universalModeLabel] === (receipt.phase === 'legacy-baseline' ? '1' : '0'), 'universal_policy_image_mode_mismatch');
    if (receipt.human.renewal) check(candidate.humanIdentity.includes(2), 'universal_policy_reader_baseline_required');
    if (receipt.phase === 'features') check(JSON.parse(originalImage.Config.Labels[storageReaderLabel]).version === 5
      && original.appRegistration.includes(1) && original.feedback.includes(1) && original.humanIdentity.includes(1)
      && originalImage.Config.Labels[universalModeLabel] === '1', 'universal_policy_reader_baseline_required');
    if (receipt.phase === 'features' && receipt.human.renewal) check(original.humanIdentity.includes(2), 'universal_policy_reader_baseline_required');
    if (receipt.selected) check(candidate.apps.includes(7) && (receipt.phase !== 'features' || original.apps.includes(7)), 'universal_policy_reader_baseline_required');
    return freeze({ schema: 'soty.universal-image-prerequisites.v1', phase: receipt.phase,
      candidateImage: candidateImage.Id, originalImage: originalImage.Id, fixtureOnly: receipt.fixtureOnly });
  } catch (error) { if (error instanceof SafeError) throw error; fail('universal_policy_image_invalid'); }
}

/** Host factory hook only, after constructing actual services/HTTP. This is a
 * bounded measurement DTO, not network-provided authority or key attestation. */
export function captureUniversalPreparedness(input) {
  try { return captureRuntimePreparedness(input); } catch { fail('universal_policy_runtime_invalid'); }
}
export function assertUniversalPreparedness(handle, input, { allowFixture = false } = {}) {
  const expected = stateFor(handle).receipt, runtime = safe(input, 'universal_policy_runtime_invalid');
  check(typeof allowFixture === 'boolean' && (!expected.fixtureOnly || allowFixture), 'universal_policy_fixture_not_production');
  closed(runtime, ['schema', 'compiledLegacyMode', 'universalConfigured', 'reviewsConfigured', 'humanHttpEnabled', 'human', 'reviews'], ['selected']);
  check(runtime.schema === UNIVERSAL_RUNTIME_SCHEMA && [runtime.compiledLegacyMode, runtime.universalConfigured, runtime.reviewsConfigured, runtime.humanHttpEnabled]
    .every(value => typeof value === 'boolean'), 'universal_policy_runtime_invalid');
  const baseline = expected.phase === 'legacy-baseline';
  check(runtime.compiledLegacyMode === baseline && runtime.universalConfigured === !baseline && runtime.reviewsConfigured === !baseline
    && runtime.humanHttpEnabled === expected.human.configured && canonicalJson(runtime.human) === canonicalJson(expected.human)
    && canonicalJson(runtime.reviews) === canonicalJson({ configurationDigest: expected.reviews.configurationDigest,
      providerCount: expected.reviews.providerCount, bindingCount: expected.reviews.bindingCount }), 'universal_policy_runtime_mismatch');
  check(canonicalJson(runtime.selected ?? null) === canonicalJson(expected.selected ?? null), 'universal_policy_runtime_mismatch');
  return freeze({ ok: true, schema: UNIVERSAL_RUNTIME_SCHEMA, phase: expected.phase, policyDigest: expected.policyDigest, fixtureOnly: expected.fixtureOnly });
}
