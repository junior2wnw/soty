import assert from 'node:assert/strict';
import { generateKeyPairSync, randomBytes, sign } from 'node:crypto';
import { mkdtempSync, readFileSync, realpathSync, rmSync, writeFileSync } from 'node:fs';
import { tmpdir } from 'node:os';
import path from 'node:path';
import { fork } from 'node:child_process';
import { DatabaseSync } from 'node:sqlite';
import { createConnectService, digestArgs } from '../../../connect/server/index.mjs';
import { createNotesService } from '../../../notes/server/index.mjs';
import { createCapabilitiesService, BUILTIN_CAPABILITIES } from '../../server/index.mjs';

export const PROJECT = 'native-lifecycle-test', ORIGIN = 'https://native-lifecycle.test';
export const INPUT = Object.freeze({ title: 'Native source 😀', body: 'Exact е\u0301 / é / 中文\nprivate text' });
export const code = expected => error => error?.code === expected;
export function good(value) { assert.equal(value.ok, true, value.error?.code); return value; }
export function identity(label = 'Synthetic owner') {
  const signing = generateKeyPairSync('ec', { namedCurve: 'prime256v1' });
  const encryption = generateKeyPairSync('ec', { namedCurve: 'prime256v1' });
  return { label, privateKey: signing.privateKey, publicJwk: signing.publicKey.export({ format: 'jwk' }),
    encryptionPublicJwk: encryption.publicKey.export({ format: 'jwk' }) };
}
export async function signed(connect, owner, op, args = {}) {
  const challenge = good(await connect.handle({ op: 'challenge', args: { operation: op, digest: digestArgs(args) }, origin: ORIGIN }));
  return { op, args, origin: ORIGIN, proof: { challengeId: challenge.challengeId, publicJwk: owner.publicJwk,
    signature: sign('sha256', Buffer.from(challenge.message), { key: owner.privateKey, dsaEncoding: 'ieee-p1363' }).toString('base64url') } };
}

/** All three real domain stores. No synthetic actorActive or authority fence. */
export function openNativeStores({ directory, now = () => 1000, enabled = true, notesLimits = {}, nativeLimits = {},
  port: wrapPort = port => port, onVerify = () => {}, verify = (token, mode, check) => check(token, mode),
  fence: wrapFence = fence => fence, migrate = false, notesName = 'notes' } = {}) {
  let caps, connect;
  assert.match(notesName, /^notes(?:-other)?$/u);
  const notes = createNotesService({ databasePath: path.join(directory, `${notesName}.sqlite`), projectId: PROJECT,
    clock: now, allowNativeMigration: migrate, limits: notesLimits,
    verifyNativeContext(context, mode) { onVerify(context, mode); return verify(context, mode, (token, action) => caps.nativeNotes.verifyContext(token, action)); } });
  try {
    caps = createCapabilitiesService({ databasePath: path.join(directory, 'caps.sqlite'), projectId: PROJECT, clock: now,
      allowNativeMigration: migrate, actorActive: actor => connect.isActorActive(actor),
      catalog: BUILTIN_CAPABILITIES.map(entry => ({ ...entry, executionEnabled: enabled })), limits: { nativeNotes: nativeLimits },
      nativeNotes: { notes: wrapPort(notes.native), withAuthorityFence: wrapFence(action => connect.withAuthorityFence(action)) } });
    connect = createConnectService({ databasePath: path.join(directory, 'connect.sqlite'), projectId: PROJECT,
      allowedOrigins: [ORIGIN], clock: now, extensions: [notes, caps] });
  } catch (error) { caps?.close(); notes.close(); throw error; }
  let closed = false;
  return { notes, caps, connect, native: caps.nativeNotes,
    close() { if (!closed) { connect.close(); caps.close(); notes.close(); closed = true; } } };
}

export async function connectedFixture(t, initial = {}) {
  const parent = realpathSync(tmpdir()), directory = mkdtempSync(path.join(parent, 'soty-native-life-'));
  const ownership = randomBytes(16).toString('hex'); writeFileSync(path.join(directory, 'owned'), ownership, { flag: 'wx' });
  let time = 1000, options = initial, stores = openNativeStores({ directory, migrate: true, now: () => time, ...options });
  const peers = [];
  t.after(async () => {
    for (const peer of peers) await peer.kill();
    stores.close();
    const actual = realpathSync(directory);
    assert.equal(path.dirname(actual), parent); assert.match(path.basename(actual), /^soty-native-life-/u);
    assert.equal(readFileSync(path.join(actual, 'owned'), 'utf8'), ownership);
    rmSync(actual, { recursive: true });
  });
  const owner = identity();
  const call = async (op, args = {}, signer = owner) => good(await stores.connect.handle(await signed(stores.connect, signer, op, args)));
  const account = await call('bootstrap', { label: owner.label, encryptionPublicJwk: owner.encryptionPublicJwk });
  const access = (op, args = {}, signer = owner) => call(`access.${op}`, { expectedAccountId: account.accountId, ...args }, signer);
  async function issue({ budget = 20, expiresAt = 100000, credentialExpiresAt = expiresAt, principalId, grantId,
    allowDelegation = false, maxDepth = 0, signer = owner, accountId = account.accountId } = {}) {
    const scoped = (op, args) => call(`access.${op}`, { expectedAccountId: accountId, ...args }, signer);
    const principal = principalId ? { id: principalId } : (await scoped('principals.create', { label: 'Native caller' })).principal;
    const grant = grantId ? { id: grantId } : (await scoped('grants.issue', { principalId: principal.id,
      capabilities: [{ capabilityId: 'notes.createDraft', version: 1 }], resources: ['notes:new'], effects: ['create'],
      recipients: ['soty:notes'], expiresAt, allowDelegation, maxDepth, budget: { unit: 'invocations', limit: budget } })).grant;
    const credential = await scoped('credentials.issue', { grantId: grant.id, audience: ORIGIN, expiresAt: credentialExpiresAt });
    return { principal, grant, credential, actor: stores.caps.authenticateCredential({ token: credential.token, audience: ORIGIN }) };
  }
  return { directory, owner, account, call, access, issue,
    get stores() { return stores; }, get notes() { return stores.notes; }, get caps() { return stores.caps; },
    get native() { return stores.native; }, get connect() { return stores.connect; },
    time(value) { if (value !== undefined) time = value; return time; },
    restart(next = {}) { stores.close(); options = { ...options, ...next }; stores = openNativeStores({ directory, now: () => time, ...options }); },
    sql(name, action) { const db = new DatabaseSync(path.join(directory, `${name}.sqlite`)); try { return action(db); } finally { db.close(); } },
    actor(token) { return stores.caps.authenticateCredential({ token, audience: ORIGIN }); },
    async child(config) { const peer = await launchNativeChild({ directory, now: time, ...config }); peers.push(peer); return peer; },
    create(actor, input = INPUT, idempotencyKey = 'native_lifecycle_key') {
      const admitted = stores.native.admit({ actor, input, idempotencyKey });
      stores.native.beginAttempt({ invocationId: admitted.invocation.invocationId });
      return stores.native.execute({ invocationId: admitted.invocation.invocationId });
    },
  };
}

export async function launchNativeChild(config) {
  const child = fork(new URL('./native-effect-child.mjs', import.meta.url), [], { execPath: process.execPath, windowsHide: true,
    execArgv: [], stdio: ['ignore', 'ignore', 'pipe', 'ipc'] });
  const messages = [], waiters = []; let exited = false, failure = null, stderrBytes = 0;
  child.stderr.on('data', value => { stderrBytes += value.length; }); // Never print test token/input from a child exception.
  const exit = new Promise(resolve => child.once('close', (code, signal) => { exited = true;
    for (const waiter of waiters.splice(0)) waiter.reject(new Error(`native_child_exit:${code}:${signal}:${stderrBytes}`));
    resolve({ code, signal }); }));
  child.once('error', error => { failure = error; for (const waiter of waiters.splice(0)) waiter.reject(error); });
  child.on('message', message => {
    const index = waiters.findIndex(waiter => waiter.phase === message.phase);
    if (index < 0) messages.push(message); else waiters.splice(index, 1)[0].resolve(message);
  });
  const wait = phase => new Promise((resolve, reject) => {
    const index = messages.findIndex(message => message.phase === phase);
    if (index >= 0) return resolve(messages.splice(index, 1)[0]);
    if (failure || exited) return reject(failure || new Error('native_child_already_exited'));
    const timer = setTimeout(() => { const index = waiters.indexOf(waiter); if (index >= 0) waiters.splice(index, 1); reject(new Error('native_child_timeout:' + phase)); }, 10000);
    const waiter = { phase, resolve(value) { clearTimeout(timer); resolve(value); }, reject(error) { clearTimeout(timer); reject(error); } };
    waiters.push(waiter);
  });
  const ready = wait('ready'); child.send({ config });
  try { await ready; } catch (error) { if (!exited) child.kill('SIGKILL'); await exit; throw error; }
  return { child, wait, exit, start: () => child.send({ start: true }),
    async kill() { if (!exited) child.kill('SIGKILL'); await exit; } };
}
