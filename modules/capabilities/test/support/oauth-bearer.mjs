import assert from 'node:assert/strict';
import { generateKeyPairSync, randomBytes, sign } from 'node:crypto';
import { mkdtempSync, realpathSync, rmSync } from 'node:fs';
import { tmpdir } from 'node:os';
import path from 'node:path';
import { DatabaseSync } from 'node:sqlite';
import { fork } from 'node:child_process';
import { createConnectService, digestArgs } from '../../../connect/server/index.mjs';
import { createNotesService, NOTES_OPERATIONS } from '../../../notes/server/index.mjs';
import { createCapabilitiesService, ACCESS_OPERATIONS, OAUTH_OPERATIONS, BUILTIN_CAPABILITIES } from '../../server/index.mjs';
import { initializeCapabilitiesSchema } from '../../server/schema.mjs';
import { config, interaction, id, NOW, ORIGIN, sha } from './oauth-artifacts.mjs';

export { ORIGIN, sha };
export const PROJECT = 'oauth-bearer-tests';
export const INPUT = Object.freeze({ title: 'OAuth original note', body: 'Личный текст 😀 e\u0301' });
export const good = result => { assert.equal(result.ok, true, result.error?.code); return result; };
export const code = expected => error => error?.code === expected;
const opaque = () => randomBytes(32).toString('base64url');
function identity() {
  const pair = generateKeyPairSync('ec', { namedCurve: 'prime256v1' });
  const encryption = generateKeyPairSync('ec', { namedCurve: 'prime256v1' });
  return { privateKey: pair.privateKey, publicJwk: pair.publicKey.export({ format: 'jwk' }),
    encryptionPublicJwk: encryption.publicKey.export({ format: 'jwk' }) };
}
export function openBearerStores({ directory, now, migrate = false, options = {} }) {
  const files = Object.fromEntries(['caps', 'connect', 'notes'].map(name => [name, path.join(directory, name + '.sqlite')]));
  if (migrate) {
    const db = new DatabaseSync(files.caps);
    try {
      initializeCapabilitiesSchema(db, { projectId: PROJECT, allowNativeMigration: true });
      initializeCapabilitiesSchema(db, { projectId: PROJECT, allowOAuthMigration: true });
    } finally { db.close(); }
  }
  let caps, notes;
  const connect = createConnectService({ databasePath: files.connect, projectId: PROJECT, allowedOrigins: [ORIGIN], clock: now,
    extensions: [{ operations: new Set([...ACCESS_OPERATIONS, ...OAUTH_OPERATIONS, 'access.invocations.list']), execute: args => caps.execute(args) },
      { operations: new Set(NOTES_OPERATIONS), execute: args => notes.execute(args) }] });
  try {
    notes = createNotesService({ databasePath: files.notes, projectId: PROJECT, clock: now,
      allowNativeMigration: migrate && !options.notesV1,
      verifyNativeContext: (token, mode) => caps.nativeNotes.verifyContext(token, mode) });
    const oauth = config({ isRegisteredRedirect({ clientId, redirectUri }) {
      const callback = clientId === 'soty-codex-cli' ? '/callback' : clientId === 'soty-opencode-cli' ? '/mcp/oauth/callback' : null;
      return callback !== null && redirectUri === 'http://127.0.0.1:19876' + callback;
    }, withAuthorityFence: action => connect.withAuthorityFence(action) });
    if (options.keyless) { delete oauth.artifactKey; delete oauth.artifactKeyId; }
    caps = createCapabilitiesService({ databasePath: files.caps, projectId: PROJECT, clock: now,
      actorActive: actor => connect.isActorActive(actor),
      catalog: BUILTIN_CAPABILITIES.map(entry => ({ ...entry, executionEnabled: options.enabled ?? true })),
      ...(!options.noNative ? { nativeNotes: { notes: options.port ? options.port(notes.native) : notes.native,
        withAuthorityFence: action => connect.withAuthorityFence(action) } } : {}),
      ...(!options.noOAuth ? { oauth } : {}) });
  } catch (error) { caps?.close(); notes?.close(); connect.close(); throw error; }
  let closed = false;
  return { caps, notes, connect, files, close() {
    if (!closed) { caps.close(); notes.close(); connect.close(); closed = true; }
  } };
}

export async function bearerFixture(t, initial = {}) {
  const parent = realpathSync(tmpdir()), directory = mkdtempSync(path.join(parent, 'soty-oauth-bearer-'));
  let time = NOW, options = initial, stores = openBearerStores({ directory, now: () => time, migrate: true, options });
  const children = [];
  t.after(async () => {
    for (const child of children) await child.stop();
    stores.close(); const resolved = realpathSync(directory);
    assert.equal(path.dirname(resolved), parent); assert.match(path.basename(resolved), /^soty-oauth-bearer-/u);
    rmSync(resolved, { recursive: true });
  });
  async function call(signer, op, args = {}) {
    const challenge = good(await stores.connect.handle({ op: 'challenge', args: { operation: op, digest: digestArgs(args) }, origin: ORIGIN }));
    return stores.connect.handle({ op, args, origin: ORIGIN, proof: { publicJwk: signer.publicJwk, challengeId: challenge.challengeId,
      signature: sign('sha256', Buffer.from(challenge.message), { key: signer.privateKey, dsaEncoding: 'ieee-p1363' }).toString('base64url') } });
  }
  async function account() {
    const signer = identity(); const result = good(await call(signer, 'bootstrap', { label: 'OAuth owner', encryptionPublicJwk: signer.encryptionPublicJwk }));
    return { signer, ...result };
  }
  const owner = await account();
  const ownerCall = (op, args = {}, actor = owner) => call(actor.signer, op, { expectedAccountId: actor.accountId, ...args });
  function sql(name, action) { const db = new DatabaseSync(stores.files[name]); try { return action(db); } finally { db.close(); } }
  function saveToken(own, model, overrides = {}, grantType = 'authorization_code') {
    const iat = Math.floor(time / 1000), resource = own.resource;
    const payload = { kind: model, jti: opaque(), iat, exp: Math.min(iat + (model === 'AccessToken' ? 300 : 86400), own.grant.exp),
      accountId: own.accountId, clientId: own.staticClientId, grantId: own.grant.jti, scope: 'notes.createDraft', expiresWithSession: false,
      ...(model === 'AccessToken' ? { aud: resource, extra: {} } : { resource, iiat: iat, rotations: 0 }),
      gty: 'authorization_code', ...overrides };
    stores.caps.oauth.artifactStore.upsert({ model, id: payload.jti, payload,
      request: { clientId: own.staticClientId, resource, scope: 'notes.createDraft', grantType } });
    return payload;
  }
  async function family({ actor = owner, clientProfile = 'soty-codex-cli', resource = ORIGIN + '/mcp', durationMs = 86400000 } = {}) {
    const payload = interaction(opaque(), time), browserNonce = opaque();
    payload.params.client_id = clientProfile; payload.params.resource = resource;
    payload.params.redirect_uri = 'http://127.0.0.1:19876' + (clientProfile === 'soty-codex-cli' ? '/callback' : '/mcp/oauth/callback');
    const oauth = stores.caps.oauth; oauth.artifactStore.upsert({ model: 'Interaction', id: payload.jti, payload });
    const context = oauth.prepareInteraction({ interactionId: payload.jti, browserNonce, durationMs });
    good(await ownerCall('oauth.connections.approve', { interactionId: payload.jti, browserNonce, contextDigest: context.contextDigest }, actor));
    const binding = oauth.beginGrantBinding({ interactionId: payload.jti, browserNonce }), own = { ...binding.connection };
    own.grant = { kind: 'Grant', jti: id(opaque()), accountId: own.accountId, clientId: own.staticClientId,
      iat: Math.floor(time / 1000), exp: Math.floor(own.expiresAt / 1000), resources: { [resource]: 'notes.createDraft' } };
    oauth.artifactStore.upsert({ model: 'Grant', id: own.grant.jti, payload: own.grant, stagedGrant: binding.context });
    oauth.endGrantBinding(binding.context);
    own.rt = saveToken(own, 'RefreshToken'); own.at = saveToken(own, 'AccessToken'); return own;
  }
  function refresh(own) {
    const oauth = stores.caps.oauth;
    assert.deepEqual(oauth.artifactStore.consume({ model: 'RefreshToken', id: own.rt.jti,
      request: { clientId: own.staticClientId, resource: own.resource, scope: 'notes.createDraft', grantType: 'refresh_token' } }), { status: 'consumed' });
    const next = { ...own };
    next.rt = saveToken(own, 'RefreshToken', { iiat: own.rt.iiat, rotations: own.rt.rotations + 1, gty: 'authorization_code refresh_token' }, 'refresh_token');
    next.at = saveToken(own, 'AccessToken', { gty: 'authorization_code refresh_token' }, 'refresh_token'); return next;
  }
  async function enroll() {
    const signer = identity();
    const request = good(await call(signer, 'enrollment.start', { label: 'Another device', encryptionPublicJwk: signer.encryptionPublicJwk }));
    good(await call(owner.signer, 'enrollment.approve', { requestId: request.requestId, wrappedKey: { schema: 'fixture', ciphertext: 'test-only' } }));
    return { signer, ...good(await call(signer, 'enrollment.finish', { requestId: request.requestId, expectedAccountId: owner.accountId })) };
  }
  const f = { directory, owner, call, ownerCall, account, enroll, family, refresh, saveToken, sql,
    get caps() { return stores.caps; }, get notes() { return stores.notes; }, get native() { return stores.caps.nativeNotes; },
    get oauth() { return stores.caps.oauth; }, get connect() { return stores.connect; }, get files() { return stores.files; },
    now: () => time, advance(ms) { time += ms; },
    reopen(next = {}) { stores.close(); options = { ...options, ...next }; stores = openBearerStores({ directory, now: () => time, options }); },
    actor(own) { return stores.caps.oauth.authenticateBearer({ token: own.at.jti, audience: own.resource }); },
    credential(own) { return sql('caps', db => db.prepare('SELECT * FROM cap_oauth_credentials WHERE token_digest=?').get(sha(own.at.jti))); },
    create(actor, key = 'oauth_native_create', input = INPUT) {
      const admitted = stores.caps.nativeNotes.admit({ actor, idempotencyKey: key, input });
      stores.caps.nativeNotes.beginAttempt({ invocationId: admitted.invocation.invocationId });
      return stores.caps.nativeNotes.execute({ invocationId: admitted.invocation.invocationId });
    },
    async child(args) { const child = await launchChild({ directory, now: time, ...args }); children.push(child); return child; },
  };
  return f;
}

async function launchChild(args) {
  const child = fork(new URL('./oauth-bearer-worker.mjs', import.meta.url), [], { execPath: process.execPath, execArgv: [],
    windowsHide: true, stdio: ['ignore', 'ignore', 'ignore', 'ipc'] });
  let readyResolve, rejectReady, finishResolve, rejectFinish, outcome, exited = false;
  const ready = new Promise((resolve, reject) => { readyResolve = resolve; rejectReady = reject; });
  const finished = new Promise((resolve, reject) => { finishResolve = resolve; rejectFinish = reject; });
  void finished.catch(() => {});
  const timer = setTimeout(() => { child.kill(); const e = new Error('bearer_child_timeout'); rejectReady(e); rejectFinish(e); }, 10000);
  child.on('message', message => {
    if (message.ready) readyResolve();
    else if (message.result) outcome = message.result;
    else if (message.failure) { const e = new Error('bearer_child_' + message.failure); rejectReady(e); rejectFinish(e); }
  });
  child.on('error', error => { rejectReady(error); rejectFinish(error); });
  child.on('close', code => {
    exited = true; clearTimeout(timer);
    if (code !== 0 || !outcome) { const e = new Error('bearer_child_exit_' + code); rejectReady(e); rejectFinish(e); }
    else finishResolve(outcome);
  });
  child.send({ initialize: args });
  try { await ready; } catch (error) { child.kill(); await finished.catch(() => {}); throw error; }
  return { start: () => child.send({ run: true }), finished,
    async stop() { if (!exited) child.kill(); await finished.catch(() => {}); } };
}
