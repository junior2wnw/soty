import assert from 'node:assert/strict';
import { generateKeyPairSync, randomBytes, sign } from 'node:crypto';
import { DatabaseSync } from 'node:sqlite';
import { mkdtempSync, realpathSync, rmSync } from 'node:fs';
import { tmpdir } from 'node:os';
import path from 'node:path';
import { createConnectService, digestArgs } from '../../../connect/server/index.mjs';
import { createCapabilitiesService, ACCESS_OPERATIONS, OAUTH_OPERATIONS } from '../../server/index.mjs';
import { initializeCapabilitiesSchema } from '../../server/schema.mjs';
import { config, interaction, id, ORIGIN, NOW } from './oauth-artifacts.mjs';

export { id, ORIGIN, NOW };
export const PROJECT = 'oauth-connections-tests';
export const code = expected => error => error?.code === expected;
export const good = value => { assert.equal(value.ok, true, value.error?.code); return value; };
function signingIdentity() {
  const signer = generateKeyPairSync('ec', { namedCurve: 'prime256v1' }), encryption = generateKeyPairSync('ec', { namedCurve: 'prime256v1' });
  return { privateKey: signer.privateKey, publicJwk: signer.publicKey.export({ format: 'jwk' }),
    encryptionPublicJwk: encryption.publicKey.export({ format: 'jwk' }) };
}
export async function connectionsFixture(t, initial = {}) {
  const parent = realpathSync(tmpdir()), directory = mkdtempSync(path.join(parent, 'soty-oauth-connections-'));
  const files = { connect: path.join(directory, 'connect.sqlite'), caps: path.join(directory, 'caps.sqlite') };
  let time = NOW, caps, settings = initial, afterOwner = null, afterFence = null;
  const handles = new Set();
  const db = new DatabaseSync(files.caps); handles.add(db);
  initializeCapabilitiesSchema(db, { projectId: PROJECT, allowNativeMigration: true });
  initializeCapabilitiesSchema(db, { projectId: PROJECT, allowOAuthMigration: true });
  const connect = createConnectService({ databasePath: files.connect, projectId: PROJECT, allowedOrigins: [ORIGIN], clock: () => time,
    extensions: [{ operations: new Set([...ACCESS_OPERATIONS, ...OAUTH_OPERATIONS]), execute(request) {
      const result = caps.execute(request); afterOwner?.(request, result); return result;
    } }] });
  function open() {
    const noKey = config(); delete noKey.artifactKey; delete noKey.artifactKeyId;
    const configuration = settings.keyless ? noKey : config();
    caps = createCapabilitiesService({ databasePath: files.caps, projectId: PROJECT, clock: () => time,
      actorActive: actor => connect.isActorActive(actor), oauth: { ...configuration, withAuthorityFence(action) {
        const result = connect.withAuthorityFence(action); afterFence?.(); return result;
      } } });
  }
  open();
  t.after(() => {
    for (const handle of handles) handle.close();
    caps.close(); connect.close();
    const actual = realpathSync(directory); assert.equal(path.dirname(actual), parent);
    assert.match(path.basename(actual), /^soty-oauth-connections-/u); rmSync(actual, { recursive: true });
  });
  async function signed(identity, op, args = {}) {
    const challenge = good(await connect.handle({ op: 'challenge', args: { operation: op, digest: digestArgs(args) }, origin: ORIGIN }));
    return { op, args, origin: ORIGIN, proof: { publicJwk: identity.publicJwk, challengeId: challenge.challengeId,
      signature: sign('sha256', Buffer.from(challenge.message), { key: identity.privateKey, dsaEncoding: 'ieee-p1363' }).toString('base64url') } };
  }
  async function call(identity, op, args = {}) { return connect.handle(await signed(identity, op, args)); }
  async function account() {
    const identity = signingIdentity(), details = good(await call(identity, 'bootstrap', { label: 'OAuth test owner', encryptionPublicJwk: identity.encryptionPublicJwk }));
    return { identity, ...details };
  }
  const owner = await account();
  const ownerCall = (op, args = {}, actor = owner) => call(actor.identity, op, { expectedAccountId: actor.accountId, ...args });
  function prepare(name = randomBytes(12).toString('hex'), options = {}) {
    const payload = interaction(name, time);
    caps.oauth.artifactStore.upsert({ model: 'Interaction', id: payload.jti, payload });
    const browserNonce = randomBytes(32).toString('base64url');
    const context = caps.oauth.prepareInteraction({ interactionId: payload.jti, browserNonce, ...options });
    return { payload, browserNonce, context,
      args: { interactionId: payload.jti, browserNonce, contextDigest: context.contextDigest },
      bindingArgs: { interactionId: payload.jti, browserNonce } };
  }
  async function approve(prepared = prepare(), actor = owner) {
    const decision = good(await ownerCall('oauth.connections.approve', prepared.args, actor));
    return { ...prepared, decision };
  }
  async function enroll() {
    const identity = signingIdentity();
    const start = good(await call(identity, 'enrollment.start', { label: 'Second OAuth owner device', encryptionPublicJwk: identity.encryptionPublicJwk }));
    good(await call(owner.identity, 'enrollment.approve', { requestId: start.requestId, wrappedKey: { schema: 'test.encrypted', ciphertext: 'test-only' } }));
    return { identity, ...good(await call(identity, 'enrollment.finish', { requestId: start.requestId, expectedAccountId: owner.accountId })) };
  }
  function grant(binding, name = randomBytes(12).toString('hex')) {
    return { kind: 'Grant', jti: id(name), iat: Math.floor(time / 1000), exp: Math.floor(binding.connection.expiresAt / 1000),
      accountId: binding.connection.accountId, clientId: binding.connection.staticClientId,
      resources: { [binding.connection.resource]: 'notes.createDraft' } };
  }
  function save(binding, payload = grant(binding)) {
    caps.oauth.artifactStore.upsert({ model: 'Grant', id: payload.jti, payload, stagedGrant: binding.context }); return payload;
  }
  return { get caps() { return caps; }, get oauth() { return caps.oauth; }, connect, db, files, owner, ownerCall, call, signed, account, enroll,
    prepare, approve, grant, save, now: () => time, advance(ms) { time += ms; },
    afterOwner(fn) { afterOwner = fn; }, afterFence(fn) { afterFence = fn; },
    reopen(next = {}) { caps.close(); settings = { ...settings, ...next }; open(); },
    openDb() { const handle = new DatabaseSync(files.caps); handles.add(handle); return handle; } };
}
