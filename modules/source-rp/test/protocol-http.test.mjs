import test from 'node:test';
import assert from 'node:assert/strict';
import { join } from 'node:path';
import { mkdtempSync, rmSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { randomBytes } from 'node:crypto';
import { environment } from '../../human-identity/test/support/renewal-fixture.mjs';
import { createSourceRpProtocol, createSourceRpSessionService, SOURCE_RP_PROFILE } from '../server/index.mjs';
import { sqliteSource, digest } from './support/sqlite-source.mjs';
const random = () => randomBytes(32).toString('base64url');
function controlledClock(t) {
  const original = globalThis.Date; let offset = 0;
  class FixtureDate extends original { constructor(...args) { super(...(args.length ? args : [original.now() + offset])); }
    static now() { return original.now() + offset; } }
  globalThis.Date = FixtureDate; t.after(() => { globalThis.Date = original; }); return seconds => { offset += seconds * 1000; };
}
async function rootProtocol(t, { renewal = true } = {}) {
  const f = await environment(t, { renewal }); let client;
  await f.configureClients(clients => { client = clients[0]; return clients; });
  const protocol = createSourceRpProtocol({ issuer: f.issuer, clientId: client.id, clientSecret: client.clientSecret,
    redirectUri: client.redirectUri, ...(renewal ? { renewalProfile: SOURCE_RP_PROFILE } : {}) });
  async function login(long = false) {
    const intent = await protocol.start(), authorization = await f.wire.request(intent.location);
    assert.equal([302, 303].includes(authorization.status), true, 'actual Root authorize');
    const page = await f.wire.request(authorization.location); assert.equal(page.status, 200);
    const context = await f.wire.request(authorization.location.href + '/context'); assert.equal(context.status, 200);
    const flow = { rp: f.rps[0], session: f.wire, location: authorization.location, context: context.body };
    await f.approve(flow, f.actor, long ? { stayInAppSeconds: 86400 } : {});
    const finished = await f.complete(flow); return protocol.exchange(finished.callback, intent);
  }
  return { f, protocol, login };
}
test('maintained OIDC actual Root basic300 requires signed opt-in before it returns any RT', async t => {
  const { f, protocol, login } = await rootProtocol(t), basic = await login();
  assert.equal(typeof basic.accessToken, 'string'); assert.equal(basic.refreshToken, undefined);
  assert.equal(await protocol.currentSubject(basic.accessToken, basic.subject) === basic.subject, true);
  const long = await login(true); assert.equal(typeof long.refreshToken, 'string');
  assert.equal(long.refreshToken !== basic.refreshToken, true);
  await assert.rejects(protocol.currentSubject(long.accessToken, 'different-subject'), error => error.status === 401);
  assert.equal(f.identity.schemaVersion, 2);
});
test('actual Root RT+durable Source CAS survives >300s controlled clock/restart and stays finite24h', async t => {
  const advance = controlledClock(t), { f, protocol, login } = await rootProtocol(t), start = Date.now(), initial = await login(true);
  const directory = mkdtempSync(join(tmpdir(), 'source-rp-http-')), native = sqliteSource(join(directory, 'source.sqlite'));
  t.after(() => { native.close(); rmSync(directory, { recursive: true, force: true, maxRetries: 3, retryDelay: 100 }); });
  const marker = { sessionIdHash: digest(random()), profileDigest: digest('root-actual-profile'), bindingDigest: digest('native-selected-workspace'),
    issuer: f.issuer, subject: initial.subject, createdAt: start, sessionExpiresAt: start + 86400000 };
  await native.seed(marker, { accessToken: initial.accessToken, refreshToken: initial.refreshToken, nonce: initial.nonce }, initial.expiresAt);
  const options = { storagePort: native.storagePort, protocol, profileDigest: marker.profileDigest, keyId: native.keyId,
    encrypt: native.encrypt, decrypt: native.decrypt };
  await createSourceRpSessionService(options).currentProof(marker); advance(310); await f.restart();
  const renewed = await createSourceRpSessionService(options).currentProof(marker);
  assert.equal(renewed.sessionGeneration, 1); assert.equal(renewed.sessionExpiresAt, marker.sessionExpiresAt);
  assert.equal(f.identity.schemaVersion, 2); await renewed.assertCurrent();
  advance(86400); await assert.rejects(createSourceRpSessionService(options).currentProof(marker), error => error.status === 401);
});
test('actual Root revocation refuses Source currentProof without manufacturing a native grant', async t => {
  const { f, protocol, login } = await rootProtocol(t), proof = await login(true);
  const backup = await f.backupDevice(), original = await f.actor.client.getLocalState();
  await backup.client.revokeDevice(original.deviceId);
  await assert.rejects(protocol.currentSubject(proof.accessToken, proof.subject), error => error.status === 401);
  await assert.rejects(protocol.renew({ refreshToken: proof.refreshToken, nonce: proof.nonce, subject: proof.subject }), error => error.status === 401);
});
test('host-only protocol rejects unreviewed URLs/fields; cold discovery outage remains 503', async () => {
  const profile = { issuer: 'http://127.0.0.1:1/human-identity', redirectUri: 'http://127.0.0.1:2/callback', clientId: 'fixture', clientSecret: random() };
  assert.throws(() => createSourceRpProtocol({ ...profile, arbitraryUrl: 'https://elsewhere.example' }));
  assert.throws(() => createSourceRpProtocol({ ...profile, issuer: 'http://private.example/human-identity' }));
  await assert.rejects(createSourceRpProtocol(profile).ready(), error => error.status === 503);
});

test('actual maintained Root RT proactively replaces AT130 for trusted minimum190 without extending Source lifetime',async t=>{
  const advance=controlledClock(t),{f,protocol,login}=await rootProtocol(t),start=Date.now(),initial=await login(true);
  const directory=mkdtempSync(join(tmpdir(),'source-rp-minimum-http-')),native=sqliteSource(join(directory,'source.sqlite'));
  t.after(()=>{native.close();rmSync(directory,{recursive:true,force:true,maxRetries:3,retryDelay:100});});
  const marker={sessionIdHash:digest(random()),profileDigest:digest('actual-profile-minimum'),bindingDigest:digest('native-selected-one'),
    issuer:f.issuer,subject:initial.subject,createdAt:start,sessionExpiresAt:start+86400000};
  await native.seed(marker,{accessToken:initial.accessToken,refreshToken:initial.refreshToken,nonce:initial.nonce},initial.expiresAt);
  const options={storagePort:native.storagePort,protocol,profileDigest:marker.profileDigest,keyId:native.keyId,encrypt:native.encrypt,decrypt:native.decrypt};
  advance(170);const ordinary=await createSourceRpSessionService(options).currentProof(marker);assert.equal(ordinary.sessionGeneration,0);
  const proof=await createSourceRpSessionService(options).currentProof(marker,{minimumAccessRemainingMs:190000});
  assert.equal(proof.sessionGeneration,1);assert.ok(proof.expiresAt-Date.now()>190000);assert.equal(proof.sessionExpiresAt,marker.sessionExpiresAt);
  await proof.assertCurrent();
});
