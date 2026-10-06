import test from 'node:test';
import assert from 'node:assert/strict';
import { DatabaseSync } from 'node:sqlite';
import { readFileSync } from 'node:fs';
import { environment } from './support/renewal-fixture.mjs';
import { verifyHumanIdToken } from './support/renewal-bff.mjs';
import { createHumanIdentityHostProfile, HumanIdentityError, HUMAN_RENEWAL_LIMITS } from '../profile.mjs';
import { createHumanIdentityService } from '../service.mjs';
import { assertBaselineHumanV1 } from './support/baseline-human-v1-reader.mjs';
import { createPrivateRpSessionStore } from './support/rp-session-store.mjs';
import { randomBytes } from 'node:crypto';
function clock(t) {
  const real = Date.now, initial = real(); let offset = 0; Date.now = () => initial + offset;
  t.after(() => { Date.now = real; }); return seconds => { offset += seconds*1000; };
}
const rows = (f, fn) => { const db=new DatabaseSync(f.identityPath,{readOnly:true});try{return fn(db);}finally{db.close();} };
async function longLogin(f, rp) { return f.login(rp, f.actor, f.wire, undefined, 86400); }

test('v1 default remains short, rejects RefreshToken model and does not silently migrate', async t => {
  const f=await environment(t,{renewal:false});await f.login();assert.equal(f.identity.schemaVersion,1);
  assert.equal(f.rps[0].verificationFixture().refreshToken,undefined);
  assert.throws(()=>f.identity.sdk.upsert({model:'RefreshToken',id:'x'.repeat(32),payload:{},expiresIn:86400}),/human_identity_artifact_invalid/u);
  await f.restart();assert.equal(rows(f,db=>db.prepare('PRAGMA user_version').get().user_version),1);
});

test('two real BFFs refresh after original AT expiry with shared exact subject and unchanged AT300/ID60', {timeout:20000}, async t => {
  const advance=clock(t),f=await environment(t);await longLogin(f,f.rps[0]);await longLogin(f,f.rps[1]);
  const old=f.rps.map(rp=>rp.verificationFixture());assert.equal(old.every(value=>typeof value.refreshToken==='string'),true);
  advance(310);
  const results=await Promise.all(f.rps.map(rp=>f.wire.request(rp.origin+'/me')));
  assert.equal(results.every(value=>value.status===200),true);assert.equal(results[0].body.identity.sub,results[1].body.identity.sub);
  assert.equal(results[0].body.identity.sub,f.actor.account.accountId);
  for(let index=0;index<f.rps.length;index++) {
    const rp=f.rps[index],tokens=rp.verificationFixture();assert.equal(rp.refreshCalls(),1);assert.equal(tokens.refreshToken!==old[index].refreshToken,true);
    const claims=await verifyHumanIdToken({token:tokens.idToken,...tokens,nonce:tokens.expectedNonce});assert.equal(claims.exp-claims.iat,60);
    assert.equal(JSON.stringify(claims.amr||[]).includes('passkey'),false);assert.equal(rp.existingLocalRow().balance,17);
  }
  assert.equal(rows(f,db=>db.prepare("SELECT max(expires_at-created_at) AS n FROM human_identity_artifacts WHERE model='AccessToken'").get().n)<=300,true);
});

test('stay0 under v2 produces no refresh token and no hidden token TTL extension', async t => {
  const advance=clock(t),f=await environment(t);await f.login();assert.equal(f.identity.schemaVersion,2);
  assert.equal(f.rps[0].verificationFixture().refreshToken,undefined);advance(310);
  assert.equal((await f.wire.request(f.rps[0].origin+'/me')).status,401);assert.equal(f.rps[0].refreshCalls(),0);
});

test('real renewal survives OP Session and original short Grant expiry without a new UI approval', async t => {
  const advance = clock(t), f = await environment(t); await longLogin(f, f.rps[0]);
  advance(4000); f.identity.compactExpiredRuntime();
  assert.equal((await f.wire.request(f.rps[0].origin + '/me')).status, 200); assert.equal(f.rps[0].refreshCalls(), 1);
  const tokens = f.rps[0].verificationFixture();
  assert.equal((await verifyHumanIdToken({ token: tokens.idToken, ...tokens, nonce: tokens.expectedNonce })).sub, f.actor.account.accountId);
});

test('rotated token replay revokes only its family; other RP stays usable', async t => {
  const advance=clock(t),f=await environment(t);await longLogin(f,f.rps[0]);await longLogin(f,f.rps[1]);
  const old=f.rps[0].verificationFixture().refreshToken;advance(310);
  assert.equal((await f.wire.request(f.rps[0].origin+'/me')).status,200);
  const repeated=await f.refresh(f.rps[0],old);assert.equal(repeated.status,400);
  assert.equal((await f.wire.request(f.rps[0].origin+'/me')).status,401);
  assert.equal((await f.wire.request(f.rps[1].origin+'/me')).status,200);
  assert.equal(rows(f,db=>db.prepare('SELECT count(*) AS n FROM human_identity_grant_bindings WHERE revoked_at IS NOT NULL').get().n),1);
});

test('simultaneous refresh of one token leaves no live winner after detected reuse; consumed tombstone survives', async t => {
  const f=await environment(t);await longLogin(f,f.rps[0]);const token=f.rps[0].verificationFixture().refreshToken;
  const responses=await Promise.all([f.refresh(f.rps[0],token),f.refresh(f.rps[0],token)]);assert.equal(responses.some(value=>value.status===400),true);
  for(const response of responses.filter(value=>value.status===200)) assert.notEqual((await f.wire.request(f.issuer+'/userinfo',{cookies:false,originHeader:null,headers:{authorization:'Bearer '+response.body.access_token}})).status,200);
  assert.equal(rows(f,db=>db.prepare("SELECT count(*) AS n FROM human_identity_artifacts WHERE model='RefreshToken' AND consumed_at IS NOT NULL").get().n),1);
  assert.equal(rows(f,db=>db.prepare('SELECT count(*) AS n FROM human_identity_grant_bindings WHERE revoked_at IS NOT NULL').get().n),1);
});

test('wrong client cannot refresh another family, removal of RP A leaves B pins/session intact', async t => {
  const advance=clock(t),f=await environment(t);await longLogin(f,f.rps[0]);await longLogin(f,f.rps[1]);
  const a=f.rps[0].verificationFixture().refreshToken;assert.equal((await f.refresh(f.rps[1],a)).status,400);
  assert.equal((await f.wire.request(f.rps[0].origin+'/me')).status,200);
  const before=rows(f,db=>db.prepare('SELECT generation FROM human_identity_client_heads WHERE client_id=?').get(f.rps[1].clientId).generation);
  await f.configureClients(clients=>clients.filter(client=>client.id!==f.rps[0].clientId));advance(310);
  assert.notEqual((await f.refresh(f.rps[0],a)).status,200);assert.equal((await f.wire.request(f.rps[1].origin+'/me')).status,200);
  assert.equal(rows(f,db=>db.prepare('SELECT generation FROM human_identity_client_heads WHERE client_id=?').get(f.rps[1].clientId).generation),before);
});

test('source Connect device revocation blocks both userinfo and refresh', async t => {
  const f=await environment(t);await longLogin(f,f.rps[0]);await longLogin(f,f.rps[1]);const tokens=f.rps.map(rp=>rp.verificationFixture());
  const backup=await f.backupDevice(),original=await f.actor.client.getLocalState();
  await backup.client.revokeDevice(original.deviceId);
  for(let i=0;i<f.rps.length;i++){assert.notEqual((await f.refresh(f.rps[i],tokens[i].refreshToken)).status,200);assert.equal((await f.wire.request(f.rps[i].origin+'/me')).status,401);}
});

test('absolute expiry does not slide; admission off preserves existing finite families and rejects new long choices', async t => {
  const advance=clock(t),f=await environment(t);await longLogin(f,f.rps[0]);const end=rows(f,db=>db.prepare('SELECT session_expires_at AS n FROM human_identity_grant_bindings WHERE stay_in_app_seconds>0').get().n);
  await f.admission(false);advance(310);assert.equal((await f.wire.request(f.rps[0].origin+'/me')).status,200);
  const flow=await f.begin(f.rps[1]);await assert.rejects(f.approve(flow,f.actor,{stayInAppSeconds:86400}),/human_identity_renewal_disabled/u);
  advance(86090);assert.equal((await f.wire.request(f.rps[0].origin+'/me')).status,401);
  assert.equal(rows(f,db=>db.prepare('SELECT session_expires_at AS n FROM human_identity_grant_bindings WHERE stay_in_app_seconds>0').get().n),end);
});

test('RP sessions survive actual SQLite reopen encrypted; singleflight serves two requests with one refresh', async t => {
  const advance=clock(t),f=await environment(t);await longLogin(f,f.rps[0]);const token=f.rps[0].verificationFixture().refreshToken;
  await f.restart();f.rps[0].reopenSessions();advance(310);
  const responses=await Promise.all([f.wire.request(f.rps[0].origin+'/me'),f.wire.request(f.rps[0].origin+'/me')]);assert.equal(responses.every(value=>value.status===200),true);assert.equal(f.rps[0].refreshCalls(),1);
  assert.equal(readFileSync(f.directory+'/rp-0.sqlite').includes(Buffer.from(token)),false);
});

test('unknown refresh ACK persists blocked CAS state across RP restart and never retries the old token', async t => {
  const advance=clock(t),f=await environment(t);await longLogin(f,f.rps[0]);await longLogin(f,f.rps[1]);
  const actualFetch=globalThis.fetch;let lost=0;
  globalThis.fetch=async (url,options)=>{const response=await actualFetch(url,options);
    if(String(url)===f.issuer+'/token'&&options?.body?.get?.('grant_type')==='refresh_token'&&lost===0){lost++;await response.arrayBuffer();throw new Error('fixture_unknown_refresh_ack');}return response;};
  try {advance(310);assert.equal((await f.wire.request(f.rps[0].origin+'/me')).status,401);f.rps[0].reopenSessions();
    assert.equal((await f.wire.request(f.rps[0].origin+'/me')).status,401);assert.equal(f.rps[0].refreshCalls(),1);assert.equal(lost,1);
  } finally {globalThis.fetch=actualFetch;}
  assert.equal((await f.wire.request(f.rps[1].origin+'/me')).status,200);
});

test('two independent SQLite handles cannot both refresh one RP session; unknown lease remains blocked after reopen', async t => {
  const f=await environment(t),path=f.directory+'/cas-fixture.sqlite',key=randomBytes(32),sid=randomBytes(32).toString('base64url');
  let a=createPrivateRpSessionStore({databasePath:path,key,clientId:'fixture-rp'}),b=createPrivateRpSessionStore({databasePath:path,key,clientId:'fixture-rp'});
  try {a.create(sid,{refreshToken:'fixture-private-value',claims:{iss:f.issuer,aud:'fixture-rp'}});const x=a.read(sid),y=b.read(sid);
    const lease=a.beginRefresh(sid,x.revision);assert.throws(()=>b.beginRefresh(sid,y.revision),/rp_session_conflict/u);
    a.close();a=createPrivateRpSessionStore({databasePath:path,key,clientId:'fixture-rp'});assert.equal(a.read(sid).state,'refreshing');
    assert.throws(()=>a.beginRefresh(sid,a.read(sid).revision),/rp_session_conflict/u);b.failRefresh(sid,lease.lease,lease.revision);assert.equal(a.read(sid).state,'blocked');
  }finally{a.close();b.close();}
});

test('consumed RT tombstone/approval stays while family live; bounded GC removes only expired runtime and keeps client history', async t => {
  const advance=clock(t),f=await environment(t);await longLogin(f,f.rps[0]);advance(310);assert.equal((await f.wire.request(f.rps[0].origin+'/me')).status,200);
  advance(4000);f.identity.compactExpiredRuntime();assert.equal(rows(f,db=>db.prepare("SELECT count(*) AS n FROM human_identity_artifacts WHERE model='RefreshToken' AND consumed_at IS NOT NULL").get().n),1);
  assert.equal(rows(f,db=>db.prepare('SELECT count(*) AS n FROM human_identity_grant_bindings').get().n),1);
  advance(82400);const compact=f.identity.compactExpiredRuntime();assert.equal(Object.values(compact).every(value=>value<=128),true);
  assert.equal(rows(f,db=>db.prepare("SELECT count(*) AS n FROM human_identity_artifacts WHERE model='RefreshToken'").get().n),0);
  assert.equal(rows(f,db=>db.prepare('SELECT count(*) AS n FROM human_identity_client_versions').get().n),2);
});

test('v1→v2 requires explicit migration, preserves old ciphertext/other client pins and rejects v1 reader after upgrade', async t => {
  const f=await environment(t,{renewal:false});await f.login(f.rps[1]);const old=f.profile,keys=old.providerKeys(),encryption=old.encryptionKey();
  const options={enabled:true,issuer:old.issuer,registryId:old.registryId,environmentId:old.environmentId,
    clients:old.providerClients().map(client=>({id:client.client_id,label:old.client(client.client_id).label,redirectUri:client.redirect_uris[0],clientSecret:client.client_secret,
      version:client.client_id===f.rps[0].clientId?2:1})),jwks:keys.jwks,cookieKeys:keys.cookieKeys,artifactKey:encryption.key,artifactKeyId:encryption.keyId,
    renewal:{admissionEnabled:true,clientIds:[f.rps[0].clientId]}};
  const profile=createHumanIdentityHostProfile(options,{shellOrigins:[f.origin]}),configuration={databasePath:f.identityPath,profile,
    actorActive:actor=>f.connect.isActorActive(actor),withAuthorityFence:callback=>f.connect.withAuthorityFence(callback)};
  f.identity.close();const snapshot=rows(f,db=>db.prepare('SELECT id_hash,payload_cipher FROM human_identity_artifacts ORDER BY id_hash').all());
  assert.throws(()=>createHumanIdentityService(configuration),/human_identity_renewal_migration_required/u);assert.equal(rows(f,db=>db.prepare('PRAGMA user_version').get().user_version),1);
  const updated=createHumanIdentityService({...configuration,allowRenewalMigration:true});
  try {assert.equal(updated.schemaVersion,2);const after=rows(f,db=>db.prepare('SELECT id_hash,payload_cipher FROM human_identity_artifacts ORDER BY id_hash').all());
    assert.equal(snapshot.length,after.length);assert.equal(snapshot.every((row,i)=>row.id_hash===after[i].id_hash&&Buffer.from(row.payload_cipher).equals(Buffer.from(after[i].payload_cipher))),true);
    assert.equal(updated.sdk.find('AccessToken',f.rps[1].verificationFixture().accessToken).accountId,f.actor.account.accountId);
    assert.throws(()=>assertBaselineHumanV1(f.identityPath),/human_identity_storage_unknown/u);
  }finally{updated.close();}
});

test('signed choice cannot be widened by replay/new request ID, and malformed choice has no long family', async t => {
  const f=await environment(t),flow=await f.begin(),requestId='immutable-stay-choice';await f.approve(flow,f.actor,{requestId});
  await assert.rejects(f.approve(flow,f.actor,{requestId,stayInAppSeconds:86400}),/human_identity_intent_conflict/u);
  await assert.rejects(f.approve(flow,f.actor,{stayInAppSeconds:86400}),/human_identity_decision_conflict/u);
  const next=await f.begin(f.rps[1]);await assert.rejects(f.approve(next,f.actor,{stayInAppSeconds:86401}),/human_identity_renewal_invalid/u);
  for (const stayInAppSeconds of [null, '86400', -1, 300]) {
    await assert.rejects(f.approve(next, f.actor, { stayInAppSeconds }), /human_identity_renewal_invalid/u);
  }
  assert.equal(rows(f,db=>db.prepare('SELECT count(*) AS n FROM human_identity_grant_bindings WHERE stay_in_app_seconds>0').get().n),0);
});

test('renewal host policy is closed and explicit; operational off preserves exact eligible client pins', async t => {
  const f = await environment(t), current = f.profile, keys = current.providerKeys(), encryption = current.encryptionKey();
  const config = { enabled: true, issuer: current.issuer, registryId: current.registryId, environmentId: current.environmentId,
    clients: current.providerClients().map(client => ({ id: client.client_id, label: current.client(client.client_id).label,
      version: current.client(client.client_id).version, redirectUri: client.redirect_uris[0], clientSecret: client.client_secret })),
    jwks: keys.jwks, cookieKeys: keys.cookieKeys, artifactKey: encryption.key, artifactKeyId: encryption.keyId };
  try {
    for (const renewal of [{ admissionEnabled: 'true', clientIds: [] }, { admissionEnabled: true, clientIds: ['unknown'] },
      { admissionEnabled: true, clientIds: [f.rps[0].clientId, f.rps[0].clientId] }, { admissionEnabled: true, clientIds: [], tokenTTL: 86400 }]) {
      assert.throws(() => createHumanIdentityHostProfile({ ...config, renewal }, { shellOrigins: [f.origin] }));
    }
    let invoked = 0; const accessor = { admissionEnabled: true };
    Object.defineProperty(accessor, 'clientIds', { enumerable: true, get() { invoked++; return []; } });
    assert.throws(() => createHumanIdentityHostProfile({ ...config, renewal: accessor }, { shellOrigins: [f.origin] })); assert.equal(invoked, 0);
    const disabled = createHumanIdentityHostProfile({ ...config, renewal: { admissionEnabled: false, clientIds: f.rps.map(rp => rp.clientId) } }, { shellOrigins: [f.origin] });
    assert.deepEqual(disabled.publicClients, current.publicClients); assert.equal(disabled.renewalAdmissionEnabled, false);
    const plain = createHumanIdentityHostProfile(config, { shellOrigins: [f.origin] });
    assert.equal(plain.protocolDigest, current.protocolDigest); assert.equal(plain.publicClients.every((client, index) => client.profileDigest !== current.publicClients[index].profileDigest), true);
  } finally { encryption.key.fill(0); }
});

test('approved duration survives context reload and admission off; current proof cannot switch actor or move the family deadline', async t => {
  const advance = clock(t), f = await environment(t), flow = await f.begin();
  assert.deepEqual(flow.context.renewal, { maximumSessionSeconds: 86400 });
  await f.approve(flow, f.actor, { stayInAppSeconds: 86400 });
  const approvedAt = rows(f, db => db.prepare("SELECT approved_at FROM human_identity_interactions WHERE decision='approved'").get().approved_at);
  advance(1); await f.admission(false);
  const reloaded = await f.wire.request(flow.location.href + '/context'); assert.equal(reloaded.status, 200);
  assert.equal(reloaded.body.decision, 'approved'); assert.deepEqual(reloaded.body.renewal, { maximumSessionSeconds: 86400, approvedSessionSeconds: 86400 });
  flow.context = reloaded.body;
  await assert.rejects(f.approve(flow, f.outsider, { stayInAppSeconds: 86400 }), /human_identity_decision_conflict/u);
  await assert.rejects(f.approve(flow, f.actor, { stayInAppSeconds: 0 }), /human_identity_decision_conflict/u);
  await f.approve(flow, f.actor, { stayInAppSeconds: 86400 });
  assert.equal(rows(f, db => db.prepare("SELECT approved_at FROM human_identity_interactions WHERE decision='approved'").get().approved_at), approvedAt);
  const finished = await f.complete(flow); assert.equal((await f.wire.request(finished.callback)).status, 200);
  assert.equal(rows(f, db => db.prepare('SELECT session_expires_at FROM human_identity_grant_bindings').get().session_expires_at), approvedAt + 86400);
  const pending = await f.begin(f.rps[1]); assert.equal(pending.context.renewal, undefined);
  await assert.rejects(f.approve(pending, f.actor, { stayInAppSeconds: 86400 }), /human_identity_renewal_disabled/u);
});

test('known per-family exhaustion refuses refresh before consume and preserves the unconsumed proof', async t => {
  clock(t);
  const f = await environment(t); await longLogin(f, f.rps[0]);
  const token = f.rps[0].verificationFixture().refreshToken, saved = f.identity.sdk.find('RefreshToken', token);
  assert.equal(typeof saved.grantId, 'string');
  const expiresIn = saved.exp - Math.floor(Date.now() / 1000);
  for (let index = 1; index < HUMAN_RENEWAL_LIMITS.refreshRowsPerFamily; index++) {
    const jti = randomBytes(32).toString('base64url');
    f.identity.sdk.upsert({ model: 'RefreshToken', id: jti, payload: { ...saved, jti }, expiresIn,
      request: { clientId: f.rps[0].clientId, grantType: 'authorization_code' } });
  }
  const result = await f.refresh(f.rps[0], token); assert.equal(result.status, 503);
  assert.equal(['temporarily_unavailable', 'server_error'].includes(result.body.error), true);
  assert.equal(f.identity.sdk.find('RefreshToken', token).consumed, undefined);
  assert.equal(rows(f, db => db.prepare('SELECT count(*) AS n FROM human_identity_grant_bindings WHERE revoked_at IS NOT NULL').get().n), 0);
  await f.restart(); assert.equal(f.identity.sdk.find('RefreshToken', token).consumed, undefined);
});

test('known page headroom exhaustion is a pre-consume refusal, and raising the trusted budget can reuse that unconsumed proof', async t => {
  clock(t);
  const f = await environment(t, { maxDatabaseBytes: 256 * 1024 }); await longLogin(f, f.rps[0]);
  const token = f.rps[0].verificationFixture().refreshToken, before = f.identity.sdk.find('RefreshToken', token);
  assert.equal(typeof before.grantId, 'string');
  for (let index = 0; index < 4; index++) {
    const jti = randomBytes(32).toString('base64url');
    f.identity.sdk.upsert({ model: 'Session', id: jti, payload: { jti, kind: 'Session', marker: 'x'.repeat(12000) }, expiresIn: 600 });
  }
  assert.equal((await f.refresh(f.rps[0], token)).status, 503);
  assert.equal(f.identity.sdk.find('RefreshToken', token).consumed, undefined);
  f.identity.close(); const replacement = createHumanIdentityService({ databasePath: f.identityPath, profile: f.profile,
    actorActive: actor => f.connect.isActorActive(actor), withAuthorityFence: callback => f.connect.withAuthorityFence(callback) });
  try {
    assert.equal(replacement.sdk.consume('RefreshToken', token, { clientId: f.rps[0].clientId, grantType: 'refresh_token' }).status, 'consumed');
  } finally { replacement.close(); }
});

test('a real engine failure after consume permanently closes only that family and keeps replay tombstones across restart', async t => {
  let fail = true;
  const f = await environment(t, { serviceDecorator(service) {
    return Object.freeze({ ...service, sdk: Object.freeze({ ...service.sdk, upsert(request) {
      if (fail && request.model === 'AccessToken' && request.request?.grantType === 'refresh_token') {
        fail = false; throw new HumanIdentityError('human_identity_storage_full', 503);
      }
      return service.sdk.upsert(request);
    } }) });
  } });
  await longLogin(f, f.rps[0]); await longLogin(f, f.rps[1]);
  const old = f.rps[0].verificationFixture().refreshToken;
  const failed = await f.refresh(f.rps[0], old); assert.equal(failed.status, 503);
  assert.equal(['temporarily_unavailable', 'server_error'].includes(failed.body.error), true);
  assert.equal(rows(f, db => db.prepare('SELECT count(*) AS n FROM human_identity_grant_bindings WHERE revoked_at IS NOT NULL').get().n), 1);
  assert.equal(rows(f, db => db.prepare("SELECT count(*) AS n FROM human_identity_artifacts WHERE model='RefreshToken' AND consumed_at IS NOT NULL").get().n), 1);
  assert.equal((await f.wire.request(f.rps[0].origin + '/me')).status, 401);
  await f.restart(); assert.equal((await f.refresh(f.rps[0], old)).status, 400);
  assert.equal((await f.wire.request(f.rps[1].origin + '/me')).status, 200);
});
