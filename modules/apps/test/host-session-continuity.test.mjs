import test from 'node:test';
import assert from 'node:assert/strict';
import {DatabaseSync} from 'node:sqlite';
import {createHostSessionContinuity} from '../server/host-session-continuity.mjs';
import {FRAME_CONTINUITY_STORAGE,frameContinuityDdl} from '../server/frame-continuity.mjs';

const actor={accountId:'own-account',deviceId:'own-device'},appId='app-'+'1'.repeat(32);
const scope=()=>({schema:'soty.app-host-scope.v1',...actor,appId,policyEpoch:1,entry:{domainId:'dom_'+'2'.repeat(32),origin:'https://app.example',pathDigest:'3'.repeat(64)},target:{revision:1,digest:'4'.repeat(64),profile:'soty.relay-restricted.v1',bindingFloor:1},instanceId:'main',runtimeOrigin:'https://app-m'+'1'.repeat(32)+'-'+'8'.repeat(24)+'.example',runtimeScopeDigest:'8'.repeat(64)});
function fixture(t){const db=new DatabaseSync(':memory:');t.after(()=>db.close());db.exec(`CREATE TABLE local_apps(id TEXT PRIMARY KEY);CREATE TABLE apps_meta(key TEXT PRIMARY KEY,value TEXT NOT NULL);`);db.prepare('INSERT INTO local_apps VALUES(?)').run(appId);db.exec(frameContinuityDdl());db.prepare('INSERT INTO apps_meta VALUES(?,?)').run('frame_continuity',FRAME_CONTINUITY_STORAGE);return{db,store:()=>createHostSessionContinuity({db,clock:()=>1000})};}

test('ordinary tagged passive member survives host replacement but never persists cookie, proof, Actor, Source or ready',{timeout:10000},t=>{
  const f=fixture(t),store=f.store(),input=scope(),initial=store.create(input);input.target.revision++;
  assert.equal(initial.claim.scope.target.revision,1);assert.equal(Object.isFrozen(initial.claim.scope.target),true);
  const persisted=JSON.stringify(f.db.prepare('SELECT * FROM app_frame_continuity').all());
  assert.equal(persisted.includes(initial.descriptor.lease),false);
  const metadata=JSON.parse(f.db.prepare('SELECT binding_json FROM app_frame_continuity').get().binding_json);
  assert.equal(metadata.kind,'soty.app-host-session.v1');
  for(const field of ['actor','cookie','password','sourceSession','sourceProfile','humanPrincipal','ready'])assert.equal(Object.hasOwn(metadata.scope,field),false);
  const restarted=f.store(),current=restarted.current(initial.descriptor,actor,appId);restarted.assertClaim(current);
  for(const altered of[{accountId:'wrong'},{deviceId:'wrong'}])assert.throws(()=>restarted.current(initial.descriptor,{...actor,...altered},appId),/app_host_session_closed/);
});

test('same-member competing lease CAS, close and late cleanup never retire successor',t=>{
  const f=fixture(t),store=f.store(),initial=store.create(scope());initial.claim=store.activateInitial(initial.claim);const one=store.prepare(initial.claim),two=store.prepare(initial.claim);
  store.promote(two.claim);assert.throws(()=>store.promote(one.claim),/app_host_session_conflict/);
  assert.equal(store.close(actor,appId,initial.descriptor),false);store.assertClaim(two.claim);
  assert.equal(store.close(actor,appId,one.descriptor,one.claim),false);store.assertClaim(two.claim);
  store.promote(two.claim);assert.equal(store.close(actor,appId,two.descriptor),true);
  assert.throws(()=>f.store().current(two.descriptor,actor,appId),/app_host_session_closed/);
});

test('duplicate physical members share exact family but close only their own lineage; every authority dimension changes family',t=>{
  const f=fixture(t),store=f.store(),one=store.create(scope()),two=store.create(scope());
  assert.equal(one.claim.family,two.claim.family);assert.notEqual(one.claim.frameId,two.claim.frameId);
  store.close(actor,appId,one.descriptor);store.assertClaim(two.claim);
  for(const change of[s=>s.accountId='another',s=>s.deviceId='another',s=>s.policyEpoch++,s=>s.entry.pathDigest='5'.repeat(64),s=>s.entry.origin='https://other.example',s=>s.target.revision++,s=>s.target.digest='6'.repeat(64),s=>s.target.bindingFloor++,s=>{s.entry.origin+=':444';s.runtimeOrigin+=':444';},s=>{s.instanceId='widget-11111111-1111-4111-8111-111111111111';s.runtimeScopeDigest='7'.repeat(64);s.runtimeOrigin='https://app-w'+s.instanceId.slice(7).replaceAll('-','')+'-'+'7'.repeat(24)+'.example';}]){
    const altered=scope();change(altered);assert.notEqual(store.create(altered).claim.family,two.claim.family);
  }
});

test('durable COMMIT failure cannot become a promoted lease, closed pending candidate remains closed',t=>{
  const f=fixture(t),store=f.store(),initial=store.create(scope());initial.claim=store.activateInitial(initial.claim);const candidate=store.prepare(initial.claim);
  f.db.exec("CREATE TRIGGER reject_host_lease BEFORE UPDATE ON app_frame_continuity BEGIN SELECT RAISE(ABORT,'injected_commit_failure');END;");
  assert.throws(()=>store.promote(candidate.claim),/injected_commit_failure/);store.assertClaim(initial.claim);
  f.db.exec('DROP TRIGGER reject_host_lease');store.close(actor,appId,initial.descriptor);
  assert.throws(()=>store.promote(candidate.claim),/app_host_session_closed/);
});
