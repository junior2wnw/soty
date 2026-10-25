import test from 'node:test';
import assert from 'node:assert/strict';
import {mkdtemp,rm,readFile,writeFile,mkdir} from 'node:fs/promises';
import {randomBytes} from 'node:crypto';
import {join,resolve,dirname} from 'node:path';
import {tmpdir} from 'node:os';
import {createSourceReadinessObserver,sourceVerifiedOperation} from '../server/operation-observer.mjs';
import {selectedResourceProfile} from '../../apps/scoped-embed/resource-profile.mjs';
import {STANDARD_ROOT_WORKSPACE_SOURCE} from '../server/index.mjs';

const appId='app-'+'a'.repeat(32),raw={schema:'soty.selected-human-embed.v2',appId,connector:{linkId:'test',hostDeviceId:'host',connectorId:'source'},
  target:{revision:2,digest:'1'.repeat(64)},sourceProfile:STANDARD_ROOT_WORKSPACE_SOURCE,
  resource:{registryId:'soty',environmentId:'fixture',tenantId:'author',appId,resourceId:'personal',selection:{kind:'soty.workspace.v1',nativeId:'personal',incarnationId:'one'}},
  issuer:'https://root.test/human-identity',clientId:'personal-list',embedOrigin:'https://source.test',nativeOrigin:'https://native.test',parentOrigin:'https://root.test'};
const profile=selectedResourceProfile(raw),event={schema:'soty.source-readiness.v1',appId,profileDigest:profile.digest,target:profile.target,
  sourceProfile:profile.sourceProfile,observedAt:1761408000000,operation:'execute',status:'commit_verified'};
// Decoder unit tests use the internal producer explicitly. Production evidence
// is independently exercised through actual BFF/Root in the installed test.
const liveEvent=()=>sourceVerifiedOperation(structuredClone(event));

async function fixture(t){const directory=await mkdtemp(join(tmpdir(),'source-readiness-')),key=randomBytes(32),observer=createSourceReadinessObserver({directory,profile:raw,key});
  t.after(async()=>{observer.close();assert.equal(dirname(resolve(directory)),resolve(tmpdir()));await rm(directory,{recursive:true,force:true});});
  return{directory,key,observer,path:join(directory,'source-readiness.json')};}

test('doctor verifies a private purpose-sealed historical observation, never current authority or client acknowledgement',async t=>{
  const f=await fixture(t);assert.equal(f.observer.inspect().integrity,'missing');f.observer.observe(liveEvent());
  const state=f.observer.inspect();assert.equal(state.integrity,'verified');assert.deepEqual(state.lastObservation,event);
  assert.equal(state.currentAuthority,'not_checked');assert.equal(state.clientAcknowledgement,'not_observed');assert.equal(state.releaseReady,false);assert.equal(state.connected,false);
  const restarted=createSourceReadinessObserver({directory:f.directory,profile:raw,key:f.key});try{assert.equal(restarted.inspect().integrity,'verified');}finally{restarted.close();}
  const wrong=createSourceReadinessObserver({directory:f.directory,profile:raw,key:randomBytes(32)});try{assert.equal(wrong.inspect().integrity,'invalid');}finally{wrong.close();}
  const content=await readFile(f.path,'utf8');assert.equal(content.includes(f.key.toString('base64url')),false);assert.equal(content.includes('subject'),false);
});

test('tamper, unknown schema, copied pins and duplicate JSON fields never become readiness',async t=>{
  const f=await fixture(t);f.observer.observe(liveEvent());const original=await readFile(f.path,'utf8');
  for(const patch of [{schema:'future'}, {mac:'f'.repeat(64)}, {record:{...event,target:{revision:3,digest:'2'.repeat(64)}}}]){
    await writeFile(f.path,JSON.stringify({...JSON.parse(original),...patch}),{mode:0o600});assert.equal(f.observer.inspect().integrity,'invalid');assert.equal(f.observer.inspect().lastObservation,null);
  }
  await writeFile(f.path,original.replace('"schema":','"schema":"duplicate","schema":'),{mode:0o600});assert.equal(f.observer.inspect().integrity,'invalid');
});

test('recording failures remain diagnostics, including a destination that cannot be replaced',async t=>{
  const f=await fixture(t);await mkdir(f.path);assert.doesNotThrow(()=>f.observer.observe(liveEvent()));
  assert.equal(f.observer.inspect().writeStatus,'observation_unavailable');assert.equal(f.observer.inspect().integrity,'invalid');
  assert.doesNotThrow(()=>f.observer.observe({...event,subject:'must-not-be-written'}));
});

test('a plain JSON event or a decoded observation cannot manufacture a live SDK observation',async t=>{
  const f=await fixture(t);f.observer.observe(event);assert.equal(f.observer.inspect().integrity,'missing');
  f.observer.observe(liveEvent());const stored=await readFile(f.path,'utf8');
  f.observer.observe(f.observer.inspect().lastObservation);assert.equal(f.observer.inspect().writeStatus,'observation_unavailable');
  assert.equal(await readFile(f.path,'utf8'),stored);
});
