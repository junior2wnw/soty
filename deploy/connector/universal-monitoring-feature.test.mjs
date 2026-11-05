import test from 'node:test';
import assert from 'node:assert/strict';
import {mkdtemp,mkdir,writeFile,readFile,rm,realpath,stat} from 'node:fs/promises';
import {tmpdir} from 'node:os';
import path from 'node:path';
import * as policy from './universal-policy.mjs';
import {captureUniversalPreparedness,validateUniversalPreparedness} from '../../modules/app-contract/universal-preparedness.mjs';
import {currentStorageReaders,storageReaderLabel,assertStorageCompatible} from './storage-guard.mjs';
import {readStorageFormat} from './storage-probe.mjs';
import {hash,createConfig} from './rollout.mjs';
import {fixture as engineFixture,args as rolloutArgs} from './test-support/rollout-engine.mjs';
import {createHttpApp} from '../../server/http-app.js';

const origin='https://monitoring.fixture.invalid',modeName='SOTY_MONITORING_MODE';
const inputs={compiledLegacyMode:false,universalConfigured:true,reviewsConfigured:true,humanHttpEnabled:false,humanProfile:null};
const runtime=(mode,installed=mode==='writer')=>captureUniversalPreparedness({...inputs,...(mode?{monitoring:{mode,schemaVersion:1,installed}}:{})});
const image=(readers=currentStorageReaders,character='a',mode='0')=>({Id:'sha256:'+character.repeat(64),Config:{Labels:{[storageReaderLabel]:typeof readers==='string'?readers:JSON.stringify(readers),[policy.universalModeLabel]:mode}}});
async function fixture(t){const base=await realpath(tmpdir()),root=await mkdtemp(path.join(base,'soty-universal-policy-'));t.after(async()=>{assert.equal(path.dirname(root),base);assert.match(path.basename(root),/^soty-universal-policy-/u);await rm(root,{recursive:true,force:true});});return{root,options:{shellOrigins:[origin],fixtureRoot:root}};}
const basePlan=()=>({schema:policy.UNIVERSAL_POLICY_SCHEMA,phase:'features',human:null,reviews:null});
async function prepare(t,f,plan){let handle;await assert.doesNotReject(async()=>{handle=await policy.prepareUniversalPolicy(plan,f.options);});t.after(()=>policy.disposeUniversalPolicy(handle));return handle;}
async function feature(t,f,from,to){return prepare(t,f,{...basePlan(),expectedCurrentPreparednessSha256:hash(runtime(from)),featureMonitoring:{expectedMode:from,mode:to}});}
async function configuration(t,f){const handle=await prepare(t,f,basePlan());return policy.applyUniversalPolicy({Env:['DATA_DIR=/data','KEEP=synthetic'],HostConfig:{Mounts:[],RestartPolicy:{Name:'unless-stopped'}},WorkingDir:'/app'},handle);}

test('actual monitoring capture is closed, immutable and additive; the legacy policy digest stays byte-compatible',async t=>{
 const measured=runtime('reader',false);assert.deepEqual(measured.monitoring,{mode:'reader',schemaVersion:1,installed:false});assert.ok(Object.isFrozen(measured.monitoring));assert.deepEqual(validateUniversalPreparedness(measured),measured);
 for(const monitoring of [{mode:'writer',schemaVersion:1,installed:false},{mode:'reader',schemaVersion:2,installed:false},{mode:'reader',schemaVersion:1,installed:0},{mode:'reader',schemaVersion:1,installed:false,registryId:'private'}])assert.throws(()=>captureUniversalPreparedness({...inputs,monitoring}));
 const f=await fixture(t),handle=await prepare(t,f,basePlan()),receipt=policy.publicUniversalPolicy(handle);assert.equal(receipt.policyDigest,'7e8c8f4af5fca36c5ad7b2410ee2ed7960e68758019a151c1da601aaeb4c1dda');assert.equal(Object.hasOwn(receipt,'featureMonitoring'),false);assert.equal(receipt.environment.some(row=>row.name===modeName),false);
});
test('a first reader-only upgrade accepts an old image without the family only while actual monitoring data is absent',async t=>{
 const f=await fixture(t),source=path.join(f.root,'empty');await mkdir(source);const format=await readStorageFormat(source);assert.equal(format.monitoring,'empty');
 const current=JSON.parse(currentStorageReaders),legacy={version:7,readers:Object.fromEntries(['rooms','apps','notes','capabilities','appRegistration','feedback','humanIdentity','connect','world'].map(key=>[key,current.readers[key]]))};legacy.readers.notes=[1,2];
 const plan={...basePlan(),expectedCurrentPreparednessSha256:hash(runtime())},handle=await prepare(t,f,plan);assert.doesNotThrow(()=>policy.assertUniversalImagePrerequisites(handle,{candidateImage:image(),originalImage:image(legacy,'b')}));assertStorageCompatible(image(legacy,'b'),format);
 const nonempty={...format,monitoring:1};assert.throws(()=>assertStorageCompatible(image(legacy,'b'),nonempty),{code:'storage_reader_incompatible'});
 const enabling=await feature(t,f,'reader','writer');assert.throws(()=>policy.assertUniversalImagePrerequisites(enabling,{candidateImage:image(),originalImage:image(legacy)}),{code:'universal_policy_monitoring_reader_required'});
});
test('writer cannot slip through a normal policy and duplicate or unsupported mode values fail closed',async t=>{
 const f=await fixture(t),handle=await prepare(t,f,basePlan()),config=await configuration(t,f);
 for(const entries of [[modeName+'=writer'],[modeName+'=reader',modeName+'=reader'],[modeName+'=unknown'],[modeName]])assert.throws(()=>policy.applyUniversalPolicy({...config,Env:[...config.Env,...entries]},handle));
 const reader={...config,Env:[...config.Env,modeName+'=reader']};assert.deepEqual(policy.applyUniversalPolicy(reader,handle),reader);
});
test('named reader-to-writer and writer-to-reader flips change only the mode; old proof and actual capture are required',async t=>{
 const f=await fixture(t);assert.equal(typeof policy.assertUniversalFeatureOriginal,'function');assert.equal(policy.assertSelectedEmbedFeatureOriginal,policy.assertUniversalFeatureOriginal);
 let config=await configuration(t,f);for(const[from,to]of[['reader','writer'],['writer','reader']]){
  const handle=await feature(t,f,from,to),before=structuredClone(config),next=policy.applyUniversalPolicy(config,handle);assert.deepEqual(config,before);assert.equal(next.Env.filter(x=>x.startsWith(modeName+'=')).join(),modeName+'='+to);
  const incomplete={...before,Env:before.Env.filter(value=>value!=='SOTY_PERSONAL_AGENT_ENABLED=0')};assert.throws(()=>policy.applyUniversalPolicy(incomplete,handle),{code:'universal_policy_monitoring_other_configuration'});
  const restored=structuredClone(next);restored.Env=restored.Env.filter(x=>!x.startsWith(modeName+'='));const originalWithoutMode={...before,Env:before.Env.filter(x=>!x.startsWith(modeName+'='))};assert.deepEqual(restored,originalWithoutMode);
  assert.doesNotThrow(()=>policy.assertUniversalFeatureOriginal(handle,{Config:before},runtime(from,from==='writer'),{allowFixture:true}));
  assert.throws(()=>policy.assertUniversalFeatureOriginal(handle,{Config:before},runtime(),{allowFixture:true}));
  assert.throws(()=>policy.assertUniversalFeatureOriginal(handle,{Config:before},runtime(to,true),{allowFixture:true}));
  assert.doesNotThrow(()=>policy.assertUniversalPreparedness(handle,runtime(to,true),{allowFixture:true}));assert.throws(()=>policy.assertUniversalPreparedness(handle,runtime(from,true),{allowFixture:true}));
  policy.assertUniversalImagePrerequisites(handle,{candidateImage:image(),originalImage:image()});assert.throws(()=>policy.assertUniversalImagePrerequisites(handle,{candidateImage:image(),originalImage:image(currentStorageReaders,'b')}),{code:'universal_policy_monitoring_image_mismatch'});
  config=next;
 }
});
test('feature plan is closed and bound to features, fresh original hash and a real mode transition',async t=>{
 const f=await fixture(t),good={...basePlan(),expectedCurrentPreparednessSha256:'a'.repeat(64),featureMonitoring:{expectedMode:'reader',mode:'writer'}};
 for(const plan of [{...good,phase:'legacy-baseline'},{...good,expectedCurrentPreparednessSha256:undefined},{...good,featureMonitoring:{expectedMode:'reader',mode:'reader'}},{...good,featureMonitoring:{expectedMode:'reader',mode:'writer',admin:true}},{...good,featureMonitoring:{expectedMode:'reader',mode:'unknown'}}])await assert.rejects(policy.prepareUniversalPolicy(plan,f.options));
});
async function rolloutFixture(t,{missingCapture=false,staleHash=false}={}){
 const f=await fixture(t),engine=engineFixture(),base=await prepare(t,f,basePlan()),original=engine.map.get(rolloutArgs.originalId);
 const configured=createConfig(original,rolloutArgs.originalImage,rolloutArgs.transaction,rolloutArgs.revision,undefined,base);original.Config.Env=configured.Env;original.HostConfig=configured.HostConfig;
 const current=missingCapture?runtime():runtime('reader',false),handle=await prepare(t,f,{...basePlan(),expectedCurrentPreparednessSha256:hash(staleHash?runtime('reader',true):current),featureMonitoring:{expectedMode:'reader',mode:'writer'}});
 const readImage=engine.engine.image;engine.engine.image=async id=>{const value=await readImage(id);value.Config.Labels[policy.universalModeLabel]='0';return value;};engine.run.allowUniversalFixture=true;
 engine.run.universalMeasurement=async id=>id===rolloutArgs.originalId?current:runtime('writer',true);
 return{...engine,request:{...rolloutArgs,candidateImage:rolloutArgs.originalImage,universalPolicy:handle,universalWitnessId:'x'.repeat(32)}};
}
test('the maintained Rollout executes the configuration-only writer feature through its original and candidate measurement gates',async t=>{
 const f=await rolloutFixture(t),before=structuredClone(f.map.get(rolloutArgs.originalId));await f.run.prepare(f.request);await f.run.promote();
 assert.equal(f.run.state.phase,'committed');assert.equal(f.run.config.Env.includes(modeName+'=writer'),true);assert.deepEqual(f.run.original.Config.Env,before.Config.Env);assert.equal(f.events.filter(value=>value==='start-candidate').length,1);
});
test('the maintained Rollout refuses missing actual reader capture and stale current hash before CREATE or STOP',async t=>{
 for(const fault of[{missingCapture:true},{staleHash:true}]){const f=await rolloutFixture(t,fault);await assert.rejects(f.run.prepare(f.request));assert.equal(f.events.some(value=>value.startsWith('create:')||value.startsWith('stop:')),false);}
});
test('actual Root captures fileless reader, initialized writer and byte-identical reader reopen from the real module',async t=>{
 const f=await fixture(t),dist=path.join(f.root,'dist'),data=path.join(f.root,'data');await mkdir(dist);await writeFile(path.join(dist,'index.html'),'<!doctype html><title>own monitoring fixture</title>');
 const file=path.join(data,'monitoring','monitoring.sqlite');let previous;
 for(const mode of['reader','writer','reader']){
  const app=createHttpApp(dist,{dataDir:data,connectOrigins:[origin],appHosting:{},appOriginTemplate:'https://{appId}.apps.fixture.invalid',namedAppZone:'',discoveryOrigin:'',universalAppsEnabled:true,monitoring:{storageMode:mode}});
  try{const measured=app.locals.captureUniversalPreparedness();assert.deepEqual(measured.monitoring,{mode,schemaVersion:1,installed:mode==='writer'||Boolean(previous),accessSha256:accessHash(accessPolicy())});if(!previous&&mode==='reader')await assert.rejects(stat(file),{code:'ENOENT'});if(previous&&mode==='reader')assert.deepEqual(await readFile(file),previous);}finally{await app.locals.closeServices();}
  if(mode==='writer'){previous=await readFile(file);assert.equal((await readStorageFormat(data)).monitoring,1);}
 }
 assert.deepEqual(await readFile(file),previous);
});

const accessPolicy=(read=[],configure=[],admin=[])=>({readAccountIds:read,configureAccountIds:configure,adminAccountIds:admin});
const accessHash=value=>hash(Object.fromEntries(Object.entries(value).map(([key,ids])=>[key,[...ids].sort()])));
const accessActor='acct_'+'a'.repeat(32),accessOther='acct_'+'b'.repeat(32);
const accessRuntime=(mode,value,installed=mode==='writer')=>captureUniversalPreparedness({...inputs,monitoring:{mode,schemaVersion:1,installed,accessSha256:accessHash(value)}});
const accessPlan=(expected,desired,mode='reader',to)=>({...basePlan(),expectedCurrentPreparednessSha256:hash(accessRuntime(mode,expected)),featureMonitoringAccess:{expected,desired},...(to?{featureMonitoring:{expectedMode:mode,mode:to}}:{})});
const accessNames=['SOTY_MONITORING_READ_ACCOUNT_IDS','SOTY_MONITORING_CONFIGURE_ACCOUNT_IDS','SOTY_MONITORING_ADMIN_ACCOUNT_IDS'];

test('monitoring access plan is first class, closed, canonical and its public receipt contains only hashes',async t=>{
 const f=await fixture(t),old=accessPolicy(),desired=accessPolicy([accessOther,accessActor],[accessActor],[accessActor]);
 const plan={...basePlan(),expectedCurrentPreparednessSha256:hash(runtime('reader')),featureMonitoringAccess:{expected:old,desired}},handle=await prepare(t,f,plan),receipt=policy.publicUniversalPolicy(handle);
 assert.deepEqual(receipt.featureMonitoringAccess,{expectedAccessSha256:accessHash(old),accessSha256:accessHash(desired)});
 assert.equal(JSON.stringify(receipt).includes(accessActor),false);assert.equal(JSON.stringify(receipt).includes(accessOther),false);
 assert.equal(receipt.environment.some(x=>accessNames.includes(x.name)),false);
 const key=Buffer.alloc(32,7),sealed=await policy.sealUniversalPolicy(handle,{key,keyId:'synthetic-access'});
 const restored=await policy.restoreUniversalPolicy(plan,sealed.packet,f.options,{key,keyId:'synthetic-access',expectedWitnessId:sealed.witnessId});t.after(()=>policy.disposeUniversalPolicy(restored));
 assert.deepEqual(policy.publicUniversalPolicy(restored),receipt);
 await assert.rejects(policy.restoreUniversalPolicy({...plan,featureMonitoringAccess:{expected:old,desired:accessPolicy([accessActor])}},sealed.packet,f.options,{key,keyId:'synthetic-access',expectedWitnessId:sealed.witnessId}));
});
test('monitoring access rejects malformed, excessive, duplicate, unknown, no-op and unbound plans',async t=>{
 const f=await fixture(t),old=accessPolicy(),desired=accessPolicy([accessActor],[accessActor],[accessActor]),good=accessPlan(old,desired);
 const invalid=[{...good,phase:'legacy-baseline'},{...good,expectedCurrentPreparednessSha256:undefined},{...good,featureMonitoringAccess:{expected:old,desired,coreAdmin:true}},
 {...good,featureMonitoringAccess:{expected:old,desired:{...desired,coreAdminAccountIds:[accessActor]}}},{...good,featureMonitoringAccess:{expected:old,desired:accessPolicy(['acct_short'])}},
 {...good,featureMonitoringAccess:{expected:old,desired:accessPolicy([accessActor,accessActor])}},{...good,featureMonitoringAccess:{expected:old,desired:accessPolicy(Array.from({length:33},(_,n)=>'acct_'+String(n).padStart(32,'0')))}},
 {...good,featureMonitoringAccess:{expected:old,desired:{readAccountIds:[accessActor],adminAccountIds:[]}}},{...good,featureMonitoringAccess:{expected:old,desired:old}}];
 for(const plan of invalid)await assert.rejects(policy.prepareUniversalPolicy(plan,f.options));
});
test('monitoring access and mode combine in one projection and all other configuration remains exact',async t=>{
 const f=await fixture(t),old=accessPolicy(),desired=accessPolicy([accessActor],[accessActor],[accessActor]),handle=await prepare(t,f,accessPlan(old,desired,'reader','writer'));
 const config=await configuration(t,f);config.Env.push('SOTY_PLATFORM_ADMIN_ACCOUNT_IDS=unchanged','SOURCE_PROVIDER_ROLE=unchanged');const before=structuredClone(config),next=policy.applyUniversalPolicy(config,handle);
 assert.deepEqual(config,before);for(const name of accessNames)assert.equal(next.Env.find(x=>x.startsWith(name+'=')),name+'='+accessActor);
 assert.equal(next.Env.includes(modeName+'=writer'),true);
 const strip=value=>{const out=structuredClone(value);out.Env=out.Env.filter(x=>![...accessNames,modeName].some(name=>x.startsWith(name+'=')));return out;};assert.deepEqual(strip(next),strip(before));
 const incomplete=structuredClone(before);incomplete.Env=incomplete.Env.filter(x=>x!=='SOTY_PERSONAL_AGENT_ENABLED=0');assert.throws(()=>policy.applyUniversalPolicy(incomplete,handle));
 const stale=structuredClone(before);stale.Env.push(accessNames[0]+'='+accessOther);assert.throws(()=>policy.applyUniversalPolicy(stale,handle));
 const duplicate=structuredClone(before);duplicate.Env.push(accessNames[0]+'=',accessNames[0]+'=');assert.throws(()=>policy.applyUniversalPolicy(duplicate,handle));
 assert.doesNotThrow(()=>policy.assertUniversalFeatureOriginal(handle,{Config:before},accessRuntime('reader',old),{allowFixture:true}));
 assert.doesNotThrow(()=>policy.assertUniversalPreparedness(handle,accessRuntime('writer',desired),{allowFixture:true}));
 for(const measured of [runtime('writer'),accessRuntime('writer',old),accessRuntime('reader',desired)])assert.throws(()=>policy.assertUniversalPreparedness(handle,measured,{allowFixture:true}));
});
test('monitoring access-only preserves an existing writer mode and requires real loaded expected rights',async t=>{
 const f=await fixture(t),old=accessPolicy([accessOther]),desired=accessPolicy([accessActor]);
 const config=await configuration(t,f);config.Env.push(modeName+'=writer',accessNames[0]+'='+accessOther);
 const handle=await prepare(t,f,accessPlan(old,desired,'writer')),next=policy.applyUniversalPolicy(config,handle);assert.equal(next.Env.includes(modeName+'=writer'),true);
 assert.doesNotThrow(()=>policy.assertUniversalFeatureOriginal(handle,{Config:config},accessRuntime('writer',old),{allowFixture:true}));
 assert.doesNotThrow(()=>policy.assertUniversalPreparedness(handle,accessRuntime('writer',desired),{allowFixture:true}));
 for(const actual of [runtime('writer'),accessRuntime('writer',desired),accessRuntime('reader',old)])assert.throws(()=>policy.assertUniversalFeatureOriginal(handle,{Config:config},actual,{allowFixture:true}));
 assert.throws(()=>policy.assertUniversalPreparedness(handle,accessRuntime('reader',desired),{allowFixture:true}));
 const readers=JSON.parse(currentStorageReaders);delete readers.readers.monitoring;readers.version=9;
 policy.assertUniversalImagePrerequisites(handle,{candidateImage:image(),originalImage:image()});
 assert.throws(()=>policy.assertUniversalImagePrerequisites(handle,{candidateImage:image(),originalImage:image(currentStorageReaders,'b')}));
 assert.throws(()=>policy.assertUniversalImagePrerequisites(handle,{candidateImage:image(readers),originalImage:image(readers)}));
});
test('monitoring mode-only also preserves an optional actually loaded access hash',async t=>{
 const f=await fixture(t),rights=accessPolicy([accessActor]),handle=await feature(t,f,'reader','writer'),config=await configuration(t,f);config.Env.push(accessNames[0]+'='+accessActor);
 policy.applyUniversalPolicy(config,handle);policy.assertUniversalFeatureOriginal(handle,{Config:config},accessRuntime('reader',rights),{allowFixture:true});
 assert.doesNotThrow(()=>policy.assertUniversalPreparedness(handle,accessRuntime('writer',rights),{allowFixture:true}));
 assert.throws(()=>policy.assertUniversalPreparedness(handle,accessRuntime('writer',accessPolicy()),{allowFixture:true}));
});
async function accessRolloutFixture(t,fault){
 const f=await fixture(t),engine=engineFixture(),base=await prepare(t,f,basePlan()),original=engine.map.get(rolloutArgs.originalId),configured=createConfig(original,rolloutArgs.originalImage,rolloutArgs.transaction,rolloutArgs.revision,undefined,base);
 original.Config.Env=configured.Env;original.HostConfig=configured.HostConfig;
 const old=accessPolicy(),desired=accessPolicy([accessActor],[accessActor],[accessActor]),current=fault==='missing'?runtime('reader'):accessRuntime('reader',old);
 const plan=accessPlan(fault==='rights'?accessPolicy([accessOther]):old,desired,'reader','writer');plan.expectedCurrentPreparednessSha256=hash(fault==='stale'?accessRuntime('reader',accessPolicy([accessOther])):current);
 const handle=await prepare(t,f,plan),readImage=engine.engine.image;engine.engine.image=async id=>{const result=await readImage(id);result.Config.Labels[policy.universalModeLabel]='0';return result;};
 engine.run.allowUniversalFixture=true;engine.run.universalMeasurement=async id=>id===rolloutArgs.originalId?current:accessRuntime('writer',desired);
 return{...engine,request:{...rolloutArgs,candidateImage:rolloutArgs.originalImage,universalPolicy:handle,universalWitnessId:'x'.repeat(32)}};
}
test('monitoring access uses the existing Rollout one-restart path with both actual policy measurements',async t=>{
 const f=await accessRolloutFixture(t);await f.run.prepare(f.request);await f.run.promote();assert.equal(f.run.state.phase,'committed');assert.equal(f.events.filter(x=>x==='start-candidate').length,1);
 for(const name of accessNames)assert.equal(f.run.config.Env.includes(name+'='+accessActor),true);
});
test('monitoring access refuses readonly missing capture, stale runtime and stale expected rights before CREATE or STOP',async t=>{
 for(const fault of ['missing','stale','rights']){const f=await accessRolloutFixture(t,fault);await assert.rejects(f.run.prepare(f.request));assert.equal(f.events.some(x=>x.startsWith('create:')||x.startsWith('stop:')),false);}
});
test('monitoring access capture comes from loaded module Sets, not the environment or mutable input',async t=>{
 const f=await fixture(t),dist=path.join(f.root,'dist'),data=path.join(f.root,'data');await mkdir(dist);await writeFile(path.join(dist,'index.html'),'<!doctype html><title>synthetic access capture</title>');
 const loaded=accessPolicy([accessActor],[accessActor],[accessActor]),expected=accessHash(loaded),env=Object.fromEntries(accessNames.map(name=>[name,accessOther]));
 env.SOTY_PLATFORM_ADMIN_ACCOUNT_IDS=accessActor;const app=createHttpApp(dist,{dataDir:data,env,connectOrigins:[origin],appHosting:{},appOriginTemplate:'https://{appId}.apps.fixture.invalid',namedAppZone:'',discoveryOrigin:'',universalAppsEnabled:true,monitoring:{storageMode:'reader',platformPolicy:loaded}});
 try{const first=app.locals.captureUniversalPreparedness();assert.equal(first.platformAdminAccessSha256,hash([accessActor]));env.SOTY_PLATFORM_ADMIN_ACCOUNT_IDS=accessOther;assert.equal(app.locals.captureUniversalPreparedness().platformAdminAccessSha256,hash([accessActor]));assert.equal(first.monitoring.accessSha256,expected);assert.notEqual(first.monitoring.accessSha256,accessHash(accessPolicy([accessOther],[accessOther],[accessOther])));
 loaded.readAccountIds[0]=accessOther;assert.equal(app.locals.captureUniversalPreparedness().monitoring.accessSha256,expected);
 assert.equal(JSON.stringify(first).includes(accessActor),false);assert.equal(JSON.stringify(first).includes(accessOther),false);assert.equal(Object.isFrozen(first.monitoring),true);
 await assert.rejects(stat(path.join(data,'monitoring','monitoring.sqlite')),{code:'ENOENT'});
 }finally{await app.locals.closeServices();}
 for(const invalid of ['', 'g'.repeat(64), 'a'.repeat(63)])assert.throws(()=>captureUniversalPreparedness({...inputs,monitoring:{mode:'reader',schemaVersion:1,installed:false,accessSha256:invalid}}));
});

test('monitoring access loaded Sets attest the same module rights without granting Core administration',async t=>{
 const f=await fixture(t),rights=accessPolicy([accessActor],[accessActor],[accessActor]),expected=accessHash(rights);let coreUpdates=0;
 const core={isActorActive:()=>true,withActorAuthorityFence:(_actor,callback)=>callback(),platformAdmission:({op})=>{if(op.endsWith('.update'))coreUpdates++;return{manageAllowed:false,revision:0,inviteOnly:false,defaultQuota:1};}};
 const {createMonitoringModule}=await import(process.env.SOTY_MONITORING_HOST_SOURCE??new URL('../../server/monitoring-module.js',import.meta.url));
 const host=createMonitoringModule({databasePath:path.join(f.root,'loaded.sqlite'),storageMode:'writer',connect:()=>core,apps:()=>({}),platformPolicy:rights});
 try{assert.equal(host.storageIdentity().accessSha256,expected);const call=id=>host.execute({actor:{accountId:id},op:'monitoring.describe',args:{expectedAccountId:id,scope:{kind:'platform'}}});
 const dto=call(accessActor);assert.equal(dto.permissions.admin,true);assert.equal(dto.actions.find(x=>x.id==='platform.admission').enabled,false);assert.throws(()=>call(accessOther),{code:'monitoring_forbidden'});
 rights.readAccountIds[0]=accessOther;assert.equal(host.storageIdentity().accessSha256,expected);assert.equal(call(accessActor).permissions.read,true);assert.throws(()=>call(accessOther),{code:'monitoring_forbidden'});assert.equal(coreUpdates,0);
 }finally{host.close();}
});

const coreEnv='SOTY_PLATFORM_ADMIN_ACCOUNT_IDS';
const coreRuntime=(ids,mode='reader',mon=accessPolicy())=>captureUniversalPreparedness({...inputs,monitoring:{mode,schemaVersion:1,installed:mode==='writer',accessSha256:accessHash(mon)},platformAdminAccessSha256:hash([...ids].sort())});
const corePlan=(expected,desired,current=coreRuntime(expected))=>({...basePlan(),expectedCurrentPreparednessSha256:hash(current),featurePlatformAdminAccess:{expected,desired}});
test('explicit Core admin access is separate, closed and hash-only in public receipts',async t=>{
 const f=await fixture(t),plan={...basePlan(),expectedCurrentPreparednessSha256:hash(runtime('reader')),featurePlatformAdminAccess:{expected:[],desired:[accessActor]}};
 const handle=await prepare(t,f,plan),receipt=policy.publicUniversalPolicy(handle);assert.deepEqual(receipt.featurePlatformAdminAccess,{expectedAccessSha256:hash([]),accessSha256:hash([accessActor])});
 assert.equal(Object.hasOwn(receipt,'featureMonitoringAccess'),false);assert.equal(receipt.environment.some(x=>x.name===coreEnv),false);assert.equal(JSON.stringify(receipt).includes(accessActor),false);
 const config=await configuration(t,f),next=policy.applyUniversalPolicy(config,handle);assert.equal(next.Env.includes(coreEnv+'='+accessActor),true);for(const name of accessNames)assert.equal(next.Env.some(x=>x.startsWith(name+'=')),false);
 const before=structuredClone(config),without=structuredClone(next);without.Env=without.Env.filter(x=>!x.startsWith(coreEnv+'='));assert.deepEqual(without,before);
});
test('Core admin access rejects wrong ids, duplicate, unknown, no-op, stale and unrelated configuration deltas',async t=>{
 const f=await fixture(t),good=corePlan([],[accessActor]);
 for(const change of [{featurePlatformAdminAccess:{expected:[],desired:['acct_short']}},{featurePlatformAdminAccess:{expected:[],desired:[accessActor,accessActor]}},{featurePlatformAdminAccess:{expected:[],desired:[accessActor],monitoringAdmin:true}},
 {featurePlatformAdminAccess:{expected:[],desired:[]}},{featurePlatformAdminAccess:{expected:[],desired:Array.from({length:33},(_,n)=>'acct_'+String(n).padStart(32,'0'))}},{expectedCurrentPreparednessSha256:undefined},{phase:'legacy-baseline'}])await assert.rejects(policy.prepareUniversalPolicy({...good,...change},f.options));
 const handle=await prepare(t,f,good),config=await configuration(t,f),stale=structuredClone(config);stale.Env.push(coreEnv+'='+accessOther);assert.throws(()=>policy.applyUniversalPolicy(stale,handle));
 const duplicate=structuredClone(config);duplicate.Env.push(coreEnv+'=',coreEnv+'=');assert.throws(()=>policy.applyUniversalPolicy(duplicate,handle));
 const incomplete=structuredClone(config);incomplete.Env=incomplete.Env.filter(x=>x!=='SOTY_PERSONAL_AGENT_ENABLED=0');assert.throws(()=>policy.applyUniversalPolicy(incomplete,handle));
});
test('Core admin loaded identity is mandatory and cannot be replaced by an Env-only guess',async t=>{
 const f=await fixture(t),handle=await prepare(t,f,corePlan([],[accessActor])),config=await configuration(t,f);policy.applyUniversalPolicy(config,handle);
 policy.assertUniversalFeatureOriginal(handle,{Config:config},coreRuntime([]),{allowFixture:true});
 policy.assertUniversalPreparedness(handle,coreRuntime([accessActor]),{allowFixture:true});
 for(const observed of [accessRuntime('reader',accessPolicy()),coreRuntime([accessOther])])assert.throws(()=>policy.assertUniversalFeatureOriginal(handle,{Config:config},observed,{allowFixture:true}));
 for(const actual of [accessRuntime('reader',accessPolicy()),coreRuntime([])])assert.throws(()=>policy.assertUniversalPreparedness(handle,actual,{allowFixture:true}));
 assert.throws(()=>policy.assertUniversalImagePrerequisites(handle,{candidateImage:image(),originalImage:image(currentStorageReaders,'b')}));
});
test('all three named role and mode features share exactly one preserved configuration and one restart',async t=>{
 const f=await fixture(t),engine=engineFixture(),base=await prepare(t,f,basePlan()),original=engine.map.get(rolloutArgs.originalId),configured=createConfig(original,rolloutArgs.originalImage,rolloutArgs.transaction,rolloutArgs.revision,undefined,base);original.Config.Env=configured.Env;original.HostConfig=configured.HostConfig;
 const old=accessPolicy(),desired=accessPolicy([accessActor],[accessActor],[accessActor]),current=coreRuntime([], 'reader',old);
 const plan={...accessPlan(old,desired,'reader','writer'),expectedCurrentPreparednessSha256:hash(current),featurePlatformAdminAccess:{expected:[],desired:[accessActor]}};
 const handle=await prepare(t,f,plan),before=structuredClone(original),reader=engine.engine.image;engine.engine.image=async id=>{const value=await reader(id);value.Config.Labels[policy.universalModeLabel]='0';return value;};engine.run.allowUniversalFixture=true;
 engine.run.universalMeasurement=async id=>id===rolloutArgs.originalId?current:coreRuntime([accessActor],'writer',desired);const args={...rolloutArgs,candidateImage:rolloutArgs.originalImage,universalPolicy:handle,universalWitnessId:'x'.repeat(32)};
 await engine.run.prepare(args);await engine.run.promote();assert.equal(engine.run.state.phase,'committed');assert.equal(engine.events.filter(x=>x==='start-candidate').length,1);
 const names=[coreEnv,modeName,...accessNames],strip=value=>value.filter(entry=>!names.some(name=>entry.startsWith(name+'=')));assert.deepEqual(strip(engine.run.config.Env),strip(before.Config.Env));
 assert.equal(engine.run.config.Env.includes(coreEnv+'='+accessActor),true);for(const name of accessNames)assert.equal(engine.run.config.Env.includes(name+'='+accessActor),true);
});
test('monitoring-only and Core-only operations never transfer each other permissions',async t=>{
 const f=await fixture(t),config=await configuration(t,f);config.Env.push(coreEnv+'='+accessOther);
 const mon=await prepare(t,f,accessPlan(accessPolicy(),accessPolicy([accessActor],[accessActor],[accessActor]),'reader','writer')),monNext=policy.applyUniversalPolicy(config,mon);assert.equal(monNext.Env.includes(coreEnv+'='+accessOther),true);
 const loaded=coreRuntime([accessOther]);policy.assertUniversalFeatureOriginal(mon,{Config:config},loaded,{allowFixture:true});assert.throws(()=>policy.assertUniversalPreparedness(mon,coreRuntime([accessActor],'writer',accessPolicy([accessActor],[accessActor],[accessActor])),{allowFixture:true}));
 const core=await prepare(t,f,corePlan([accessOther],[accessActor],loaded)),coreNext=policy.applyUniversalPolicy(config,core);for(const name of accessNames)assert.equal(coreNext.Env.some(x=>x.startsWith(name+'=')),false);
});

test('legacy baseline upgrade preserves additive loaded Core identity without enabling a role operation',async t=>{
 const f=await fixture(t),handle=await prepare(t,f,basePlan()),engine=engineFixture(),readImage=engine.engine.image;
 engine.engine.image=async id=>{const result=await readImage(id);result.Config.Labels[policy.universalModeLabel]=id===rolloutArgs.candidateImage?'0':'1';return result;};engine.run.allowUniversalFixture=true;
 engine.run.universalMeasurement=async()=>captureUniversalPreparedness({compiledLegacyMode:true,universalConfigured:false,reviewsConfigured:false,humanHttpEnabled:false,humanProfile:null,monitoring:{mode:'reader',schemaVersion:1,installed:false},platformAdminAccessSha256:hash([])});
 await engine.run.prepare({...rolloutArgs,universalPolicy:handle,universalWitnessId:'x'.repeat(32)});assert.equal(engine.run.state.phase,'prepared');assert.equal(engine.events.some(x=>x.startsWith('stop:')),false);assert.equal(engine.run.config.Env.some(x=>x.startsWith(coreEnv+'=')),false);
});

test('Core access-only preserves writer mode, supports explicit revocation and does not require unrelated monitoring metadata',async t=>{
 const f=await fixture(t),config=await configuration(t,f);config.Env.push(modeName+'=writer',coreEnv+'='+accessActor);
 const old=coreRuntime([accessActor],'writer'),handle=await prepare(t,f,corePlan([accessActor],[],old)),next=policy.applyUniversalPolicy(config,handle);assert.equal(next.Env.includes(modeName+'=writer'),true);assert.equal(next.Env.includes(coreEnv+'='),true);
 policy.assertUniversalFeatureOriginal(handle,{Config:config},old,{allowFixture:true});policy.assertUniversalPreparedness(handle,coreRuntime([],'writer'),{allowFixture:true});assert.throws(()=>policy.assertUniversalPreparedness(handle,coreRuntime([accessActor],'writer'),{allowFixture:true}));
 const noMonitoring=captureUniversalPreparedness({...inputs,platformAdminAccessSha256:hash([])}),standalone=await prepare(t,f,corePlan([],[accessActor],noMonitoring)),plain=await configuration(t,f);policy.applyUniversalPolicy(plain,standalone);
 policy.assertUniversalFeatureOriginal(standalone,{Config:plain},noMonitoring,{allowFixture:true});policy.assertUniversalPreparedness(standalone,captureUniversalPreparedness({...inputs,platformAdminAccessSha256:hash([accessActor])}),{allowFixture:true});
 for(const value of ['', 'x'.repeat(64), 'a'.repeat(63)])assert.throws(()=>captureUniversalPreparedness({...inputs,platformAdminAccessSha256:value}));
});
test('Core access missing actual identity, stale loaded rights or stale runtime refuses before CREATE and STOP',async t=>{
 for(const fault of['missing','rights','stale']){
  const f=await fixture(t),engine=engineFixture(),base=await prepare(t,f,basePlan()),original=engine.map.get(rolloutArgs.originalId),configured=createConfig(original,rolloutArgs.originalImage,rolloutArgs.transaction,rolloutArgs.revision,undefined,base);original.Config.Env=configured.Env;original.HostConfig=configured.HostConfig;
  const current=fault==='missing'?runtime('reader'):coreRuntime([]),plan=corePlan(fault==='rights'?[accessOther]:[],[accessActor],current);if(fault==='stale')plan.expectedCurrentPreparednessSha256=hash(coreRuntime([accessOther]));
  const handle=await prepare(t,f,plan),readImage=engine.engine.image;engine.engine.image=async id=>{const value=await readImage(id);value.Config.Labels[policy.universalModeLabel]='0';return value;};engine.run.allowUniversalFixture=true;engine.run.universalMeasurement=async()=>current;
  await assert.rejects(engine.run.prepare({...rolloutArgs,candidateImage:rolloutArgs.originalImage,universalPolicy:handle,universalWitnessId:'x'.repeat(32)}));assert.equal(engine.events.some(x=>x.startsWith('create:')||x.startsWith('stop:')),false);
 }
});
