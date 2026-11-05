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
  try{const measured=app.locals.captureUniversalPreparedness();assert.deepEqual(measured.monitoring,{mode,schemaVersion:1,installed:mode==='writer'||Boolean(previous)});if(!previous&&mode==='reader')await assert.rejects(stat(file),{code:'ENOENT'});if(previous&&mode==='reader')assert.deepEqual(await readFile(file),previous);}finally{await app.locals.closeServices();}
  if(mode==='writer'){previous=await readFile(file);assert.equal((await readStorageFormat(data)).monitoring,1);}
 }
 assert.deepEqual(await readFile(file),previous);
});
