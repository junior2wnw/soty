import test from 'node:test';
import assert from 'node:assert/strict';
import { mkdtemp, writeFile, readFile, rm, realpath } from 'node:fs/promises';
import { tmpdir } from 'node:os';
import path from 'node:path';
import { createHash, randomBytes, generateKeyPairSync, publicEncrypt, createCipheriv, constants } from 'node:crypto';
import { fixture as engineFixture, args } from './test-support/rollout-engine.mjs';
import { createConfig, hash, preservationHash } from './rollout.mjs';
import { prepareUniversalPolicy, disposeUniversalPolicy, captureUniversalPreparedness, universalModeLabel,
  applyUniversalPolicy, publicUniversalPolicy, assertUniversalPolicyCurrent, bindUniversalRollout, sealUniversalPolicy } from './universal-policy.mjs';
import { loadInstalledApplicationsEntry } from '../../server/installed-applications-entry.js';
import { STANDARD_ROOT_WORKSPACE_SOURCE } from '../../modules/source-app/server/standard-profile.mjs';
import { prepareInstalledModuleRotation, stageInstalledModuleRotation, admitInstalledModuleRotation,
  restoreInstalledModuleOriginal, markInstalledModuleRestartSuspended, disposeInstalledModuleRotation } from './installed-module-rotation.mjs';

const sha = value => createHash('sha256').update(value).digest('hex');
function scope(character) { const appId='app-'+character.repeat(32);return { appId,target:{revision:2,digest:'5'.repeat(64)},sourceProfile:STANDARD_ROOT_WORKSPACE_SOURCE,
 resource:{registryId:'root-apps',environmentId:'production',tenantId:'synthetic-owner',appId,resourceId:'workspace-'+character,
 selection:{kind:'soty.workspace.v1',nativeId:'workspace-'+character,incarnationId:'incarnation-'+character}} }; }
const scopes=[scope('1'),scope('2'),scope('3')];
const code = added => Buffer.from('const scopes='+JSON.stringify(scopes)+';const extra='+JSON.stringify(scope('4'))+';\nexport default async function(api){const rows=scopes.map(scope=>api.createCurrentContextSourceApplication(scope));'
 +(added?'rows.push(...api.createSelectedOtsovSourceApplications(extra));':'')+'const composition=await api.loadInstalledExistingApplications({createRegistry:()=>rows});return {...composition,close(){}};}\n');
function encryptOriginal(bytes) { const recipient=generateKeyPairSync('rsa',{modulusLength:2048}).publicKey,key=randomBytes(32),iv=randomBytes(12);
 const header={schema:'soty.root-encrypted-backup.v1',algorithm:'RSA-OAEP-SHA256/AES-256-GCM',keyId:sha(recipient.export({type:'spki',format:'der'})),
 key:publicEncrypt({key:recipient,padding:constants.RSA_PKCS1_OAEP_PADDING,oaepHash:'sha256'},key).toString('base64'),iv:iv.toString('base64')};
 const cipher=createCipheriv('aes-256-gcm',key,iv);cipher.setAAD(Buffer.from(JSON.stringify(Object.fromEntries(Object.entries(header).sort()))));
 return Buffer.from(JSON.stringify({header,ciphertext:Buffer.concat([cipher.update(bytes),cipher.final(),cipher.getAuthTag()]).toString('base64')})); }
async function fixture(t,fault={}) {
 const root=await mkdtemp(path.join(await realpath(tmpdir()),'soty-universal-policy-installed-rotation-')),source=path.join(root,'installed.mjs');
 const oldBytes=code(false),nextBytes=code(true),f=engineFixture(fault),policies=[],handles=[];
 const options={fixtureRoot:root,shellOrigins:['https://soty.fixture.invalid']};await writeFile(source,oldBytes,{mode:0o600});
 const current=captureUniversalPreparedness({compiledLegacyMode:false,universalConfigured:true,reviewsConfigured:true,humanProfile:null,humanHttpEnabled:false});
 const oldPlan={schema:'soty.universal-rollout-policy.v1',phase:'features',human:null,reviews:null,
 expectedCurrentPreparednessSha256:hash(current),installedApplications:{source,sha256:sha(oldBytes)}};
 const oldPolicy=await prepareUniversalPolicy(oldPlan,options);policies.push(oldPolicy);
 const old=f.map.get(args.originalId);old.HostConfig.Binds=[];old.HostConfig.Mounts=Array.from({length:6},(_,i)=>({Type:'bind',Source:'/synthetic/preserved-'+i,Target:i===0?'/data':'/run/preserved-'+i,ReadOnly:i!==0}));
 old.HostConfig.Mounts.push({Type:'bind',Source:source,Target:'/run/config/soty-installed-applications.mjs',ReadOnly:true});
 old.HostConfig.Mounts[0]={Type:'volume',Source:'synthetic-data',Target:'/data',ReadOnly:false,VolumeOptions:{NoCopy:true}};
 old.Mounts=old.HostConfig.Mounts.map(m=>({Type:m.Type,...(m.Type==='volume'?{Name:m.Source}:{}),Source:m.Type==='volume'?'/docker/volumes/'+m.Source:m.Source,Destination:m.Target,RW:!m.ReadOnly}));
 const configured=createConfig(old,args.originalImage,args.transaction,args.revision,undefined,oldPolicy);old.Config.Env=configured.Env;old.HostConfig=configured.HostConfig;
 const oldEntry=await loadInstalledApplicationsEntry({SOTY_INSTALLED_APPLICATIONS_MODULE:source,SOTY_INSTALLED_APPLICATIONS_SHA256:sha(oldBytes)});const oldRefs=oldEntry.sourceReferences;await oldEntry.close();
 const probe=path.join(root,'metadata-only.mjs');await writeFile(probe,nextBytes,{mode:0o600});const nextEntry=await loadInstalledApplicationsEntry({SOTY_INSTALLED_APPLICATIONS_MODULE:probe,SOTY_INSTALLED_APPLICATIONS_SHA256:sha(nextBytes)});const nextRefs=nextEntry.sourceReferences;await nextEntry.close();
 const key=randomBytes(32),custody={key,keyId:'synthetic-rotation-custody'};
 bindUniversalRollout(oldPolicy,{preservationFingerprint:preservationHash(createConfig(old,args.originalImage,args.transaction,args.revision,undefined,oldPolicy)),originalId:args.originalId,originalImage:args.originalImage,candidateImage:args.originalImage,revision:args.revision,transaction:args.transaction});
 const oldWitness=await sealUniversalPolicy(oldPolicy,custody);
 const encrypted=encryptOriginal(oldBytes),snapshot=structuredClone(old),ordered=structuredClone(old.Mounts).sort((a,b)=>a.Destination.localeCompare(b.Destination));
 const input={original:snapshot,source,expectedEnvironmentSha256:sha(Buffer.from(JSON.stringify(old.Config.Env))),expectedMountsSha256:sha(Buffer.from(JSON.stringify(ordered))),
 expectedOldSha256:sha(oldBytes),approvedNewSha256:sha(nextBytes),approvedModule:nextBytes,expectedOldReferences:oldRefs,approvedReferences:nextRefs,oldPolicy,options,
 restoreOldPolicy:async()=>{const restored=await prepareUniversalPolicy(oldPlan,options);policies.push(restored);return restored;},
 encryptedRollback:encrypted,encryptedRollbackSha256:sha(encrypted),rollbackFile:path.join(root,'original-module.encrypted.json')};
 const image=f.engine.image;f.engine.image=async id=>{const result=await image(id);result.Config.Labels[universalModeLabel]='0';return result;};
 f.run.allowUniversalFixture=true;f.run.universalMeasurement=async()=>current;
 t.after(async()=>{for(const handle of handles)disposeInstalledModuleRotation(handle);for(const policy of policies)try{disposeUniversalPolicy(policy);}catch{}
 assert.equal(path.dirname(root),await realpath(tmpdir()));assert.match(path.basename(root),/^soty-universal-policy-installed-rotation-/);await rm(root,{recursive:true,force:true});});
 async function stage(change={}) { const rotation=await prepareInstalledModuleRotation({...input,...change});handles.push(rotation);
 await f.run.guard({...args,candidateImage:args.originalImage,universalPolicy:oldPolicy});
 await f.run.restartPolicy(args.originalId,{Name:'no',MaximumRetryCount:0});markInstalledModuleRestartSuspended(rotation,await f.engine.inspect(args.originalId));
 try { await stageInstalledModuleRotation(rotation);
 const nextPolicy=await prepareUniversalPolicy({...oldPlan,installedApplications:{source,sha256:sha(nextBytes)}},options);policies.push(nextPolicy);await admitInstalledModuleRotation(rotation,nextPolicy);
 f.run.installedModuleRotation=rotation;return {rotation,request:{...args,candidateImage:args.originalImage,universalPolicy:nextPolicy,universalWitnessId:randomBytes(24).toString('base64url')},nextPolicy}; }
 catch(error){await restoreInstalledModuleOriginal(rotation);await f.run.restartPolicy(args.originalId,snapshot.HostConfig.RestartPolicy);throw error;} }
 return {...f,source,root,oldBytes,nextBytes,snapshot,input,stage,policies,handles,custody,oldWitness};
}

test('explicit one-key rotation uses real policy/createConfig and retains the measured original full Env/7mounts',async t=>{
 const f=await fixture(t),{request,nextPolicy}=await f.stage();
 assert.throws(()=>applyUniversalPolicy({Env:f.snapshot.Config.Env,HostConfig:f.snapshot.HostConfig},nextPolicy),{code:'universal_policy_preexisting_configuration'});
 await f.run.prepare(request);const newWitness=await sealUniversalPolicy(nextPolicy,f.custody);assert.notEqual(newWitness.policyDigest,f.oldWitness.policyDigest);assert.equal(f.oldWitness.packet.includes(f.oldBytes),false);assert.equal(newWitness.packet.includes(f.nextBytes),false);assert.deepEqual(f.run.original.Config.Env,f.snapshot.Config.Env);assert.equal(f.run.config.Env.length,f.snapshot.Config.Env.length);
 const expected=f.snapshot.Config.Env.map(v=>v==='SOTY_INSTALLED_APPLICATIONS_SHA256='+sha(f.oldBytes)?'SOTY_INSTALLED_APPLICATIONS_SHA256='+sha(f.nextBytes):v);
 assert.deepEqual(f.run.config.Env,expected);assert.deepEqual(f.run.config.HostConfig.Mounts,f.snapshot.HostConfig.Mounts);
 assert.deepEqual(f.run.config.HostConfig.RestartPolicy,f.snapshot.HostConfig.RestartPolicy);
 assert.deepEqual(f.map.get(args.originalId).HostConfig.RestartPolicy,{Name:'no',MaximumRetryCount:0});
 await f.run.promote();assert.equal(f.run.state.phase,'committed');assert.equal(f.map.get(args.originalId).State.Running,false);
 assert.equal(sha(await readFile(f.source)),sha(f.nextBytes));const backup=await readFile(f.input.rollbackFile);assert.equal(backup.includes(f.oldBytes),false);
});
test('wrong old hash rejects before module write, CREATE or STOP',async t=>{
 const f=await fixture(t);await assert.rejects(f.stage({expectedOldSha256:'0'.repeat(64)}),{code:'installed_module_rotation_old_sha'});
 assert.deepEqual(f.events,[]);assert.equal(sha(await readFile(f.source)),sha(f.oldBytes));
});
test('failed candidate READY restores byte-exact module and OLD proof before retained OLD container START',async t=>{
 const f=await fixture(t,{op:'readiness',when:'before'}),{request,nextPolicy}=await f.stage();await f.run.prepare(request);
 const start=f.engine.start;let observed=false;f.engine.start=async id=>{if(id===args.originalId){observed=true;
 assert.equal(sha(await readFile(f.source)),sha(f.oldBytes));assert.deepEqual(f.map.get(id).Config.Env,f.snapshot.Config.Env);
 assert.deepEqual(f.map.get(id).HostConfig.RestartPolicy,f.snapshot.HostConfig.RestartPolicy);
 // Candidate proof is intentionally invalid after restoration; OLD proof is separate.
 await assert.rejects(assertUniversalPolicyCurrent(nextPolicy),{code:'universal_policy_file_changed'});}
 return start(id);};
 await assert.rejects(f.run.promote());assert.equal(observed,true);assert.equal(f.run.state.phase,'restored');
 assert.equal(f.map.get(args.originalId).State.Running,true);assert.equal(f.map.get(f.run.candidate.Id).State.Running,false);
 assert.equal(sha(await readFile(f.source)),sha(f.oldBytes));assert.equal(f.events.filter(e=>e==='start-old').length,1);
});

for(const [name,change,expectedCode] of [
 ['wrong-new',f=>({approvedNewSha256:'0'.repeat(64)}),'installed_module_rotation_new_sha'],
 ['wrong-path',f=>({source:path.join(f.root,'foreign.mjs')}),'installed_module_rotation_path'],
 ['other-env',f=>{const original=structuredClone(f.snapshot);original.Config.Env[1]='TOKEN=unapproved-value';return {original};},'installed_module_rotation_environment_cas'],
 ['other-mount',f=>{const original=structuredClone(f.snapshot);original.Mounts[1].Source='/synthetic/unapproved';return {original};},'installed_module_rotation_mounts'],
 ['wrong-old-ref',f=>({expectedOldReferences:[{...f.input.expectedOldReferences[0],digest:'0'.repeat(64)}]}),'installed_module_rotation_refs'],
 ['wrong-new-ref',f=>({approvedReferences:f.input.approvedReferences.slice(0,-1)}),'installed_module_rotation_refs']
])test(name+' fails without CREATE/STOP and retains original CODE',async t=>{
 const f=await fixture(t);await assert.rejects(f.stage(change(f)),{code:expectedCode});
 assert.equal(f.events.some(e=>e.startsWith('create:')||e.startsWith('stop:')),false);assert.equal(sha(await readFile(f.source)),sha(f.oldBytes));assert.equal(f.map.get(args.originalId).State.Running,true);assert.deepEqual(f.map.get(args.originalId).HostConfig.RestartPolicy,f.snapshot.HostConfig.RestartPolicy);
});
test('a changed full original Env after prepare is refused before STOP and the paired module is restored',async t=>{
 const f=await fixture(t),{request}=await f.stage();await f.run.prepare(request);
 f.map.get(args.originalId).Config.Env[1]='TOKEN=changed-after-prepare';
 await assert.rejects(f.run.promote(),{code:'recovery_required'});
 assert.equal(f.events.some(e=>e.startsWith('stop:')),false);assert.equal(f.run.state.phase,'recovery_required');
 assert.equal(sha(await readFile(f.source)),sha(f.oldBytes));assert.equal(f.map.get(args.originalId).State.Running,true);
});

test('rotation cannot silently upgrade the runtime image',async t=>{
 const f=await fixture(t),{request}=await f.stage();await assert.rejects(f.run.prepare({...request,candidateImage:args.candidateImage}),{code:'installed_module_rotation_image'});
 assert.equal(f.events.some(e=>e.startsWith('create:')||e.startsWith('stop:')),false);await restoreInstalledModuleOriginal(f.run.installedModuleRotation);
 assert.equal(sha(await readFile(f.source)),sha(f.oldBytes));
});

test('CODE staging refuses an unsuspended original restart policy',async t=>{
 const f=await fixture(t),rotation=await prepareInstalledModuleRotation(f.input);f.handles.push(rotation);
 await assert.rejects(stageInstalledModuleRotation(rotation),{code:'installed_module_rotation_restart_policy'});
 assert.equal(sha(await readFile(f.source)),sha(f.oldBytes));assert.deepEqual(f.events,[]);
});

test('a pre-STOP active-job refusal restores original CODE/proof/restart policy without restarting the original',async t=>{
 const f=await fixture(t,{active:true}),{request}=await f.stage();await f.run.prepare(request);
 await assert.rejects(f.run.promote(),{code:'precheck_not_quiescent'});
 assert.equal(f.run.state.phase,'aborted');assert.equal(sha(await readFile(f.source)),sha(f.oldBytes));
 assert.deepEqual(f.map.get(args.originalId).HostConfig.RestartPolicy,f.snapshot.HostConfig.RestartPolicy);
 assert.equal(f.events.some(e=>e.startsWith('stop:')||e==='start-old'),false);
});

test('a concurrent HostConfig change between staging and CREATE is rejected against the original full snapshot',async t=>{
 const f=await fixture(t),{rotation,request}=await f.stage();f.map.get(args.originalId).HostConfig.Memory+=1;
 await assert.rejects(f.run.prepare(request),{code:'installed_module_rotation_original'});
 assert.equal(f.events.some(e=>e.startsWith('create:')||e.startsWith('stop:')),false);
 await restoreInstalledModuleOriginal(rotation);assert.equal(sha(await readFile(f.source)),sha(f.oldBytes));
 assert.equal(f.map.get(args.originalId).State.Running,true);
});

test('canonical property order admission accepts reordered JSON reference keys without publishing anything',async t=>{
 const f=await fixture(t),permute=value=>Array.isArray(value)?value.map(permute):value&&typeof value==='object'?Object.fromEntries(Object.keys(value).reverse().map(k=>[k,permute(value[k])])):value;
 const result=await prepareInstalledModuleRotation({...f.input,expectedOldReferences:permute(f.input.expectedOldReferences),approvedReferences:permute(f.input.approvedReferences),validateOnly:true});
 assert.equal(result.validated,true);assert.equal(result.approvedReferences,6);assert.deepEqual(f.events,[]);
 await assert.rejects(readFile(f.input.rollbackFile),{code:'ENOENT'});assert.equal(sha(await readFile(f.source)),sha(f.oldBytes));
});

test('canonical property order admission still denies a changed reference field and changed array order',async t=>{
 const f=await fixture(t),refs=structuredClone(f.input.expectedOldReferences);refs[0].digest='0'.repeat(64);
 await assert.rejects(prepareInstalledModuleRotation({...f.input,expectedOldReferences:refs,validateOnly:true}),{code:'installed_module_rotation_refs'});
 await assert.rejects(prepareInstalledModuleRotation({...f.input,expectedOldReferences:[...f.input.expectedOldReferences].reverse(),validateOnly:true}),{code:'installed_module_rotation_refs'});
 assert.deepEqual(f.events,[]);await assert.rejects(readFile(f.input.rollbackFile),{code:'ENOENT'});
});
