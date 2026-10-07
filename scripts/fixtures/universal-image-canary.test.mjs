import test from 'node:test';
import assert from 'node:assert/strict';
import { mkdtemp, mkdir, rm, writeFile } from 'node:fs/promises';
import { tmpdir } from 'node:os';
import path from 'node:path';
import { randomBytes, generateKeyPairSync } from 'node:crypto';
import { DatabaseSync } from 'node:sqlite';
import { CANARY_BASE,CANARY_SOURCE,CANARY_REVISION,OLD_IMAGE,CANARY_BASELINE_IMAGE,CANARY_FEATURE_IMAGE,CANARY_LABEL,checkedCanaryOptions,parseControllerArgs,
  childConfiguration,checkedImage,checkedOwnedContainer,parseNativeTap,canaryHarnessManifest,canaryHarnessDigest } from './universal-image-canary.mjs';
import { sealCanaryPayload,openCanaryPayload,CANARY_EVIDENCE_QUERIES,snapshotCanaryTree,createSyntheticActorSeed,hydrateSyntheticActorSeed,checkedCanaryRestoreFiles } from './universal-canary-runtime.mjs';
import { currentStorageReaders,assertStorageCompatible } from '../../deploy/connector/storage-guard.mjs';
import { createHttpApp } from '../../server/http-app.js';
import { createRoomStore } from '../../server/room-store.js';
import { createNotesService } from '../../modules/notes/server/index.mjs';
import { createCapabilitiesService } from '../../modules/capabilities/server/index.mjs';
import { emptyState,validateState } from '../../modules/connect/browser/storage.mjs';
import { publicJwk,signingDeviceId,sealLocalRoot,verifyInstallation,openLocalRoot } from '../../modules/connect/browser/crypto.mjs';

const nonce='1'.repeat(32),baselineImage=CANARY_BASELINE_IMAGE,featureImage=CANARY_FEATURE_IMAGE;
const options={ownedRoot:CANARY_BASE+'/canary-'+nonce,fixtureSource:CANARY_SOURCE,fixtureHarness:CANARY_BASE+'/harness-'+nonce,harnessSha256:'7'.repeat(64),revision:CANARY_REVISION,oldImage:OLD_IMAGE,baselineImage,featureImage};
const fails=code=>error=>error.code===code;
test('only exact reviewed namespace, revision, distinct immutable images and matching nonce are admitted',()=>{
  const captured=checkedCanaryOptions(options);assert.equal(captured.nonce,nonce);assert.equal(checkedCanaryOptions(captured).nonce,nonce);
  for(const change of [{ownedRoot:CANARY_BASE+'/canary-'+nonce+'/../real-data'},{ownedRoot:'/data/canary-'+nonce},{fixtureSource:CANARY_BASE+'/source-0928839'},
    {revision:'0'.repeat(40)},{fixtureHarness:CANARY_SOURCE},{harnessSha256:'unknown'},{oldImage:baselineImage},{nonce:'4'.repeat(32)},{featureImage:baselineImage},{featureImage:'latest'},{shell:'arbitrary'}])assert.throws(()=>checkedCanaryOptions({...options,...change}));
  const args=['--owned-root',options.ownedRoot,'--fixture-source',CANARY_SOURCE,'--fixture-harness',options.fixtureHarness,'--harness-sha256',options.harnessSha256,'--baseline-image',baselineImage,'--feature-image',featureImage,'--old-image',OLD_IMAGE,'--expected-revision',CANARY_REVISION];
  assert.equal(parseControllerArgs(args).revision,CANARY_REVISION);assert.throws(()=>parseControllerArgs([...args,'--owned-root',options.ownedRoot]),fails('canary_arguments_invalid'));
  assert.throws(()=>parseControllerArgs([...args,'--command','sh']),fails('canary_arguments_invalid'));
});
test('child mounts use exact host paths, isolated data, no ports, no Docker socket or inherited configuration',()=>{
  const volume='soty-universal-canary-'+nonce+'-data',body=childConfiguration(options,{role:'feature-one',image:featureImage,volume,mode:'feature'});
  assert.equal(body.Entrypoint[0],'node');assert.equal(body.Cmd[0],options.fixtureHarness+'/universal-canary-runtime.mjs');
  assert.equal(body.HostConfig.NetworkMode,'none');assert.equal(body.HostConfig.ReadonlyRootfs,true);assert.equal(body.HostConfig.Mounts[0].Source,CANARY_SOURCE);
  assert.equal(body.HostConfig.Mounts[0].Target,CANARY_SOURCE);assert.equal(body.HostConfig.Mounts[0].ReadOnly,true);assert.equal(body.HostConfig.Mounts[1].Source,options.fixtureHarness);assert.equal(body.HostConfig.Mounts[1].ReadOnly,true);assert.equal(body.HostConfig.Mounts[3].Source,volume);
  assert.equal(body.HostConfig.Mounts.some(value=>value.Target.includes('docker.sock')),false);assert.equal(Object.hasOwn(body.HostConfig,'PortBindings'),false);
  assert.equal(body.Env.some(value=>/PASSWORD|TOKEN|PRIVATE|HUMAN_IDENTITY_CONFIG/u.test(value)),false);
  const legacy=childConfiguration(options,{role:'baseline-one',image:baselineImage,volume,mode:'baseline'});assert.ok(legacy.Env.includes('SOTY_UNIVERSAL_OPERATOR_ENABLED=1'));
  assert.ok(legacy.Env.includes('SOTY_UNIVERSAL_APPS_ENABLED=false'));assert.equal(legacy.Env.some(value=>value.startsWith('SOTY_HUMAN')),false);
  assert.throws(()=>childConfiguration(options,{role:'feature-one',image:featureImage,volume:'production'}),fails('canary_volume_invalid'));
  assert.throws(()=>childConfiguration(options,{role:'feature-one',image:featureImage,volume,networkContainer:'serving'}),fails('canary_network_identity'));
});
test('reviewed harness is a separate closed source packet; its manifest cannot masquerade as the application Git archive',()=>{
  const files=['universal-canary-bff.mjs','universal-canary-runtime.mjs','universal-image-canary.mjs'].map((file,index)=>({path:file,sha256:String(index+1).repeat(64)}));
  const manifest=canaryHarnessManifest(files);assert.equal(manifest.schema,'soty.synthetic-image-harness.v1');assert.equal(manifest.applicationRevision,CANARY_REVISION);
  assert.equal(canaryHarnessDigest(manifest).length,64);assert.throws(()=>canaryHarnessManifest([...files,{path:'secret.env',sha256:'1'.repeat(64)}]),fails('canary_harness_manifest_invalid'));
  assert.throws(()=>canaryHarnessDigest({...manifest,applicationRevision:'0'.repeat(40)}),fails('canary_harness_manifest_invalid'));
  const changed=structuredClone(files);changed[0].sha256='9'.repeat(64);assert.notEqual(canaryHarnessDigest(canaryHarnessManifest(changed)),canaryHarnessDigest(manifest));
});
test('immutable image mode/revision/reader metadata and exact container ownership are checked',()=>{
  const image={Id:featureImage,Config:{Labels:{'org.opencontainers.image.revision':CANARY_REVISION,'io.soty.universal.legacy':'0','io.soty.storage.readers':currentStorageReaders}}};
  assert.equal(checkedImage(image,{id:featureImage,legacy:0}),featureImage);
  assert.throws(()=>checkedImage(image,{id:featureImage,legacy:1}),fails('canary_image_pin_mismatch'));
  assert.throws(()=>checkedImage(image,{id:baselineImage,legacy:0}),fails('canary_image_pin_mismatch'));
  const wrongReader=structuredClone(image),readers=JSON.parse(currentStorageReaders);readers.readers.humanIdentity=[1];wrongReader.Config.Labels['io.soty.storage.readers']=JSON.stringify(readers);
  assert.throws(()=>checkedImage(wrongReader,{id:featureImage,legacy:0}),fails('canary_human_reader2_required'));
  const apps6=structuredClone(image),priorReaders=JSON.parse(currentStorageReaders);priorReaders.readers.apps=[1,2,3,4,5,6];apps6.Config.Labels['io.soty.storage.readers']=JSON.stringify(priorReaders);
  assert.throws(()=>checkedImage(apps6,{id:featureImage,legacy:0}),fails('canary_apps_reader7_required'));
  const record={id:'4'.repeat(64),name:'owned',image:featureImage,nonce,role:'feature-one'};
  const container={Id:record.id,Name:'/owned',Image:featureImage,Config:{Labels:{[CANARY_LABEL]:nonce,[CANARY_LABEL+'.role']:record.role,[CANARY_LABEL+'.revision']:CANARY_REVISION}}};
  assert.equal(checkedOwnedContainer(container,record),container);assert.throws(()=>checkedOwnedContainer({...container,Image:baselineImage},record),fails('canary_container_identity'));
  assert.throws(()=>checkedOwnedContainer(container,{...record,nonce:'5'.repeat(32)}),fails('canary_container_identity'));
});
test('old reader3 refuses the real seven-store vector independently of new image labels, before any START',()=>{
  const image={Id:OLD_IMAGE,Config:{Labels:{'io.soty.storage.readers':JSON.stringify({version:3,readers:{rooms:[1,2],apps:[1,2,3,4,5,6],notes:[1,2],capabilities:[1,2,3]}})}}};
  const seven={ok:true,schema:'soty.storage-format.v5',rooms:2,apps:7,notes:2,capabilities:3,appRegistration:1,feedback:1,humanIdentity:2};
  assert.throws(()=>assertStorageCompatible(image,seven),fails('storage_reader_incompatible'));
  const apps6={Id:'sha256:'+'8'.repeat(64),Config:{Labels:{'io.soty.storage.readers':JSON.stringify({version:5,readers:{rooms:[1,2],apps:[1,2,3,4,5,6],notes:[1,2],capabilities:[1,2,3],appRegistration:[1],feedback:[1],humanIdentity:[1,2]}})}}};
  assert.throws(()=>assertStorageCompatible(apps6,seven),fails('storage_reader_incompatible'));
});
test('private ciphertext is authenticated to exact synthetic nonce, purpose and externally retained custody key',()=>{
  const key=randomBytes(32),value={marker:'synthetic-private-value',consumed:true},sealed=sealCanaryPayload(value,key,nonce,'coherent-backup');
  assert.equal(sealed.includes(Buffer.from(value.marker)),false);assert.equal(openCanaryPayload(sealed,key,nonce,'coherent-backup').consumed,true);
  const changed=Buffer.from(sealed);changed[changed.length-1]^=1;
  assert.throws(()=>openCanaryPayload(changed,key,nonce,'coherent-backup'));assert.throws(()=>openCanaryPayload(sealed,randomBytes(32),nonce,'coherent-backup'));
  assert.throws(()=>openCanaryPayload(sealed,key,'6'.repeat(32),'coherent-backup'));assert.throws(()=>openCanaryPayload(sealed,key,nonce,'state:actor'));
  assert.throws(()=>sealCanaryPayload(value,key,'../outside','coherent-backup'),fails('canary_custody_invalid'));
});
test('synthetic JWK custody roundtrip imports nonextractable keys and the actual Connect vault remains readable',async()=>{
  const seed=await createSyntheticActorSeed({emptyState,publicJwk,signingDeviceId,sealLocalRoot}),key=randomBytes(32);
  const cipher=sealCanaryPayload(seed,key,nonce,'state:actor'),restored=openCanaryPayload(cipher,key,nonce,'state:actor'),state=await hydrateSyntheticActorSeed(restored);
  validateState(state,{projectId:'soty',endpoint:'http://127.0.0.1:8080/api/connect/rpc'});const installation=state.installations[0];
  assert.equal(installation.signingPrivateKey.extractable,false);assert.equal(installation.encryptionPrivateKey.extractable,false);assert.equal(installation.storageKey.extractable,false);
  assert.equal(await verifyInstallation(installation),undefined);const root=await openLocalRoot(installation,'soty');assert.equal(root.length,32);root.fill(0);
  assert.equal(Object.hasOwn(installation.signingPublicJwk,'d'),false);
});
test('native gate cannot report seven Linux cases on skipped/failing/partial or unbounded output',()=>{
  const valid=Buffer.from('TAP version 13\n# tests 9\n# pass 8\n# fail 0\n# skipped 1\n');assert.equal(parseNativeTap(valid).nativeTests,7);
  for(const text of ['# tests 9\n# pass 1\n# fail 0\n# skipped 8\n','# tests 9\n# pass 7\n# fail 1\n# skipped 1\n',valid.toString()+'not ok 1 - failure\n',''])assert.throws(()=>parseNativeTap(Buffer.from(text)),fails('canary_native_tests_failed'));
  assert.throws(()=>parseNativeTap(Buffer.alloc(256*1024+1)),fails('canary_native_output_limit'));
});
test('backup traversal is bounded by depth and aggregate directory count before reading an unbounded tree',async t=>{
  const directory=await mkdtemp(path.join(tmpdir(),'soty-canary-tree-'));t.after(async()=>{assert.equal(path.dirname(directory),path.resolve(tmpdir()));assert.match(path.basename(directory),/^soty-canary-tree-/u);await rm(directory,{recursive:true,force:true});});
  const deep=path.join(directory,'deep');await mkdir(deep);let current=deep;for(let index=0;index<9;index++){current=path.join(current,'level');await mkdir(current);}
  await assert.rejects(snapshotCanaryTree(deep,'data'),fails('canary_backup_directory_limit'));
  const wide=path.join(directory,'wide');await mkdir(wide);for(let index=0;index<65;index++)await mkdir(path.join(wide,'d'+index));
  await assert.rejects(snapshotCanaryTree(wide,'private'),fails('canary_backup_directory_limit'));
});
test('entire authenticated archive is validated before writes, including closed config, traversal, depth and file-directory collisions',()=>{
  const configs=['fixture-keys.json','synthetic-human.json','clock.json'].map(file=>({namespace:'configuration',path:file,bytes:Buffer.from('{}').toString('base64')}));
  const valid={schema:'soty.synthetic-cold-backup.v1',files:[...configs,{namespace:'data',path:'notes/notes.sqlite',bytes:'AA=='}]};
  assert.equal(checkedCanaryRestoreFiles(valid).length,4);
  for(const bad of [{namespace:'configuration',path:'backup-custody.key',bytes:'AA=='},{namespace:'data',path:'../outside',bytes:'AA=='},
    {namespace:'data',path:Array(11).fill('deep').join('/'),bytes:'AA=='},{namespace:'data',path:'file',bytes:'AA'}])assert.throws(()=>checkedCanaryRestoreFiles({...valid,files:[...valid.files,bad]}));
  assert.throws(()=>checkedCanaryRestoreFiles({...valid,files:[...valid.files,{namespace:'data',path:'notes',bytes:'AA=='}]}),fails('canary_restore_path'));
  assert.throws(()=>checkedCanaryRestoreFiles({...valid,files:valid.files.slice(1)}),fails('canary_restore_configuration'));
});
test('every bounded evidence query prepares against actual Root seven-store constructor schemas',async t=>{
  const directory=await mkdtemp(path.join(tmpdir(),'soty-canary-query-')),dataDir=path.join(directory,'data'),dist=path.join(directory,'dist');await mkdir(dist);await mkdir(dataDir);
  await writeFile(path.join(dist,'index.html'),'<!doctype html><title>Synthetic</title>');let app,rooms;
  t.after(async()=>{await app?.locals.closeServices();rooms?.close();assert.equal(path.dirname(directory),path.resolve(tmpdir()));assert.match(path.basename(directory),/^soty-canary-query-/u);await rm(directory,{recursive:true,force:true,maxRetries:3,retryDelay:100});});
  const jwk=generateKeyPairSync('rsa',{modulusLength:2048}).privateKey.export({format:'jwk'});Object.assign(jwk,{kid:'synthetic',alg:'RS256',use:'sig'});
  const origin='http://127.0.0.1:8080',humanIdentity={enabled:true,issuer:origin+'/human-identity',registryId:'soty',environmentId:'production',
    clients:[{id:'canary-alpha',label:'Synthetic',redirectUri:'http://127.0.0.1:8082/oidc/callback',clientSecret:randomBytes(32).toString('base64url'),version:2}],
    jwks:{keys:[jwk]},cookieKeys:[randomBytes(32).toString('base64url')],artifactKey:randomBytes(32),artifactKeyId:'synthetic',renewal:{admissionEnabled:true,clientIds:['canary-alpha']}};
  createNotesService({databasePath:path.join(dataDir,'notes','notes.sqlite'),projectId:'soty',allowNativeMigration:true}).close();
  createCapabilitiesService({databasePath:path.join(dataDir,'capabilities','capabilities.sqlite'),projectId:'soty',allowNativeMigration:true,actorActive:()=>false}).close();
  createCapabilitiesService({databasePath:path.join(dataDir,'capabilities','capabilities.sqlite'),projectId:'soty',allowOAuthMigration:true,actorActive:()=>false}).close();
  app=createHttpApp(dist,{dataDir,connectOrigins:[origin],appHosting:{},appOriginTemplate:'http://{appId}.localhost:8080',namedAppZone:'',discoveryOrigin:'',capabilityAudience:origin,nativeNotesEnabled:true,
    universalAppsEnabled:true,humanIdentity,humanIdentityRenewalMigration:true,allowScopedEmbedMigration:true,scopedEmbedRegistryConfigured:true});rooms=createRoomStore(dataDir);
  for(const [store,[file,queries]]of Object.entries(CANARY_EVIDENCE_QUERIES)){const db=new DatabaseSync(path.join(dataDir,file),{readOnly:true});try{for(const query of Object.values(queries)){
    assert.match(query,/^SELECT /u);assert.match(query,/ LIMIT [0-9]+$/u);assert.ok(db.prepare(query).all().length<=16,'bounded '+store+' query');}}
    finally{db.close();}}
});
