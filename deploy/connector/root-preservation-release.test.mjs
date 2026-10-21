import test from 'node:test';
import assert from 'node:assert/strict';
import { spawnSync } from 'node:child_process';
import { fileURLToPath } from 'node:url';
import { cloneConfiguration,sourceFence,sameObservation,assertRootDockerTransport,privateBindFence,rootDockerEngine,restoredBinds } from './root-preservation-runtime.mjs';
import { original,args } from './test-support/rollout-engine.mjs';
import { createSyntheticRootFixture } from './root-preservation-fixture.mjs';
import { mkdtemp,rm,writeFile,symlink,readFile } from 'node:fs/promises';
import { createHash } from 'node:crypto';
import { tmpdir } from 'node:os';
import path from 'node:path';
import { DatabaseSync } from 'node:sqlite';
import { migrateAppsSchema } from '../../modules/apps/server/schema.mjs';
import { migrateWorld } from '../../modules/world/server/schema.mjs';
import { migrateField } from '../../modules/world/server/field.mjs';
import { observeRootState } from './root-state-observer.mjs';
import { readStorageFormat } from './storage-probe.mjs';
import { assertStorageCompatible,currentStorageReaders,storageReaderLabel } from './storage-guard.mjs';
import { mkdir } from 'node:fs/promises';
import { migrateWorld as migrateWorld3 } from '../../modules/world/test/support/legacy-world-schema-v3.mjs';
import { materializeRootPreservationStage } from './root-preservation-delivery.mjs';
import { validateRootPreservationPlan } from './root-preservation-release.mjs';
import { SYNTHETIC_APPLICATION_TOKENS } from './root-preservation-qualify.mjs';
import { createApplicationTokenAuthenticator } from '../../server/gonka-proxy.js';

test('disposable synthetic application-token file uses the real stock file schema',async t=>{
  const directory=await mkdtemp(path.join(tmpdir(),'soty-synthetic-tokens-'));t.after(()=>rm(directory,{recursive:true,force:true}));
  const file=path.join(directory,'applications.json');await writeFile(file,JSON.stringify(SYNTHETIC_APPLICATION_TOKENS),{mode:0o600,flag:'wx'});
  const authenticate=createApplicationTokenAuthenticator('',{filePath:file});assert.equal(authenticate.ready,true);
  const entry=SYNTHETIC_APPLICATION_TOKENS.applications[0];assert.equal(await authenticate(entry.token),entry.id);
});

test('v2 controller refuses old World3 plan schema and any caller-selected format override',()=>{
  const plan={schema:'soty.root-preservation-release.v2',...args,transaction:'f'.repeat(32),helperImage:args.storageProbeImage,
    expectedConfigurationSha256:'1'.repeat(64),stockManifestSha256:'1b6e85270a7d4d42cd077fd89fd6ed6a10b802e27b337a8f377499d5907b5b2f',
    sourcePins:Object.fromEntries(['root-preservation-release.mjs','root-preservation-runtime.mjs','root-preservation-observe-runner.mjs','root-state-observer.mjs'].map(name=>[name,'2'.repeat(64)])),directory:path.resolve('fixture-run'),dockerSocket:'/fixture/socket'};
  assert.doesNotThrow(()=>validateRootPreservationPlan(plan));
  for(const invalid of [{...plan,schema:'soty.root-preservation-release.v1'},{...plan,formats:{world:3,apps:8}}])assert.throws(()=>validateRootPreservationPlan(invalid),error=>error.code==='release_plan_invalid');
});

test('qualification stage pins frozen stock closure and actual CURRENT World6 initializer imports',async()=>{
  const root=new URL('../../',import.meta.url),manifest=JSON.parse(await readFile(new URL('./root-preservation-qualification.manifest.json',import.meta.url),'utf8'));
  const stock=JSON.parse(await readFile(new URL('./cold-verification-stage.stock.json',import.meta.url),'utf8')),files=new Map(manifest.files.map(entry=>[entry.path,entry]));
  const newFiles=new Set([...manifest.roots,'deploy/connector/cold-verification-stage.stock.json']);
  assert.equal(manifest.stockRevision,'cbeb42586ff501899652b2010188415ce5b68d16');assert.equal(files.size,manifest.files.length);
  assert.equal(files.has('modules/world/test/support/legacy-world-schema-v3.mjs'),false);assert.ok(files.has('modules/world/server/schema.mjs'));assert.ok(files.has('modules/world/server/field.mjs'));
  for(const entry of stock.stock)assert.ok(files.has(entry.path));
  for(const entry of manifest.files){
    const raw=await readFile(new URL(entry.path,root)),bytes=process.platform==='win32'&&!newFiles.has(entry.path)?Buffer.from(raw.toString('utf8').replaceAll('\r\n','\n')):raw;
    assert.equal(bytes.length,entry.bytes,entry.path);assert.equal(createHash('sha256').update(bytes).digest('hex'),entry.sha256,entry.path);
    if(!/\.(?:mjs|js|ts)$/u.test(entry.path))continue;
    const source=bytes.toString('utf8'),imports=[...source.matchAll(/(?:^|\n)\s*(?:import\s+(?:[^;'"`]*?\s+from\s+)?|export\s+(?:\{[^}]*\}|\*)\s+from\s+)['"]([^'"]+)['"]/gu)].map(match=>match[1]);
    const assets=[...source.matchAll(/new URL\(['"]([^'"]+\.(?:mjs|js|ts|json|sql))['"]\s*,\s*import\.meta\.url\)/gu)].map(match=>match[1]);
    for(const spec of [...imports,...assets])if(spec.startsWith('.')){
      const target=path.posix.normalize(path.posix.join(path.posix.dirname(entry.path),spec));
      if(target!=='deploy/connector/root-preservation-qualification.manifest.json')assert.ok(files.has(target),entry.path+' -> '+target);
    }else assert.ok(spec.startsWith('node:')||spec==='sharp'&&manifest.externalPackages.sharp.version==='0.34.5',spec);
  }
});

test('source-only delivery materializes exact raw pins and refuses reuse of a retained stage',async t=>{
  const root=await mkdtemp(path.join(tmpdir(),'soty-world6-delivery-'));t.after(()=>rm(root,{recursive:true,force:true}));
  const directory=path.join(root,'new-stage'),receipt=await materializeRootPreservationStage(directory);
  assert.equal(receipt.ok,true);assert.equal(receipt.schema,'soty.root-preservation-delivery.v2');
  const manifest=JSON.parse(await readFile(path.join(directory,'deploy/connector/root-preservation-qualification.manifest.json'),'utf8'));
  for(const entry of [...manifest.files,...manifest.tests])assert.equal(createHash('sha256').update(await readFile(path.join(directory,entry.path))).digest('hex'),entry.sha256,entry.path);
  assert.equal(createHash('sha256').update(await readFile(path.join(directory,'deploy/connector/root-state-observer.mjs'))).digest('hex'),'2a83ee137eec457b733aac33e09151843bf72a3d43ab11ab313518dbb5f7dc34');
  await assert.rejects(materializeRootPreservationStage(directory),error=>error.code==='EEXIST');
});

test('real synthetic fixture initializes CURRENT World6/Apps14 and all seven unchanged formats',async t=>{
  const root=await mkdtemp(path.join(tmpdir(),'soty-root-nine-fixture-'));
  t.after(()=>rm(root,{recursive:true,force:true}));
  const result=await createSyntheticRootFixture(root);assert.equal(result.ok,true);assert.equal(Object.keys(result.stores).length,9);
  assert.deepEqual(result.stores,{rooms:2,apps:14,notes:2,capabilities:3,appRegistration:1,feedback:1,humanIdentity:3,connect:4,world:6});assert.equal(result.unknownCanary,true);
  const world=new DatabaseSync(path.join(root,'world','world.sqlite'));
  try{assert.equal(world.prepare("SELECT value FROM world_meta WHERE key='field_schema_version'").get().value,'1');
    assert.equal(world.prepare('SELECT count(*) AS n FROM profiles').get().n,0);assert.equal(world.prepare('SELECT count(*) AS n FROM memberships').get().n,0);
    assert.ok(world.prepare("SELECT 1 FROM sqlite_schema WHERE name='world_direct_messages'").get());
  }finally{world.close();}
  for(const [file,table] of [['feedback/feedback.sqlite','feedback_meta'],['app-registration/registry.sqlite','registration_metadata'],['human-identity/identity.sqlite','human_identity_meta']]){
    const db=new DatabaseSync(path.join(root,file),{readOnly:true});
    try{assert.equal(db.prepare(`SELECT value FROM ${table} WHERE key='registry_id'`).get().value,'soty');
      assert.equal(db.prepare(`SELECT value FROM ${table} WHERE key='environment_id'`).get().value,'production');
    }finally{db.close();}
  }
});

test('genuine World3/Apps8 stores cannot pass the unchanged actual v7 CURRENT image guard',async t=>{
  const root=await mkdtemp(path.join(tmpdir(),'soty-negative-world3-apps8-'));t.after(()=>rm(root,{recursive:true,force:true}));
  for(const store of ['world','apps'])await mkdir(path.join(root,store));
  const world=new DatabaseSync(path.join(root,'world','world.sqlite')),apps=new DatabaseSync(path.join(root,'apps','registry.sqlite'));
  try{migrateWorld3(world,'soty');migrateAppsSchema(apps,{allowScopedEmbedMigration:true,allowSelectedResourceMigration:true});}finally{world.close();apps.close();}
  const actual=await readStorageFormat(root);assert.equal(actual.world,3);assert.equal(actual.apps,8);
  assert.throws(()=>assertStorageCompatible({Id:args.candidateImage,Config:{Labels:{[storageReaderLabel]:currentStorageReaders}}},actual),error=>error.code==='storage_reader_incompatible');
});

test('normal CURRENT initializer reopen preserves every SQL table/value and nonSQL file',{skip:process.platform!=='linux'},async t=>{
  const root=await mkdtemp('/dev/shm/soty-current-reopen-'),ramRoot=await mkdtemp('/dev/shm/soty-current-observer-');
  t.after(()=>rm(root,{recursive:true,force:true}));t.after(()=>rm(ramRoot,{recursive:true,force:true}));
  await createSyntheticRootFixture(root);const before=await observeRootState({source:root,ramRoot});
  assert.equal(before.sqliteDatabases,10);
  const world=new DatabaseSync(path.join(root,'world','world.sqlite')),apps=new DatabaseSync(path.join(root,'apps','registry.sqlite'));
  try{migrateWorld(world,'soty');migrateField(world);migrateAppsSchema(apps);}finally{world.close();apps.close();}
  const after=await observeRootState({source:root,ramRoot});assert.equal(sameObservation(before,after),true);
});

test('clone configuration preserves Env/normal entrypoint but remaps every bind in original Mounts order',()=>{
  const value=original();value.Mounts.push({Type:'bind',Source:'/private/file',Destination:'/run/key',RW:false},{Type:'bind',Source:'/private/tree',Destination:'/run/config',RW:true});
  const before=structuredClone(value),config=cloneConfiguration(value,args.candidateImage,{Name:'new-data'},{Name:'new-config',Mountpoint:'/owned/config'},'1'.repeat(32),'candidate');
  assert.deepEqual(value,before);assert.deepEqual(config.Env,value.Config.Env);assert.deepEqual(config.Cmd,value.Config.Cmd);
  assert.equal(config.HostConfig.NetworkMode,'none');assert.deepEqual(config.HostConfig.PortBindings,{});
  assert.equal(config.HostConfig.LogConfig.Type,'none');assert.equal(config.HostConfig.RestartPolicy.Name,'no');assert.equal(config.HostConfig.PublishAllPorts,false);
  assert.equal(Object.hasOwn(config.HostConfig,'Binds'),false);
  assert.deepEqual(config.HostConfig.Mounts.map(m=>[m.Source,m.Target]),[['new-data','/data'],['/owned/config/binds/000000','/run/key'],['/owned/config/binds/000001','/run/config']]);
  assert.doesNotMatch(JSON.stringify(config.HostConfig.Mounts),/\/private\//u);
});
test('source fence detects any Env, state or bind identity drift without returning private values',()=>{
  const value=original(),base=sourceFence(value);assert.match(base,/^[a-f0-9]{64}$/u);
  for(const change of [c=>c.Config.Env.push('PRIVATE_SENTINEL'),c=>c.State.Running=false,c=>c.Mounts[0].Source='/other']){
    const copy=structuredClone(value);change(copy);assert.notEqual(sourceFence(copy),base);
  }
  assert.equal(sameObservation({ok:true,schema:'soty.root-state-observation.v1',treeSha256:'a',files:1},{ok:true,schema:'soty.root-state-observation.v1',treeSha256:'b',files:1}),false);
});

test('Root inspections preserve every mount field while stabilizing fences and multiple archived bind indices',async()=>{
  const value=original();value.Mounts.push({Type:'bind',Source:'/private/key',Destination:'/run/z-key',RW:false,Propagation:'rprivate',Unrecognized:{must:'remain'}},{Type:'bind',Source:'/private/config',Destination:'/run/a-config',RW:true});
  const untouched=structuredClone(value);let reads=0;
  const engine=rootDockerEngine({inspect:async()=>{const copy=structuredClone(value);if(++reads%2)copy.Mounts.reverse();return copy;}});
  const a=await engine.inspect('same'),b=await engine.inspect('same');assert.equal(sourceFence(a),sourceFence(b));assert.deepEqual(value,untouched);
  assert.deepEqual(restoredBinds(a,{Mountpoint:'/restored'}),restoredBinds(b,{Mountpoint:'/restored'}));
  assert.deepEqual(restoredBinds(a,{Mountpoint:'/restored'}).map(m=>[m.Destination,m.Source]),[['/run/a-config','/restored/binds/000000'],['/run/z-key','/restored/binds/000001']]);
  assert.deepEqual(a.Mounts.find(m=>m.Destination==='/run/z-key').Unrecognized,{must:'remain'});
  const base=sourceFence(a);
  for(const change of [m=>m.Source='/other',m=>m.RW=true,m=>m.Propagation='shared',m=>m.Unrecognized.must='changed']){const copy=structuredClone(a);change(copy.Mounts.find(m=>m.Destination==='/run/z-key'));assert.notEqual(sourceFence(copy),base);}
  for(const mounts of [null,[{}],[{Destination:'/same'},{Destination:'/same'}],[{Destination:'relative'}]])await assert.rejects(rootDockerEngine({inspect:async()=>({Mounts:mounts})}).inspect('same'),e=>e.code==='preservation_mounts_invalid');
});

test('GPG CLI and Engine must target the same explicit local daemon without context/TLS overrides',()=>{
  const socket='/fixture/socket',env={DOCKER_HOST:'unix://'+socket};
  assert.doesNotThrow(()=>assertRootDockerTransport(socket,env));
  for(const change of [{DOCKER_HOST:'unix:///other'},{DOCKER_HOST:'tcp://example.invalid:2376'},...['DOCKER_CONTEXT','DOCKER_TLS','DOCKER_TLS_VERIFY','DOCKER_CERT_PATH','DOCKER_API_VERSION'].map(key=>({[key]:'unexpected'}))]){
    assert.throws(()=>assertRootDockerTransport(socket,{...env,...change}),error=>error.code==='release_docker_transport_mismatch');
  }
});

test('private bind byte/stat fence detects content changes and rejects symbolic links',{skip:process.platform!=='linux'},async t=>{
  const root=await mkdtemp(path.join(tmpdir(),'soty-private-bind-test-'));t.after(()=>rm(root,{recursive:true,force:true}));
  const file=path.join(root,'invented.txt');await writeFile(file,'Synthetic only.');
  const mounts=[{Type:'bind',Source:root}],before=await privateBindFence(mounts);
  assert.deepEqual(await privateBindFence(mounts),before);
  await writeFile(file,'Other synthetic bytes.');assert.notDeepEqual(await privateBindFence(mounts),before);
  await symlink(file,path.join(root,'alias'));await assert.rejects(privateBindFence(mounts),error=>error.code==='private_bind_invalid');
});
test('real stock controller + fixed orchestration failure scenarios in an isolated module-mock process',t=>{
  const env={...process.env};delete env.NODE_TEST_CONTEXT;
  env.DOCKER_HOST='unix:///fixture/socket';
  for(const key of ['DOCKER_CONTEXT','DOCKER_TLS','DOCKER_TLS_VERIFY','DOCKER_CERT_PATH','DOCKER_API_VERSION'])delete env[key];
  const result=spawnSync(process.execPath,['--experimental-test-module-mocks','--test','--test-reporter=spec',fileURLToPath(new URL('./test-support/root-preservation-scenarios.mjs',import.meta.url))],{env,encoding:'utf8',windowsHide:true,timeout:90000});
  assert.equal(result.status,0,(result.stdout+'\n'+result.stderr).slice(-14000));
  assert.match(result.stdout,/ℹ tests [1-9][0-9]*/u);assert.match(result.stdout,/ℹ fail 0/u);
  t.diagnostic(result.stdout.split('\n').filter(line=>/^ℹ (tests|pass|fail|skipped)/u.test(line)).join('; '));
});
