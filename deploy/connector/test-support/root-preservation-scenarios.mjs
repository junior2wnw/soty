import test,{mock} from 'node:test';
import assert from 'node:assert/strict';
import * as fs from 'node:fs/promises';
import { tmpdir } from 'node:os';
import path from 'node:path';
import { pathToFileURL,fileURLToPath } from 'node:url';
import { createHash } from 'node:crypto';
import { Readable } from 'node:stream';
import { fixture,args,modelProxies } from './rollout-engine.mjs';
import { currentStorageReaders,storageReaderLabel } from '../storage-guard.mjs';

// Module mocks replace GPG transport only in this child. No production probe
// callback is installed. The stock productionProbe builds its real helper body.
const stage=await fs.mkdtemp(path.join(tmpdir(),'soty-preservation-source-'));
const origin=new URL('../../../',import.meta.url),manifest=JSON.parse(await fs.readFile(new URL('../cold-verification-stage.stock.json',import.meta.url),'utf8'));
const assets=['root-preservation-release.mjs','root-preservation-runtime.mjs','root-preservation-observe-runner.mjs','root-state-observer.mjs'];
for(const file of [...manifest.stock.map(e=>e.path),'deploy/connector/cold-verification-stage.stock.json',...assets.map(f=>'deploy/connector/'+f)]){
  const target=path.join(stage,file);await fs.mkdir(path.dirname(target),{recursive:true});
  const bytes=await fs.readFile(new URL(file,origin));
  await fs.writeFile(target,manifest.stock.some(entry=>entry.path===file)?Buffer.from(bytes.toString('utf8').replaceAll('\r\n','\n')):bytes);
}
const sha=v=>createHash('sha256').update(v).digest('hex'),privateDirs=new Set();let bindRace;
if(!process.getuid)process.getuid=()=>0;
mock.module('node:fs/promises',{namedExports:{...fs,open:async(file,...args)=>{
  const handle=await fs.open(file,...args),read=handle.read.bind(handle);
  if(bindRace&&String(file).endsWith('/b.txt'))handle.read=async(...values)=>{
    if(!bindRace.changed){bindRace.changed=true;await fs.writeFile(bindRace.first,'Synthetic mutation after first file was checked.');}return read(...values);
  };
  return handle;
},lstat:async(file,...args)=>{
  const stat=await fs.lstat(file,...args);if(!privateDirs.has(String(file)))return stat;
  return new Proxy(stat,{get(target,key){if(key==='mode')return 0o40700;if(key==='uid')return process.getuid();const value=Reflect.get(target,key);return typeof value==='function'?value.bind(target):value;}});
}}});
let active;
const backupValue=operationId=>({ok:true,encrypted:true,operationId,sourceCheckpointSha256:'1'.repeat(64),
  volume:{sha256:'2'.repeat(64),inventorySha256:'3'.repeat(64),bytes:512,entries:20,fileBytes:100},configuration:{sha256:'4'.repeat(64),inventorySha256:'5'.repeat(64),bytes:512,entries:3,fileBytes:100}});
mock.module(pathToFileURL(path.join(stage,'deploy/connect/gpg-release-backup.mjs')).href,{namedExports:{
  backupStopped:async(options,engine)=>{active.calls.push('backup');active.key=options.key;const source=await engine.inspect(options.containerId);assert.equal(source.State.Running,false);
    assert.deepEqual(Object.keys(options).sort(),['containerId','directory','image','key','operationId']);assert.equal(options.containerId,args.originalId);
    assert.equal(options.key.length,64);assert.deepEqual(active.appStarts,[]);active.backupOperation=options.operationId;
    assert.equal((await engine.inspect(options.containerId)).HostConfig.RestartPolicy.Name,'no');
    if(active.fault==='backup')throw Error('PRIVATE_SENTINEL /fixture/private-secret');
    if(active.fault==='backup-forged-code')throw Object.assign(Error('PRIVATE_SENTINEL'),{code:'backup_receipt_invalid',path:'/fixture/private-secret'});
    if(active.fault==='backup-private-code')throw new SafeError('private_synthetic_sensitive');
    if(active.multiBinds){
      // Model stock configArchive's fresh-inspect enumeration, without sorting
      // in this mock. Only the mandatory production adapter may stabilize it.
      active.archivedBinds=await Promise.all(source.Mounts.filter(m=>m.Type==='bind').map(async(m,index)=>({
        prefix:'binds/'+String(index).padStart(6,'0'),source:m.Source,destination:m.Destination,readOnly:m.RW===false,
        bytes:await fs.readFile(m.Source),mode:(await fs.stat(m.Source)).mode&0o777
      })));
      assert.deepEqual(active.archivedBinds.map(m=>m.destination),['/run/a-config','/run/z-key']);
    }
    return backupValue(options.operationId);},
  restoreIsolated:async(options)=>{active.calls.push(options.file.endsWith('configuration.gpg')?'restore:configuration':'restore:volume');
    if(active.fault==='restore')throw Error('PRIVATE_SENTINEL');const part=options.file.endsWith('configuration.gpg')?'configuration':'volume',entry=backupValue('')[part];
    assert.equal(path.basename(options.file),part==='volume'?'volume.gpg':'configuration.gpg');assert.equal(options.expectedSha256,entry.sha256);assert.equal(options.key,active.key);
    assert.notEqual(options.operationId,active.backupOperation);assert.ok(!active.restores.some(value=>value.operationId===options.operationId));assert.deepEqual(active.appStarts,[]);
    active.restores.push({part,operationId:options.operationId,sha256:options.expectedSha256});
    if(active.fault==='config-restore'&&part==='configuration')throw Error('PRIVATE_SENTINEL');
    const name='soty-isolated-restore-'+options.operationId;let mountpoint='/docker/volumes/'+name;
    if(active.bindDirectory){
      mountpoint=path.join(active.bindDirectory,name);await fs.mkdir(path.join(mountpoint,'binds'),{recursive:true,mode:0o700});
      if(part==='configuration'){
        if(active.multiBinds){
          assert.equal(active.archivedBinds.length,2);
          for(const bind of active.archivedBinds){const target=path.join(mountpoint,bind.prefix);await fs.writeFile(target,bind.bytes,{flag:'wx',mode:bind.mode});await fs.chmod(target,bind.mode);}
        }else await fs.copyFile(active.bind,path.join(mountpoint,'binds','000000'));
      }
    }
    active.volumes.set(name,{Name:name,Driver:'local',Scope:'local',Options:null,Mountpoint:mountpoint,
      Labels:{'io.soty.isolated-restore.owner':'a'.repeat(32),'io.soty.isolated-restore.operation':options.operationId,'io.soty.isolated-restore':'1'}});
    return {ok:true,operationId:options.operationId,isolated:true,authenticated:true,readbackVerified:active.fault!=='readback',sha256:entry.sha256,inventorySha256:entry.inventorySha256};}
}});
const {rootPreservationRelease}=await import(pathToFileURL(path.join(stage,'deploy/connector/root-preservation-release.mjs')));
const {DockerApi,SafeError}=await import(pathToFileURL(path.join(stage,'deploy/connector/docker-api.mjs')));
const {preservationHash,createConfig}=await import(pathToFileURL(path.join(stage,'deploy/connector/rollout.mjs')));
const {privateBindFence,rootDockerEngine}=await import(pathToFileURL(path.join(stage,'deploy/connector/root-preservation-runtime.mjs')));
const body=value=>{const b=Buffer.from(JSON.stringify(value)),h=Buffer.alloc(8);h[0]=1;h.writeUInt32BE(b.length,4);return Buffer.concat([h,b]);};
const observed=()=>({ok:true,schema:'soty.root-state-observation.v1',treeSha256:'a'.repeat(64),files:19,directories:10,sqliteDatabases:9,tables:70,rows:40});
const format={ok:true,schema:'soty.storage-format.v7',rooms:2,apps:14,notes:2,capabilities:3,appRegistration:1,feedback:1,humanIdentity:3,connect:4,world:6};
const observerResourceFaults={
  'swap-absent':host=>{delete host.MemorySwap;},
  'swap-null':host=>{host.MemorySwap=null;},
  'swap-zero':host=>{host.MemorySwap=0;},
  'swap-unlimited':host=>{host.MemorySwap=-1;},
  'swap-extra':host=>{host.MemorySwap=2147483648;},
  'swap-less':host=>{host.MemorySwap=536870912;},
  'swap-string':host=>{host.MemorySwap='1073741824';},
  'memory-absent':host=>{delete host.Memory;},
  'memory-null':host=>{host.Memory=null;},
  'memory-zero':host=>{host.Memory=0;},
  'equal-double':host=>{host.Memory=host.MemorySwap=2147483648;},
  'cpu':host=>{host.NanoCpus=0;},
  'pids':host=>{host.PidsLimit=0;},
  'tmpfs':host=>{host.Tmpfs={};},
  'caps':host=>{host.CapAdd=['SYS_ADMIN'];},
};
async function setup(t,fault='',prepare=true){
  const f=fixture();active={f,fault,calls:[],appStarts:[],maintenance:[],volumes:new Map(),key:null,cloneStopped:false,restores:[],observers:[],observerStarts:[],sourceObservations:0,
    multiBinds:fault.startsWith('mount-order'),inspectReads:new Map(),rawOriginalOrders:new Set(),rawMountArrays:[],cloneRequests:[]};const own=active;
  const original=f.map.get(args.originalId);original.Mounts=original.Mounts.filter(m=>m.Type==='volume');delete original.HostConfig.Binds;
  if(['bind-late','source-bind-identity'].includes(fault)){
    const dir=await fs.mkdtemp(path.join(tmpdir(),'soty-approved-bind-'));t.after(()=>fs.rm(dir,{recursive:true,force:true}));
    own.bind=path.join(dir,'invented.txt');await fs.writeFile(own.bind,'Approved synthetic bytes.',{mode:0o600});
    if(fault==='source-bind-identity')own.bindDirectory=dir;
    original.Mounts.push({Type:'bind',Source:own.bind,Destination:'/run/synthetic-config',RW:false});original.HostConfig.Binds=[own.bind+':/run/synthetic-config:ro'];
  }
  if(own.multiBinds){
    const directory=await fs.mkdtemp(path.join(tmpdir(),'soty-two-bind-order-'));own.bindDirectory=directory;t.after(()=>fs.rm(directory,{recursive:true,force:true}));
    own.bindSpecs=[{source:path.join(directory,'first.txt'),destination:'/run/z-key',bytes:Buffer.from('Synthetic Z key, deliberately different.'),mode:0o600},
      {source:path.join(directory,'second.txt'),destination:'/run/a-config',bytes:Buffer.from('Synthetic A configuration with other bytes.'),mode:0o640}];
    for(const bind of own.bindSpecs){await fs.writeFile(bind.source,bind.bytes,{flag:'wx',mode:bind.mode});await fs.chmod(bind.source,bind.mode);
      original.Mounts.push({Type:'bind',Source:bind.source,Destination:bind.destination,RW:false,Mode:'ro',Propagation:'rprivate'});}
    original.HostConfig.Binds=own.bindSpecs.map(bind=>bind.source+':'+bind.destination+':ro');
  }
  const request=f.engine.request,start=f.engine.start,stop=f.engine.stop,create=f.engine.create,inspect=f.engine.inspect,helperOutput=f.engine.helperOutput,execs=new Map();let count=0,maintenance=false;
  const observerInspects=new Map();
  f.engine.inspect=async id=>{
    if(fault==='prior-helper'&&String(id).startsWith('soty-preservation-'))return {Id:'0'.repeat(64)};
    if(own.cloneStopped&&id===own.observers[0]?.id
      &&(fault==='settle-before'&&own.sourceObservations===1||fault==='settle-after'&&own.sourceObservations===2)){
      const error=new SafeError('storage_helper_unresolved');error.message='PRIVATE_SENTINEL';error.stack='/fixture/private-secret';throw error;
    }
    const c=f.map.get(id);
    if(fault==='observer-before-start-swap'&&c?.Config?.Labels?.['io.soty.root-preservation.role']==='observer'){
      const count=(observerInspects.get(id)||0)+1;observerInspects.set(id,count);
      if(count===2)c.HostConfig.MemorySwap=0;
    }
    const value=await inspect(id);
    if(own.multiBinds){
      const reads=(own.inspectReads.get(value.Id)||0)+1;own.inspectReads.set(value.Id,reads);if(reads%2)value.Mounts.reverse();
      if(value.Id===args.originalId){own.rawOriginalOrders.add(sha(JSON.stringify(value.Mounts)));own.rawMountArrays.push({value:value.Mounts,targets:value.Mounts.map(m=>m.Destination)});}
    }
    return value;
  };
  f.engine.create=async(name,config)=>{
    const role=config.Labels?.['io.soty.root-preservation.role'];
    if(own.multiBinds&&['fallback','candidate'].includes(role))own.cloneRequests.push(structuredClone(config));
    if(role==='candidate'&&fault==='create-unknown')throw Error('lost');
    const made=await create(name,config);
    for(const mount of f.map.get(made.Id).Mounts)if(mount.Type==='volume'&&own.volumes.has(mount.Name))mount.Source=own.volumes.get(mount.Name).Mountpoint;
    if(role==='observer'){
      own.observers.push({id:made.Id,request:structuredClone(config)});
      if(fault==='observer-cgroup-v2')f.map.get(made.Id).HostConfig.MemorySwappiness=null;
      observerResourceFaults[fault]?.(f.map.get(made.Id).HostConfig);
    }
    if(role==='observer'&&fault==='observer-mount')delete f.map.get(made.Id).Mounts[0].RW;
    if(role==='observer'&&fault==='observer-network')f.map.get(made.Id).HostConfig.NetworkMode='bridge';
    if(role==='candidate'&&fault==='create-lost-ack')throw Error('lost');return made;
  };
  f.engine.image=async id=>{const readers=JSON.parse(currentStorageReaders);
    if(fault==='image-world3')readers.readers.world=[3];if(fault==='image-apps8')readers.readers.apps=[1,2,3,4,5,6,7,8];
    return {Id:id,Config:{Labels:{'org.opencontainers.image.revision':args.revision,[storageReaderLabel]:JSON.stringify(readers)}}};};
  f.engine.start=async id=>{
    const c=f.map.get(id),role=c.Config?.Labels?.['io.soty.root-preservation.role'];
    if(role==='observer')own.observerStarts.push(id);
    if(id===args.originalId||['candidate','fallback'].includes(role)||c.Config?.Labels?.['io.soty.connector-rollout']&&!c.Config.Labels['io.soty.connector-rollout.helper'])own.appStarts.push(id);
    if(role==='candidate'&&fault==='start-unknown'){own.calls.push('clone:candidate:start');throw Error('lost');}
    await start(id);Object.assign(c.State,{StartedAt:'2026-01-01T00:00:00Z',Restarting:false,Pid:c.State.Running?1:0});
    if(c.Config?.Labels?.['io.soty.storage.probe'])Object.assign(c.State,{Running:false,Status:'exited',ExitCode:0,Pid:0});
    if(role==='candidate'||role==='fallback')own.calls.push('clone:'+role+':start');
    if(role==='candidate'&&fault==='start-lost-ack')throw Error('lost');
  };
  f.engine.stop=async id=>{await stop(id);const c=f.map.get(id),role=c.Config?.Labels?.['io.soty.root-preservation.role'];Object.assign(c.State,{Pid:0,Restarting:false,ExitCode:0,OOMKilled:false,Error:''});
    if(id===args.originalId&&fault==='bind-late')await fs.writeFile(own.bind,'Unapproved synthetic bytes after preflight.');
    if(role==='candidate'){
      own.cloneStopped=true;
      if(fault==='source-bind-identity'){
        const stat=await fs.stat(own.bind);await fs.utimes(own.bind,stat.atime,new Date(stat.mtimeMs+2000));
      }
      if(['stop-killed','stop-oom','stop-failed'].includes(fault))Object.assign(c.State,{ExitCode:fault==='stop-killed'?137:fault==='stop-failed'?17:0,OOMKilled:fault==='stop-oom'});
    }};
  f.engine.helperOutput=async id=>{
    const labels=f.map.get(id)?.Config?.Labels||{};if(labels['io.soty.storage.probe'])return fault==='source-world3'?{...format,world:3,apps:8}:fault==='source-apps8'?{...format,apps:8}:fault==='source-rooms1'?{...format,rooms:1}:format;
    const verb=labels['io.soty.connector-rollout.helper'];if(verb){own.maintenance.push(verb);if(verb==='enter')maintenance=true;if(verb==='leave'&&fault!=='leave')maintenance=false;return {ok:true,count:0,activeJobs:[],maintenance,schema:'synthetic'};}
    return helperOutput(id);
  };
  f.engine.request=async(method,route,data,...rest)=>{
    if(method==='POST'&&route.endsWith('/kill?signal=SIGKILL')){const id=route.split('/')[2];own.calls.push('kill:observer');await stop(id);return {};}
    if(method==='GET'&&route.startsWith('/volumes/')&&own.volumes.has(decodeURIComponent(route.slice(9))))return structuredClone(own.volumes.get(decodeURIComponent(route.slice(9))));
    if(method==='POST'&&/^\/containers\/[^/]+\/exec$/u.test(route)){const id=(++count+100000).toString(16).padStart(64,'0');execs.set(id,{id,container:route.split('/')[2],command:data.Cmd});return {Id:id};}
    if(route.startsWith('/exec/')){
      const exec=execs.get(route.split('/')[2]);if(method==='GET')return {ID:exec.id,ContainerID:exec.container,Running:false,ExitCode:0,ProcessConfig:{privileged:false,tty:false,entrypoint:exec.command[0],arguments:exec.command.slice(1)}};
      const isObserver=exec.command.includes('/run/root-observer/root-preservation-observe-runner.mjs');
      if(isObserver){const output=observed(),c=f.map.get(exec.container),volume=c.HostConfig.Mounts[0].Source;
        if(volume==='synthetic-data')own.sourceObservations++;
        if(fault==='observer-exec')throw Error('lost');
        if(fault==='final-observer-exec'&&own.cloneStopped&&volume==='synthetic-data')throw Error('PRIVATE_SENTINEL /fixture/private-secret');
        if(fault==='clone-data'&&own.cloneStopped&&volume.startsWith('soty-isolated-restore-'))output.treeSha256='b'.repeat(64);
        if(fault==='source-data'&&own.cloneStopped&&volume==='synthetic-data')output.treeSha256='c'.repeat(64);
        if(fault==='source-config'&&own.cloneStopped&&volume==='synthetic-data')original.Config.Env.push('INVENTED_DRIFT=1');
        if(own.multiBinds&&own.cloneStopped&&volume==='synthetic-data'){
          const bind=original.Mounts.find(m=>m.Destination==='/run/z-key');
          if(fault==='mount-order-rw')bind.RW=true;
          if(fault==='mount-order-source')bind.Source=own.bindSpecs[1].source;
          if(fault==='mount-order-duplicate')original.Mounts.push(structuredClone(bind));
          if(fault==='mount-order-missing')delete bind.Destination;
        }
        return body(output);
      }
      const models=fault==='serving-model'&&exec.container===own.servingCandidate
        ?{...modelProxies,agentModelProxy:{...modelProxies.agentModelProxy,model:'synthetic-changed-model'}}:modelProxies;
      return body(fault==='health'?{ok:false}:{ok:true,modelProxies:models,applicationPolicySha256:null,storage:{ok:true,storageReady:true,maintenance:true,schema:'soty.connector-storage-ready.v1'}});
    }
    return request(method,route,data,...rest);
  };
  const directory=await fs.mkdtemp(path.join(tmpdir(),'soty-preservation-run-'));privateDirs.add(directory);
  t.after(async()=>{await fs.rm(directory,{recursive:true,force:true});privateDirs.delete(directory);});
  const sourcePins=Object.fromEntries(await Promise.all(assets.map(async file=>[file,sha(await fs.readFile(path.join(stage,'deploy/connector',file)))])));
  const plan={schema:'soty.root-preservation-release.v2',...args,transaction:'f'.repeat(32),helperImage:args.storageProbeImage,
    expectedConfigurationSha256:preservationHash(createConfig(original,original.Image,'f'.repeat(32))),stockManifestSha256:sha(await fs.readFile(path.join(stage,'deploy/connector/cold-verification-stage.stock.json'))),sourcePins,directory,dockerSocket:'/fixture/socket'};
  const records=[],controller=rootPreservationRelease(plan,{engine:f.engine,record:async value=>records.push(value)});
  if(!prepare)return {f,own,records,controller,plan};
  const prepared=await controller.prepare(),approval={approved:true,legacyNoPendingWritesObserved:true,...Object.fromEntries(['originalId','candidateId','configurationSha256','revision','storageProbeImage'].map(key=>[key,prepared.rollout[key]])),
    sourcePinsSha256:prepared.sourcePinsSha256,bindFenceSha256:prepared.bindFenceSha256};
  own.servingCandidate=prepared.rollout.candidateId;
  return {f,own,records,controller,prepared,approval,plan};
}
test('separate prepare/promote awaits both restores and exact clone checks before serving candidate START',async t=>{
  const x=await setup(t);const state=await x.controller.promote({prepared:x.prepared,approval:x.approval,stdin:Readable.from([Buffer.from('A'.repeat(64)+'\n')])});
  assert.equal(state.phase,'committed');assert.deepEqual(x.own.calls,['backup','restore:volume','restore:configuration','clone:fallback:start','clone:candidate:start']);
  assert.equal(x.own.key.every(b=>b===0),true);assert.equal(state.coldVerification.state,'verified');assert.equal(state.preservationQualification.clones.length,2);
  assert.equal(state.preservationFailure,undefined);assert.equal(state.preservationFailures,undefined);
  assert.equal(state.preservationQualification.clones.find(clone=>clone.role==='fallback').image,x.plan.originalImage);
  assert.ok([...x.own.volumes.keys()].every(name=>name.startsWith('soty-isolated-restore-')&&name!=='synthetic-data'));
  assert.deepEqual(x.own.restores.map(value=>value.part),['volume','configuration']);assert.equal(new Set(x.own.restores.map(value=>value.operationId)).size,2);
  assert.doesNotMatch(JSON.stringify(x.records),/synthetic-sensitive|PRIVATE_SENTINEL|"Env"/u);
});

test('Root adapter retains DockerApi prototype methods and their public receiver options without a socket',async()=>{
  const raw=new DockerApi({socketPath:'/fixture/receiver-only',timeoutMs:70000}),calls=[];
  raw.request=async function(method,route){
    assert.equal(this.socketPath,'/fixture/receiver-only');assert.equal(this.timeoutMs,70000);calls.push([method,route]);
    return method==='GET'?{Id:args.originalId,Mounts:[{Destination:'/z'},{Destination:'/a'}]}:{};
  };
  const engine=rootDockerEngine(raw);assert.ok(engine instanceof DockerApi);
  assert.deepEqual((await engine.inspect(args.originalId)).Mounts.map(m=>m.Destination),['/a','/z']);
  await engine.start(args.originalId);await engine.stop(args.originalId);
  assert.deepEqual(calls,[['GET','/containers/'+args.originalId+'/json'],['POST','/containers/'+args.originalId+'/start'],['POST','/containers/'+args.originalId+'/stop?t=10']]);
});

async function assertTwoBindRestores(x){
  assert.deepEqual(x.own.archivedBinds.map(m=>[m.prefix,m.destination]),[['binds/000000','/run/a-config'],['binds/000001','/run/z-key']]);
  assert.deepEqual(x.own.restores.map(value=>value.part),['volume','configuration']);assert.equal(x.own.cloneRequests.length,2);
  const configuration=[...x.own.volumes.values()].find(volume=>volume.Labels['io.soty.isolated-restore.operation']===x.own.restores[1].operationId);
  for(const request of x.own.cloneRequests){
    const binds=request.HostConfig.Mounts.filter(m=>m.Type==='bind');assert.equal(binds.length,2);
    for(const bind of binds){
      const expected=x.own.bindSpecs.find(m=>m.destination===bind.Target),index=bind.Target==='/run/a-config'?'000000':'000001';
      assert.equal(bind.Source,path.join(configuration.Mountpoint,'binds',index));assert.equal(bind.ReadOnly,true);
      assert.deepEqual(await fs.readFile(bind.Source),expected.bytes);assert.equal((await fs.stat(bind.Source)).mode&0o777,expected.mode);
      assert.ok(!x.own.bindSpecs.some(m=>m.source===bind.Source));
    }
  }
}

test('alternating Docker mount order with two distinct real binds completes the whole injected factory',{skip:process.platform!=='linux'},async t=>{
  const x=await setup(t,'mount-order'),storedOrder=x.f.map.get(args.originalId).Mounts.map(m=>m.Destination);
  const state=await x.controller.promote({prepared:x.prepared,approval:x.approval,stdin:Readable.from(['A'.repeat(64)+'\n'])});
  assert.equal(state.phase,'committed');assert.equal(state.coldVerification.state,'verified');assert.equal(state.preservationFailure,undefined);
  assert.equal(x.own.rawOriginalOrders.size,2);assert.ok(x.own.inspectReads.get(args.originalId)>10);
  for(const raw of x.own.rawMountArrays)assert.deepEqual(raw.value.map(m=>m.Destination),raw.targets);
  assert.deepEqual(x.f.map.get(args.originalId).Mounts.map(m=>m.Destination),storedOrder);await assertTwoBindRestores(x);
  assert.equal(x.own.key.every(value=>value===0),true);assert.ok(!x.own.appStarts.includes(args.originalId));
  assert.deepEqual(x.own.calls,['backup','restore:volume','restore:configuration','clone:fallback:start','clone:candidate:start']);
  const journal=JSON.stringify(x.records);for(const bind of x.own.bindSpecs)assert.equal(journal.includes(bind.source),false);
});

for(const [fault,code] of [['rw','preservation_source_container_changed'],['source','preservation_source_container_changed'],['duplicate','preservation_mounts_invalid'],['missing','preservation_mounts_invalid']])test(`mount permutation never conceals real ${fault} drift after both clones`,{skip:process.platform!=='linux'},async t=>{
  const x=await setup(t,'mount-order-'+fault);
  await assert.rejects(x.controller.promote({prepared:x.prepared,approval:x.approval,stdin:Readable.from(['A'.repeat(64)+'\n'])}),error=>error.code==='recovery_required');
  diagnosticRecovery(x,[{stage:'settle',code}]);await assertTwoBindRestores(x);assert.equal(x.own.sourceObservations,2);
  assert.deepEqual(x.own.calls,['backup','restore:volume','restore:configuration','clone:fallback:start','clone:candidate:start']);
});

test('normalized cgroup-v2 observer with null swappiness preserves exact zero-swap limits and completes the fixed composition',async t=>{
  const x=await setup(t,'observer-cgroup-v2');
  const state=await x.controller.promote({prepared:x.prepared,approval:x.approval,stdin:Readable.from(['A'.repeat(64)+'\n'])});
  assert.equal(state.phase,'committed');assert.equal(state.coldVerification.state,'verified');assert.ok(x.own.observers.length>1);
  for(const {id,request} of x.own.observers){
    assert.equal(Object.hasOwn(request.HostConfig,'MemorySwappiness'),false);
    assert.equal(request.HostConfig.Memory,1073741824);assert.equal(request.HostConfig.MemorySwap,1073741824);
    assert.equal(request.HostConfig.NanoCpus,1000000000);assert.equal(request.HostConfig.PidsLimit,32);
    assert.equal(x.f.map.get(id).HostConfig.MemorySwappiness,null);assert.equal(x.f.map.get(id).State.Running,false);
    assert.equal(x.own.observerStarts.filter(value=>value===id).length,1);
    assert.ok(x.records.some(state=>state.preservationHelper?.id===id&&state.preservationHelper.state==='created'));
  }
  assert.deepEqual(x.own.restores.map(value=>value.part),['volume','configuration']);
  assert.equal(x.own.key.every(value=>value===0),true);
});

for(const fault of [...Object.keys(observerResourceFaults),'observer-before-start-swap'])test(`observer ${fault} refuses before observer/application START and leaves recovery_required`,async t=>{
  const x=await setup(t,fault);
  await assert.rejects(x.controller.promote({prepared:x.prepared,approval:x.approval,stdin:Readable.from(['A'.repeat(64)+'\n'])}),error=>error.code==='recovery_required');
  assert.equal(x.records.at(-1).phase,'recovery_required');assert.equal(x.records.at(-1).coldVerification.state,'running');
  assert.equal(x.f.map.get(args.originalId).State.Running,false);assert.equal(x.f.map.get(args.originalId).HostConfig.RestartPolicy.Name,'no');
  assert.equal(x.own.observers.length,1);assert.equal(x.f.map.get(x.own.observers[0].id).State.Running,false);
  assert.deepEqual(x.own.observerStarts,[]);assert.deepEqual(x.own.appStarts,[]);assert.deepEqual(x.own.calls,[]);
});

test('fixed CURRENT composition blocks legacy rollback after serving START and readiness failure',async t=>{
  const x=await setup(t,'serving-model');
  await assert.rejects(x.controller.promote({prepared:x.prepared,approval:x.approval,stdin:Readable.from(['A'.repeat(64)+'\n'])}),error=>error.code==='recovery_required');
  const state=x.records.at(-1);assert.equal(state.phase,'recovery_required');assert.equal(state.failureCode,'release_legacy_rollback_forbidden');
  assert.equal(state.activationFailureCode,'candidate_model_readiness_changed');assert.equal(state.coldVerification.state,'verified');
  assert.ok(x.own.appStarts.includes(x.prepared.rollout.candidateId));assert.ok(!x.own.appStarts.includes(args.originalId));
  assert.equal(x.f.map.get(args.originalId).State.Running,false);assert.equal(x.f.map.get(args.originalId).HostConfig.RestartPolicy.Name,'no');
  assert.equal(x.f.map.get(x.prepared.rollout.candidateId).State.Running,false);assert.deepEqual(x.own.maintenance,['status','status','enter']);
  assert.equal(x.own.key.every(value=>value===0),true);
});
for(const fault of ['backup','restore','config-restore','readback','source-data','source-config','start-unknown','create-unknown','prior-helper','observer-mount','observer-network','observer-exec'])test(`unknown ${fault} prevents serving candidate and original recovery START`,async t=>{
  const x=await setup(t,fault);await assert.rejects(x.controller.promote({prepared:x.prepared,approval:x.approval,stdin:Readable.from(['A'.repeat(64)+'\n'])}),e=>e.code==='recovery_required');
  assert.equal(x.records.at(-1).phase,'recovery_required');assert.equal(x.f.map.get(args.originalId).State.Running,false);
  if(x.own.key)assert.equal(x.own.key.every(b=>b===0),true);assert.ok(!x.f.events.includes('start-old'));
  if(fault==='observer-exec')assert.ok(x.own.calls.includes('kill:observer'));
});

function diagnosticRecovery(x,failures){
  const state=x.records.at(-1);assert.equal(state.phase,'recovery_required');assert.equal(state.failureCode,'cold_verification_unresolved');
  assert.equal(state.coldVerification.state,'running');assert.deepEqual(state.preservationFailure,failures.at(-1));assert.deepEqual(state.preservationFailures,failures);
  for(const record of x.records)if(record.preservationFailures){
    assert.ok(record.preservationFailures.length>=1&&record.preservationFailures.length<=2);
    for(const failure of record.preservationFailures)assert.deepEqual(Object.keys(failure).sort(),['code','stage']);
  }
  assert.equal(x.f.map.get(args.originalId).State.Running,false);assert.equal(x.f.map.get(args.originalId).HostConfig.RestartPolicy.Name,'no');
  assert.equal(x.f.map.get(x.prepared.rollout.candidateId).State.Running,false);
  assert.ok(!x.own.appStarts.includes(args.originalId));assert.ok(!x.own.appStarts.includes(x.prepared.rollout.candidateId));
  assert.deepEqual(x.own.maintenance,['status']);assert.equal(x.own.key.every(value=>value===0),true);
  assert.doesNotMatch(JSON.stringify(x.records),/PRIVATE_SENTINEL|private_synthetic_sensitive|\/fixture\/private-secret|"Env"/u);
}

for(const fault of ['backup','backup-forged-code','backup-private-code'])test(`bounded diagnosis redacts ${fault} without treating unknown effects as unchanged`,async t=>{
  const x=await setup(t,fault);
  await assert.rejects(x.controller.promote({prepared:x.prepared,approval:x.approval,stdin:Readable.from(['A'.repeat(64)+'\n'])}),error=>error.code==='recovery_required');
  diagnosticRecovery(x,[{stage:'qualification',code:'preservation_operation_failed'},{stage:'settle',code:'preservation_gpg_unsettled'}]);
  assert.deepEqual(x.own.calls,['backup']);assert.equal(x.own.sourceObservations,1);
});

test('real restore receipt refusal retains both bounded causes without legacy rollback or original START',async t=>{
  const x=await setup(t,'readback');
  await assert.rejects(x.controller.promote({prepared:x.prepared,approval:x.approval,stdin:Readable.from(['A'.repeat(64)+'\n'])}),error=>error.code==='recovery_required');
  diagnosticRecovery(x,[{stage:'qualification',code:'restore_receipt_invalid'},{stage:'settle',code:'preservation_gpg_unsettled'}]);
  assert.deepEqual(x.own.calls,['backup','restore:volume']);assert.deepEqual(x.own.appStarts,[]);
});

for(const [fault,code] of [['source-data','preservation_source_data_changed'],['source-config','preservation_source_container_changed']])test(`final ${fault} diagnosis distinguishes the exact fence after both clones settle`,async t=>{
  const x=await setup(t,fault);
  await assert.rejects(x.controller.promote({prepared:x.prepared,approval:x.approval,stdin:Readable.from(['A'.repeat(64)+'\n'])}),error=>error.code==='recovery_required');
  diagnosticRecovery(x,[{stage:'settle',code}]);assert.equal(x.own.sourceObservations,2);
  assert.deepEqual(x.own.calls,['backup','restore:volume','restore:configuration','clone:fallback:start','clone:candidate:start']);
});

for(const [fault,observations] of [['settle-before',1],['settle-after',2]])test(`known SafeError in ${fault} is recorded without message, stack or recovery START`,async t=>{
  const x=await setup(t,fault);
  await assert.rejects(x.controller.promote({prepared:x.prepared,approval:x.approval,stdin:Readable.from(['A'.repeat(64)+'\n'])}),error=>error.code==='recovery_required');
  diagnosticRecovery(x,[{stage:'settle',code:'storage_helper_unresolved'}]);assert.equal(x.own.sourceObservations,observations);
  assert.deepEqual(x.own.restores.map(value=>value.part),['volume','configuration']);
});

test('unknown final source observer error stays generic and recovery_required after helper stop',async t=>{
  const x=await setup(t,'final-observer-exec');
  await assert.rejects(x.controller.promote({prepared:x.prepared,approval:x.approval,stdin:Readable.from(['A'.repeat(64)+'\n'])}),error=>error.code==='recovery_required');
  diagnosticRecovery(x,[{stage:'settle',code:'preservation_operation_failed'}]);assert.ok(x.own.calls.includes('kill:observer'));
  assert.ok([...x.f.map.values()].filter(c=>c.Config?.Labels?.['io.soty.root-preservation.role']).every(c=>c.State.Running===false));
});

test('final private bind identity drift is diagnosed even when content remains identical',{skip:process.platform!=='linux'},async t=>{
  const x=await setup(t,'source-bind-identity'),mounts=x.f.map.get(args.originalId).Mounts.filter(m=>m.Type==='bind'),before=await privateBindFence(mounts);
  await assert.rejects(x.controller.promote({prepared:x.prepared,approval:x.approval,stdin:Readable.from(['A'.repeat(64)+'\n'])}),error=>{
    assert.equal(error.code,'recovery_required',JSON.stringify(x.records.at(-1).preservationFailures));return true;
  });
  diagnosticRecovery(x,[{stage:'settle',code:'preservation_source_bind_changed'}]);
  const after=await privateBindFence(mounts);assert.equal(after.contentSha256,before.contentSha256);assert.notEqual(after.identitySha256,before.identitySha256);
  assert.deepEqual(x.own.restores.map(value=>value.part),['volume','configuration']);assert.equal(x.own.sourceObservations,2);
  assert.doesNotMatch(JSON.stringify(x.records),new RegExp(x.own.bindDirectory));
});

for(const fault of ['create-lost-ack','start-lost-ack'])test(`lost ${fault} reconciles retained exact ID without repeating START`,async t=>{
  const x=await setup(t,fault),state=await x.controller.promote({prepared:x.prepared,approval:x.approval,stdin:Readable.from(['A'.repeat(64)+'\n'])});
  assert.equal(state.phase,'committed');assert.equal(x.own.calls.filter(v=>v==='clone:candidate:start').length,1);
});
test('settled cloned data mismatch recovers same original only after source equality',async t=>{
  const x=await setup(t,'clone-data');await assert.rejects(x.controller.promote({prepared:x.prepared,approval:x.approval,stdin:Readable.from(['A'.repeat(64)+'\n'])}),e=>e.code==='cold_verification_rejected');
  assert.equal(x.records.at(-1).phase,'restored');assert.equal(x.f.map.get(args.originalId).State.Running,true);
  assert.equal(x.f.map.get(args.originalId).Image,x.plan.originalImage);assert.equal(x.f.map.get(args.originalId).Mounts[0].Name,'synthetic-data');
  assert.deepEqual(x.records.at(-1).preservationFailures,[{stage:'qualification',code:'clone_data_changed'}]);
});

for(const fault of ['stop-killed','stop-oom','stop-failed'])test(`${fault} is a negative cold qualification even when the clone is confirmed stopped`,async t=>{
  const x=await setup(t,fault);await assert.rejects(x.controller.promote({prepared:x.prepared,approval:x.approval,stdin:Readable.from(['A'.repeat(64)+'\n'])}),e=>e.code==='cold_verification_rejected');
  assert.equal(x.records.at(-1).phase,'restored');assert.equal(x.f.map.get(args.originalId).State.Running,true);
  assert.equal(x.records.at(-1).coldVerification.state,'rejected');assert.equal(x.records.at(-1).preservationQualification,undefined);
});
test('approval and drain cannot be self-approved or skipped',async t=>{
  const x=await setup(t);await assert.rejects(x.controller.promote({prepared:x.prepared,approval:{...x.approval,legacyNoPendingWritesObserved:false},stdin:Readable.from(['A'.repeat(64)+'\n'])}),e=>e.code==='supervised_receipt_mismatch');
  assert.deepEqual(x.own.calls,[]);assert.equal(x.f.map.get(args.originalId).State.Running,true);
});

test('changed current configuration and malformed custody key reject before STOP',async t=>{
  const x=await setup(t);x.f.map.get(args.originalId).Config.Env.push('INVENTED_DRIFT=1');
  await assert.rejects(x.controller.promote({prepared:x.prepared,approval:x.approval,stdin:Readable.from(['A'.repeat(64)+'\n'])}),e=>e.code==='release_configuration_changed');
  x.f.map.get(args.originalId).Config.Env.pop();
  await assert.rejects(x.controller.promote({prepared:x.prepared,approval:x.approval,stdin:Readable.from(['short\n'])}),e=>e.code==='release_key_invalid');
  assert.equal(x.f.map.get(args.originalId).State.Running,true);assert.deepEqual(x.own.calls,[]);
});

for(const fault of ['image-world3','image-apps8'])test(`${fault} refuses before prepare with no application START`,async t=>{
  const x=await setup(t,fault,false);await assert.rejects(x.controller.prepare(),e=>e.code==='release_reader_mismatch');
  assert.equal(x.f.map.get(args.originalId).State.Running,true);assert.deepEqual(x.own.appStarts,[]);assert.deepEqual(x.own.calls,[]);
});

for(const fault of ['source-world3','source-apps8','source-rooms1'])test(`${fault} actual v7 observation refuses before STOP or any application START`,async t=>{
  const x=await setup(t,fault);await assert.rejects(x.controller.promote({prepared:x.prepared,approval:x.approval,stdin:Readable.from(['A'.repeat(64)+'\n'])}),e=>['storage_reader_incompatible','release_format_mismatch'].includes(e.code));
  assert.equal(x.f.map.get(args.originalId).State.Running,true);assert.equal(x.f.map.get(x.prepared.rollout.candidateId).State.Running,false);
  assert.deepEqual(x.own.appStarts,[]);assert.deepEqual(x.own.calls,[]);
});

test('source pin mismatch rejects before STOP',async t=>{
  const x=await setup(t),controller=rootPreservationRelease({...x.plan,sourcePins:{...x.plan.sourcePins,'root-state-observer.mjs':'0'.repeat(64)}},{engine:x.f.engine,record:async()=>{}});
  await assert.rejects(controller.prepare(),e=>e.code==='release_source_changed');assert.equal(x.f.map.get(args.originalId).State.Running,true);
});

test('admission boundary failure leaves candidate for supervised reconciliation without old-image rollback',async t=>{
  const x=await setup(t,'leave');await assert.rejects(x.controller.promote({prepared:x.prepared,approval:x.approval,stdin:Readable.from(['A'.repeat(64)+'\n'])}),e=>e.code==='leave_boundary_ambiguous');
  assert.equal(x.records.at(-1).phase,'recovery_required');assert.equal(x.f.map.get(args.originalId).State.Running,false);assert.ok(!x.f.events.includes('start-old'));
});

test('bind fence detects an early file changed during a later real file read',{skip:process.platform!=='linux'},async t=>{
  const directory=await fs.mkdtemp(path.join(tmpdir(),'soty-bind-race-'));t.after(()=>fs.rm(directory,{recursive:true,force:true}));
  await fs.writeFile(path.join(directory,'a.txt'),'Synthetic first file.');await fs.writeFile(path.join(directory,'b.txt'),'Synthetic second file.');
  bindRace={first:path.join(directory,'a.txt'),changed:false};
  try{await assert.rejects(privateBindFence([{Type:'bind',Source:directory}]),error=>error.code==='private_bind_changed');assert.equal(bindRace.changed,true);}
  finally{bindRace=undefined;}
});

test('private config changed between approved preflight and STOP refuses before backup',{skip:process.platform!=='linux'},async t=>{
  const x=await setup(t,'bind-late');await assert.rejects(x.controller.promote({prepared:x.prepared,approval:x.approval,stdin:Readable.from(['A'.repeat(64)+'\n'])}),error=>error.code==='recovery_required');
  assert.equal(x.records.at(-1).phase,'recovery_required');assert.equal(x.f.map.get(args.originalId).State.Running,false);assert.deepEqual(x.own.calls,[]);
});
test.after(async()=>{mock.restoreAll();await fs.rm(stage,{recursive:true,force:true});});
