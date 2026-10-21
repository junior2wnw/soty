import { constants } from 'node:fs';
import { open,lstat,realpath,opendir } from 'node:fs/promises';
import { createHash,randomBytes } from 'node:crypto';
import { fileURLToPath } from 'node:url';
import path from 'node:path';
import { SafeError } from './docker-api.mjs';
import { hash } from './rollout.mjs';
import { modelReadiness } from './runtime.mjs';
import { guardStorageStart,requireStorageStartReceipt,storageReaders } from './storage-guard.mjs';

export const OWNER='io.soty.root-preservation.owner';
const ROLE='io.soty.root-preservation.role',ID=/^[a-f0-9]{64}$/u,IMAGE=/^sha256:[a-f0-9]{64}$/u;
const must=(ok,code)=>{if(!ok)throw new SafeError(code);};
const delay=ms=>new Promise(resolve=>setTimeout(resolve,ms));
const stamp=s=>[s.dev,s.ino,s.mode,s.uid,s.gid,s.nlink,s.size,s.mtimeNs,s.ctimeNs].join(':');
const utf8=new TextDecoder('utf-8',{fatal:true});
// Docker inspects enumerate mounts from an unordered map. Keep every field,
// but use one destination order for fences AND stock GPG bind-index archives.
export function rootDockerEngine(engine){
  const view=Object.create(engine);
  view.inspect=async id=>{
    const value=await engine.inspect(id),seen=new Set();
    must(Array.isArray(value?.Mounts),'preservation_mounts_invalid');
    const mounts=[...value.Mounts];
    for(const mount of mounts){const target=mount?.Destination;
      must(typeof target==='string'&&path.posix.isAbsolute(target)&&!target.includes('\0')&&!seen.has(target),'preservation_mounts_invalid');seen.add(target);}
    mounts.sort((a,b)=>a.Destination<b.Destination?-1:a.Destination>b.Destination?1:0);
    return {...value,Mounts:mounts};
  };
  return view;
}
export function sameObservation(a,b){return a?.ok===true&&b?.ok===true&&a.schema==='soty.root-state-observation.v1'
  &&b.schema===a.schema&&a.treeSha256===b.treeSha256&&['files','directories','sqliteDatabases','tables','rows'].every(key=>a[key]===b[key]);}
export function sourceFence(value){return hash({Id:value.Id,Name:value.Name,Image:value.Image,Config:value.Config,HostConfig:value.HostConfig,
  Mounts:value.Mounts,State:value.State,RestartCount:value.RestartCount,NetworkSettings:value.NetworkSettings});}

// Stock GPG streams use /usr/bin/docker, while Rollout uses Engine HTTP.
// Require an explicit identical local daemon; never inherit a current context.
export function assertRootDockerTransport(socket,environment=process.env){
  must(typeof socket==='string'&&path.posix.isAbsolute(socket)&&!socket.includes('\0')
    &&environment.DOCKER_HOST==='unix://'+socket
    &&['DOCKER_CONTEXT','DOCKER_TLS','DOCKER_TLS_VERIFY','DOCKER_CERT_PATH','DOCKER_API_VERSION'].every(key=>!environment[key]),'release_docker_transport_mismatch');
}

/** Private stat/content fences: paths, inode identities and hashes never leave RAM. */
export async function privateBindFence(mounts){
  const identity=createHash('sha256'),content=createHash('sha256'),fences=[];let entries=0,bytes=0;const deadline=Date.now()+60000;
  const add=(rel,s,digest='')=>{
    must(++entries<=20000&&Date.now()<deadline,'private_bind_limit');
    identity.update(JSON.stringify([rel,stamp(s),digest]));
    content.update(JSON.stringify([rel,s.isDirectory()?'directory':'file',String(s.mode&0o7777n),String(s.uid),String(s.gid),s.isFile()?String(s.size):'',digest]));
  };
  const visit=async(file,rel,depth,physical)=>{
    must(depth<=32,'private_bind_limit');const before=await lstat(file,{bigint:true});
    must(!before.isSymbolicLink()&&(before.isDirectory()||before.isFile()&&before.nlink===1n),'private_bind_invalid');
    const handle=await open(file,constants.O_RDONLY|constants.O_NOFOLLOW|constants.O_NONBLOCK|(before.isDirectory()?constants.O_DIRECTORY:0));
    try{
      must(stamp(before)===stamp(await handle.stat({bigint:true})),'private_bind_changed');
      fences.push({physical,stamp:stamp(before)});
      if(before.isDirectory()){
        add(rel,before);const names=[],dir=await opendir('/proc/self/fd/'+handle.fd,{encoding:'buffer'});
        for await(const entry of dir){must(names.length+entries<20000,'private_bind_limit');names.push(utf8.decode(entry.name));}
        for(const name of names.sort())await visit('/proc/self/fd/'+handle.fd+'/'+name,rel+'/'+name,depth+1,path.posix.join(physical,name));
      }else{
        bytes+=Number(before.size);must(Number.isSafeInteger(bytes)&&bytes<=192*1024**2,'private_bind_limit');
        const digest=createHash('sha256'),buffer=Buffer.alloc(65536);let at=0;
        for(;;){must(Date.now()<deadline,'private_bind_limit');const {bytesRead}=await handle.read(buffer,0,buffer.length,at);if(!bytesRead)break;at+=bytesRead;must(BigInt(at)<=before.size,'private_bind_changed');digest.update(buffer.subarray(0,bytesRead));}
        must(BigInt(at)===before.size,'private_bind_changed');add(rel,before,digest.digest('hex'));
      }
      must(stamp(before)===stamp(await handle.stat({bigint:true}))&&stamp(before)===stamp(await lstat(file,{bigint:true})),'private_bind_changed');
    }finally{await handle.close();}
  };
  for(const [index,mount] of mounts.entries()){
    must(mount.Type==='bind'&&path.posix.isAbsolute(mount.Source),'private_bind_invalid');
    must(await realpath(mount.Source)===mount.Source,'private_bind_invalid');await visit(mount.Source,String(index).padStart(6,'0'),0,mount.Source);
  }
  // Recheck early entries after all later reads, not merely their own read.
  for(const fence of fences)must(Date.now()<deadline&&await realpath(fence.physical)===fence.physical
    &&stamp(await lstat(fence.physical,{bigint:true}))===fence.stamp,'private_bind_changed');
  return {identitySha256:identity.digest('hex'),contentSha256:content.digest('hex')};
}

export async function restoredVolume(engine,operationId){
  const name='soty-isolated-restore-'+operationId,value=await engine.request('GET','/volumes/'+name);
  must(value.Name===name&&value.Driver==='local'&&value.Scope==='local'&&path.posix.isAbsolute(value.Mountpoint||'')
    &&(!value.Options||Object.keys(value.Options).length===0)&&value.Labels?.['io.soty.isolated-restore.operation']===operationId
    &&/^[a-f0-9]{32}$/u.test(value.Labels?.['io.soty.isolated-restore.owner']||'')&&value.Labels?.['io.soty.isolated-restore']==='1','restore_volume_invalid');
  return value;
}
export function restoredBinds(original,configuration){
  return original.Mounts.filter(m=>m.Type==='bind').map((mount,index)=>({...mount,
    Source:path.posix.join(configuration.Mountpoint,'binds',String(index).padStart(6,'0'))}));
}
export function cloneConfiguration(original,image,data,configuration,transaction,role){
  must(IMAGE.test(image)&&['candidate','fallback'].includes(role),'clone_identity_invalid');
  const host=structuredClone(original.HostConfig),config=structuredClone(original.Config);
  must(!host.Privileged&&!host.AutoRemove&&!host.Devices?.length&&!host.DeviceRequests?.length&&!host.VolumesFrom?.length
    &&!['host'].includes(host.PidMode)&&!['host'].includes(host.IpcMode),'clone_profile_unsupported');
  must(original.Mounts.every(m=>m.Type==='bind'||m.Type==='volume'&&m.Destination==='/data'),'clone_profile_unsupported');
  const binds=restoredBinds(original,configuration);
  must(data.Name!==original.Mounts.find(m=>m.Destination==='/data')?.Name&&data.Name!==configuration.Name,'clone_source_invalid');
  config.Image=image;config.Labels={...config.Labels,[OWNER]:transaction,[ROLE]:role};
  delete config.Labels['io.soty.storage.readers'];
  delete host.Binds;delete host.VolumesFrom;delete host.Links;
  host.Mounts=[{Type:'volume',Source:data.Name,Target:'/data',ReadOnly:false,VolumeOptions:{NoCopy:true}},
    ...binds.map(m=>({Type:'bind',Source:m.Source,Target:m.Destination,ReadOnly:m.RW===false}))];
  Object.assign(host,{NetworkMode:'none',PortBindings:{},PublishAllPorts:false,RestartPolicy:{Name:'no',MaximumRetryCount:0},LogConfig:{Type:'none',Config:{}},AutoRemove:false});
  return {...config,HostConfig:host,NetworkingConfig:{EndpointsConfig:{}}};
}

// Fixed local health request. No provider/model invocation and no caller command/URL.
const HEALTH_SCRIPT=`try{const h=await(await fetch('http://127.0.0.1:8080/health',{signal:AbortSignal.timeout(5000)})).json();
const out={ok:h.ok===true,modelProxies:{},applicationPolicySha256:h.applicationModelProxy?.policySha256||null};
for(const name of ['agentModelProxy','applicationModelProxy']){const p=h[name];if(p?.ready!==true)throw Error();const row={ready:true};
for(const key of ['model','transport','path','upstreamModel','routing','providerStrategy'])if(p[key]!==undefined){if(typeof p[key]!=='string'||p[key].length>256)throw Error();row[key]=p[key];}
if(p.models!==undefined){if(!Array.isArray(p.models)||p.models.length>128||p.models.some(v=>typeof v!=='string'||v.length>256))throw Error();row.models=[...p.models].sort();}out.modelProxies[name]=row;}
try{const s=await(await fetch('http://127.0.0.1:8080/api/connectors/storage-ready',{signal:AbortSignal.timeout(5000)})).json();out.storage={ok:s.ok===true,schema:s.schema,storageReady:s.storageReady===true,maintenance:s.maintenance===true};}catch{out.storage=null;}
console.log(JSON.stringify(out));}catch{console.log('{"ok":false}');}`;
function stdoutJSON(output){
  const pieces=[];let at=0,total=0;must(Buffer.isBuffer(output),'fixed_exec_invalid');
  while(at<output.length){must(at+8<=output.length&&output[at]===1&&output[at+1]===0&&output[at+2]===0&&output[at+3]===0,'fixed_exec_invalid');
    const n=output.readUInt32BE(at+4);must(n<=output.length-at-8&&(total+=n)<=65536,'fixed_exec_invalid');pieces.push(output.subarray(at+8,at+8+n));at+=8+n;}
  try{return JSON.parse(utf8.decode(Buffer.concat(pieces)));}catch{throw new SafeError('fixed_exec_invalid');}
}
async function fixedExec(engine,id,operation){
  const command=operation==='health'?['node','--input-type=module','-e',HEALTH_SCRIPT]:['node','/run/root-observer/root-preservation-observe-runner.mjs'];
  const before=await engine.inspect(id);must(before.Id===id&&before.State.Running,'fixed_exec_identity');
  const made=await engine.request('POST',`/containers/${id}/exec`,{Cmd:command,AttachStdout:true,AttachStderr:false,AttachStdin:false,Tty:false,Privileged:false},false,4096);
  must(ID.test(made?.Id||''),'fixed_exec_unresolved');
  const output=await engine.request('POST',`/exec/${made.Id}/start`,{Detach:false,Tty:false},true,66560);
  const execution=await engine.request('GET',`/exec/${made.Id}/json`,undefined,false,8192),after=await engine.inspect(id);
  must(execution.ID===made.Id&&execution.ContainerID===id&&execution.Running===false&&execution.ExitCode===0
    &&execution.ProcessConfig?.privileged===false&&execution.ProcessConfig?.tty===false&&execution.ProcessConfig?.entrypoint===command[0]
    &&JSON.stringify(execution.ProcessConfig?.arguments)===JSON.stringify(command.slice(1))
    &&after.Image===before.Image&&after.State.Running&&after.State.StartedAt===before.State.StartedAt,'fixed_exec_unresolved');
  return stdoutJSON(output);
}
export async function rootRuntimeReadiness(engine,id,candidate=false){
  const deadline=Date.now()+60000;
  while(Date.now()<deadline){
    const value=await fixedExec(engine,id,'health');
    if(value?.ok===true){const modelProxies=modelReadiness(value.modelProxies);
      must(value.applicationPolicySha256===null||typeof value.applicationPolicySha256==='string'&&/^[a-f0-9]{64}$/u.test(value.applicationPolicySha256),'clone_health_failed');
      if(!candidate)return {ok:true,modelProxies,applicationPolicySha256:value.applicationPolicySha256||null};
      if(value.storage?.ok===true&&value.storage.schema==='soty.connector-storage-ready.v1'&&value.storage.storageReady===true&&value.storage.maintenance===true)
        return {...value.storage,ok:true,modelProxies,applicationPolicySha256:value.applicationPolicySha256||null};
    }
    await delay(250);
  }
  throw new SafeError('clone_health_failed');
}

/** Closed observer/clone lifecycle; no general command, custom probe or target port. */
export function preservationSandbox({engine,helperImage,storageProbeImage,transaction,record=async()=>{}}){
  const owned=[],guards=[],pending=[];let seq=0;
  const identity=(c,item)=>{
    if(c.Id!==item.id||c.Name!=='/'+item.name||c.Image!==item.image||c.Config.Labels?.[OWNER]!==transaction||c.Config.Labels?.[ROLE]!==item.role)return false;
    const host=c.HostConfig,expected=item.config.HostConfig;
    if(host.NetworkMode!=='none'||host.RestartPolicy?.Name!=='no'||(host.RestartPolicy.MaximumRetryCount||0)!==0||host.LogConfig?.Type!=='none'
      ||Object.keys(host.LogConfig.Config||{}).length||Object.keys(host.PortBindings||{}).length||host.PublishAllPorts===true||host.Privileged===true
      ||Object.keys(c.NetworkSettings?.Networks||{}).some(key=>key!=='none')||c.Mounts?.length!==expected.Mounts.length)return false;
    for(const key of ['Env','User','Entrypoint','Cmd','WorkingDir'])if(item.config[key]!==undefined&&JSON.stringify(c.Config[key])!==JSON.stringify(item.config[key]))return false;
    for(const mount of expected.Mounts){const actual=c.Mounts.find(m=>m.Destination===mount.Target);
      if(!actual||actual.Type!==mount.Type||typeof actual.RW!=='boolean'||actual.RW!==!mount.ReadOnly||(mount.Type==='volume'?actual.Name!==mount.Source:actual.Source!==mount.Source))return false;}
    // MemorySwap is RAM+swap; equal positive caps leave zero swap allowance.
    if(item.role==='observer'&&(host.ReadonlyRootfs!==true||host.Memory!==1073741824||host.MemorySwap!==1073741824
      ||host.NanoCpus!==1000000000||host.PidsLimit!==32||hash(host.CapDrop)!==hash(expected.CapDrop)
      ||hash(host.CapAdd)!==hash(expected.CapAdd)||hash(host.SecurityOpt)!==hash(expected.SecurityOpt)||hash(host.Tmpfs)!==hash(expected.Tmpfs)))return false;
    return true;
  };
  const note=async(item,state)=>record({schema:'soty.root-preservation-helper.v1',id:item.id||null,name:item.name,role:item.role,state});
  const create=async(role,body)=>{
    const item={role,image:body.Image,name:'soty-preservation-'+transaction+'-'+(++seq),id:null,startPending:false,config:body};owned.push(item);
    try{await engine.inspect(item.name);pending.push(item);throw new SafeError('preservation_prior_execution');}
    catch(error){if(error.code!=='engine_http_404')throw error;}
    await note(item,'creating');let response;try{response=await engine.create(item.name,body);}catch{}
    let found;for(let i=0;i<12;i++){try{found=await engine.inspect(response?.Id||item.name);break;}catch{}await delay(100);}
    if(!found){pending.push(item);throw new SafeError('preservation_create_unresolved');}item.id=found.Id;
    must(ID.test(item.id)&&identity(found,item)&&found.State.Running===false&&found.State.Status==='created','preservation_identity');
    await note(item,'created');return item;
  };
  const start=async(item,application=false)=>{
    const before=await engine.inspect(item.id);must(identity(before,item)&&before.State.Running===false&&before.State.Status==='created','preservation_prior_execution');
    if(application){
      const state={},context={engine,probeImage:storageProbeImage,transactionId:randomBytes(16).toString('hex'),getState:()=>state,
        record:async fields=>{Object.assign(state,fields);await record({schema:'soty.root-preservation-storage.v1',...fields});}};guards.push({context,state});
      const receipt=await guardStorageStart(context,before);requireStorageStartReceipt(receipt,item.id);item.guard=receipt;
    }
    const guarded=await engine.inspect(item.id);must(identity(guarded,item)&&guarded.State.Running===false&&guarded.State.Status==='created','preservation_prior_execution');
    item.startPending=true;await note(item,'starting');try{await engine.start(item.id);}catch{}
    for(let i=0;i<12;i++){const value=await engine.inspect(item.id);must(identity(value,item),'preservation_identity');
      if(value.State.Running){item.startPending=false;await note(item,'running');return;}
      if(value.State.Status==='exited'&&value.State.StartedAt&&value.State.StartedAt!=='0001-01-01T00:00:00Z'){item.startPending=false;throw new SafeError('clone_boot_failed');}await delay(100);}
    throw new SafeError('preservation_start_unresolved');
  };
  const stop=async(item,hard=false)=>{
    let value=await engine.inspect(item.id);must(identity(value,item),'preservation_identity');
    if(hard&&value.State.Running){
      try{await engine.request('POST',`/containers/${item.id}/kill?signal=SIGKILL`);}catch{}
      value=await engine.inspect(item.id);must(identity(value,item)&&value.State.Running===false&&value.State.Status==='exited','preservation_stop_unresolved');
      item.startPending=false;await note(item,'killed');return;
    }
    if(value.State.Running){try{await engine.stop(item.id);}catch{}}
    for(let i=0;i<12;i++){value=await engine.inspect(item.id);must(identity(value,item),'preservation_identity');
      if(value.State.Running===false&&value.State.Restarting!==true&&['created','exited'].includes(value.State.Status)){
        if(item.startPending&&!(value.State.Status==='exited'&&value.State.StartedAt&&value.State.StartedAt!=='0001-01-01T00:00:00Z'))throw new SafeError('preservation_start_unresolved');
        item.startPending=false;await note(item,'stopped');return;}
      await delay(100);}
    // Hard stop only this exact owned helper/clone, never original/serving IDs.
    try{await engine.request('POST',`/containers/${item.id}/kill?signal=SIGKILL`);}catch{}
    value=await engine.inspect(item.id);must(identity(value,item)&&value.State.Running===false&&value.State.Status==='exited','preservation_stop_unresolved');
    item.startPending=false;await note(item,'killed');
    if(item.role!=='observer')throw new SafeError('clone_stop_forced');
  };
  const observe=async volume=>{
    must(typeof volume.Name==='string'&&path.posix.isAbsolute(volume.Mountpoint||''),'observation_volume_invalid');
    const assets=['root-state-observer.mjs','root-preservation-observe-runner.mjs'];
    const body={Image:helperImage,User:'0:0',Entrypoint:['node'],Cmd:['-e','setInterval(()=>{},1000)'],WorkingDir:'/',Labels:{[OWNER]:transaction,[ROLE]:'observer'},
      HostConfig:{NetworkMode:'none',PortBindings:{},RestartPolicy:{Name:'no'},LogConfig:{Type:'none',Config:{}},ReadonlyRootfs:true,Privileged:false,
        Memory:1073741824,MemorySwap:1073741824,NanoCpus:1000000000,PidsLimit:32,CapDrop:['ALL'],CapAdd:['DAC_READ_SEARCH'],SecurityOpt:['no-new-privileges'],
        Tmpfs:{'/ram':'rw,noexec,nosuid,nodev,size=268435456,mode=0700'},Mounts:[{Type:'volume',Source:volume.Name,Target:'/source',ReadOnly:true,VolumeOptions:{NoCopy:true}},
          ...assets.map(file=>({Type:'bind',Source:fileURLToPath(new URL('./'+file,import.meta.url)),Target:'/run/root-observer/'+file,ReadOnly:true}))]},NetworkingConfig:{EndpointsConfig:{}}};
    const item=await create('observer',body);let value,execFailed=true;
    let timer;
    try{await start(item);value=await Promise.race([fixedExec(engine,item.id,'observe'),new Promise((_,reject)=>{timer=setTimeout(()=>reject(new SafeError('root_observation_timeout')),65000);})]);execFailed=false;}
    finally{clearTimeout(timer);await stop(item,execFailed);}
    must(value?.ok===true&&value.schema==='soty.root-state-observation.v1'&&/^[a-f0-9]{64}$/u.test(value.treeSha256||'')
      &&Object.keys(value).sort().join(',')==='directories,files,ok,rows,schema,sqliteDatabases,tables,treeSha256'
      &&['files','directories','sqliteDatabases','tables','rows'].every(key=>Number.isSafeInteger(value[key])&&value[key]>=0),'root_observation_failed');
    return value;
  };
  const bootClone=async({original,image,data,configuration,role,source,modelSha256,applicationPolicySha256})=>{
    const before=await observe(data);must(sameObservation(before,source),'clone_initial_data_changed');
    const configBefore=await observe(configuration),binds=restoredBinds(original,configuration),privateBefore=await privateBindFence(binds);
    const item=await create(role,cloneConfiguration(original,image,data,configuration,transaction,role));let health;
    try{
      await start(item,true);const deadline=Date.now()+60000;
      do{health=await fixedExec(engine,item.id,'health');if(health?.ok===true)break;await delay(250);}while(Date.now()<deadline);
      must(health?.ok===true&&hash(modelReadiness(health.modelProxies))===modelSha256&&(health.applicationPolicySha256||null)===(applicationPolicySha256||null),'clone_health_failed');
    }finally{await stop(item);}
    const stopped=await engine.inspect(item.id);
    must(identity(stopped,item)&&stopped.State.Running===false&&stopped.State.Restarting===false&&stopped.State.Status==='exited'
      &&stopped.State.ExitCode===0&&stopped.State.OOMKilled===false&&!stopped.State.Error,'clone_stop_failed');
    must(sameObservation(before,await observe(data))&&sameObservation(configBefore,await observe(configuration)),'clone_data_changed');
    must(hash(privateBefore)===hash(await privateBindFence(binds)),'clone_bind_changed');
    return {role,id:item.id,image,storageGuard:item.guard,unchanged:true,health:true};
  };
  const settle=async()=>{
    must(pending.length===0,'preservation_create_unresolved');
    for(const item of [...owned].reverse())if(item.id)await stop(item);
    for(const {state,context} of guards){const h=state.storageGuardHelper;if(!h)continue;
      const value=await engine.inspect(h.id||h.name);must(value.Name==='/'+h.name&&value.Image===h.image&&value.Config.Labels?.['io.soty.storage.probe']===context.transactionId,'storage_helper_unresolved');
      if(value.State.Running){try{await engine.stop(value.Id);}catch{}}
      const after=await engine.inspect(value.Id);must(after.State.Running===false&&after.State.Status==='exited','storage_helper_unresolved');}
  };
  return {observe,bootClone,settle};
}

export async function requireWorld6Apps14Images(engine,images){
  for(const id of images){const image=await engine.image(id);must(image.Id===id,'release_image_changed');const readers=storageReaders(image);
    must(JSON.stringify(readers.world)==='[6]'&&JSON.stringify(readers.apps)==='[1,2,3,4,5,6,7,8,9,10,11,12,13,14]','release_reader_mismatch');}
}
