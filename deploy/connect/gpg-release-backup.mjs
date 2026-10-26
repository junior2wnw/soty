import { DockerApi } from '../connector/docker-api.mjs';
import { archiveHelperArgs } from './backup.mjs';
import { bad,sha,limits,keyBytes,TarInventory,encryptTar,verifyTar } from './gpg-tar.mjs';
import { randomBytes } from 'node:crypto';
import { spawn } from 'node:child_process';
import { constants,createReadStream } from 'node:fs';
import { mkdir,lstat,readdir,open,realpath } from 'node:fs/promises';
import path from 'node:path';
import { pathToFileURL } from 'node:url';
import { assertArtifactMountIsolation } from './gpg-mount-proof.mjs';
import { listAllContainerMounts } from './gpg-container-inventory.mjs';

const ID=/^[a-f0-9]{32}$/,CONTAINER=/^[a-f0-9]{64}$/,IMAGE=/^sha256:[a-f0-9]{64}$/;
const OWNER='io.soty.isolated-restore.owner',OP='io.soty.isolated-restore.operation';
const HELPER='io.soty.backup.helper',ISOLATED='io.soty.isolated-restore';
const validate=(id,image)=>{if(!ID.test(id??'')||!IMAGE.test(image??''))bad('backup_identity_invalid');};

function processDocker(args,cap){
  const child=spawn('/usr/bin/docker',args,{stdio:['pipe','pipe','ignore']});
  const done=new Promise((resolve,reject)=>{
    child.once('error',()=>reject(Error('helper_failed')));
    child.once('close',code=>code===0?resolve():reject(Error('helper_failed')));
  });
  done.catch(()=>{});
  const timer=setTimeout(()=>child.kill('SIGKILL'),cap.wallMs);
  done.finally(()=>clearTimeout(timer)).catch(()=>{});
  return {child,done};
}
async function absentContainer(engine,name){
  try{await engine.inspect(name);}catch(error){if(error.code==='engine_http_404')return;throw error;}
  bad('backup_helper_exists');
}
async function pinnedImage(engine,image){if((await engine.image(image)).Id!==image)bad('backup_image_invalid');}
async function stopOwned(engine,identity,label,nonce,image){
  let value;
  try{value=await engine.inspect(identity);}catch(error){if(error.code==='engine_http_404')return;throw error;}
  if(value.Config?.Labels?.[label]!==nonce||value.Image!==image||!CONTAINER.test(value.Id??''))bad('backup_helper_unresolved');
  const loggingOff=value.HostConfig?.LogConfig?.Type==='none'&&Object.keys(value.HostConfig.LogConfig.Config??{}).length===0;
  if(value.State?.Running===true)await engine.stop(value.Id);
  try{value=await engine.inspect(value.Id);}catch(error){if(error.code==='engine_http_404'){if(!loggingOff)bad('backup_helper_unresolved');return;}throw error;}
  if(value.State?.Running!==false||!loggingOff||value.HostConfig?.LogConfig?.Type!=='none'||Object.keys(value.HostConfig.LogConfig.Config??{}).length!==0)bad('backup_helper_unresolved');
}
async function readonlyArchive(engine,containerId,operationId,phase,image,cap,nonce){
  const helperName='soty-readonly-'+operationId+'-'+phase;
  await absentContainer(engine,helperName);
  const args=archiveHelperArgs({containerId,helperName,image});
  args.splice(args.indexOf('--network'),0,'--log-driver=none','--label',HELPER+'='+nonce);
  args.splice(args.indexOf('-C'),0,'--format=posix','--pax-option=delete=atime,delete=ctime');
  const running=processDocker(args,cap);
  running.child.stdin.end();
  return {...running,cleanup:async()=>{
    running.child.kill('SIGKILL');
    await running.done.catch(()=>{});
    await stopOwned(engine,helperName,HELPER,nonce,image);
  }};
}
function sourceState(value){return sha(JSON.stringify({Id:value.Id,Image:value.Image,Config:value.Config,HostConfig:value.HostConfig,Mounts:canonicalMounts(value.Mounts),State:value.State,RestartCount:value.RestartCount}));}
function canonicalMounts(mounts){
  return mounts.map(value=>({value,key:JSON.stringify(value)}))
    .sort((a,b)=>a.key<b.key?-1:a.key>b.key?1:0).map(entry=>entry.value);
}
async function sourceFence(engine,id){
  const original=await engine.inspect(id);
  if(original.Id!==id||original.State?.Running!==false||!IMAGE.test(original.Image??''))bad('backup_requires_stopped_source');
  const mounts=original.Mounts;
  if(!Array.isArray(mounts)||mounts.some(m=>typeof m.Destination!=='string'))bad('backup_volume_profile_invalid');
  const dirs=(original.Config?.Env??[]).filter(x=>typeof x==='string'&&x.startsWith('DATA_DIR='));
  const data=mounts.filter(m=>m.Destination==='/data');
  if(dirs.length!==1||dirs[0]!=='DATA_DIR=/data'||data.length!==1||data[0].Type!=='volume'||data[0].RW!==true||mounts.some(m=>m.Destination.startsWith('/data/')||m.Type!=='bind'&&!(m.Type==='volume'&&m.Destination==='/data'))||(original.HostConfig?.Mounts??[]).some(m=>m.Target==='/data'&&m.VolumeOptions?.Subpath))bad('backup_volume_profile_invalid');
  const mount=data[0];
  if(typeof mount.Name!=='string'||!path.posix.isAbsolute(mount.Source??''))bad('backup_volume_profile_invalid');
  const volume=await engine.request('GET','/volumes/'+encodeURIComponent(mount.Name));
  if(volume.Name!==mount.Name||volume.Driver!=='local'||volume.Scope!=='local'||volume.Mountpoint!==mount.Source||volume.Options&&Object.keys(volume.Options).length)bad('backup_volume_profile_invalid');
  const running=await engine.request('GET','/containers/json');
  if(!Array.isArray(running))bad('backup_writer_check_failed');
  const overlaps=(a,b)=>a===b||a.startsWith(b.replace(/\/$/,'')+'/')||b.startsWith(a.replace(/\/$/,'')+'/');
  for(let c of running){
    if(!Array.isArray(c.Mounts)||(c.Mounts??[]).some(m=>m.Type!=='tmpfs'&&(!m.Source||!path.posix.isAbsolute(m.Source))))c=await engine.inspect(c.Id);
    if(!Array.isArray(c.Mounts))bad('backup_writer_check_failed');
    if(c.Mounts.some(m=>m.Type!=='tmpfs'&&m.RW!==false&&(m.Name===mount.Name||typeof m.Source==='string'&&overlaps(m.Source,mount.Source))))bad('backup_other_writer_running');
  }
  return original;
}
async function artifactDirectory(directory,original,engine,image,operationId,cap){
  const absolute=path.resolve(directory),parent=await realpath(path.dirname(absolute));
  if(path.dirname(absolute)!==parent)bad('backup_directory_invalid');
  const parentStat=await lstat(parent,{bigint:true});
  if(!parentStat.isDirectory()||parentStat.isSymbolicLink())bad('backup_directory_invalid');
  let protectedVolume=false;
  for(const mount of original.Mounts){
    let source;
    try{source=await realpath(mount.Source);}
    catch(error){
      if(error.code!=='EACCES'||mount.Type!=='volume'||mount.Destination!=='/data')throw error;
      protectedVolume=true;continue;
    }
    if(absolute===source||absolute.startsWith(source.replace(/\/$/,'')+'/')||source.startsWith(absolute+'/'))bad('backup_directory_overlaps_source');
  }
  if(protectedVolume){
    await assertArtifactMountIsolation({engine,image,operationId,original,parent,parentStat,artifactBase:path.basename(absolute),cap,
      run:processDocker,stop:(id,label,nonce,expectedImage)=>stopOwned(engine,id,label,nonce,expectedImage)});
    const after=await lstat(parent,{bigint:true});
    if(await realpath(parent)!==parent||['dev','ino','mode','uid','gid'].some(key=>parentStat[key]!==after[key]))bad('backup_directory_invalid');
  }
  await mkdir(absolute,{mode:0o700});
  const owned=await lstat(absolute);
  if(!owned.isDirectory()||owned.isSymbolicLink()||(owned.mode&0o077)!==0||owned.uid!==process.getuid())bad('backup_directory_invalid');
}

// POSIX tar metadata, not a cryptographic format. Private inspect and bind names
// exist only in memory and in GPG's encrypted stream, never in a sidecar file.
function octal(n,len){const value=Number(n).toString(8);if(!Number.isSafeInteger(Number(n))||Number(n)<0||value.length>=len)bad('backup_metadata_invalid');return Buffer.from(value.padStart(len-1,'0')+'\0');}
function header(type,size,mode,uid,gid){
  const h=Buffer.alloc(512);h.write('entry',0);octal(mode,8).copy(h,100);octal(uid,8).copy(h,108);octal(gid,8).copy(h,116);octal(size,12).copy(h,124);octal(0,12).copy(h,136);h.fill(32,148,156);h[156]=type.charCodeAt(0);h.write('ustar\0',257);h.write('00',263);const sum=h.reduce((a,b)=>a+b,0);Buffer.from(sum.toString(8).padStart(6,'0')+'\0 ').copy(h,148);return h;
}
function paxRecord(k,v){const body=Buffer.from(k+'='+v+'\n');let n=body.length+2;for(;;){const next=body.length+String(n).length+1;if(next===n)return Buffer.concat([Buffer.from(n+' '),body]);n=next;}}
function* prefix(name,type,size,{mode=0o600,uid=0,gid=0,mtime='0'}={}){
  const p=Buffer.concat([paxRecord('path',name),paxRecord('mtime',mtime),paxRecord('uid',String(uid)),paxRecord('gid',String(gid))]);
  yield header('x',p.length,0o600,0,0);yield p;yield Buffer.alloc((512-p.length%512)%512);yield header(type,size,mode,0,0);
}
const nsTime=ns=>{const v=BigInt(ns),negative=v<0n,n=negative?-v:v;return (negative?'-':'')+(n/1000000000n)+'.'+String(n%1000000000n).padStart(9,'0');};
const sameStat=(a,b)=>a.ino===b.ino&&a.dev===b.dev&&a.mode===b.mode&&a.uid===b.uid&&a.gid===b.gid&&a.nlink===b.nlink&&a.size===b.size&&a.mtimeNs===b.mtimeNs&&a.ctimeNs===b.ctimeNs;
async function* configArchive(original,cap,seen){
  let count=0,total=0;yield* prefix('.','5',0,{mode:0o700});
  const metadata=Buffer.from(JSON.stringify({schema:'soty.encrypted-container-config.v1',original,
    bindRoots:(original.Mounts??[]).filter(m=>m.Type==='bind').map((m,i)=>({archivePrefix:'binds/'+String(i).padStart(6,'0'),source:m.Source,destination:m.Destination,readOnly:m.RW===false}))}));
  if(metadata.length>8*1024**2)bad('backup_limit_exceeded');
  yield* prefix('metadata.json','0',metadata.length);yield metadata;yield Buffer.alloc((512-metadata.length%512)%512);
  yield* prefix('binds','5',0,{mode:0o700});
  async function* walk(from,to,depth){
    if(!path.isAbsolute(from)||depth>cap.pathDepth||++count>cap.entries)bad('backup_limit_exceeded');
    const before=await lstat(from,{bigint:true});
    if(before.isSymbolicLink()||!before.isFile()&&!before.isDirectory()||before.isFile()&&before.nlink!==1n||(before.mode&0o7000n)!==0n)bad('backup_bind_type_unsupported');
    seen.set(from,before);
    const attrs={mode:Number(before.mode&0o777n),uid:Number(before.uid),gid:Number(before.gid),mtime:nsTime(before.mtimeNs)};
    if(before.isDirectory()){
      yield* prefix(to,'5',0,attrs);
      for(const child of (await readdir(from)).sort())yield* walk(path.join(from,child),to+'/'+child,depth+1);
    }else{
      const size=Number(before.size);
      if(!Number.isSafeInteger(size)||size>cap.fileBytes||(total+=size)>cap.totalBytes)bad('backup_limit_exceeded');
      const f=await open(from,constants.O_RDONLY|constants.O_NOFOLLOW);
      try{
        if(!sameStat(before,await f.stat({bigint:true})))bad('backup_bind_changed');
        yield* prefix(to,'0',size,attrs);
        for await(const piece of createReadStream(null,{fd:f.fd,autoClose:false,start:0}))yield piece;
        yield Buffer.alloc((512-size%512)%512);
      }finally{await f.close();}
    }
    if(!sameStat(before,await lstat(from,{bigint:true})))bad('backup_bind_changed');
  }
  const binds=(original.Mounts??[]).filter(m=>m.Type==='bind');
  for(let i=0;i<binds.length;i++)yield* walk(binds[i].Source,'binds/'+String(i).padStart(6,'0'),0);
  yield Buffer.alloc(1024);
}

/** Explicit operator port. It never stops/starts/migrates the source. */
export async function backupStopped({containerId,image,operationId,directory,key,cap={}},engine=new DockerApi()){
  validate(operationId,image);keyBytes(key);
  if(!CONTAINER.test(containerId??'')||!path.isAbsolute(directory??''))bad('backup_identity_invalid');
  const value=limits(cap),original=await sourceFence(engine,containerId),checkpoint=sourceState(original);
  await pinnedImage(engine,image);
  await artifactDirectory(directory,original,engine,image,operationId,value);
  const archive=await readonlyArchive(engine,containerId,operationId,'volume',image,value,randomBytes(16).toString('hex'));
  let volume;
  try{volume=await encryptTar({file:path.join(directory,'volume.gpg'),key,input:archive.child.stdout,cap:value});await archive.done;}
  finally{await archive.cleanup();}
  const seen=new Map();
  const configuration=await encryptTar({file:path.join(directory,'configuration.gpg'),key,input:configArchive(original,value,seen),cap:value});
  for(const [file,stat] of seen)if(!sameStat(stat,await lstat(file,{bigint:true})))bad('backup_bind_changed');
  if(sourceState(await sourceFence(engine,containerId))!==checkpoint)bad('backup_source_changed');
  const volumeProof=await verifyTar({file:path.join(directory,'volume.gpg'),key,expectedSha256:volume.sha256,cap:value});
  const configProof=await verifyTar({file:path.join(directory,'configuration.gpg'),key,expectedSha256:configuration.sha256,cap:value});
  if(sourceState(await sourceFence(engine,containerId))!==checkpoint)bad('backup_source_changed');
  for(const [file,stat] of seen)if(!sameStat(stat,await lstat(file,{bigint:true})))bad('backup_bind_changed');
  return {ok:true,encrypted:true,operationId,sourceCheckpointSha256:checkpoint,
    volume:{sha256:volume.sha256,bytes:volume.bytes,entries:volumeProof.entries,fileBytes:volumeProof.fileBytes,inventorySha256:volumeProof.inventorySha256},
    configuration:{sha256:configuration.sha256,bytes:configuration.bytes,entries:configProof.entries,fileBytes:configProof.fileBytes,inventorySha256:configProof.inventorySha256}};
}

function writeChunk(stream,chunk){
  if(stream.destroyed||stream.writableEnded)bad('restore_sink_closed');
  return new Promise((resolve,reject)=>{stream.write(chunk,error=>error?reject(Error('restore_sink_closed')):resolve());});
}
/** Authentication and complete tar validation precede even CREATE of a volume.
 * Volume/container names are derived here; there is no caller target/path port. */
export async function restoreIsolated({file,expectedSha256,image,operationId,key,cap={}},engine=new DockerApi()){
  validate(operationId,image);
  const value=limits(cap),proof=await verifyTar({file,key,expectedSha256,cap:value});
  await pinnedImage(engine,image);
  const volume='soty-isolated-restore-'+operationId,helper='soty-isolated-extract-'+operationId,nonce=randomBytes(16).toString('hex');
  try{await engine.request('GET','/volumes/'+volume);bad('restore_target_exists');}catch(error){if(error.code!=='engine_http_404')throw error;}
  await absentContainer(engine,helper);
  const labels={[OWNER]:nonce,[OP]:operationId,[ISOLATED]:'1'};
  await engine.request('POST','/volumes/create',{Name:volume,Driver:'local',Labels:labels});
  let extractId,extraction;
  const assertOwned=async()=>{
    const v=await engine.request('GET','/volumes/'+volume);
    if(v.Name!==volume||v.Driver!=='local'||v.Scope!=='local'||!path.posix.isAbsolute(v.Mountpoint??'')||v.Options&&Object.keys(v.Options).length||v.Labels?.[OWNER]!==nonce||v.Labels?.[OP]!==operationId||v.Labels?.[ISOLATED]!=='1')bad('restore_target_invalid');
    const all=await listAllContainerMounts(engine);
    if(!Array.isArray(all))bad('restore_target_invalid');
    const overlaps=(a,b)=>a===b||a.startsWith(b.replace(/\/$/,'')+'/')||b.startsWith(a.replace(/\/$/,'')+'/');
    for(let c of all){
      if(!Array.isArray(c.Mounts)||(c.Mounts??[]).some(m=>m.Type!=='tmpfs'&&!path.posix.isAbsolute(m.Source??'')))c=await engine.inspect(c.Id);
      if(!Array.isArray(c.Mounts)||c.Id!==extractId&&c.Mounts.some(m=>m.Name===volume||m.Type!=='tmpfs'&&typeof m.Source==='string'&&overlaps(m.Source,v.Mountpoint)))bad('restore_target_invalid');
    }
  };
  const ownedExtract=async()=>{
    const c=await engine.inspect(extractId);
    if(c.Id!==extractId||c.Config?.Labels?.[OWNER]!==nonce||c.Config?.Labels?.[OP]!==operationId||c.Image!==image||c.HostConfig?.ReadonlyRootfs!==true||c.HostConfig?.NetworkMode!=='none'||c.HostConfig?.Privileged===true||c.HostConfig?.LogConfig?.Type!=='none'||Object.keys(c.HostConfig.LogConfig.Config??{}).length!==0||c.Mounts?.length!==1||c.Mounts[0].Type!=='volume'||c.Mounts[0].Name!==volume||c.Mounts[0].Destination!=='/data'||c.Mounts[0].RW!==true)bad('restore_target_invalid');
    return c;
  };
  try{
    await assertOwned();
    const created=await engine.create(helper,{Image:image,Entrypoint:['tar'],Cmd:['--extract','--file=-','--directory=/data','--numeric-owner','--same-owner','--same-permissions','--delay-directory-restore'],User:'0:0',Labels:labels,
      OpenStdin:true,StdinOnce:true,AttachStdin:true,AttachStdout:true,AttachStderr:true,Tty:false,
      HostConfig:{NetworkMode:'none',ReadonlyRootfs:true,Privileged:false,LogConfig:{Type:'none',Config:{}},CapDrop:['ALL'],CapAdd:['CHOWN','FOWNER','DAC_OVERRIDE'],SecurityOpt:['no-new-privileges'],RestartPolicy:{Name:'no'},Mounts:[{Type:'volume',Source:volume,Target:'/data',ReadOnly:false,VolumeOptions:{NoCopy:true}}]}});
    if(!CONTAINER.test(created?.Id??''))bad('restore_target_invalid');
    extractId=created.Id;const initial=await ownedExtract();
    if(initial.State?.Running!==false)bad('restore_target_invalid');
    await assertOwned();
    extraction=processDocker(['start','--attach','--interactive',extractId],value);
    extraction.child.stdout.resume();
    extraction.child.stdin.on('error',()=>{});
    const second=await verifyTar({file,key,expectedSha256,cap:value,sink:chunk=>writeChunk(extraction.child.stdin,chunk)});
    extraction.child.stdin.end();await extraction.done;
    if(second.plainSha256!==proof.plainSha256||second.inventorySha256!==proof.inventorySha256)bad('restore_archive_changed');
    const own=await ownedExtract();
    if(own.State?.Running!==false||own.State?.ExitCode!==0)bad('restore_target_invalid');
    await assertOwned();
    const readback=await readonlyArchive(engine,extractId,operationId,'readback',image,value,randomBytes(16).toString('hex'));
    const inventory=new TarInventory(value);
    try{for await(const chunk of readback.child.stdout)inventory.feed(chunk);await readback.done;}
    finally{await readback.cleanup();}
    if(inventory.finish().inventorySha256!==proof.inventorySha256)bad('restore_readback_failed');
    await assertOwned();
    if((await ownedExtract()).State?.Running!==false)bad('restore_target_invalid');
    return {ok:true,operationId,isolated:true,authenticated:true,readbackVerified:true,sha256:expectedSha256,entries:proof.entries,fileBytes:proof.fileBytes,inventorySha256:proof.inventorySha256};
  }catch{
    extraction?.child.stdin.destroy();extraction?.child.kill('SIGKILL');await extraction?.done.catch(()=>{});
    if(extractId)await stopOwned(engine,extractId,OWNER,nonce,image);
    // Retain this owned volume and stopped helper. Never remove evidence or
    // continue a release after an ambiguous/failing extract/readback.
    bad('restore_incomplete_quarantined');
  }
}

async function stdinKey(){
  const parts=[];let size=0;
  for await(const piece of process.stdin){size+=piece.length;if(size>65)bad('backup_key_invalid');parts.push(piece);}
  const bytes=Buffer.concat(parts);
  if(bytes.at(-1)!==10)bad('backup_key_invalid');return keyBytes(bytes.subarray(0,-1));
}
async function readPlan(file){
  const f=await open(file,constants.O_RDONLY|constants.O_NOFOLLOW);
  try{
    const stat=await f.stat();if(!stat.isFile()||stat.size>65536||(stat.mode&0o077)!==0)bad('backup_plan_invalid');
    const value=JSON.parse(await f.readFile('utf8'));
    if(!value||typeof value!=='object'||Array.isArray(value))bad('backup_plan_invalid');
    const allowed={backup:['operation','containerId','image','operationId','directory','cap'],verify:['operation','file','expectedSha256','cap'],'restore-isolated':['operation','file','expectedSha256','image','operationId','cap']}[value.operation];
    if(!allowed||Object.keys(value).some(k=>!allowed.includes(k)))bad('backup_plan_invalid');
    return value;
  }finally{await f.close();}
}
export {readPlan,stdinKey};

if(process.argv[1]&&import.meta.url===pathToFileURL(path.resolve(process.argv[1])).href){
  let key;
  try{
    if(Number(process.versions.node.split('.')[0])!==24)bad('backup_runtime_unqualified');
    if(process.platform!=='linux'||process.argv.length!==4||process.argv[2]!=='--plan')bad('backup_arguments_invalid');
    const {operation,...options}=await readPlan(process.argv[3]);key=await stdinKey();
    let result;
    if(operation==='backup')result=await backupStopped({...options,key});
    else if(operation==='restore-isolated')result=await restoreIsolated({...options,key});
    else{const p=await verifyTar({...options,key});result={ok:true,authenticated:true,sha256:p.sha256,entries:p.entries,fileBytes:p.fileBytes,inventorySha256:p.inventorySha256};}
    process.stdout.write(JSON.stringify(result)+'\n');
  }catch{process.stdout.write('{"ok":false,"code":"backup_operation_failed"}\n');process.exitCode=1;}
  finally{key?.fill(0);}
}
