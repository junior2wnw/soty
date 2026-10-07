import {spawn} from 'node:child_process';
import {Readable} from 'node:stream';
import {readFile,writeFile,lstat,realpath} from 'node:fs/promises';
import {createHash,generateKeyPairSync} from 'node:crypto';
import {join} from 'node:path';
import {isDeepStrictEqual} from 'node:util';
import {encryptBackup} from '../../../deploy/connect/backup.mjs';
import {inspectRestorableBackup} from '../../../deploy/connect/restore-backup.mjs';
import {assertInstalledSourceImage} from './image-guard.mjs';
import {LOCAL_LINUX_FEEDBACK_PLACEMENT as placement} from '../server/linux-feedback-local-placement.mjs';
import {SOURCE_COLD_PROFILE as profile} from './cold-profile.mjs';

const sha=bytes=>createHash('sha256').update(bytes).digest('hex'),check=value=>{if(!value)throw Error('source_cold_guard_refused');};
const same=isDeepStrictEqual;
const limits={archiveBytes:16777216,plaintextBytes:16777216,fileBytes:4194304,extractedBytes:8388608,entries:64,headers:128,pathBytes:4096,pathDepth:8,externalFiles:4,externalBytes:131072,wallMs:30000,idleMs:10000};
const owned=[];
function command(args,input,limit=2097152){
  const child=spawn(profile.dockerBinary,['--host','unix://'+profile.socket,...args],{stdio:['pipe','pipe','ignore'],env:{PATH:'/usr/bin:/bin'}});
  let bytes=0,failed=false,closed=false,streamUncertain=false,timer,force;const pieces=[];
  const stop=()=>{if(closed)return;failed=true;child.kill('SIGTERM');force??=setTimeout(()=>{if(!closed)child.kill('SIGKILL');},500);};
  const done=new Promise((resolve,reject)=>{child.once('error',()=>{failed=true;stop();});child.stdout.on('data',chunk=>{bytes+=chunk.length;
    if(bytes>limit)stop();else pieces.push(chunk);});child.once('close',code=>{closed=true;clearTimeout(timer);clearTimeout(force);code===0&&!failed?resolve(Buffer.concat(pieces)):reject(Object.assign(Error('source_cold_command_failed'),{code:streamUncertain?'source_cold_stream_unknown':'source_cold_command_failed'}));});});done.catch(()=>{});
  timer=setTimeout(stop,40000);child.stdin.on('error',()=>{streamUncertain=true;stop();});try{child.stdin.end(input);}catch{streamUncertain=true;stop();}
  return{done,stop:async()=>{if(!closed)stop();await done.catch(()=>{});}};
}
const run=async(args,input,limit)=>await command(args,input,limit).done;
const inspect=async id=>JSON.parse((await run(['inspect',id])).toString());
export function assertSourceColdContainer(item,actual){const c=actual[0];check(/^[a-f0-9]{64}$/.test(c.Id)&&(item.id===null||c.Id===item.id)&&c.Name==='/'+item.name&&c.Image===item.image&&c.Config.Labels['io.soty.source.cold']===item.nonce
  &&c.Config.User==='1000:1000'&&c.Config.WorkingDir==='/app/source-app'&&same(c.Config.Env,item.expectedEnv)
  &&(!c.HostConfig.GroupAdd||c.HostConfig.GroupAdd.length===0)&&same(c.HostConfig.Tmpfs,item.tmpfs)
  &&c.HostConfig.ReadonlyRootfs===true&&c.HostConfig.NetworkMode==='none'&&c.HostConfig.Privileged===false
  &&c.HostConfig.Memory===268435456&&c.HostConfig.MemorySwap===268435456&&c.HostConfig.PidsLimit===64
  &&c.HostConfig.NanoCpus===1000000000&&c.HostConfig.CpuPeriod===0&&c.HostConfig.CpuQuota===0&&c.HostConfig.CpuShares===0
  &&c.HostConfig.CpusetCpus===''&&c.HostConfig.CpusetMems===''&&same(c.HostConfig.CapDrop,['ALL'])
  &&(!c.HostConfig.CapAdd||c.HostConfig.CapAdd.length===0)&&same(c.HostConfig.SecurityOpt,['no-new-privileges'])
  &&(!c.HostConfig.Devices||c.HostConfig.Devices.length===0)&&(!c.HostConfig.DeviceRequests||c.HostConfig.DeviceRequests.length===0)
  &&(!c.HostConfig.DeviceCgroupRules||c.HostConfig.DeviceCgroupRules.length===0)&&(!c.HostConfig.Binds||c.HostConfig.Binds.length===0)
  &&(!c.HostConfig.VolumesFrom||c.HostConfig.VolumesFrom.length===0)&&(!c.HostConfig.PortBindings||Object.keys(c.HostConfig.PortBindings).length===0)
  &&c.HostConfig.PublishAllPorts===false&&c.HostConfig.PidMode===''&&c.HostConfig.IpcMode==='private'&&c.HostConfig.UTSMode===''
  &&c.HostConfig.RestartPolicy.Name==='no'&&c.HostConfig.RestartPolicy.MaximumRetryCount===0&&c.HostConfig.LogConfig.Type==='none'
  &&Object.keys(c.HostConfig.LogConfig.Config??{}).length===0
  &&same(c.HostConfig.Ulimits,[{Name:'core',Soft:0,Hard:0}])
  &&same(c.Config.Entrypoint,['/usr/local/bin/node'])&&same(c.Config.Cmd,item.argv)
  &&c.Mounts.length===2&&c.Mounts.filter(m=>m.Type==='volume').length===1&&c.Mounts.some(m=>m.Type==='volume'&&m.Name===item.volume&&m.Destination===item.target&&m.RW===item.rw)
  &&c.Mounts.filter(m=>m.Type==='bind').length===1&&c.Mounts.some(m=>m.Type==='bind'&&m.Source===item.packet&&m.Destination==='/probe'&&m.RW===false&&m.Propagation==='rprivate')
  &&c.HostConfig.Mounts.filter(m=>m.Type==='volume').length===1
  &&(c.HostConfig.Mounts.find(m=>m.Type==='volume').VolumeOptions?.Subpath??'')===(item.subpath??''));return c;}
const spec=assertSourceColdContainer;
async function create(config,role,volume,target,rw,argv,subpath){const tmpfs={'/tmp':'rw,nosuid,nodev,noexec,size=67108864,uid=1000,gid=1000,mode=700',
  ...(target==='/data'?{}:{'/data':'rw,nosuid,nodev,noexec,size=1048576,uid=1000,gid=1000,mode=700'})};
  const item={nonce:config.nonce,image:config.imageId,packet:config.packetDirectory,role,name:'codex-soty-source-cold-'+config.nonce+'-'+role,volume,target,rw,argv,subpath,
    expectedEnv:config.expectedEnv,tmpfs,id:null};owned.push(item);
  const args=['create','--pull=never','--name',item.name,'--label','io.soty.source.cold='+config.nonce,'--user','1000:1000','--network','none','--read-only',
    '--cap-drop','ALL','--security-opt','no-new-privileges','--memory','256m','--memory-swap','256m','--pids-limit','64','--cpus','1','--ulimit','core=0:0','--log-driver','none','--restart','no',
    '--tmpfs','/tmp:rw,nosuid,nodev,noexec,size=67108864,uid=1000,gid=1000,mode=700',
    ...(target==='/data'?[]:['--tmpfs','/data:rw,nosuid,nodev,noexec,size=1048576,uid=1000,gid=1000,mode=700']),
    '--mount','type=volume,src='+volume+',dst='+target+(rw?'':',readonly')+(subpath?',volume-subpath='+subpath:''),
    '--mount','type=bind,src='+config.packetDirectory+',dst=/probe,readonly,bind-propagation=rprivate','--workdir','/app/source-app','--interactive','--entrypoint','/usr/local/bin/node',config.imageId,...argv];
  item.id=(await run(args)).toString().trim();check(/^[a-f0-9]{64}$/.test(item.id));spec(item,await inspect(item.id));return item;}
async function execute(item,input,limit){const handle=command(['start','--attach','--interactive',item.id],input,limit);try{const result=await handle.done,c=spec(item,await inspect(item.id));
  check(c.State.Status==='exited'&&c.State.ExitCode===0&&!c.State.OOMKilled);return result;}finally{await handle.stop();}}
async function noWriters(volume){const ids=(await run(['container','ls','--quiet','--no-trunc'])).toString().trim().split(/\s+/).filter(Boolean);
  for(const id of ids){const c=(await inspect(id))[0];check(!c.Mounts?.some(m=>m.Type==='volume'&&m.Name===volume&&m.RW===true));}}

export async function runSourceCold(config){
  check(process.platform==='linux'&&process.getuid()===1000&&Object.keys(config).sort().join(',')==='imageId,nonce,packetDirectory,packetManifestSha256,sourceCommit');
  check(/^[a-f0-9]{32}$/.test(config.nonce)&&config.packetDirectory===placement.lab+'/source-cold-'+config.nonce);
  check(config.imageId===profile.imageId&&config.sourceCommit===profile.sourceCommit&&config.packetManifestSha256===profile.packetManifestSha256);
  const stat=await lstat(config.packetDirectory);check(stat.isDirectory()&&!stat.isSymbolicLink()&&stat.uid===1000&&(stat.mode&0o777)===0o700&&await realpath(config.packetDirectory)===config.packetDirectory);
  const image=(JSON.parse((await run(['image','inspect',config.imageId])).toString()))[0];assertInstalledSourceImage(image,{imageId:config.imageId,sourceCommit:config.sourceCommit,packetManifestSha256:config.packetManifestSha256});
  // Exact private env witness comes ONLY from the independently pinned image,
  // never from operator/author JSON and never goes into a public receipt.
  config={...config,expectedEnv:image.Config.Env.slice()};
  const volumes=['codex-soty-source-cold-'+config.nonce+'-source','codex-soty-source-cold-'+config.nonce+'-target'];
  // The reviewed guardian creates/custodies empty physical volumes beforehand.
  // This runner verifies them and never adopts an arbitrary existing volume.
  for(const volume of volumes){const v=JSON.parse((await run(['volume','inspect',volume])).toString())[0];check(v.Name===volume&&v.Driver==='local'&&v.Labels?.['io.soty.source.cold']===config.nonce);}
  let phase='seed',passed=false,cleanupUnknown=false,code='none';const cleanup=[];
  try{
    await noWriters(volumes[0]);await noWriters(volumes[1]);
    const seed=await create(config,'seed',volumes[0],'/data',true,['/probe/fixture.mjs','seed']);
    const before=JSON.parse((await execute(seed)).toString());check(before.schema==='soty.source-cold-witness.v1'&&before.authenticationProved===false&&before.data.reader.format===3);
    await writeFile(join(config.packetDirectory,'before.private.json'),JSON.stringify(before)+'\n',{flag:'wx',mode:0o600});
    phase='archive';await noWriters(volumes[0]);
    const archive=await create(config,'archive',volumes[0],'/data',false,['--input-type=module','-e',"import{spawn}from'node:child_process';const c=spawn('tar',['-C','/data','-cf','-','.'],{stdio:['ignore','pipe','ignore']});c.stdout.pipe(process.stdout);c.on('close',n=>{process.exitCode=n;});"]);
    const tar=await execute(archive,undefined,16777216);await noWriters(volumes[0]);
    const inventory={files:before.files.slice().sort((a,b)=>a.path<b.path?-1:a.path>b.path?1:0),stores:[{id:'ordinary-native',required:true,present:true,format:'soty.ordinary-native.v3',identitySha256:before.data.nativeIdentityDigest,
      paths:before.files.filter(f=>f.type==='file').map(f=>f.path).sort()}],external:[]};
    const manifest={version:1,generationId:config.nonce,checkpointSha256:sha(JSON.stringify(before.data)),inventory};
    const witness={generationId:config.nonce,checkpointSha256:manifest.checkpointSha256,inventorySha256:sha(JSON.stringify(inventory))};
    const key=generateKeyPairSync('rsa',{modulusLength:3072,publicKeyEncoding:{format:'pem',type:'spki'},privateKeyEncoding:{format:'pem',type:'pkcs8'}});
    const file=join(config.packetDirectory,'backup.enc');await encryptBackup({output:file,publicKey:key.publicKey,metadata:{offline:true,dataFormat:'tar',secrets:{},restoreManifest:manifest,restoreFiles:{}},stream:Readable.from([tar])});tar.fill(0);
    const expectedSha256=sha(await readFile(file)),expectedManifestSha256=sha(JSON.stringify(manifest)),input={file,privateKeyPem:key.privateKey,expectedSha256,expectedManifestSha256,sourceWitness:witness,limits};
    phase='dry_inspect';check((await inspectRestorableBackup(input)).inventoryMatched===true);
    phase='restore';const restore=await create(config,'restore',volumes[1],'/target',true,['/probe/extract.mjs']);
    const restored=JSON.parse((await execute(restore,JSON.stringify({privateKeyPem:key.privateKey,expectedSha256,expectedManifestSha256,sourceWitness:witness,limits,targetId:config.nonce}))).toString());key.privateKey='';
    check(restored.passed===true&&restored.authenticated===true&&restored.inventoryMatched===true);
    phase='compare';await noWriters(volumes[1]);const verify=await create(config,'verify',volumes[1],'/data',true,['/probe/fixture.mjs','witness'],'restore/data');
    const after=JSON.parse((await execute(verify)).toString());await writeFile(join(config.packetDirectory,'after.private.json'),JSON.stringify(after)+'\n',{flag:'wx',mode:0o600});
    check(JSON.stringify(before.data)===JSON.stringify(after.data)&&JSON.stringify(before.files)===JSON.stringify(after.files));
    phase='negatives';const negative=await create(config,'negatives',volumes[1],'/data',true,['/probe/fixture.mjs','negatives'],'restore/data');
    const denied=JSON.parse((await execute(negative)).toString());check(denied.wrongCipherKeyDenied===true&&denied.foreignRealmDenied===true);
    phase='entry';const entry=await create(config,'entry',volumes[1],'/data',true,['/probe/entry.mjs'],'restore/data');
    const entryProof=JSON.parse((await execute(entry)).toString());check(entryProof.passed===true&&entryProof.currentReader===true&&entryProof.legacyRefusedBeforeStart===true
      &&entryProof.foreignRealmDenied===true&&entryProof.missingKeyDenied===true&&entryProof.wrongKeyBeforeListenerDenied===true&&entryProof.actualInstallerListening===true&&entryProof.spoofedReadDenied===true);
    passed=true;
  }catch(error){passed=false;code=['source_cold_stream_unknown','source_cold_command_failed'].includes(error?.code)?error.code:'source_cold_guard_refused';
    if(code==='source_cold_stream_unknown')cleanupUnknown=true;}
  finally{for(const item of owned.reverse())try{const c=spec(item,await inspect(item.id??item.name));
    if(c.State.Running)await run(['kill','--signal','SIGKILL',c.Id]);const state=spec(item,await inspect(c.Id));check(!state.State.Running);
    await run(['rm',c.Id]);cleanup.push({role:item.role,stopped:true,removed:true});}catch{cleanupUnknown=true;cleanup.push({role:item.role,stopped:false,removed:false});}}
  return{schema:'soty.source-cold-receipt.v1',passed:passed&&!cleanupUnknown,phase,code,physicalVolumes:2,cleanupUnknown,cleanup,
    cipherCompared:passed,rolesCompared:passed,mediaCompared:passed,receiptCompared:passed,entryProved:passed,negativeChecks:passed?5:0,authenticationProved:false,models:false,productionReady:false};
}
