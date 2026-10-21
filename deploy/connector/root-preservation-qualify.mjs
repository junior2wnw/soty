#!/usr/bin/env node
import { open,readFile,realpath,lstat,writeFile } from 'node:fs/promises';
import { constants,fstatSync } from 'node:fs';
import { createHash,randomBytes } from 'node:crypto';
import { fileURLToPath,pathToFileURL } from 'node:url';
import path from 'node:path';
import { DockerApi,SafeError } from './docker-api.mjs';
import { createConfig,preservationHash } from './rollout.mjs';
import { guardStorageStart,requireStorageStartReceipt } from './storage-guard.mjs';
import { rootPreservationRelease,verifyRootReleaseSources } from './root-preservation-release.mjs';
import { rootRuntimeReadiness,requireWorld6Apps14Images,OWNER,assertRootDockerTransport } from './root-preservation-runtime.mjs';
import { journal } from './journal.mjs';

const must=(ok,code)=>{if(!ok)throw new SafeError(code);};
const sha=b=>createHash('sha256').update(b).digest('hex'),wait=ms=>new Promise(r=>setTimeout(r,ms));
export const SYNTHETIC_APPLICATION_TOKENS=Object.freeze({applications:Object.freeze([
  Object.freeze({id:'synthetic_app',token:'synthetic_application_token_'.padEnd(48,'a')})
])});
async function pinnedFixtureStage(expected){
  const file=new URL('./root-preservation-qualification.manifest.json',import.meta.url),bytes=await readFile(file);
  must(sha(bytes)===expected,'qualification_source_changed');const manifest=JSON.parse(bytes),root=new URL('../../',import.meta.url);
  must(manifest.schema==='soty.root-preservation-qualification-sources.v2'&&manifest.profile.world===6&&manifest.profile.apps===14,'qualification_source_changed');
  for(const entry of manifest.files){const bytes=await readFile(new URL(entry.path,root));must(bytes.length===entry.bytes&&sha(bytes)===entry.sha256,'qualification_source_changed');}
  return manifest;
}
/** Fresh, initially empty disposable Docker daemon ONLY. No existing Root ID,
 * production configuration, credentials, target volume or general script port. */
export async function qualifyRootPreservation(plan,{stdin=process.stdin}={}){
  must(plan&&Object.keys(plan).sort().join(',')===['schema','dockerSocket','originalImage','candidateImage','helperImage','storageProbeImage','revision','transaction','directory','sourcePins','qualificationManifestSha256'].sort().join(',')
    &&plan.schema==='soty.root-preservation-fixture-plan.v2'&&typeof plan.transaction==='string'&&/^[a-f0-9]{32}$/u.test(plan.transaction)
    &&typeof plan.revision==='string'&&/^[a-f0-9]{40}$/u.test(plan.revision)
    &&[plan.originalImage,plan.candidateImage,plan.helperImage,plan.storageProbeImage].every(value=>typeof value==='string'&&/^sha256:[a-f0-9]{64}$/u.test(value))
    &&path.isAbsolute(plan.directory||'')&&path.isAbsolute(plan.dockerSocket||''),'qualification_plan_invalid');
  assertRootDockerTransport(plan.dockerSocket);
  const sources=await pinnedFixtureStage(plan.qualificationManifestSha256);
  const directory=await realpath(plan.directory),stat=await lstat(directory);
  must(directory===plan.directory&&stat.isDirectory()&&stat.uid===process.getuid()&&(stat.mode&0o077)===0,'qualification_directory_invalid');
  const engine=new DockerApi({socketPath:plan.dockerSocket,timeoutMs:70000}),all=await engine.request('GET','/containers/json?all=1');
  must(Array.isArray(all)&&all.length===0,'qualification_requires_empty_disposable_daemon');
  await requireWorld6Apps14Images(engine,[plan.originalImage,plan.candidateImage]);
  const candidate=await engine.image(plan.candidateImage);
  must(candidate.Id===plan.candidateImage&&candidate.Config?.Labels?.['org.opencontainers.image.revision']===plan.revision,'qualification_revision_mismatch');
  for(const id of [plan.helperImage,plan.storageProbeImage])must((await engine.image(id)).Id===id,'qualification_image_invalid');
  const volumeName='soty-preservation-fixture-'+plan.transaction,volumeLabels={[OWNER]:plan.transaction,'io.soty.root-preservation.fixture':'1'};
  try{await engine.request('GET','/volumes/'+volumeName);throw new SafeError('qualification_target_exists');}catch(error){if(error.code!=='engine_http_404')throw error;}
  await engine.request('POST','/volumes/create',{Name:volumeName,Driver:'local',Labels:volumeLabels});
  const volume=await engine.request('GET','/volumes/'+volumeName);
  must(volume.Name===volumeName&&volume.Driver==='local'&&volume.Scope==='local'&&volume.Labels?.[OWNER]===plan.transaction
    &&volume.Labels?.['io.soty.root-preservation.fixture']==='1'&&(!volume.Options||!Object.keys(volume.Options).length),'qualification_volume_invalid');
  const createOwned=async(name,config)=>{
    let made;try{made=await engine.create(name,config);}catch{}
    const actual=await engine.inspect(made?.Id||name);
    must(actual.Name==='/'+name&&actual.Image===config.Image&&actual.Config.Labels?.[OWNER]===plan.transaction&&actual.State.Status==='created'&&actual.State.Running===false,'qualification_create_unresolved');
    return actual;
  };
  const startOnce=async container=>{
    try{await engine.start(container.Id);}catch{}
    for(let i=0;i<60;i++){const current=await engine.inspect(container.Id);
      must(current.Id===container.Id&&current.Image===container.Image&&current.Config.Labels?.[OWNER]===plan.transaction,'qualification_identity_changed');
      if(current.State.Running||current.State.Status==='exited')return current;await wait(100);}
    throw new SafeError('qualification_start_unresolved');
  };
  // Genuine World6 initializer imports Sharp. Retain the immutable full image's
  // /app/node_modules; overlay only the exact pinned public source files.
  const init=await createOwned('soty-preservation-init-'+plan.transaction,{Image:plan.candidateImage,User:'0:0',WorkingDir:'/app',Entrypoint:['node'],
    Cmd:['/app/deploy/connector/root-preservation-fixture.mjs','--initialize-owned-fixture','0','0'],Labels:volumeLabels,
    HostConfig:{NetworkMode:'none',PortBindings:{},RestartPolicy:{Name:'no'},LogConfig:{Type:'none',Config:{}},ReadonlyRootfs:true,CapDrop:['ALL'],CapAdd:['CHOWN','FOWNER','DAC_OVERRIDE'],
      SecurityOpt:['no-new-privileges'],Memory:1073741824,MemorySwap:1073741824,PidsLimit:32,Tmpfs:{'/tmp':'rw,noexec,nosuid,nodev,size=268435456,mode=0700'},
      Mounts:[{Type:'volume',Source:volumeName,Target:'/data',ReadOnly:false,VolumeOptions:{NoCopy:true}},
        ...sources.files.map(entry=>({Type:'bind',Source:fileURLToPath(new URL('../../'+entry.path,import.meta.url)),Target:'/app/'+entry.path,ReadOnly:true}))]}});
  let initialized=await startOnce(init);
  for(let i=0;initialized.State.Running&&i<120;i++){await wait(250);initialized=await engine.inspect(init.Id);}
  if(initialized.State.Running){try{await engine.request('POST',`/containers/${init.Id}/kill?signal=SIGKILL`);}catch{}throw new SafeError('qualification_initializer_unresolved');}
  must(initialized.State.Status==='exited'&&initialized.State.ExitCode===0,'qualification_initializer_failed');
  // This file and these Env values are invented fixture material, never copied
  // from an installation. The GPG custody key still comes only from private stdin.
  const tokenFile=path.join(directory,'fixture-application-tokens.json');
  await writeFile(tokenFile,JSON.stringify(SYNTHETIC_APPLICATION_TOKENS),{flag:'wx',mode:0o600});
  // Multiple independent bind files exercise genuine GPG producer/restore
  // indices; the token file sorts between these deliberately reversed mounts.
  const bindFirst=path.join(directory,'fixture-a-config.txt'),bindLast=path.join(directory,'fixture-z-key.txt');
  await writeFile(bindFirst,'Synthetic configuration, distinct from the token and key fixtures.\n',{flag:'wx',mode:0o640});
  await writeFile(bindLast,'Synthetic key marker; no production credential.\n',{flag:'wx',mode:0o600});
  const image=await engine.image(plan.originalImage),config={...structuredClone(image.Config),Image:plan.originalImage,User:'0:0',
    Env:['DATA_DIR=/data','PORT=8080','HOST=0.0.0.0','SOTY_GONKA_BASE_URL=http://127.0.0.1:9/v1','SOTY_GONKA_API_KEY=synthetic-no-real-upstream-key',
      'SOTY_GONKA_APPLICATION_TOKENS_FILE=/run/fixture-application-tokens.json'],Labels:{...image.Config.Labels,...volumeLabels},
    HostConfig:{NetworkMode:'none',PortBindings:{},RestartPolicy:{Name:'no',MaximumRetryCount:0},LogConfig:{Type:'none',Config:{}},Memory:1073741824,PidsLimit:180,
      Mounts:[{Type:'volume',Source:volumeName,Target:'/data',ReadOnly:false,VolumeOptions:{NoCopy:true}},
        {Type:'bind',Source:bindLast,Target:'/run/z-fixture-key.txt',ReadOnly:true},
        {Type:'bind',Source:tokenFile,Target:'/run/fixture-application-tokens.json',ReadOnly:true},
        {Type:'bind',Source:bindFirst,Target:'/run/a-fixture-config.txt',ReadOnly:true}]},
    NetworkingConfig:{EndpointsConfig:{}}};
  const original=await createOwned('soty-online-chat',config),guardState={};
  const guard=await guardStorageStart({engine,probeImage:plan.storageProbeImage,transactionId:randomBytes(16).toString('hex'),getState:()=>guardState,record:async fields=>Object.assign(guardState,fields)},original);
  requireStorageStartReceipt(guard,original.Id);must(guard.world===6&&guard.apps===14,'qualification_formats_invalid');
  const running=await startOnce(original);must(running.State.Running===true,'qualification_boot_failed');
  await rootRuntimeReadiness(engine,original.Id);
  const current=await engine.inspect(original.Id),releasePlan={schema:'soty.root-preservation-release.v2',originalId:current.Id,originalImage:plan.originalImage,
    candidateImage:plan.candidateImage,revision:plan.revision,helperImage:plan.helperImage,storageProbeImage:plan.storageProbeImage,transaction:plan.transaction,
    expectedConfigurationSha256:preservationHash(createConfig(current,current.Image,plan.transaction)),stockManifestSha256:'1b6e85270a7d4d42cd077fd89fd6ed6a10b802e27b337a8f377499d5907b5b2f',
    sourcePins:plan.sourcePins,directory,dockerSocket:plan.dockerSocket};
  await verifyRootReleaseSources(releasePlan);
  const controller=rootPreservationRelease(releasePlan,{engine,record:state=>journal(path.join(directory,'fixture-rollout.json'),state)}),prepared=await controller.prepare();
  // Automatic approval is confined to this freshly created labelled synthetic
  // original. Production prepare never approves or fabricates drain evidence.
  const approval={approved:true,legacyNoPendingWritesObserved:true,sourcePinsSha256:prepared.sourcePinsSha256,bindFenceSha256:prepared.bindFenceSha256,
    ...Object.fromEntries(['originalId','candidateId','configurationSha256','revision','storageProbeImage'].map(key=>[key,prepared.rollout[key]]))};
  const result=await controller.promote({prepared,approval,stdin});
  return {ok:true,schema:'soty.root-preservation-disposable-qualification.v2',syntheticOnly:true,transaction:plan.transaction,
    originalId:current.Id,candidateId:result.candidateId,originalImage:plan.originalImage,candidateImage:plan.candidateImage,
    phase:result.phase,coldVerification:result.coldVerification,qualification:result.preservationQualification};
}
if(process.argv[1]&&import.meta.url===pathToFileURL(path.resolve(process.argv[1])).href){
  let result;
  try{
    must(process.platform==='linux'&&Number(process.versions.node.split('.')[0])===24&&Number(process.versions.node.split('.')[1])>=15,'qualification_runtime_unqualified');
    must(process.argv.length===4&&process.argv[2]==='--plan','qualification_arguments_invalid');const pipe=fstatSync(0);must(pipe.isFIFO()||pipe.isSocket(),'qualification_key_pipe_required');
    const file=process.argv[3];must(await realpath(file)===path.resolve(file),'qualification_plan_invalid');const handle=await open(file,constants.O_RDONLY|constants.O_NOFOLLOW);
    let plan;try{const stat=await handle.stat();must(stat.isFile()&&stat.nlink===1&&stat.uid===process.getuid()&&(stat.mode&0o077)===0&&stat.size<=65536,'qualification_plan_invalid');plan=JSON.parse(await handle.readFile('utf8'));}finally{await handle.close();}
    result=await qualifyRootPreservation(plan);
  }catch(error){result={ok:false,code:error instanceof SafeError?error.code:'qualification_failed'};}
  process.stdout.write(JSON.stringify(result)+'\n');if(!result.ok)process.exitCode=1;
}
