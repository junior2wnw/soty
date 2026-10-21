#!/usr/bin/env node
import { readFile,open,lstat,realpath,unlink } from 'node:fs/promises';
import { constants,fstatSync } from 'node:fs';
import { createHash,randomBytes } from 'node:crypto';
import { fileURLToPath,pathToFileURL } from 'node:url';
import path from 'node:path';
import { DockerApi,SafeError } from './docker-api.mjs';
import { Rollout,hash,createConfig,preservationHash } from './rollout.mjs';
import { productionMaintenance } from './runtime.mjs';
import { requireStorageStartReceipt } from './storage-guard.mjs';
import { journal } from './journal.mjs';
import { backupStopped,restoreIsolated } from '../connect/gpg-release-backup.mjs';
import { keyBytes } from '../connect/gpg-tar.mjs';
import { privateBindFence,sourceFence,sameObservation,restoredVolume,restoredBinds,preservationSandbox,requireWorld6Apps14Images,rootRuntimeReadiness,assertRootDockerTransport,rootDockerEngine } from './root-preservation-runtime.mjs';

const must=(ok,code)=>{if(!ok)throw new SafeError(code);};
const SHA=/^[a-f0-9]{64}$/u,IMAGE=/^sha256:[a-f0-9]{64}$/u;
const STOCK='1b6e85270a7d4d42cd077fd89fd6ed6a10b802e27b337a8f377499d5907b5b2f';
const ASSETS=['root-preservation-release.mjs','root-preservation-runtime.mjs','root-preservation-observe-runner.mjs','root-state-observer.mjs'];
const CURRENT_FORMAT=Object.freeze({rooms:2,apps:14,notes:2,capabilities:3,appRegistration:1,feedback:1,humanIdentity:3,connect:4,world:6});
const STORES=Object.keys(CURRENT_FORMAT);
// Closed diagnostic vocabulary, never an error message, arbitrary code or DTO.
const PRESERVATION_FAILURE_CODES=new Set([
  'backup_receipt_invalid','restore_receipt_invalid','restore_volume_invalid','restored_bind_changed','release_docker_transport_mismatch',
  'private_bind_invalid','private_bind_limit','private_bind_changed',
  'preservation_identity','preservation_mounts_invalid','preservation_create_unresolved','preservation_prior_execution','preservation_start_unresolved','preservation_stop_unresolved',
  'observation_volume_invalid','root_observation_failed','root_observation_timeout','fixed_exec_invalid','fixed_exec_identity','fixed_exec_unresolved',
  'clone_identity_invalid','clone_profile_unsupported','clone_source_invalid','clone_initial_data_changed','clone_format_changed',
  'clone_boot_failed','clone_health_failed','clone_stop_failed','clone_stop_forced','clone_data_changed','clone_bind_changed',
  'storage_helper_identity','storage_helper_unresolved','storage_start_guard_missing','storage_reader_incompatible','storage_runtime_changed',
  'storage_format_unknown','storage_format_unreadable','storage_probe_failed','storage_probe_invalid','storage_other_writer',
  'preservation_gpg_unsettled','preservation_source_data_changed','preservation_source_container_changed','preservation_source_bind_changed'
]);
const sha=value=>createHash('sha256').update(value).digest('hex');
function closed(value,keys){must(value&&typeof value==='object'&&!Array.isArray(value)&&Object.keys(value).sort().join(',')===[...keys].sort().join(','),'release_plan_invalid');}
export function validateRootPreservationPlan(plan){
  closed(plan,['schema','originalId','originalImage','candidateImage','revision','storageProbeImage','helperImage','transaction','expectedConfigurationSha256','stockManifestSha256','sourcePins','directory','dockerSocket']);
  must(plan.schema==='soty.root-preservation-release.v2'&&typeof plan.originalId==='string'&&SHA.test(plan.originalId)&&[plan.originalImage,plan.candidateImage,plan.helperImage,plan.storageProbeImage].every(v=>typeof v==='string'&&IMAGE.test(v))
    &&typeof plan.revision==='string'&&/^[a-f0-9]{40}$/u.test(plan.revision)&&typeof plan.transaction==='string'&&/^[a-f0-9]{32}$/u.test(plan.transaction)
    &&typeof plan.expectedConfigurationSha256==='string'&&SHA.test(plan.expectedConfigurationSha256)&&plan.stockManifestSha256===STOCK,'release_plan_invalid');
  closed(plan.sourcePins,ASSETS);must(ASSETS.every(name=>typeof plan.sourcePins[name]==='string'&&SHA.test(plan.sourcePins[name])),'release_source_pins_invalid');
  must(typeof plan.directory==='string'&&path.isAbsolute(plan.directory)&&!plan.directory.includes('\0')&&typeof plan.dockerSocket==='string'&&path.isAbsolute(plan.dockerSocket),'release_plan_invalid');
  return structuredClone(plan);
}
export async function verifyRootReleaseSources(plan){
  const root=new URL('../../',import.meta.url),bytes=await readFile(new URL('./cold-verification-stage.stock.json',import.meta.url));
  must(sha(bytes)===STOCK,'release_stock_manifest_changed');const manifest=JSON.parse(bytes);
  must(manifest.stockRevision==='cbeb42586ff501899652b2010188415ce5b68d16'&&manifest.stock.length===60&&manifest.patches.length===1,'release_stock_manifest_changed');
  for(const entry of manifest.stock){const pin=manifest.patches.find(p=>p.path===entry.path)||entry;const data=await readFile(new URL(entry.path,root));
    must(data.length===pin.bytes&&sha(data)===pin.sha256,'release_stock_source_changed');}
  for(const file of ASSETS)must(sha(await readFile(new URL('./'+file,import.meta.url)))===plan.sourcePins[file],'release_source_changed');
  return hash({stock:STOCK,sourcePins:plan.sourcePins});
}
async function stdinKey(input){
  const pieces=[];let size=0;
  try{for await(const piece of input){const bytes=Buffer.from(piece);pieces.push(bytes);size+=bytes.length;must(size<=65,'release_key_invalid');}
    const bytes=Buffer.concat(pieces);try{must(bytes.length===65&&bytes[64]===10,'release_key_invalid');return Buffer.from(keyBytes(bytes.subarray(0,64)));}finally{bytes.fill(0);}
  }finally{for(const piece of pieces)piece.fill(0);}
}
async function assertDirectory(directory){
  must(await realpath(directory)===path.resolve(directory),'release_directory_invalid');const stat=await lstat(directory);
  must(stat.isDirectory()&&!stat.isSymbolicLink()&&(stat.mode&0o077)===0&&stat.uid===process.getuid(),'release_directory_invalid');
}
function validateApproval(approval,prepared,sourcePinsSha256){
  const state=prepared.rollout;
  must(approval?.approved===true&&approval.legacyNoPendingWritesObserved===true&&approval.originalId===state.originalId
    &&approval.candidateId===state.candidateId&&approval.configurationSha256===state.configurationSha256&&approval.revision===state.revision,'supervised_receipt_mismatch');
  must(approval.storageProbeImage===state.storageProbeImage,'supervised_storage_probe_mismatch');
  must(['preserveQueuedSha256','applicationPolicySha256','universalPolicyDigest','universalWitnessId'].every(key=>(approval[key]??null)===null&&(state[key]??null)===null),'supervised_policy_receipt_mismatch');
  must(approval.sourcePinsSha256===sourcePinsSha256&&approval.bindFenceSha256===prepared.bindFenceSha256,'supervised_preservation_receipt_mismatch');
}

export function rootPreservationMaintenance(engine){
  const maintenance=productionMaintenance(engine);
  return (verb,run)=>{
    // Legacy JSON conversion discards unknown committed SQLite tables.
    must(verb!=='rollback','release_legacy_rollback_forbidden');
    return maintenance(verb,run);
  };
}

/** Fixed production composition. Dependency injection is limited to the existing
 * Engine port and durable journal sink; no backup/probe/observer/command hook. */
export function rootPreservationRelease(input,{engine,record}={}){
  const plan=validateRootPreservationPlan(input);engine=rootDockerEngine(engine??new DockerApi({socketPath:plan.dockerSocket,timeoutMs:70000}));
  must(typeof record==='function','release_journal_required');let run,sourcePinsSha256;
  const args={originalId:plan.originalId,originalImage:plan.originalImage,candidateImage:plan.candidateImage,revision:plan.revision,transaction:plan.transaction,storageProbeImage:plan.storageProbeImage};
  const preflight=async()=>{
    assertRootDockerTransport(plan.dockerSocket);sourcePinsSha256=await verifyRootReleaseSources(plan);await assertDirectory(plan.directory);
    await requireWorld6Apps14Images(engine,[plan.originalImage,plan.candidateImage]);
    for(const image of [plan.helperImage,plan.storageProbeImage])must((await engine.image(image)).Id===image,'release_image_changed');
    const original=await engine.inspect(plan.originalId);
    must(original.Id===plan.originalId&&original.Image===plan.originalImage&&original.Name==='/soty-online-chat'&&original.State.Running,'original_identity_mismatch');
    must(preservationHash(createConfig(original,original.Image,plan.transaction))===plan.expectedConfigurationSha256,'release_configuration_changed');
    return original;
  };
  const cold=(key,bindFenceSha256)=>async binding=>{
    const original=await engine.inspect(plan.originalId),beforeContainer=sourceFence(original);
    must(binding.originalId===plan.originalId&&binding.originalImage===plan.originalImage&&binding.candidateImage===plan.candidateImage&&binding.revision===plan.revision&&binding.transaction===plan.transaction,'cold_binding_changed');
    const binds=original.Mounts.filter(m=>m.Type==='bind'),beforeBinds=await privateBindFence(binds);
    must(hash(beforeBinds)===bindFenceSha256,'cold_binding_changed');
    const mount=original.Mounts.find(m=>m.Type==='volume'&&m.Destination==='/data');must(mount,'release_data_mount_invalid');
    const volume=await engine.request('GET','/volumes/'+encodeURIComponent(mount.Name));
    const sandbox=preservationSandbox({engine,helperImage:plan.helperImage,storageProbeImage:plan.storageProbeImage,transaction:plan.transaction,
      record:fields=>run.note(run.state.phase,{preservationHelper:fields})});
    const sourceGuard=await run.guardStart(original.Id);requireStorageStartReceipt(sourceGuard,original.Id);
    must(STORES.every(store=>sourceGuard[store]===CURRENT_FORMAT[store]),'release_reader_mismatch');
    const source=await sandbox.observe(volume);let failure=false,gpgSettled=false,backup,clones=[];
    const failures=[];
    const noteFailure=async(stage,error)=>{
      const value={stage,code:error instanceof SafeError&&PRESERVATION_FAILURE_CODES.has(error.code)?error.code:'preservation_operation_failed'};
      failures.push(value);
      await run.note(run.state.phase,{preservationFailure:value,preservationFailures:failures.slice(-2)});
    };
    const operationId=randomBytes(16).toString('hex'),volumeRestoreId=randomBytes(16).toString('hex'),configRestoreId=randomBytes(16).toString('hex');
    const directory=path.join(plan.directory,'backup-'+operationId);
    try{
      assertRootDockerTransport(plan.dockerSocket);backup=await backupStopped({containerId:original.Id,image:plan.helperImage,operationId,directory,key},engine);
      must(backup?.ok===true&&backup.encrypted===true&&backup.operationId===operationId,'backup_receipt_invalid');
      for(const [file,part,id] of [['volume.gpg','volume',volumeRestoreId],['configuration.gpg','configuration',configRestoreId]]){
        assertRootDockerTransport(plan.dockerSocket);const restored=await restoreIsolated({file:path.join(directory,file),expectedSha256:backup[part].sha256,image:plan.helperImage,operationId:id,key},engine);
        must(restored?.ok===true&&restored.isolated===true&&restored.authenticated===true&&restored.readbackVerified===true&&restored.operationId===id
          &&restored.sha256===backup[part].sha256&&restored.inventorySha256===backup[part].inventorySha256,'restore_receipt_invalid');
      }
      gpgSettled=true;
      const data=await restoredVolume(engine,volumeRestoreId),configuration=await restoredVolume(engine,configRestoreId);
      must((await privateBindFence(restoredBinds(original,configuration))).contentSha256===beforeBinds.contentSha256,'restored_bind_changed');
      for(const [role,image] of [['fallback',plan.originalImage],['candidate',plan.candidateImage]]){
        const clone=await sandbox.bootClone({original,image,data,configuration,role,source,modelSha256:run.healthSha256,applicationPolicySha256:run.originalPolicySha256});
        must(STORES.every(store=>clone.storageGuard[store]===sourceGuard[store]),'clone_format_changed');clones.push(clone);
      }
    }catch(error){failure=true;await noteFailure('qualification',error);}
    // No producer exception can attest a late GPG helper's quiescence. Settled
    // negative clone qualification may recover only after every source fence.
    try{
      await sandbox.settle();must(gpgSettled,'preservation_gpg_unsettled');const after=await sandbox.observe(volume);await sandbox.settle();
      must(gpgSettled,'preservation_gpg_unsettled');
      must(sameObservation(source,after),'preservation_source_data_changed');
      must(sourceFence(await engine.inspect(original.Id))===beforeContainer,'preservation_source_container_changed');
      must(hash(await privateBindFence(binds))===hash(beforeBinds),'preservation_source_bind_changed');
    }catch(error){await noteFailure('settle',error);throw new SafeError('root_preservation_unresolved');}
    if(failure)return {ok:false,sourceUnchanged:true};
    const qualification={schema:'soty.root-preservation-qualification.v2',binding,sourcePinsSha256,observerSha256:plan.sourcePins['root-state-observer.mjs'],
      source,volumeRestoreId,configRestoreId,volume:backup.volume,configuration:backup.configuration,clones};
    await run.note(run.state.phase,{preservationQualification:qualification});
    return {ok:true,sourceUnchanged:true,proof:{schema:'soty.rollout-cold-verification.v1',operationId,
      volumeSha256:backup.volume.sha256,configurationSha256:backup.configuration.sha256,volumeInventorySha256:backup.volume.inventorySha256,
      configurationInventorySha256:backup.configuration.inventorySha256,qualificationSha256:hash(qualification)}};
  };
  const makeRun=coldVerification=>new Rollout({engine,maintenance:rootPreservationMaintenance(engine),
    ready:kind=>rootRuntimeReadiness(engine,kind==='candidate'?run.candidate.Id:plan.originalId,kind==='candidate'),record,coldVerification});
  return {
    async prepare(){
      const original=await preflight(),bindFenceSha256=hash(await privateBindFence(original.Mounts.filter(m=>m.Type==='bind')));
      run=makeRun(undefined);await run.prepare(args);
      return {schema:'soty.root-preservation-prepare.v2',sourcePinsSha256,expectedConfigurationSha256:plan.expectedConfigurationSha256,bindFenceSha256,rollout:run.state};
    },
    async promote({prepared,approval,stdin=process.stdin}){
      const original=await preflight();
      must(prepared?.schema==='soty.root-preservation-prepare.v2'&&prepared.sourcePinsSha256===sourcePinsSha256&&prepared.expectedConfigurationSha256===plan.expectedConfigurationSha256
        &&prepared.bindFenceSha256===hash(await privateBindFence(original.Mounts.filter(m=>m.Type==='bind'))),'prepared_receipt_mismatch');
      validateApproval(approval,prepared,sourcePinsSha256);const prior=prepared.rollout;
      must(prior.phase==='prepared'&&prior.originalId===plan.originalId&&prior.originalImage===plan.originalImage&&prior.candidateImage===plan.candidateImage
        &&prior.storageProbeImage===plan.storageProbeImage&&prior.transaction===plan.transaction&&prior.revision===plan.revision,'prepared_receipt_mismatch');
      const key=await stdinKey(stdin);
      try{
        run=makeRun(cold(key,prepared.bindFenceSha256));await run.guard(args);
        must(prior.configurationSha256===run.state.configurationSha256&&prior.modelReadinessSha256===run.healthSha256,'prepared_receipt_mismatch');
        run.candidate=await engine.inspect(prior.candidateId);run.validateCandidate(run.candidate,{stopped:true});run.state=prior;
        const format=await run.guardStart(original.Id,{running:true});requireStorageStartReceipt(format,original.Id);
        must(STORES.every(store=>format[store]===CURRENT_FORMAT[store]),'release_format_mismatch');
        return await run.promote();
      }finally{key.fill(0);}
    }
  };
}

async function privateJSON(file){
  must(await realpath(file)===path.resolve(file),'release_private_file_invalid');const fd=await open(file,constants.O_RDONLY|constants.O_NOFOLLOW);
  try{const stat=await fd.stat();must(stat.isFile()&&stat.nlink===1&&stat.uid===process.getuid()&&(stat.mode&0o077)===0&&stat.size<=65536,'release_private_file_invalid');return JSON.parse(await fd.readFile('utf8'));}finally{await fd.close();}
}
export async function rootPreservationCli(argv){
  let lock,lockPath;try{
    must(process.platform==='linux'&&Number(process.versions.node.split('.')[0])===24&&Number(process.versions.node.split('.')[1])>=15,'release_runtime_unqualified');
    const [action,...rest]=argv,options={};must(['prepare','promote'].includes(action)&&rest.length%2===0&&rest.length<=4,'release_cli_invalid');
    for(let i=0;i<rest.length;i+=2){must(['--plan','--reviewed-receipt'].includes(rest[i])&&!Object.hasOwn(options,rest[i])&&typeof rest[i+1]==='string','release_cli_invalid');options[rest[i]]=rest[i+1];}
    must(options['--plan']&&(action==='promote')===Boolean(options['--reviewed-receipt']),'release_cli_invalid');
    const plan=validateRootPreservationPlan(await privateJSON(options['--plan']));await assertDirectory(plan.directory);
    lockPath=path.join(plan.directory,'release.lock');lock=await open(lockPath,'wx',0o600);
    const controller=rootPreservationRelease(plan,{record:state=>journal(path.join(plan.directory,'rollout.json'),state)});
    if(action==='prepare'){
      must(!(await lstat(path.join(plan.directory,'prepared.json')).catch(()=>null)),'release_prepared_exists');
      const prepared=await controller.prepare();await journal(path.join(plan.directory,'prepared.json'),prepared);
      return {ok:true,...prepared};
    }
    const stat=fstatSync(0);must(stat.isFIFO()||stat.isSocket(),'release_key_pipe_required');
    const state=await controller.promote({prepared:await privateJSON(path.join(plan.directory,'prepared.json')),approval:await privateJSON(options['--reviewed-receipt'])});
    return {ok:true,...state};
  }catch(error){return {ok:false,code:error instanceof SafeError?error.code:'root_preservation_failed'};}
  finally{if(lock){await lock.close();await unlink(lockPath);}}
}
if(process.argv[1]&&import.meta.url===pathToFileURL(path.resolve(process.argv[1])).href){
  const result=await rootPreservationCli(process.argv.slice(2));process.stdout.write(JSON.stringify(result)+'\n');if(!result.ok)process.exitCode=1;
}
