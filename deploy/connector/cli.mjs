#!/usr/bin/env node
import {readFile,open,unlink} from 'node:fs/promises';
import path from 'node:path';
import {pathToFileURL} from 'node:url';
import {DockerApi,SafeError} from './docker-api.mjs';
import {Rollout} from './rollout.mjs';
import {productionMaintenance,readiness,universalMeasurement} from './runtime.mjs';
import {journal} from './journal.mjs';
import {readApprovedPolicy} from './application-policy.mjs';
import {openUniversalCliPolicy} from './universal-cli-policy.mjs';
import {readUniversalLocalFile,parseUniversalLocalJson} from './universal-policy.mjs';
// Never print inspect, request/config bodies, private fingerprints, helper logs or error causes.
const flags=new Set(['journal','original-id','original-image','candidate-image','storage-probe-image','revision','transaction','preserve-queued-sha256',
  'legacy-recovery','policy-file','policy-sha256','reviewed-receipt','health-origin','docker-socket','universal-plan','universal-custody-file',
  'universal-witness-file','universal-shell-origins','fixture-root','fixture-mode']);
const require=(ok,code='invalid_cli')=>{if(!ok)throw new SafeError(code);};
async function receipt(file,context){
 if(context){const input=await readUniversalLocalFile(file,context,{privateFile:true});try{return parseUniversalLocalJson(input.bytes);}finally{input.dispose();}}
 const descriptor=await open(file,'r');try{const stat=await descriptor.stat();require(stat.isFile()&&stat.size<=65536,'receipt_file_invalid');
  const bytes=await descriptor.readFile();require(bytes.length<=65536,'receipt_file_invalid');return parseUniversalLocalJson(bytes);
 }finally{await descriptor.close();}
}
export async function runConnectorCli(argv){
 let lock,lockPath,universal;
 try{
  const [action,...rest]=argv,opts=Object.create(null);require(['prepare','promote'].includes(action)&&rest.length%2===0&&rest.length<=40);
  for(let i=0;i<rest.length;i+=2){const flag=rest[i];require(typeof flag==='string'&&flag.startsWith('--')&&flags.has(flag.slice(2))&&!Object.hasOwn(opts,flag.slice(2))&&typeof rest[i+1]==='string'&&rest[i+1].length>0);opts[flag.slice(2)]=rest[i+1];}
  require(opts.journal);
  const fixture=opts['fixture-mode']==='synthetic-local';require(!opts['fixture-mode']||fixture);require(Boolean(opts['fixture-root'])===fixture);
  const shellOrigins=opts['universal-shell-origins']?opts['universal-shell-origins'].split(','):[];
  const universalRequested=Boolean(opts['universal-plan']||opts['universal-custody-file']||opts['universal-witness-file']);
  require(universalRequested||!opts['universal-shell-origins']&&!fixture,'universal_cli_invalid');
  const context=universalRequested?{shellOrigins,...(fixture?{fixtureRoot:opts['fixture-root']}: {})}:null;
  const args={originalId:opts['original-id'],originalImage:opts['original-image'],candidateImage:opts['candidate-image'],storageProbeImage:opts['storage-probe-image'],revision:opts.revision,transaction:opts.transaction};
  if(opts['preserve-queued-sha256'])args.preserveQueuedSha256=opts['preserve-queued-sha256'];
  if(opts['legacy-recovery']){require(opts['legacy-recovery']==='committed-state','invalid_recovery_mode');args.legacyRecovery=true;}
  if(opts['policy-file']||opts['policy-sha256'])args.applicationPolicy=await readApprovedPolicy(opts['policy-file'],opts['policy-sha256']);
  const socket=opts['docker-socket'];if(socket)require(path.isAbsolute(socket)||process.platform==='win32'&&/^\\\\.\\pipe\\soty-fixture-[a-z0-9-]+$/u.test(socket),'engine_socket_invalid');
  if(fixture)require(socket&&(process.platform==='win32'?socket.includes('soty-fixture-'):path.dirname(socket)===opts['fixture-root']&&path.basename(socket)==='engine.sock'),'universal_fixture_engine_required');
  lockPath=opts.journal+'.lock';lock=await open(lockPath,'wx',0o600);
  let prior,approval;
  if(action==='promote'){require(opts['reviewed-receipt']);prior=await receipt(opts.journal,context);approval=await receipt(opts['reviewed-receipt'],context);}
  if(universalRequested){universal=await openUniversalCliPolicy({action,planFile:opts['universal-plan'],custodyFile:opts['universal-custody-file'],witnessFile:opts['universal-witness-file'],shellOrigins,fixtureRoot:fixture?opts['fixture-root']:undefined,approval});
   args.universalPolicy=universal.handle;if(universal.witnessId)args.universalWitnessId=universal.witnessId;
  }
  const engine=new DockerApi(socket?{socketPath:socket}:{});
  const run=new Rollout({engine,maintenance:productionMaintenance(engine),ready:readiness(opts['health-origin']||'http://127.0.0.1:18182'),
   universalMeasurement:universalMeasurement(engine),allowUniversalFixture:fixture,record:state=>journal(opts.journal,state)});
  if(action==='prepare'){
   if(universal){await run.guard(args);const published=await universal.publishWitness();args.universalWitnessId=published.witnessId;}
   await run.prepare(args);
  }else{
   require(approval.approved===true&&approval.originalId===args.originalId&&approval.candidateId===prior.candidateId&&approval.configurationSha256===prior.configurationSha256&&approval.revision===args.revision,'supervised_receipt_mismatch');
   require(approval.storageProbeImage===args.storageProbeImage&&prior.storageProbeImage===args.storageProbeImage,'supervised_storage_probe_mismatch');
   require(args.legacyRecovery?approval.legacyRecovery===true&&prior.legacyRecovery===true&&approval.legacyNoAssignedJobsObserved===true:approval.legacyNoPendingWritesObserved===true,'supervised_drain_or_recovery_receipt_missing');
   require((approval.preserveQueuedSha256||null)===(args.preserveQueuedSha256||null)&&(prior.preserveQueuedSha256||null)===(args.preserveQueuedSha256||null),'supervised_queued_receipt_mismatch');
   require((approval.applicationPolicySha256||null)===(args.applicationPolicy?.sha256||null)&&(prior.applicationPolicySha256||null)===(args.applicationPolicy?.sha256||null),'supervised_policy_receipt_mismatch');
   require((approval.universalPolicyDigest||null)===(universal?.policyDigest||null)&&(prior.universalPolicyDigest||null)===(universal?.policyDigest||null)
    &&(approval.universalWitnessId||null)===(universal?.witnessId||null)&&(prior.universalWitnessId||null)===(universal?.witnessId||null),'supervised_universal_receipt_mismatch');
   await run.guard(args);
   require(prior.phase==='prepared'&&prior.originalId===args.originalId&&prior.candidateImage===args.candidateImage&&prior.transaction===args.transaction
    &&prior.configurationSha256===run.state.configurationSha256&&prior.modelReadinessSha256===run.healthSha256,'prepared_receipt_mismatch');
   run.candidate=await engine.inspect(prior.candidateId);run.validateCandidate(run.candidate,{stopped:true});
   const origin=new URL(opts['health-origin']||'http://127.0.0.1:18182');
   require(origin.protocol==='http:'&&origin.hostname==='127.0.0.1'&&run.original.HostConfig.PortBindings?.['8080/tcp']?.some(b=>b.HostIp==='127.0.0.1'&&b.HostPort===origin.port),'health_binding_mismatch');
   run.state=prior;await run.promote();
  }
  return {ok:true,...run.state};
 }catch(error){return {ok:false,code:error instanceof SafeError?error.code:'rollout_failed'};}
 finally{universal?.dispose();if(lock){await lock.close();await unlink(lockPath);}}
}
if(process.argv[1]&&import.meta.url===pathToFileURL(path.resolve(process.argv[1])).href){
 const result=await runConnectorCli(process.argv.slice(2));process.stdout.write(JSON.stringify(result)+'\n');if(result.ok!==true)process.exitCode=1;
}
