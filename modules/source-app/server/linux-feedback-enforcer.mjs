import * as fs from 'node:fs/promises';
import {resolve,join} from 'node:path';
import {createHash,randomBytes} from 'node:crypto';
import {fields,check,digest} from './wire.mjs';
import {feedbackJobInput,feedbackJobBudget,createFeedbackProcessorEngine} from './feedback-job-contract.mjs';
import {createFeedbackJobEnforcer} from './feedback-job-enforcer.mjs';
import {LINUX_FEEDBACK_WORKER_SOURCE} from './linux-feedback-worker-source.mjs';
import {createDockerCommandRunner,createLocalWslDockerCommandRunner,runFixedLinuxPacket} from './linux-feedback-lifecycle.mjs';
import {LOCAL_LINUX_FEEDBACK_PLACEMENT} from './linux-feedback-local-placement.mjs';

export const SYNTHETIC_FEEDBACK_IMAGE='sha256:c03a61d12e03870747e9013860fc36e23e53920da9b07ea1e795b3fef9628ae6';
const LAB='/home/ai2/codex-soty-universal-20261007-9f8dcd71';
const modes=['success','cpu','wall','cancel','scratch','output'];
const sha=bytes=>createHash('sha256').update(bytes).digest('hex');
const maxBudget=Object.freeze({wallMs:60000,cpuMs:10000,cleanupMs:2000,scratchBytes:2097152,outputBytes:32768,mediaBytes:1048576,attachments:3,parallel:1});
const sleep=ms=>new Promise(done=>setTimeout(done,ms));

/** Narrow LAB host port. Docker custody belongs to the trusted supervisor,
 * NEVER to the processor/author JSON. No real model/user media is enabled.
 * Host/current Native proof remains the Source job service's responsibility. */
export function createSyntheticLinuxFeedbackProcessor(options){
  return processor(options,null);
}
/** NEW immutable local-Linux placement. Resources and process protocol are
 * unchanged; its engine3 digest also pins the fixed daemon/path/cleanup policy. */
export function createSyntheticLocalLinuxFeedbackProcessor(options){
  return processor(options,LOCAL_LINUX_FEEDBACK_PLACEMENT);
}
function processor(options,placement){
  const value=fields(options,['directory'],['dockerBinary','scenario','onEvidence']);
  const directory=value.directory,docker=value.dockerBinary??'/usr/bin/docker',scenario=value.scenario??'success';
  const lab=placement?.lab??LAB;
  check(directory===(placement?.directory??LAB+'/source-feedback-jobs')&&docker==='/usr/bin/docker'&&modes.includes(scenario)
    &&(value.onEvidence===undefined||typeof value.onEvidence==='function'),'source_feedback_processor_not_ready',503);
  const workerSha256=sha(LINUX_FEEDBACK_WORKER_SOURCE),ref=Object.freeze({id:'local.synthetic-linux.'+scenario,version:placement?3:2,
    digest:digest(placement?{schema:'soty.synthetic-linux-feedback.v3',placement,
      image:SYNTHETIC_FEEDBACK_IMAGE,workerSha256,scenario,purpose:'ocr',maxBudget}
      :{schema:'soty.synthetic-linux-feedback.v2',lifecycleProfile:'soty.fixed-linux-feedback-lifecycle.v2',
      image:SYNTHETIC_FEEDBACK_IMAGE,workerSha256,scenario,purpose:'ocr',maxBudget})});
  const engine=createFeedbackProcessorEngine({ref,purposes:['ocr'],localOnly:true,synthetic:true,process:async()=>{throw new Error('fixed_enforcer_required');}});
  const commands=placement?createLocalWslDockerCommandRunner(docker):createDockerCommandRunner(docker);let prepared=false;
  function supported(budget){budget=feedbackJobBudget(budget);return budget.cpuMs>=1000&&budget.cpuMs%1000===0&&budget.cleanupMs>=500;}
  async function prepare(){
    check(process.platform==='linux'&&process.getuid()===1000,'source_feedback_processor_not_ready',503);
    const parent=await fs.lstat(lab);check(parent.isDirectory()&&!parent.isSymbolicLink()&&parent.uid===1000&&(parent.mode&0o777)===0o700,'source_feedback_processor_not_ready',503);
    check(resolve(directory)===directory&&await fs.realpath(lab)===lab,'source_feedback_processor_not_ready',503);
    if(placement){const socket=await fs.lstat(placement.socketContainer);
      check(socket.isSocket()&&!socket.isSymbolicLink()&&socket.gid===placement.socketGid&&(socket.mode&0o777)===0o660
        &&await fs.realpath(placement.socketContainer)===placement.socketContainer,'source_feedback_processor_not_ready',503);}
    await fs.mkdir(directory,{mode:0o700}).catch(error=>{if(error.code!=='EEXIST')throw error;});
    const current=await fs.lstat(directory);check(current.isDirectory()&&!current.isSymbolicLink()&&current.uid===1000&&(current.mode&0o777)===0o700,'source_feedback_processor_not_ready',503);
    check(JSON.parse(await commands.run(['image','inspect',SYNTHETIC_FEEDBACK_IMAGE,'--format','{{json .Id}}']))===SYNTHETIC_FEEDBACK_IMAGE,'source_feedback_processor_not_ready',503);
    prepared=true;
  }
  async function execute({input,purpose,budget,signal,beforeStart}){
    check(prepared&&process.platform==='linux'&&supported(budget)&&purpose==='ocr'&&!signal.aborted,'source_feedback_processor_not_ready',503);
    input=feedbackJobInput(input);budget=feedbackJobBudget(budget);
    const nonce=randomBytes(16).toString('hex'),packetDirectory=join(directory,'packet-'+nonce),packet={schema:'soty.synthetic-feedback-worker.v1',input,purpose,budget,scenario,engineRef:ref,inputDigest:digest(input)},packetText=JSON.stringify(packet);
    const tmpfs={
      '/scratch':'rw,nosuid,nodev,noexec,size='+budget.scratchBytes+',uid=1000,gid=1000,mode=700',
      '/data':'rw,nosuid,nodev,noexec,size=1048576,uid=1000,gid=1000,mode=700',
      '/tmp':'rw,nosuid,nodev,noexec,size=1048576,uid=1000,gid=1000,mode=700'};
    const command=['--signal=TERM','--kill-after='+(budget.cleanupMs/1000)+'s',(budget.wallMs/1000)+'s',
      '/usr/bin/prlimit','--cpu='+(budget.cpuMs/1000)+':'+(budget.cpuMs/1000),'--fsize='+budget.scratchBytes+':'+budget.scratchBytes,'--nofile=32:32','--',
      '/usr/local/bin/node','--permission','--allow-fs-read=/probe','--allow-fs-read=/proc/self/limits','--allow-fs-write=/scratch','--allow-fs-write=/data','/probe/worker.mjs'];
    const plan={directory,packetDirectory,packetText,workerSource:LINUX_FEEDBACK_WORKER_SOURCE,image:SYNTHETIC_FEEDBACK_IMAGE,
      name:'codex-soty-feedback-job-'+nonce,label:'io.soty.feedback.packet',packetSha256:sha(packetText),tmpfs,command,budget,scenario,engineRef:ref,workerSha256};
    const result=fields(await runFixedLinuxPacket(plan,{fs,commands,sleep},{signal,beforeStart,onEvidence:value.onEvidence}),
      ['schema','inputDigest','engineRef','output','metrics','externalNetworkDenied']);
    check(result.schema==='soty.synthetic-feedback-process-receipt.v1'&&result.inputDigest===packet.inputDigest&&digest(result.engineRef)===digest(ref)
      &&result.externalNetworkDenied===true,'source_feedback_processor_unknown',503);
    const metrics=fields(result.metrics,['userCpuMicros','systemCpuMicros','maxRssKiB','wallMs']);
    check(Object.values(metrics).every(number=>Number.isSafeInteger(number)&&number>=0)
      &&metrics.userCpuMicros+metrics.systemCpuMicros<=budget.cpuMs*1000&&metrics.maxRssKiB<=131072&&metrics.wallMs<=budget.wallMs,
      'source_feedback_processor_unknown',503);
    value.onEvidence?.({scenario,phase:'process_receipt',engineRef:ref,packetSha256:plan.packetSha256,workerSha256,metrics,synthetic:true});
    return result.output;
  }
  const enforcer=createFeedbackJobEnforcer({engine,platform:'linux',maxBudget,syntheticTestOnly:false,
    assertHostBounds:({engineRef,budget})=>prepared&&process.platform==='linux'&&digest(engineRef)===digest(ref)&&supported(budget),execute});
  return Object.freeze({engine,enforcer,prepare,ref,workerSha256,image:SYNTHETIC_FEEDBACK_IMAGE,productionReady:false,
    ...(placement?{placement}: {})});
}
