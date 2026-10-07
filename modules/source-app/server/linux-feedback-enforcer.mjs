import {spawn} from 'node:child_process';
import {mkdir,readFile,writeFile,lstat,realpath,rm} from 'node:fs/promises';
import {resolve,join} from 'node:path';
import {createHash,randomBytes} from 'node:crypto';
import {fields,check,digest} from './wire.mjs';
import {feedbackJobInput,feedbackJobBudget,createFeedbackProcessorEngine} from './feedback-job-contract.mjs';
import {createFeedbackJobEnforcer} from './feedback-job-enforcer.mjs';
import {LINUX_FEEDBACK_WORKER_SOURCE} from './linux-feedback-worker-source.mjs';

export const SYNTHETIC_FEEDBACK_IMAGE='sha256:c03a61d12e03870747e9013860fc36e23e53920da9b07ea1e795b3fef9628ae6';
const LAB='/home/ai2/codex-soty-universal-20261007-9f8dcd71';
const modes=['success','cpu','wall','cancel','scratch','output'];
const sha=bytes=>createHash('sha256').update(bytes).digest('hex');
const maxBudget=Object.freeze({wallMs:60000,cpuMs:10000,cleanupMs:2000,scratchBytes:2097152,outputBytes:32768,mediaBytes:1048576,attachments:3,parallel:1});
const format='{"id":{{json .Id}},"image":{{json .Image}},"user":{{json .Config.User}},"labels":{{json .Config.Labels}},"entrypoint":{{json .Config.Entrypoint}},"cmd":{{json .Config.Cmd}},"readonly":{{json .HostConfig.ReadonlyRootfs}},"network":{{json .HostConfig.NetworkMode}},"memory":{{json .HostConfig.Memory}},"swap":{{json .HostConfig.MemorySwap}},"pids":{{json .HostConfig.PidsLimit}},"caps":{{json .HostConfig.CapDrop}},"security":{{json .HostConfig.SecurityOpt}},"tmpfs":{{json .HostConfig.Tmpfs}},"mounts":{{json .Mounts}},"state":{{json .State.Status}},"exitCode":{{json .State.ExitCode}},"oom":{{json .State.OOMKilled}}}';

/** Narrow LAB host port. Docker custody belongs to the trusted supervisor,
 * NEVER to the processor/author JSON. No real model/user media is enabled.
 * Host/current Native proof remains the Source job service's responsibility. */
export function createSyntheticLinuxFeedbackProcessor(options){
  const value=fields(options,['directory'],['dockerBinary','scenario','onEvidence']);
  const directory=value.directory,docker=value.dockerBinary??'/usr/bin/docker',scenario=value.scenario??'success';
  check(directory===LAB+'/source-feedback-jobs'&&docker==='/usr/bin/docker'&&modes.includes(scenario)
    &&(value.onEvidence===undefined||typeof value.onEvidence==='function'),'source_feedback_processor_not_ready',503);
  const workerSha256=sha(LINUX_FEEDBACK_WORKER_SOURCE),ref=Object.freeze({id:'local.synthetic-linux.'+scenario,version:1,
    digest:digest({schema:'soty.synthetic-linux-feedback.v1',image:SYNTHETIC_FEEDBACK_IMAGE,workerSha256,scenario,purpose:'ocr',maxBudget})});
  const engine=createFeedbackProcessorEngine({ref,purposes:['ocr'],localOnly:true,synthetic:true,process:async()=>{throw new Error('fixed_enforcer_required');}});
  let prepared=false;
  async function cli(args,{limit=16384,timeout=15000,signal}={}){
    const child=spawn(docker,args,{stdio:['ignore','pipe','pipe'],env:{PATH:'/usr/bin:/bin'},windowsHide:true});
    let stdout='',bytes=0,failed=false;
    let force;
    const stop=()=>{failed=true;child.kill('SIGTERM');force??=setTimeout(()=>child.kill('SIGKILL'),500);},timer=setTimeout(stop,timeout);signal?.addEventListener('abort',stop,{once:true});
    child.stdout.on('data',part=>{bytes+=part.length;if(bytes>limit)stop();else stdout+=part;});child.stderr.resume();
    let code;try{code=await new Promise((resolve,reject)=>{child.once('error',()=>reject(new Error('fixed_docker_unavailable')));child.once('exit',resolve);});}
    finally{clearTimeout(timer);clearTimeout(force);signal?.removeEventListener('abort',stop);}
    check(!failed&&code===0,'source_feedback_processor_unknown',503);return stdout.trim();
  }
  function supported(budget){budget=feedbackJobBudget(budget);return budget.cpuMs>=1000&&budget.cpuMs%1000===0&&budget.cleanupMs>=500;}
  async function prepare(){
    check(process.platform==='linux'&&process.getuid()===1000,'source_feedback_processor_not_ready',503);
    const parent=await lstat(LAB);check(parent.isDirectory()&&!parent.isSymbolicLink()&&parent.uid===1000&&(parent.mode&0o777)===0o700,'source_feedback_processor_not_ready',503);
    check(resolve(directory)===directory&&await realpath(LAB)===LAB,'source_feedback_processor_not_ready',503);
    await mkdir(directory,{mode:0o700}).catch(error=>{if(error.code!=='EEXIST')throw error;});
    const current=await lstat(directory);check(current.isDirectory()&&!current.isSymbolicLink()&&current.uid===1000&&(current.mode&0o777)===0o700,'source_feedback_processor_not_ready',503);
    check(JSON.parse(await cli(['image','inspect',SYNTHETIC_FEEDBACK_IMAGE,'--format','{{json .Id}}']))===SYNTHETIC_FEEDBACK_IMAGE,'source_feedback_processor_not_ready',503);
    prepared=true;
  }
  async function execute({input,purpose,budget,signal,beforeStart}){
    check(prepared&&process.platform==='linux'&&supported(budget)&&purpose==='ocr'&&!signal.aborted,'source_feedback_processor_not_ready',503);
    input=feedbackJobInput(input);budget=feedbackJobBudget(budget);
    const nonce=randomBytes(16).toString('hex'),packetDirectory=join(directory,'packet-'+nonce),packet={schema:'soty.synthetic-feedback-worker.v1',input,purpose,budget,scenario,engineRef:ref,inputDigest:digest(input)};
    await mkdir(packetDirectory,{mode:0o700});
    await writeFile(join(packetDirectory,'worker.mjs'),LINUX_FEEDBACK_WORKER_SOURCE,{mode:0o600});
    await writeFile(join(packetDirectory,'input.json'),JSON.stringify(packet),{mode:0o600});
    const label='io.soty.feedback.packet',packetSha256=sha(JSON.stringify(packet)),tmpfs={
      '/scratch':'rw,nosuid,nodev,noexec,size='+budget.scratchBytes+',uid=1000,gid=1000,mode=700',
      '/data':'rw,nosuid,nodev,noexec,size=1048576,uid=1000,gid=1000,mode=700',
      '/tmp':'rw,nosuid,nodev,noexec,size=1048576,uid=1000,gid=1000,mode=700'};
    const command=['--signal=TERM','--kill-after='+(budget.cleanupMs/1000)+'s',(budget.wallMs/1000)+'s',
      '/usr/bin/prlimit','--cpu='+(budget.cpuMs/1000)+':'+(budget.cpuMs/1000),'--fsize='+budget.scratchBytes+':'+budget.scratchBytes,'--nofile=32:32','--',
      '/usr/local/bin/node','--permission','--allow-fs-read=/probe','--allow-fs-read=/proc/self/limits','--allow-fs-write=/scratch','--allow-fs-write=/data','/probe/worker.mjs'];
    let id,attached=false,stopping=null;
    const stop=()=>{if(!id||stopping)return;stopping=(async()=>{await cli(['kill','--signal','SIGTERM',id]).catch(()=>{});
      await new Promise(resolve=>setTimeout(resolve,Math.min(500,budget.cleanupMs)));await cli(['kill','--signal','SIGKILL',id]).catch(()=>{});})();};
    try{
      check(!signal.aborted,'source_feedback_processor_unknown',503);
      id=await cli(['create','--pull=never','--name','codex-soty-feedback-job-'+nonce,'--label',label+'='+packetSha256,
        '--read-only','--network','none','--cap-drop','ALL','--security-opt','no-new-privileges','--user','1000:1000',
        '--memory','128m','--memory-swap','128m','--pids-limit','32','--cpus','1','--ulimit','core=0:0','--log-driver','none',
        '--mount','type=bind,src='+packetDirectory+',dst=/probe,readonly',...Object.entries(tmpfs).flatMap(([path,options])=>['--tmpfs',path+':'+options]),
        '--workdir','/probe','--entrypoint','/usr/bin/timeout',SYNTHETIC_FEEDBACK_IMAGE,...command]);
      check(/^[a-f0-9]{64}$/u.test(id),'source_feedback_processor_unknown',503);
      const actual=JSON.parse(await cli(['inspect','--format',format,id]));
      check(actual.id===id&&actual.image===SYNTHETIC_FEEDBACK_IMAGE&&actual.labels?.[label]===packetSha256&&actual.state==='created'
        &&actual.user==='1000:1000'&&actual.readonly===true&&actual.network==='none'&&actual.memory===134217728&&actual.swap===134217728
        &&actual.pids===32&&JSON.stringify(actual.caps)===JSON.stringify(['ALL'])&&actual.security?.includes('no-new-privileges')
        &&JSON.stringify(actual.entrypoint)===JSON.stringify(['/usr/bin/timeout'])&&JSON.stringify(actual.cmd)===JSON.stringify(command)
        &&digest(actual.tmpfs)===digest(tmpfs)&&actual.mounts.length===1&&actual.mounts[0].Type==='bind'&&actual.mounts[0].Source===packetDirectory
        &&actual.mounts[0].Destination==='/probe'&&actual.mounts[0].RW===false,'source_feedback_processor_not_ready',503);
      check(typeof beforeStart==='function','source_feedback_processor_not_ready',503);
      await beforeStart();
      signal.addEventListener('abort',stop,{once:true});check(!signal.aborted,'source_feedback_processor_unknown',503);attached=true;
      const outputPromise=cli(['start','--attach',id],{limit:budget.outputBytes,timeout:budget.wallMs+budget.cleanupMs+5000,signal});
      // Attach may reject while startup observation is in progress. Keep its
      // real failure for await below without an unhandled rejection.
      outputPromise.catch(()=>{});
      const startDeadline=Date.now()+2000;
      while(Date.now()<startDeadline){const live=JSON.parse(await cli(['inspect','--format',format,id]));
        if(live.state==='running'){value.onEvidence?.({scenario,phase:'started',engineRef:ref,packetSha256,workerSha256,synthetic:true});break;}
        if(live.state==='exited')break;await new Promise(resolve=>setTimeout(resolve,20));}
      const stdout=await outputPromise;
      check(!signal.aborted,'source_feedback_processor_unknown',503);
      const after=JSON.parse(await cli(['inspect','--format',format,id]));
      check(after.state==='exited'&&after.exitCode===0&&after.oom===false,'source_feedback_processor_unknown',503);
      const result=fields(JSON.parse(stdout),['schema','inputDigest','engineRef','output','metrics','externalNetworkDenied']);
      check(result.schema==='soty.synthetic-feedback-process-receipt.v1'&&result.inputDigest===packet.inputDigest&&digest(result.engineRef)===digest(ref)
        &&result.externalNetworkDenied===true,'source_feedback_processor_unknown',503);
      value.onEvidence?.({scenario,engineRef:ref,packetSha256,workerSha256,stopped:true,exitCode:after.exitCode,oom:after.oom,metrics:result.metrics,synthetic:true});
      return result.output;
    }finally{
      if(attached)signal.removeEventListener('abort',stop);
      if(id&&/^[a-f0-9]{64}$/u.test(id)){
        stop();await stopping;
        const finalState=JSON.parse(await cli(['inspect','--format',format,id]).catch(()=>'{"state":"unknown"}'));
        value.onEvidence?.({scenario,engineRef:ref,packetSha256,workerSha256,stopped:finalState.state==='exited',
          exitCode:finalState.exitCode??null,oom:finalState.oom??null,synthetic:true});
        await cli(['rm','--force',id]).catch(()=>{});
      }
      check(await realpath(packetDirectory)===packetDirectory&&packetDirectory.startsWith(directory+'/packet-'),'source_feedback_processor_unknown',503);
      await rm(packetDirectory,{recursive:true,force:true,maxRetries:5,retryDelay:30});
    }
  }
  const enforcer=createFeedbackJobEnforcer({engine,platform:'linux',maxBudget,syntheticTestOnly:false,
    assertHostBounds:({engineRef,budget})=>prepared&&process.platform==='linux'&&digest(engineRef)===digest(ref)&&supported(budget),execute});
  return Object.freeze({engine,enforcer,prepare,ref,workerSha256,image:SYNTHETIC_FEEDBACK_IMAGE,productionReady:false});
}
