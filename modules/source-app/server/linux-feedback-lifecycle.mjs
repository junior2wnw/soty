import {spawn} from 'node:child_process';
import {check,SourceAppError,digest} from './wire.mjs';
import {LOCAL_LINUX_FEEDBACK_PLACEMENT} from './linux-feedback-local-placement.mjs';
import {linuxFeedbackFailure} from './linux-feedback-diagnostic.mjs';

// Internal host implementation only. This module is not a package/RPC export;
// author JSON cannot supply a command runner, filesystem port or packet plan.
export const LINUX_FEEDBACK_INSPECT_FORMAT='{"id":{{json .Id}},"name":{{json .Name}},"image":{{json .Image}},"user":{{json .Config.User}},"labels":{{json .Config.Labels}},"workdir":{{json .Config.WorkingDir}},"entrypoint":{{json .Config.Entrypoint}},"cmd":{{json .Config.Cmd}},"readonly":{{json .HostConfig.ReadonlyRootfs}},"network":{{json .HostConfig.NetworkMode}},"memory":{{json .HostConfig.Memory}},"swap":{{json .HostConfig.MemorySwap}},"pids":{{json .HostConfig.PidsLimit}},"nanoCpus":{{json .HostConfig.NanoCpus}},"cpuPeriod":{{json .HostConfig.CpuPeriod}},"cpuQuota":{{json .HostConfig.CpuQuota}},"cpuShares":{{json .HostConfig.CpuShares}},"cpuRealtimePeriod":{{json .HostConfig.CpuRealtimePeriod}},"cpuRealtimeRuntime":{{json .HostConfig.CpuRealtimeRuntime}},"cpuCount":{{json .HostConfig.CpuCount}},"cpuPercent":{{json .HostConfig.CpuPercent}},"cpusetCpus":{{json .HostConfig.CpusetCpus}},"cpusetMems":{{json .HostConfig.CpusetMems}},"ulimits":{{json .HostConfig.Ulimits}},"log":{{json .HostConfig.LogConfig}},"privileged":{{json .HostConfig.Privileged}},"caps":{{json .HostConfig.CapDrop}},"capAdd":{{json .HostConfig.CapAdd}},"security":{{json .HostConfig.SecurityOpt}},"devices":{{json .HostConfig.Devices}},"deviceRequests":{{json .HostConfig.DeviceRequests}},"deviceRules":{{json .HostConfig.DeviceCgroupRules}},"binds":{{json .HostConfig.Binds}},"volumesFrom":{{json .HostConfig.VolumesFrom}},"ports":{{json .HostConfig.PortBindings}},"publishPorts":{{json .HostConfig.PublishAllPorts}},"pidMode":{{json .HostConfig.PidMode}},"ipcMode":{{json .HostConfig.IpcMode}},"utsMode":{{json .HostConfig.UTSMode}},"restart":{{json .HostConfig.RestartPolicy}},"tmpfs":{{json .HostConfig.Tmpfs}},"mounts":{{json .Mounts}},"state":{{json .State.Status}},"exitCode":{{json .State.ExitCode}},"oom":{{json .State.OOMKilled}}}';

/** Waits for CLOSE, including pipes, even after timeout/output overflow. All
 * spawned commands belong to this supervisor; stderr is never disclosed. */
export function createDockerCommandRunner(binary){return commandRunner(binary,args=>args);}

// A separate constructor-approved local placement. Never inherit DOCKER_HOST
// or accept a caller-selected daemon/context. This fixed socket is mounted only
// into the trusted supervisor, not the processor container.
function localWslDockerArgs(args,socket){
  check(Array.isArray(args)&&args.length>0&&args.every(arg=>typeof arg==='string'
    &&!['--host','-H','--context','--config','-c'].includes(arg)&&!/^--(?:host|context|config)=/u.test(arg)&&!/^-(?:H|c)./u.test(arg)),
  'source_feedback_processor_not_ready',503);
  return ['--host','unix://'+socket,...args];
}
export const fixedLocalWslDockerArgs=args=>localWslDockerArgs(args,LOCAL_LINUX_FEEDBACK_PLACEMENT.socketContainer);
export const fixedLocalWslHostDockerArgs=args=>localWslDockerArgs(args,LOCAL_LINUX_FEEDBACK_PLACEMENT.socketHost);
export function createLocalWslDockerCommandRunner(binary){
  check(binary==='/usr/bin/docker','source_feedback_processor_not_ready',503);
  return commandRunner(binary,fixedLocalWslDockerArgs);
}
export function createLocalWslHostDockerCommandRunner(binary){
  check(binary===LOCAL_LINUX_FEEDBACK_PLACEMENT.dockerHostBinary,'source_feedback_processor_not_ready',503);
  return commandRunner(binary,fixedLocalWslHostDockerArgs);
}

function commandRunner(binary,captureArgs){
  function start(args,{limit=16384,timeout=15000,signal,collectExitOneReceipt=false}={}){
    const child=spawn(binary,captureArgs(args),{stdio:['ignore','pipe','pipe'],env:{PATH:'/usr/bin:/bin'},windowsHide:true});
    const chunks=[];let bytes=0,failed=false,closed=false,force,timer,exitClass='none';
    function stop(reason='interrupted'){
      if(closed)return;failed=true;if(exitClass==='none')exitClass=reason;child.kill('SIGTERM');
      force??=setTimeout(()=>{if(!closed)child.kill('SIGKILL');},500);
    }
    const aborted=()=>stop('aborted');
    const result=new Promise((resolve,reject)=>{
      child.once('error',()=>{failed=true;exitClass='spawn_failed';});
      child.stdout.on('data',part=>{bytes+=part.length;if(bytes>limit)stop('output_limit');else chunks.push(part);});
      child.stderr.resume();
      child.once('close',code=>{
        closed=true;clearTimeout(timer);clearTimeout(force);signal?.removeEventListener('abort',aborted);
        // Only the fixed public supervisor fixture asks for bounded JSON on
        // exit1. Worker/Native execution uses the unchanged exit0-only default.
        if(failed||code!==0&&!(collectExitOneReceipt===true&&code===1)){const error=new SourceAppError('source_feedback_processor_unknown',503);
          error.linuxCliDiagnostic=Object.freeze({exitClass:exitClass==='none'?'nonzero':exitClass});reject(error);}
        else resolve(Buffer.concat(chunks).toString('utf8').trim());
      });
      timer=setTimeout(()=>stop('timeout'),timeout);signal?.addEventListener('abort',aborted,{once:true});if(signal?.aborted)aborted();
    });
    // Observation may fail before the consumer reaches await result.
    result.catch(()=>{});
    return Object.freeze({result,stopAndWait:async()=>{stop();await result.catch(()=>{});}});
  }
  return Object.freeze({start,run:(args,options)=>start(args,options).result});
}

const emptyList=value=>value===null||Array.isArray(value)&&value.length===0;
const emptyObject=value=>value===null||value&&typeof value==='object'&&!Array.isArray(value)&&Object.keys(value).length===0;
const equal=(a,b)=>a!==undefined&&b!==undefined&&digest(a)===digest(b);
export function assertFixedLinuxSpec(actual,plan){
  const a=actual??{},groups={
    identity:/^[a-f0-9]{64}$/u.test(a.id)&&a.name==='/'+plan.name&&a.image===plan.image&&a.labels?.[plan.label]===plan.packetSha256,
    state:['created','running','exited'].includes(a.state),
    limits:a.memory===134217728&&a.swap===134217728&&a.pids===32,
    cpu:a.nanoCpus===1000000000&&a.cpuPeriod===0&&a.cpuQuota===0&&a.cpuShares===0&&a.cpuRealtimePeriod===0&&a.cpuRealtimeRuntime===0&&a.cpuCount===0&&a.cpuPercent===0&&a.cpusetCpus===''&&a.cpusetMems==='',
    hardening:a.user==='1000:1000'&&a.workdir==='/probe'&&a.readonly===true&&a.network==='none'
      &&equal(a.ulimits,[{Name:'core',Soft:0,Hard:0}])&&a.log?.Type==='none'&&emptyObject(a.log.Config)
      &&a.privileged===false&&equal(a.caps,['ALL'])&&emptyList(a.capAdd)&&equal(a.security,['no-new-privileges'])
      &&emptyList(a.devices)&&emptyList(a.deviceRequests)&&emptyList(a.deviceRules),
    namespaces:emptyList(a.binds)&&emptyList(a.volumesFrom)&&emptyObject(a.ports)&&a.publishPorts===false
      &&a.pidMode===''&&a.ipcMode==='private'&&a.utsMode===''&&a.restart?.Name==='no'&&a.restart.MaximumRetryCount===0,
    command:equal(a.entrypoint,['/usr/bin/timeout'])&&equal(a.cmd,plan.command)&&equal(a.tmpfs,plan.tmpfs),
    mounts:Array.isArray(a.mounts)&&a.mounts.length===1&&a.mounts[0].Type==='bind'
      &&a.mounts[0].Source===plan.packetDirectory&&a.mounts[0].Destination==='/probe'&&a.mounts[0].RW===false&&a.mounts[0].Propagation==='rprivate',
  };
  const mismatch=Object.entries(groups).filter(([,valid])=>!valid).map(([name])=>name);
  if(mismatch.length){const error=new SourceAppError('source_feedback_processor_not_ready',503);error.specMismatchGroups=Object.freeze(mismatch);throw error;}
  return actual;
}

function createArgs(plan){return ['create','--pull=never','--name',plan.name,'--label',plan.label+'='+plan.packetSha256,
  '--read-only','--network','none','--cap-drop','ALL','--security-opt','no-new-privileges','--user','1000:1000',
  '--memory','128m','--memory-swap','128m','--pids-limit','32','--cpus','1','--ulimit','core=0:0','--log-driver','none',
  '--ipc','private','--restart','no','--mount','type=bind,src='+plan.packetDirectory+',dst=/probe,readonly,bind-propagation=rprivate',
  ...Object.entries(plan.tmpfs).flatMap(([path,options])=>['--tmpfs',path+':'+options]),
  '--workdir','/probe','--entrypoint','/usr/bin/timeout',plan.image,...plan.command];}

/** Lifecycle kernel shared by the real fixed host and controlled failure
 * tests. A failed/unknown cleanup never returns output to Native final SQL. */
export async function runFixedLinuxPacket(plan,{fs,commands,sleep}, {signal,beforeStart,onEvidence}){
  let packetOwned=false,createAttempted=false,ownedId,attach,primary,output,stopping,stage='packet';
  let cleanupUnknown=false,observerFailed=false,stopped=false,exitCode=null,oom=null,containerRemoved=false,packetRemoved=false;
  const evidence=extra=>({scenario:plan.scenario,engineRef:plan.engineRef,packetSha256:plan.packetSha256,
    workerSha256:plan.workerSha256,synthetic:true,...extra});
  function emit(extra){try{onEvidence?.(evidence(extra));}catch{observerFailed=true;throw new SourceAppError('source_feedback_processor_unknown',503);}}
  async function inspect(locator){return assertFixedLinuxSpec(JSON.parse(await commands.run(['inspect','--format',LINUX_FEEDBACK_INSPECT_FORMAT,locator])),plan);}
  async function stopOwned(){
    if(!ownedId)return;if(stopping)return stopping;
    stopping=(async()=>{
      try{
        const stopDeadline=Date.now()+plan.budget.cleanupMs;
        let state=await inspect(ownedId);check(state.id===ownedId,'source_feedback_processor_unknown',503);
        if(state.state==='running'){
          await commands.run(['kill','--signal','SIGTERM',ownedId]);await sleep(Math.min(500,plan.budget.cleanupMs));
          state=await inspect(ownedId);if(state.state==='running')await commands.run(['kill','--signal','SIGKILL',ownedId]);
          while(state.state==='running'&&Date.now()<stopDeadline){await sleep(20);state=await inspect(ownedId);}
          if(state.state==='running')cleanupUnknown=true;
        }
      }catch{cleanupUnknown=true;}
    })();return stopping;
  }
  const onAbort=()=>{void stopOwned();};
  try{
    // mkdir AND both writes are owned by the same finally block.
    await fs.mkdir(plan.packetDirectory,{mode:0o700});packetOwned=true;
    await fs.writeFile(plan.packetDirectory+'/worker.mjs',plan.workerSource,{mode:0o600});
    await fs.writeFile(plan.packetDirectory+'/input.json',plan.packetText,{mode:0o600});
    check(!signal.aborted,'source_feedback_processor_unknown',503);
    stage='create';createAttempted=true;
    const returnedId=await commands.run(createArgs(plan));
    // The daemon name is a locator, never authority. Verify every selected
    // immutable spec before either START or adopting an ID for cleanup.
    stage='created_spec';const created=await inspect(plan.name);ownedId=created.id;
    check(returnedId===ownedId&&created.state==='created','source_feedback_processor_unknown',503);
    stage='authority';check(typeof beforeStart==='function','source_feedback_processor_not_ready',503);await beforeStart();
    signal.addEventListener('abort',onAbort,{once:true});check(!signal.aborted,'source_feedback_processor_unknown',503);
    stage='start';attach=commands.start(['start','--attach',ownedId],{limit:plan.budget.outputBytes,timeout:plan.budget.wallMs+plan.budget.cleanupMs+5000,signal});
    attach.result.catch(()=>{});
    const deadline=Date.now()+2000;
    stage='running_spec';while(Date.now()<deadline){
      const live=await inspect(ownedId);check(live.id===ownedId,'source_feedback_processor_unknown',503);
      if(live.state==='running'){emit({phase:'started'});break;}
      if(live.state==='exited')break;await sleep(20);
    }
    stage='attach';const stdout=await attach.result;check(!signal.aborted,'source_feedback_processor_unknown',503);
    stage='stopped_spec';const after=await inspect(ownedId);check(after.id===ownedId&&after.state==='exited'&&after.exitCode===0&&after.oom===false,'source_feedback_processor_unknown',503);
    stage='receipt';const result=JSON.parse(stdout);
    // Parsing/closed receipt validation belongs to the fixed engine caller.
    output=result;
  }catch(error){primary=error;try{emit({phase:'failure',stage,...linuxFeedbackFailure(error)});}catch{}}
  finally{
    signal.removeEventListener('abort',onAbort);
    if(createAttempted&&!ownedId){
      // CREATE is never retried. Unknown CLI delivery may still have created
      // the random exact name. Only a full proof may adopt it for cleanup.
      try{ownedId=(await inspect(plan.name)).id;}catch{cleanupUnknown=true;}
    }
    if(ownedId){
      await stopOwned();
      // Observer/inspect failure may precede awaiting attach. Always close
      // and join our CLI, including its pipes, before returning or throwing.
      try{await attach?.stopAndWait();}catch{cleanupUnknown=true;}
      try{
        const finalState=await inspect(ownedId);check(finalState.id===ownedId,'source_feedback_processor_unknown',503);
        stopped=['created','exited'].includes(finalState.state);exitCode=finalState.exitCode;oom=finalState.oom;
        if(!stopped)cleanupUnknown=true;
      }catch{cleanupUnknown=true;}
      // The ID was independently proved above. Best effort cleanup still
      // runs after observation failure, but cannot erase that uncertainty.
      try{
        await commands.run(['rm','--force',ownedId]);
        const remaining=await commands.run(['container','ls','--all','--no-trunc','--filter','id='+ownedId,'--format','{{.ID}}'],{limit:128});
        check(remaining==='','source_feedback_processor_unknown',503);containerRemoved=true;
      }catch{cleanupUnknown=true;}
    }else try{await attach?.stopAndWait();}catch{cleanupUnknown=true;}
    // Unknown container delivery/stop/removal retains this exact owned packet
    // for bounded operator cleanup. Do not erase data still reachable by an
    // uncertain processor, even though no Native result will be returned.
    if(packetOwned&&!cleanupUnknown){
      try{
        const state=await fs.lstat(plan.packetDirectory);
        check(state.isDirectory()&&!state.isSymbolicLink()&&state.uid===1000&&(state.mode&0o777)===0o700
          &&await fs.realpath(plan.packetDirectory)===plan.packetDirectory
          &&/^packet-[a-f0-9]{32}$/u.test(plan.packetDirectory.slice(plan.directory.length+1))
          &&plan.packetDirectory.startsWith(plan.directory+'/'),'source_feedback_processor_unknown',503);
        await fs.rm(plan.packetDirectory,{recursive:true,force:false,maxRetries:5,retryDelay:30});packetRemoved=true;
      }catch{cleanupUnknown=true;}
    }
    try{emit({phase:'cleanup',stopped,exitCode,oom,containerRemoved,packetRemoved,cleanupUnknown,observerFailed,
      ...(primary?{failureStage:stage,...linuxFeedbackFailure(primary)}:{})});}catch{}
  }
  if(cleanupUnknown)throw new SourceAppError('source_feedback_processor_cleanup_unknown',503);
  if(primary)throw primary;
  check(!observerFailed,'source_feedback_processor_unknown',503);
  return output;
}
