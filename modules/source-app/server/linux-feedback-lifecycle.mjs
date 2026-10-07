import {spawn} from 'node:child_process';
import {check,SourceAppError,digest} from './wire.mjs';

// Internal host implementation only. This module is not a package/RPC export;
// author JSON cannot supply a command runner, filesystem port or packet plan.
export const LINUX_FEEDBACK_INSPECT_FORMAT='{"id":{{json .Id}},"name":{{json .Name}},"image":{{json .Image}},"user":{{json .Config.User}},"labels":{{json .Config.Labels}},"workdir":{{json .Config.WorkingDir}},"entrypoint":{{json .Config.Entrypoint}},"cmd":{{json .Config.Cmd}},"readonly":{{json .HostConfig.ReadonlyRootfs}},"network":{{json .HostConfig.NetworkMode}},"memory":{{json .HostConfig.Memory}},"swap":{{json .HostConfig.MemorySwap}},"pids":{{json .HostConfig.PidsLimit}},"nanoCpus":{{json .HostConfig.NanoCpus}},"cpuPeriod":{{json .HostConfig.CpuPeriod}},"cpuQuota":{{json .HostConfig.CpuQuota}},"cpuShares":{{json .HostConfig.CpuShares}},"cpuRealtimePeriod":{{json .HostConfig.CpuRealtimePeriod}},"cpuRealtimeRuntime":{{json .HostConfig.CpuRealtimeRuntime}},"cpuCount":{{json .HostConfig.CpuCount}},"cpuPercent":{{json .HostConfig.CpuPercent}},"cpusetCpus":{{json .HostConfig.CpusetCpus}},"cpusetMems":{{json .HostConfig.CpusetMems}},"ulimits":{{json .HostConfig.Ulimits}},"log":{{json .HostConfig.LogConfig}},"privileged":{{json .HostConfig.Privileged}},"caps":{{json .HostConfig.CapDrop}},"capAdd":{{json .HostConfig.CapAdd}},"security":{{json .HostConfig.SecurityOpt}},"devices":{{json .HostConfig.Devices}},"deviceRequests":{{json .HostConfig.DeviceRequests}},"deviceRules":{{json .HostConfig.DeviceCgroupRules}},"binds":{{json .HostConfig.Binds}},"volumesFrom":{{json .HostConfig.VolumesFrom}},"ports":{{json .HostConfig.PortBindings}},"publishPorts":{{json .HostConfig.PublishAllPorts}},"pidMode":{{json .HostConfig.PidMode}},"ipcMode":{{json .HostConfig.IpcMode}},"utsMode":{{json .HostConfig.UTSMode}},"restart":{{json .HostConfig.RestartPolicy}},"tmpfs":{{json .HostConfig.Tmpfs}},"mounts":{{json .Mounts}},"state":{{json .State.Status}},"exitCode":{{json .State.ExitCode}},"oom":{{json .State.OOMKilled}}}';

/** Waits for CLOSE, including pipes, even after timeout/output overflow. All
 * spawned commands belong to this supervisor; stderr is never disclosed. */
export function createDockerCommandRunner(binary){
  function start(args,{limit=16384,timeout=15000,signal}={}){
    const child=spawn(binary,args,{stdio:['ignore','pipe','pipe'],env:{PATH:'/usr/bin:/bin'},windowsHide:true});
    const chunks=[];let bytes=0,failed=false,closed=false,force,timer;
    function stop(){
      if(closed)return;failed=true;child.kill('SIGTERM');
      force??=setTimeout(()=>{if(!closed)child.kill('SIGKILL');},500);
    }
    const result=new Promise((resolve,reject)=>{
      child.once('error',()=>{failed=true;});
      child.stdout.on('data',part=>{bytes+=part.length;if(bytes>limit)stop();else chunks.push(part);});
      child.stderr.resume();
      child.once('close',code=>{
        closed=true;clearTimeout(timer);clearTimeout(force);signal?.removeEventListener('abort',stop);
        if(failed||code!==0)reject(new SourceAppError('source_feedback_processor_unknown',503));
        else resolve(Buffer.concat(chunks).toString('utf8').trim());
      });
      timer=setTimeout(stop,timeout);signal?.addEventListener('abort',stop,{once:true});if(signal?.aborted)stop();
    });
    // Observation may fail before the consumer reaches await result.
    result.catch(()=>{});
    return Object.freeze({result,stopAndWait:async()=>{stop();await result.catch(()=>{});}});
  }
  return Object.freeze({start,run:(args,options)=>start(args,options).result});
}

const emptyList=value=>value===null||Array.isArray(value)&&value.length===0;
const emptyObject=value=>value===null||value&&typeof value==='object'&&!Array.isArray(value)&&Object.keys(value).length===0;
const equal=(a,b)=>digest(a)===digest(b);
export function assertFixedLinuxSpec(actual,plan){
  check(actual&&/^[a-f0-9]{64}$/u.test(actual.id)&&actual.name==='/'+plan.name&&actual.image===plan.image
    &&actual.labels?.[plan.label]===plan.packetSha256&&['created','running','exited'].includes(actual.state)
    &&actual.user==='1000:1000'&&actual.workdir==='/probe'&&actual.readonly===true&&actual.network==='none'
    &&actual.memory===134217728&&actual.swap===134217728&&actual.pids===32&&actual.nanoCpus===1000000000
    &&actual.cpuPeriod===0&&actual.cpuQuota===0&&actual.cpuShares===0&&actual.cpuRealtimePeriod===0&&actual.cpuRealtimeRuntime===0&&actual.cpuCount===0&&actual.cpuPercent===0&&actual.cpusetCpus===''&&actual.cpusetMems===''
    &&equal(actual.ulimits,[{Name:'core',Soft:0,Hard:0}])&&actual.log?.Type==='none'&&emptyObject(actual.log.Config)
    &&actual.privileged===false&&equal(actual.caps,['ALL'])&&emptyList(actual.capAdd)&&equal(actual.security,['no-new-privileges'])
    &&emptyList(actual.devices)&&emptyList(actual.deviceRequests)&&emptyList(actual.deviceRules)
    &&emptyList(actual.binds)&&emptyList(actual.volumesFrom)&&emptyObject(actual.ports)&&actual.publishPorts===false
    &&actual.pidMode===''&&actual.ipcMode==='private'&&actual.utsMode===''
    &&actual.restart?.Name==='no'&&actual.restart.MaximumRetryCount===0
    &&equal(actual.entrypoint,['/usr/bin/timeout'])&&equal(actual.cmd,plan.command)&&equal(actual.tmpfs,plan.tmpfs)
    &&Array.isArray(actual.mounts)&&actual.mounts.length===1&&actual.mounts[0].Type==='bind'
    &&actual.mounts[0].Source===plan.packetDirectory&&actual.mounts[0].Destination==='/probe'
    &&actual.mounts[0].RW===false&&actual.mounts[0].Propagation==='rprivate','source_feedback_processor_not_ready',503);
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
  let packetOwned=false,createAttempted=false,ownedId,attach,primary,output,stopping;
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
    createAttempted=true;
    const returnedId=await commands.run(createArgs(plan));
    // The daemon name is a locator, never authority. Verify every selected
    // immutable spec before either START or adopting an ID for cleanup.
    const created=await inspect(plan.name);ownedId=created.id;
    check(returnedId===ownedId&&created.state==='created','source_feedback_processor_unknown',503);
    check(typeof beforeStart==='function','source_feedback_processor_not_ready',503);await beforeStart();
    signal.addEventListener('abort',onAbort,{once:true});check(!signal.aborted,'source_feedback_processor_unknown',503);
    attach=commands.start(['start','--attach',ownedId],{limit:plan.budget.outputBytes,timeout:plan.budget.wallMs+plan.budget.cleanupMs+5000,signal});
    attach.result.catch(()=>{});
    const deadline=Date.now()+2000;
    while(Date.now()<deadline){
      const live=await inspect(ownedId);check(live.id===ownedId,'source_feedback_processor_unknown',503);
      if(live.state==='running'){emit({phase:'started'});break;}
      if(live.state==='exited')break;await sleep(20);
    }
    const stdout=await attach.result;check(!signal.aborted,'source_feedback_processor_unknown',503);
    const after=await inspect(ownedId);check(after.id===ownedId&&after.state==='exited'&&after.exitCode===0&&after.oom===false,'source_feedback_processor_unknown',503);
    const result=JSON.parse(stdout);
    // Parsing/closed receipt validation belongs to the fixed engine caller.
    output=result;
  }catch(error){primary=error;}
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
    if(packetOwned){
      try{
        const state=await fs.lstat(plan.packetDirectory);
        check(state.isDirectory()&&!state.isSymbolicLink()&&state.uid===1000&&(state.mode&0o777)===0o700
          &&await fs.realpath(plan.packetDirectory)===plan.packetDirectory
          &&/^packet-[a-f0-9]{32}$/u.test(plan.packetDirectory.slice(plan.directory.length+1))
          &&plan.packetDirectory.startsWith(plan.directory+'/'),'source_feedback_processor_unknown',503);
        await fs.rm(plan.packetDirectory,{recursive:true,force:false,maxRetries:5,retryDelay:30});packetRemoved=true;
      }catch{cleanupUnknown=true;}
    }
    try{emit({phase:'cleanup',stopped,exitCode,oom,containerRemoved,packetRemoved,cleanupUnknown,observerFailed});}catch{}
  }
  if(cleanupUnknown)throw new SourceAppError('source_feedback_processor_cleanup_unknown',503);
  if(primary)throw primary;
  check(!observerFailed,'source_feedback_processor_unknown',503);
  return output;
}
