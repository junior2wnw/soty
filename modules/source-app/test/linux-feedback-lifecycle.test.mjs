import test from 'node:test';
import assert from 'node:assert/strict';
import {mkdtemp,readFile,rm} from 'node:fs/promises';
import {tmpdir} from 'node:os';
import {join} from 'node:path';
import {SourceAppError} from '../server/wire.mjs';
import {assertFixedLinuxSpec,runFixedLinuxPacket,createDockerCommandRunner} from '../server/linux-feedback-lifecycle.mjs';
import {linuxFeedbackFailure,linuxFeedbackCleanupFailure} from '../server/linux-feedback-diagnostic.mjs';

// Controlled host failures are lifecycle evidence, not Linux/Docker isolation
// acceptance. The actual installed Source→OS packet remains a separate gate.
function fixture(options={}){
  const directory='/home/ai2/codex-soty-universal-20261007-9f8dcd71/source-feedback-jobs',nonce='1'.repeat(32);
  const plan={directory,packetDirectory:directory+'/packet-'+nonce,packetText:'{}',workerSource:'fixed',image:'sha256:'+'2'.repeat(64),
    name:'codex-soty-feedback-job-'+nonce,label:'io.soty.feedback.packet',packetSha256:'3'.repeat(64),workerSha256:'4'.repeat(64),
    command:['fixed'],tmpfs:{'/scratch':'fixed'},budget:{wallMs:1000,cleanupMs:500,outputBytes:1024},scenario:'success',engineRef:{id:'synthetic',version:1,digest:'5'.repeat(64)}};
  const spec={id:'6'.repeat(64),name:'/'+plan.name,image:plan.image,labels:{[plan.label]:plan.packetSha256},user:'1000:1000',workdir:'/probe',
    entrypoint:['/usr/bin/timeout'],cmd:plan.command,readonly:true,network:'none',memory:134217728,swap:134217728,pids:32,
    nanoCpus:1000000000,cpuPeriod:0,cpuQuota:0,cpuShares:0,cpuRealtimePeriod:0,cpuRealtimeRuntime:0,cpuCount:0,cpuPercent:0,
    cpusetCpus:'',cpusetMems:'',ulimits:[{Name:'core',Soft:0,Hard:0}],log:{Type:'none',Config:{}},
    privileged:false,caps:['ALL'],capAdd:null,security:['no-new-privileges'],devices:[],deviceRequests:null,deviceRules:null,
    binds:null,volumesFrom:null,ports:{},publishPorts:false,pidMode:'',ipcMode:'private',utsMode:'',restart:{Name:'no',MaximumRetryCount:0},
    tmpfs:plan.tmpfs,mounts:[{Type:'bind',Source:plan.packetDirectory,Destination:'/probe',RW:false,Propagation:'rprivate'}],state:'created',exitCode:0,oom:false};
  const state={creates:0,starts:0,kills:0,removes:0,packetRemoves:0,writes:0,joined:0,before:0,container:null,attachClosed:true,evidence:[]};
  const controller=new AbortController();
  let finish,timer,inspectFault=false;
  const fs={
    async mkdir(){if(options.mkdirFault)throw new Error('mkdir_fault');},
    async writeFile(){state.writes++;if(options.writeFault===state.writes)throw new Error('write_fault');},
    async lstat(){return {isDirectory:()=>true,isSymbolicLink:()=>false,uid:1000,mode:0o700};},
    async realpath(path){return options.pathChanged?path+'-foreign':path;},
    async rm(){state.packetRemoves++;if(options.packetRmFault)throw Object.assign(new Error('private/path/not-logged'),{code:'EACCES'});}
  };
  const commands={
    async run(args){
      if(args[0]==='create'){
        state.creates++;if(!options.createAbsent)state.container=structuredClone({...spec,...options.specDelta});
        if(options.createUnknown)throw new SourceAppError('source_feedback_processor_unknown',503);return spec.id;
      }
      if(args[0]==='inspect'){
        if(options.observeFault&&state.starts&&!inspectFault){inspectFault=true;throw new Error('observe_fault');}
        if(options.cleanupInspectFault&&state.attachClosed&&state.starts)throw new Error('cleanup_inspect_fault');
        if(!state.container)throw new Error('not_found');return JSON.stringify(state.container);
      }
      if(args[0]==='kill'){
        if(options.killRaceExited||options.killRaceRunning){
          state.kills++;if(options.killRaceExited){state.container.state='exited';state.container.exitCode=143;}
          // Abort closes the owned attach CLI even when the daemon process is
          // still running; the latter must retain unknown cleanup authority.
          finish?.(new SourceAppError('source_feedback_processor_unknown',503));
          const error=new SourceAppError('source_feedback_processor_unknown',503);error.linuxCliDiagnostic={exitClass:'nonzero'};throw error;
        }
        state.kills++;state.container.state='exited';state.container.exitCode=143;clearTimeout(timer);
        finish?.(new SourceAppError('source_feedback_processor_unknown',503));return '';
      }
      if(args[0]==='rm'){
        state.removes++;if(options.containerRmFault)throw new Error('container_rm_fault');state.container=null;return '';
      }
      if(args[0]==='container')return state.container?.id??'';
      throw new Error('unexpected_command');
    },
    start(args){
      assert.deepEqual(args,['start','--attach',spec.id]);state.starts++;state.container.state='running';state.attachClosed=false;
      const result=new Promise((resolve,reject)=>{
        finish=error=>{if(state.attachClosed)return;state.attachClosed=true;error?reject(error):resolve('{"output":{"text":"synthetic"}}');};
        if(!options.held)timer=setTimeout(()=>{state.container.state='exited';finish();},5);
      });result.catch(()=>{});
      return {result,async stopAndWait(){state.joined++;clearTimeout(timer);finish(new SourceAppError('source_feedback_processor_unknown',503));await result.catch(()=>{});}};
    }
  };
  const run=()=>runFixedLinuxPacket(plan,{fs,commands,sleep:async()=>{}},{signal:controller.signal,beforeStart:async()=>{state.before++;},
    onEvidence:item=>{state.evidence.push(item);if(options.abortOnStarted&&item.phase==='started')controller.abort();if(options.observerFault&&item.phase==='started')throw new Error('observer_fault');}});
  return {plan,spec,state,run};
}

test('success waits for stopped container, exact removal proof and packet cleanup BEFORE output',async()=>{
  const f=fixture();assert.deepEqual(await f.run(),{output:{text:'synthetic'}});
  assert.equal(f.state.creates,1);assert.equal(f.state.starts,1);assert.equal(f.state.joined,1);assert.equal(f.state.attachClosed,true);
  assert.equal(f.state.removes,1);assert.equal(f.state.packetRemoves,1);
  assert.equal(f.state.evidence.at(-1).cleanupUnknown,false);assert.equal(f.state.evidence.at(-1).containerRemoved,true);
});

test('packet mkdir/write failures never create a container; owned partial writes always cleaned',async()=>{
  const mkdir=fixture({mkdirFault:true});await assert.rejects(mkdir.run());assert.equal(mkdir.state.creates,0);assert.equal(mkdir.state.packetRemoves,0);
  for(const writeFault of [1,2]){const f=fixture({writeFault});await assert.rejects(f.run());assert.equal(f.state.creates,0);assert.equal(f.state.packetRemoves,1);}
});

test('unknown CREATE recovers exact existing name only for cleanup, never retries CREATE or START',async()=>{
  const f=fixture({createUnknown:true});await assert.rejects(f.run(),error=>error.code==='source_feedback_processor_unknown');
  assert.equal(f.state.creates,1);assert.equal(f.state.starts,0);assert.equal(f.state.before,0);assert.equal(f.state.removes,1);
  assert.equal(f.state.evidence.at(-1).cleanupUnknown,false);
  const absent=fixture({createUnknown:true,createAbsent:true});await assert.rejects(absent.run(),error=>error.code==='source_feedback_processor_cleanup_unknown');
  assert.equal(absent.state.creates,1);assert.equal(absent.state.starts,0);assert.equal(absent.state.removes,0);
  assert.equal(absent.state.packetRemoves,0,'unknown daemon delivery retains exact owned packet');
  assert.equal(absent.state.evidence.at(-1).cleanupUnknown,true);
});

test('unknown CREATE with foreign identity/spec never adopts or deletes that container',async()=>{
  for(const specDelta of [{name:'/foreign'},{image:'sha256:'+'7'.repeat(64)},{labels:{}},{cmd:['different']},{privileged:true}]){
    const f=fixture({createUnknown:true,specDelta});await assert.rejects(f.run(),error=>error.code==='source_feedback_processor_cleanup_unknown');
    assert.equal(f.state.starts,0);assert.equal(f.state.kills,0);assert.equal(f.state.removes,0);
  }
});

test('safe failure identifies exact lifecycle phase/spec group while raw error/CLI strings never enter evidence',async()=>{
  const f=fixture({specDelta:{nanoCpus:0}});await assert.rejects(f.run());
  const failure=f.state.evidence.find(item=>item.phase==='failure');assert.equal(failure.stage,'created_spec');
  assert.equal(failure.code,'source_feedback_processor_not_ready');assert.deepEqual(failure.specMismatchGroups,['cpu']);
  assert.equal(f.state.packetRemoves,0);assert.equal(f.state.evidence.at(-1).cleanupUnknown,true);
  const raw=Object.assign(new Error('private-token-value'),{code:'private-token-value',stderr:'private-token-value',linuxCliDiagnostic:{exitClass:'private-token-value'},specMismatchGroups:['private-token-value']});
  assert.equal(JSON.stringify(linuxFeedbackFailure(raw)).includes('private-token-value'),false);
  const getter=Object.defineProperty({},'code',{get(){throw Error('do-not-call');}});assert.equal(linuxFeedbackFailure(getter).code,'unclassified');
});

test('real owned CLI exit1 stays rejected for workers; only fixed supervisor diagnostic option collects bounded JSON',async()=>{
  const runner=createDockerCommandRunner(process.execPath),args=['--input-type=module','-e','console.log(JSON.stringify({synthetic:true}));process.exitCode=1'];
  await assert.rejects(runner.run(args),error=>linuxFeedbackFailure(error).cliExitClass==='nonzero');
  assert.deepEqual(JSON.parse(await runner.run(args,{collectExitOneReceipt:true})),{synthetic:true});
});

test('a successful CREATE response with foreign spec cannot START or delete an unproved ID',async()=>{
  const f=fixture({specDelta:{labels:{}}});await assert.rejects(f.run(),error=>error.code==='source_feedback_processor_cleanup_unknown');
  assert.equal(f.state.creates,1);assert.equal(f.state.starts,0);assert.equal(f.state.before,0);assert.equal(f.state.removes,0);
});

test('observer or startup inspection failure stops and joins owned attach, then cleans verified container',async()=>{
  for(const option of [{observerFault:true},{observeFault:true}]){
    const f=fixture({...option,held:true});await assert.rejects(f.run());
    assert.equal(f.state.kills,1);assert.equal(f.state.attachClosed,true);assert.equal(f.state.joined,1);assert.equal(f.state.removes,1);
    assert.equal(f.state.packetRemoves,1);assert.equal(f.state.evidence.at(-1).cleanupUnknown,false);
  }
});

test('rm/inspect/packet cleanup failures block successful output and expose cleanupUnknown',async()=>{
  for(const option of [{containerRmFault:true},{cleanupInspectFault:true},{packetRmFault:true},{pathChanged:true}]){
    const f=fixture(option);await assert.rejects(f.run(),error=>error.code==='source_feedback_processor_cleanup_unknown');
    assert.equal(f.state.attachClosed,true);assert.equal(f.state.evidence.at(-1).cleanupUnknown,true);
  }
});

test('one failed kill is reconciled by fresh exact stopped proof; still-running or unknown stays retained',async()=>{
  const stopped=fixture({held:true,abortOnStarted:true,killRaceExited:true});await assert.rejects(stopped.run());
  assert.equal(stopped.state.kills,1);assert.equal(stopped.state.starts,1);assert.equal(stopped.state.packetRemoves,1);
  const cleanup=stopped.state.evidence.at(-1);assert.equal(cleanup.cleanupUnknown,false);assert.equal(cleanup.stopped,true);
  assert.deepEqual(cleanup.cleanupFailures,[{stage:'stop_term',code:'source_feedback_processor_unknown',cliExitClass:'nonzero',specMismatchGroups:[],fsClass:'none',reconciliation:'stopped'}]);
  const running=fixture({held:true,abortOnStarted:true,killRaceRunning:true});await assert.rejects(running.run());
  assert.equal(running.state.kills,1);assert.equal(running.state.packetRemoves,0);assert.equal(running.state.evidence.at(-1).cleanupUnknown,true);
});

test('cleanup diagnostics expose fixed phase/FS class only, not filesystem paths or arbitrary error properties',async()=>{
  const f=fixture({packetRmFault:true});await assert.rejects(f.run());const cleanup=f.state.evidence.at(-1);
  assert.equal(cleanup.cleanupFailures[0].stage,'packet_rm');assert.equal(cleanup.cleanupFailures[0].fsClass,'EACCES');
  assert.equal(JSON.stringify(cleanup).includes('private/path'),false);
  assert.deepEqual(linuxFeedbackCleanupFailure('caller-secret',{code:'caller-secret',path:'caller-secret'}),
    {stage:'unknown',code:'unclassified',cliExitClass:'none',specMismatchGroups:[],fsClass:'none',reconciliation:'unknown'});
});

test('uncertain container removal/inspection retains owned reachable packet and cannot return Native output',async()=>{
  for(const option of [{containerRmFault:true},{cleanupInspectFault:true}]){
    const f=fixture(option);await assert.rejects(f.run(),error=>error.code==='source_feedback_processor_cleanup_unknown');
    assert.equal(f.state.packetRemoves,0);assert.equal(f.state.evidence.at(-1).packetRemoved,false);
    assert.equal(f.state.evidence.at(-1).cleanupUnknown,true);
  }
});

test('preSTART spec denies widened CPU/core/log/workdir/propagation/devices/caps/socket/privilege',()=>{
  const f=fixture();assertFixedLinuxSpec(f.spec,f.plan);
  const deltas=[{nanoCpus:0},{cpuQuota:1},{cpuPeriod:1},{cpuShares:1},{cpuRealtimePeriod:1},{cpuRealtimeRuntime:1},{cpuCount:1},{cpuPercent:1},
    {cpusetCpus:'0'},{ulimits:[]},{log:{Type:'json-file',Config:{}}},
    {log:{Type:'none',Config:{extra:'x'}}},{workdir:'/'},{privileged:true},{devices:[{PathOnHost:'/dev/a'}]},{deviceRequests:[{}]},
    {deviceRules:['a *:* rwm']},{capAdd:['SYS_ADMIN']},{pidMode:'host'},{ipcMode:'host'},{restart:{Name:'always',MaximumRetryCount:0}},
    {mounts:[{...f.spec.mounts[0],Propagation:'rshared'}]},
    {mounts:[...f.spec.mounts,{Type:'bind',Source:'/var/run/docker.sock',Destination:'/var/run/docker.sock',RW:true}]}];
  for(const delta of deltas)assert.throws(()=>assertFixedLinuxSpec({...f.spec,...delta},f.plan));
});

test('real command timeout/stop waits for owned OS child CLOSE; no live child after rejected result',async t=>{
  const directory=await mkdtemp(join(tmpdir(),'soty-linux-cli-lifecycle-'));t.after(()=>rm(directory,{recursive:true,force:true,maxRetries:5,retryDelay:30}));
  const pidPath=join(directory,'child.pid'),runner=createDockerCommandRunner(process.execPath);
  const script="require('node:fs').writeFileSync(process.argv[1],String(process.pid));process.on('SIGTERM',()=>{});setInterval(()=>{},1000);";
  const handle=runner.start(['-e',script,pidPath],{timeout:500});await assert.rejects(handle.result);await handle.stopAndWait();
  const pid=Number(await readFile(pidPath,'utf8'));assert.throws(()=>process.kill(pid,0));
});
