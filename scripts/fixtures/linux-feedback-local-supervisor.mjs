import * as fs from 'node:fs/promises';
import {resolve,dirname,join} from 'node:path';
import {createHash} from 'node:crypto';
import {pathToFileURL} from 'node:url';
import {createLocalWslHostDockerCommandRunner} from '../../modules/source-app/server/linux-feedback-lifecycle.mjs';
import {canonical,digest} from '../../modules/source-app/server/wire.mjs';
import {LOCAL_LINUX_FEEDBACK_PLACEMENT as placement} from '../../modules/source-app/server/linux-feedback-local-placement.mjs';

// PUBLIC Node18-compatible HOST guard. Operator config is reviewed immutable
// SHA/GID data, not publisher/HTTP input. The Source runtime runs on Node24.
const LAB=placement.lab;
const WORKER='sha256:c03a61d12e03870747e9013860fc36e23e53920da9b07ea1e795b3fef9628ae6';
const IMPORTS=['/app/server/http-app.js','/app/modules/connect/browser/client.mjs','/app/modules/apps/server/schema.mjs','/app/node_modules/openid-client/build/index.js'];
const sha=value=>createHash('sha256').update(value).digest('hex'),same=(a,b)=>canonical(a)===canonical(b);
const check=value=>{if(!value)throw new Error('supervisor_guard_refused');};
const hex=value=>typeof value==='string'&&/^[a-f0-9]{64}$/u.test(value);
const image=value=>typeof value==='string'&&/^sha256:[a-f0-9]{64}$/u.test(value);
const empty=value=>value===null||Array.isArray(value)&&value.length===0;
const emptyObject=value=>value===null||value&&typeof value==='object'&&!Array.isArray(value)&&Object.keys(value).length===0;
const format='{"id":{{json .Id}},"name":{{json .Name}},"image":{{json .Image}},"labels":{{json .Config.Labels}},"user":{{json .Config.User}},"tmpdir":{{ $ok := false }}{{range .Config.Env}}{{if eq . "TMPDIR=/tmp"}}{{ $ok = true }}{{end}}{{end}}{{json $ok}},"workdir":{{json .Config.WorkingDir}},"entrypoint":{{json .Config.Entrypoint}},"cmd":{{json .Config.Cmd}},"groups":{{json .HostConfig.GroupAdd}},"readonly":{{json .HostConfig.ReadonlyRootfs}},"network":{{json .HostConfig.NetworkMode}},"memory":{{json .HostConfig.Memory}},"swap":{{json .HostConfig.MemorySwap}},"pids":{{json .HostConfig.PidsLimit}},"nanoCpus":{{json .HostConfig.NanoCpus}},"cpuPeriod":{{json .HostConfig.CpuPeriod}},"cpuQuota":{{json .HostConfig.CpuQuota}},"cpuShares":{{json .HostConfig.CpuShares}},"cpusetCpus":{{json .HostConfig.CpusetCpus}},"cpusetMems":{{json .HostConfig.CpusetMems}},"ulimits":{{json .HostConfig.Ulimits}},"ports":{{json .HostConfig.PortBindings}},"publishPorts":{{json .HostConfig.PublishAllPorts}},"pidMode":{{json .HostConfig.PidMode}},"ipcMode":{{json .HostConfig.IpcMode}},"utsMode":{{json .HostConfig.UTSMode}},"caps":{{json .HostConfig.CapDrop}},"capAdd":{{json .HostConfig.CapAdd}},"security":{{json .HostConfig.SecurityOpt}},"privileged":{{json .HostConfig.Privileged}},"devices":{{json .HostConfig.Devices}},"requests":{{json .HostConfig.DeviceRequests}},"rules":{{json .HostConfig.DeviceCgroupRules}},"log":{{json .HostConfig.LogConfig}},"restart":{{json .HostConfig.RestartPolicy}},"tmpfs":{{json .HostConfig.Tmpfs}},"mounts":{{json .Mounts}},"state":{{json .State.Status}},"exitCode":{{json .State.ExitCode}},"oom":{{json .State.OOMKilled}}}';
const commands=createLocalWslHostDockerCommandRunner(placement.dockerHostBinary);
const sleep=ms=>new Promise(done=>setTimeout(done,ms));
const toolScript="import {access} from 'node:fs/promises';import {constants} from 'node:fs';if(process.getuid()!==1000||!process.version.startsWith('v24.'))throw Error('tool_guard');for(const p of ['/usr/bin/timeout','/usr/bin/prlimit'])await access(p,constants.X_OK);console.log(JSON.stringify({toolsPresent:true,node:process.version}));";

async function checkedDirectory(path,mode=0o700){const stat=await fs.lstat(path);check(stat.isDirectory()&&!stat.isSymbolicLink()&&stat.uid===1000&&(stat.mode&0o777)===mode&&await fs.realpath(path)===path);}
async function readBounded(path,max=4194304){const stat=await fs.lstat(path);check(stat.isFile()&&!stat.isSymbolicLink()&&stat.size<=max);return fs.readFile(path);}
async function inspect(locator){return JSON.parse(await commands.run(['inspect','--format',format,locator]));}
export function assertSupervisorContainer(actual,spec){
  check(/^[a-f0-9]{64}$/u.test(actual.id)&&actual.name==='/'+spec.name&&actual.image===spec.image
    &&actual.labels?.['io.soty.feedback.supervisor']===spec.label&&actual.user==='1000:1000'&&actual.workdir==='/probe'
    &&same(actual.entrypoint,['/usr/local/bin/node'])&&same(actual.cmd,spec.command)
    &&(spec.groups.length===0?empty(actual.groups):same(actual.groups,spec.groups))
    &&actual.readonly===true&&actual.network==='none'&&actual.memory===spec.memory&&actual.swap===spec.memory&&actual.pids===spec.pids
    &&actual.nanoCpus===1000000000&&actual.cpuPeriod===0&&actual.cpuQuota===0
    &&actual.cpuShares===0&&actual.cpusetCpus===''&&actual.cpusetMems===''&&same(actual.ulimits,[{Name:'core',Soft:0,Hard:0}])
    &&emptyObject(actual.ports)&&actual.publishPorts===false&&actual.pidMode===''&&actual.ipcMode==='private'&&actual.utsMode===''
    &&same(actual.caps,['ALL'])&&empty(actual.capAdd)&&same(actual.security,['no-new-privileges'])&&actual.privileged===false
    &&empty(actual.devices)&&empty(actual.requests)&&empty(actual.rules)&&actual.log?.Type==='none'&&emptyObject(actual.log.Config)
    &&actual.restart?.Name==='no'&&actual.restart.MaximumRetryCount===0&&same(actual.tmpfs,spec.tmpfs)
    &&['created','running','exited'].includes(actual.state)&&actual.mounts.length===spec.mounts.length);
  for(const expected of spec.mounts){const found=actual.mounts.filter(item=>item.Destination===expected.target);check(found.length===1&&found[0].Type==='bind'
    &&found[0].Source===expected.source&&found[0].RW===expected.rw&&found[0].Propagation==='rprivate');}
  // Formatter projects only the exact TMPDIR boolean, never the env values.
  check(actual.tmpdir===true);return actual;
}
function argsFor(spec){return ['create','--pull=never','--name',spec.name,'--label','io.soty.feedback.supervisor='+spec.label,
  '--user','1000:1000',...spec.groups.flatMap(g=>['--group-add',g]),'--read-only','--network','none','--cap-drop','ALL','--security-opt','no-new-privileges',
  '--memory',String(spec.memory),'--memory-swap',String(spec.memory),'--pids-limit',String(spec.pids),'--cpus','1','--ulimit','core=0:0','--log-driver','none','--restart','no',
  '--env','TMPDIR=/tmp',...spec.mounts.flatMap(m=>['--mount','type=bind,src='+m.source+',dst='+m.target+(m.rw?'':',readonly')+',bind-propagation=rprivate']),
  ...Object.entries(spec.tmpfs).flatMap(([path,options])=>['--tmpfs',path+':'+options]),'--workdir','/probe','--entrypoint','/usr/local/bin/node',spec.image,...spec.command];}

const containers=[];
async function create(spec){const item={spec,attempted:true,id:null,attach:null};containers.push(item);
  const returned=await commands.run(argsFor(spec));const actual=assertSupervisorContainer(await inspect(spec.name),spec);item.id=actual.id;
  check(returned===item.id&&actual.state==='created');return item;}
async function cleanup(item){
  let unknown=false,removed=false,stopped=false;
  if(!item.id){try{item.id=assertSupervisorContainer(await inspect(item.spec.name),item.spec).id;}catch{unknown=true;}}
  if(item.id){
    try{let state=assertSupervisorContainer(await inspect(item.id),item.spec);check(state.id===item.id);
      if(state.state==='running'){
        await commands.run(['kill','--signal','SIGTERM',item.id]);const deadline=Date.now()+2000;
        do{await sleep(50);state=assertSupervisorContainer(await inspect(item.id),item.spec);}while(state.state==='running'&&Date.now()<deadline);
        if(state.state==='running')await commands.run(['kill','--signal','SIGKILL',item.id]);
      }
    }catch{unknown=true;}
    try{await item.attach?.stopAndWait();}catch{unknown=true;}
    try{const state=assertSupervisorContainer(await inspect(item.id),item.spec);stopped=['created','exited'].includes(state.state);if(!stopped)unknown=true;}catch{unknown=true;}
    try{await commands.run(['rm','--force',item.id]);check(await commands.run(['container','ls','--all','--no-trunc','--filter','id='+item.id,'--format','{{.ID}}'],{limit:128})==='');removed=true;}catch{unknown=true;}
  }else try{await item.attach?.stopAndWait();}catch{unknown=true;}
  return {role:item.spec.role,stopped,removed,cleanupUnknown:unknown};
}

async function main(){
let runDirectory,ownDirectory=false,ownJobs=false,result,primary,cleanupUnknown=false;const cleanupEvidence=[];
try{
  check(process.platform==='linux'&&process.getuid()===1000&&process.argv.length===3);await checkedDirectory(LAB);
  const packetDirectory=resolve(dirname(process.argv[2]));check(packetDirectory.startsWith(LAB+'/linux-feedback-local-jobs-')&&/^linux-feedback-local-jobs-[a-f0-9]{32}$/u.test(packetDirectory.slice(LAB.length+1)));
  await checkedDirectory(packetDirectory);
  const config=JSON.parse((await readBounded(process.argv[2],8192)).toString('utf8'));
  check(same(Object.keys(config).sort(),['schema','nonce','rootImage','rootRevision','fixtureSha256','manifestSha256','supervisorSha256','nativeRevokeSha256','dockerCliSha256','socketGid','imports'].sort())
    &&config.schema==='soty.source-feedback-local-supervisor.v1'&&/^[a-f0-9]{32}$/u.test(config.nonce)&&image(config.rootImage)
    &&/^[a-f0-9]{40}$/u.test(config.rootRevision)&&hex(config.fixtureSha256)&&hex(config.manifestSha256)&&hex(config.supervisorSha256)&&hex(config.nativeRevokeSha256)&&hex(config.dockerCliSha256)
    &&Number.isSafeInteger(config.socketGid)&&config.socketGid>0&&config.socketGid<=2147483647
    &&Array.isArray(config.imports)&&config.imports.length===4&&config.imports.every((item,i)=>same(Object.keys(item).sort(),['path','sha256'])&&item.path===IMPORTS[i]&&hex(item.sha256)));
  check(sha(await readBounded(packetDirectory+'/fixture.mjs'))===config.fixtureSha256&&sha(await readBounded(packetDirectory+'/manifest.json',65536))===config.manifestSha256);
  check(sha(await readBounded(packetDirectory+'/supervisor.mjs'))===config.supervisorSha256
    &&sha(await readBounded(packetDirectory+'/native-revoke.mjs',8192))===config.nativeRevokeSha256);
  const manifest=JSON.parse((await readBounded(packetDirectory+'/manifest.json',65536)).toString('utf8'));
  check(manifest.schema==='soty.source-feedback-local-linux-packet.v1'&&manifest.image===WORKER&&manifest.nonce===packetDirectory.slice(-32)
    &&manifest.fixtureSha256===config.fixtureSha256&&manifest.supervisorSha256===config.supervisorSha256&&manifest.nativeRevokeSha256===config.nativeRevokeSha256
    &&same(manifest.externalRootImports,IMPORTS)&&manifest.sourceFiles.length===38&&manifest.placementDigest===digest(placement)&&manifest.cases===7
    &&manifest.models===false&&manifest.productionReady===false);
  const cliPath=await fs.realpath(placement.dockerHostBinary);check(cliPath===placement.dockerHostBinary);check(sha(await readBounded(cliPath,134217728))===config.dockerCliSha256);
  const socket=await fs.lstat(placement.socketHost);check(socket.isSocket()&&!socket.isSymbolicLink()&&socket.gid===placement.socketGid&&config.socketGid===placement.socketGid&&(socket.mode&0o777)===0o660&&await fs.realpath(placement.socketHost)===placement.socketHost);
  check(JSON.parse(await commands.run(['image','inspect',config.rootImage,'--format','{{json .Id}}']))===config.rootImage);
  check(JSON.parse(await commands.run(['image','inspect',WORKER,'--format','{{json .Id}}']))===WORKER);
  runDirectory=LAB+'/feedback-supervisor-'+config.nonce;await fs.mkdir(runDirectory,{mode:0o700});ownDirectory=true;await checkedDirectory(runDirectory);
  const stub=runDirectory+'/lab-parent';await fs.mkdir(stub,{mode:0o700});await fs.mkdir(stub+'/source-feedback-jobs',{mode:0o700});
  const jobs=LAB+'/source-feedback-jobs';await fs.mkdir(jobs,{mode:0o700});ownJobs=true;await checkedDirectory(jobs);check((await fs.readdir(jobs)).length===0);
  const common={label:sha(JSON.stringify(config)),groups:[],memory:134217728,pids:32,tmpfs:{'/tmp':'rw,nosuid,nodev,noexec,size=1048576,uid=1000,gid=1000,mode=700','/data':'rw,nosuid,nodev,noexec,size=1048576,uid=1000,gid=1000,mode=700'}};
  const tools=await create({...common,role:'worker-tools',name:'codex-soty-feedback-tools-'+config.nonce,image:WORKER,command:['--input-type=module','-e',toolScript],mounts:[]});
  tools.attach=commands.start(['start','--attach',tools.id],{limit:1024,timeout:10000});const toolResult=JSON.parse(await tools.attach.result);
  const toolState=assertSupervisorContainer(await inspect(tools.id),tools.spec);check(toolState.state==='exited'&&toolState.exitCode===0&&!toolState.oom&&toolResult.toolsPresent===true&&/^v24\./u.test(toolResult.node));
  const spec={...common,role:'joint-source',name:'codex-soty-feedback-supervisor-'+config.nonce,image:config.rootImage,
    groups:[String(config.socketGid)],memory:536870912,pids:128,command:['/probe/fixture.mjs'],
    tmpfs:{...common.tmpfs,'/tmp':'rw,nosuid,nodev,noexec,size=268435456,uid=1000,gid=1000,mode=700'},mounts:[
      {source:packetDirectory,target:'/probe',rw:false},{source:cliPath,target:'/usr/bin/docker',rw:false},
      {source:placement.socketHost,target:placement.socketContainer,rw:false},{source:stub,target:LAB,rw:false},{source:jobs,target:jobs,rw:true}]};
  const supervisor=await create(spec);
  // Read-only extraction of exactly four public runtime imports. Nothing
  // from image configuration, secrets or private Root data is copied.
  for(let i=0;i<IMPORTS.length;i++){
    const copy=join(runDirectory,'runtime-'+i+'.mjs');await commands.run(['cp','-L',supervisor.id+':'+IMPORTS[i],copy]);check(sha(await readBounded(copy))===config.imports[i].sha256);
  }
  check((await fs.readdir(jobs)).length===0);
  supervisor.attach=commands.start(['start','--attach',supervisor.id],{limit:131072,timeout:180000});
  const output=JSON.parse(await supervisor.attach.result),state=assertSupervisorContainer(await inspect(supervisor.id),spec);
  check(state.state==='exited'&&state.exitCode===0&&!state.oom&&output.schema==='soty.source-feedback-linux-receipt.v1'
    &&output.nonce===manifest.nonce&&output.image===WORKER&&output.placementDigest===digest(placement)&&output.synthetic===true&&output.models===false&&output.productionReady===false
    &&output.passed===true&&Array.isArray(output.results)&&output.results.length===7&&output.results.every(item=>item.passed===true));
  result={schema:'soty.source-feedback-local-supervisor-receipt.v1',nonce:config.nonce,rootImage:config.rootImage,rootRevision:config.rootRevision,
    workerImage:WORKER,placementDigest:digest(placement),sourceCommit:manifest.sourceCommit,fixtureSha256:config.fixtureSha256,manifestSha256:config.manifestSha256,
    importsVerified:4,sourceFiles:38,workerTools:toolResult,sourceResult:output,models:false,productionReady:false};
}catch(error){primary=error;}
finally{
  for(const item of containers.reverse()){const evidence=await cleanup(item);cleanupEvidence.push(evidence);if(evidence.cleanupUnknown)cleanupUnknown=true;}
  // Retain only our bounded evidence/mountpoint on unknown container cleanup;
  // never erase data still reachable by an uncertain supervisor/child.
  if(!cleanupUnknown&&ownJobs){try{await checkedDirectory(LAB+'/source-feedback-jobs');await fs.rmdir(LAB+'/source-feedback-jobs');}catch{cleanupUnknown=true;}}
  if(!cleanupUnknown&&ownDirectory){try{await checkedDirectory(runDirectory);await fs.rm(runDirectory,{recursive:true,force:false,maxRetries:5,retryDelay:30});}catch{cleanupUnknown=true;}}
}
// Full env/inspect/CLI errors are never emitted, even on refusal.
if(primary||cleanupUnknown){console.log(JSON.stringify({schema:'soty.source-feedback-local-supervisor-receipt.v1',passed:false,cleanupUnknown,cleanupEvidence,models:false,productionReady:false}));process.exitCode=1;}
else console.log(JSON.stringify({...result,passed:true,cleanupUnknown:false,cleanupEvidence}));
}
if(process.argv[1]&&pathToFileURL(resolve(process.argv[1])).href===import.meta.url)await main();
