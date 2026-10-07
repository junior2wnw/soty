// Node18 HOST launcher. Native crypto/restore runs only in the fixed Node24
// supervisor. The public Source helpers never receive a Docker socket.
import {readFile,lstat,realpath,mkdir} from 'node:fs/promises';
import {createHash} from 'node:crypto';
import {resolve,dirname} from 'node:path';
import {pathToFileURL} from 'node:url';
import {isDeepStrictEqual} from 'node:util';
import {createLocalWslHostDockerCommandRunner} from '../server/linux-feedback-lifecycle.mjs';
import {LOCAL_LINUX_FEEDBACK_PLACEMENT as placement} from '../server/linux-feedback-local-placement.mjs';
import {SOURCE_COLD_PROFILE as profile} from './cold-profile.mjs';
import {sourceColdLaunchDiagnostic} from './cold-launch-diagnostic.mjs';
const commands=createLocalWslHostDockerCommandRunner(placement.dockerHostBinary),sha=bytes=>createHash('sha256').update(bytes).digest('hex');
const check=value=>{if(!value)throw Error('source_cold_supervisor_refused');};
// Docker's JSON objects may have different key insertion order. Array order,
// values and extra/missing fields still have to match the reviewed spec.
const same=isDeepStrictEqual;
export function assertSourceColdSupervisor(actual,spec){
  const c=actual[0],h=c.HostConfig;
  check(/^[a-f0-9]{64}$/.test(c.Id)&&c.Name==='/'+spec.name&&c.Image===profile.supervisorImage&&c.Config.User==='1000:1000'
    &&c.Config.Labels['io.soty.source.cold-supervisor']===spec.nonce&&c.Config.WorkingDir===spec.directory&&same(c.Config.Env,spec.expectedEnv)
    &&same(c.Config.Entrypoint,['/usr/local/bin/node'])&&same(c.Config.Cmd,spec.argv)
    &&h.ReadonlyRootfs===true&&h.NetworkMode==='none'&&h.Memory===536870912&&h.MemorySwap===536870912&&h.PidsLimit===128&&h.NanoCpus===1000000000
    &&h.CpuPeriod===0&&h.CpuQuota===0&&h.CpuShares===0&&h.CpusetCpus===''&&h.CpusetMems===''&&h.Privileged===false
    &&same(h.GroupAdd,['1001'])&&same(h.CapDrop,['ALL'])&&(!h.CapAdd||h.CapAdd.length===0)&&same(h.SecurityOpt,['no-new-privileges'])
    &&(!h.Devices||h.Devices.length===0)&&(!h.DeviceRequests||h.DeviceRequests.length===0)&&(!h.DeviceCgroupRules||h.DeviceCgroupRules.length===0)
    &&(!h.Binds||h.Binds.length===0)&&(!h.VolumesFrom||h.VolumesFrom.length===0)&&(!h.PortBindings||Object.keys(h.PortBindings).length===0)&&h.PublishAllPorts===false
    &&h.PidMode===''&&h.IpcMode==='private'&&h.UTSMode===''&&h.RestartPolicy.Name==='no'&&h.RestartPolicy.MaximumRetryCount===0
    &&h.LogConfig.Type==='none'&&Object.keys(h.LogConfig.Config??{}).length===0&&same(h.Ulimits,[{Name:'core',Soft:0,Hard:0}])&&same(h.Tmpfs,spec.tmpfs)
    &&c.Mounts.length===spec.mounts.length&&c.Mounts.every(m=>m.Type==='bind')
    &&spec.mounts.every(m=>c.Mounts.some(actual=>actual.Source===m.source&&actual.Destination===m.target&&actual.RW===m.rw&&actual.Propagation==='rprivate')));
  return c;
}
async function inspect(id){return JSON.parse(await commands.run(['inspect',id],{limit:65536}));}
export async function launchSourceColdSupervisor(path){
  let item,attach,passed=false,cleanupUnknown=false,phase='preflight',sourceResult,failure,launchDiagnostic;
  try{
    check(process.platform==='linux'&&process.getuid()===1000&&resolve(path)===path);const directory=dirname(path),stat=await lstat(path);
    check(stat.isFile()&&!stat.isSymbolicLink()&&stat.uid===1000&&(stat.mode&0o777)===0o600&&stat.size<=8192&&await realpath(path)===path);
    const config=JSON.parse(await readFile(path,'utf8'));
    check(Object.keys(config).sort().join(',')==='coldManifestSha256,dockerCliSha256,imageId,nonce,packetManifestSha256,sourceCommit'
      &&/^[a-f0-9]{32}$/.test(config.nonce)&&directory===placement.lab+'/source-cold-'+config.nonce&&config.imageId===profile.imageId
      &&config.sourceCommit===profile.sourceCommit&&config.packetManifestSha256===profile.packetManifestSha256);
    const folder=await lstat(directory),parent=await lstat(placement.lab);check(folder.isDirectory()&&!folder.isSymbolicLink()&&folder.uid===1000&&(folder.mode&0o777)===0o700
      &&parent.isDirectory()&&!parent.isSymbolicLink()&&parent.uid===1000&&(parent.mode&0o777)===0o700);
    check(sha(await readFile(directory+'/manifest.json'))===config.coldManifestSha256&&sha(await readFile(placement.dockerHostBinary))===config.dockerCliSha256);
    const socket=await lstat(placement.socketHost);check(socket.isSocket()&&!socket.isSymbolicLink()&&socket.gid===1001&&(socket.mode&0o777)===0o660&&await realpath(placement.socketHost)===placement.socketHost);
    const base=JSON.parse(await commands.run(['image','inspect',profile.supervisorImage],{limit:65536}))[0];check(base.Id===profile.supervisorImage);
    const stub=directory+'/lab-parent';await mkdir(stub,{mode:0o700});
    // The read-only parent bind needs this one public empty nested target
    // before OCI overlays the exact reviewed packet bind. No whole LAB mount.
    await mkdir(stub+'/source-cold-'+config.nonce,{mode:0o700});
    item={nonce:config.nonce,name:'codex-soty-source-cold-supervisor-'+config.nonce,directory,expectedEnv:base.Config.Env.slice(),argv:[directory+'/guardian.mjs',path],id:null,
      tmpfs:{'/tmp':'rw,nosuid,nodev,noexec,size=268435456,uid=1000,gid=1000,mode=700','/data':'rw,nosuid,nodev,noexec,size=1048576,uid=1000,gid=1000,mode=700'},
      mounts:[{source:stub,target:placement.lab,rw:false},{source:directory,target:directory,rw:true},{source:placement.dockerHostBinary,target:profile.dockerBinary,rw:false},{source:placement.socketHost,target:profile.socket,rw:false}]};
    phase='create';const args=['create','--pull=never','--name',item.name,'--label','io.soty.source.cold-supervisor='+config.nonce,'--user','1000:1000','--group-add','1001',
      '--read-only','--network','none','--cap-drop','ALL','--security-opt','no-new-privileges','--memory','512m','--memory-swap','512m','--pids-limit','128','--cpus','1','--ulimit','core=0:0','--log-driver','none','--restart','no',
      ...Object.entries(item.tmpfs).flatMap(([name,options])=>['--tmpfs',name+':'+options]),...item.mounts.flatMap(m=>['--mount','type=bind,src='+m.source+',dst='+m.target+(m.rw?'':',readonly')+',bind-propagation=rprivate']),
      '--workdir',directory,'--entrypoint','/usr/local/bin/node',profile.supervisorImage,...item.argv];
    item.id=await commands.run(args);const c=assertSourceColdSupervisor(await inspect(item.id),item);check(c.Id===item.id&&c.State.Status==='created');
    phase='run';attach=commands.start(['start','--attach',item.id],{timeout:180000,limit:65536,collectExitOneReceipt:true});
    sourceResult=JSON.parse(await attach.result);const stopped=assertSourceColdSupervisor(await inspect(item.id),item);check(stopped.State.Status==='exited'&&!stopped.State.OOMKilled&&stopped.State.ExitCode===0);
    passed=sourceResult.schema==='soty.source-cold-receipt.v1'&&sourceResult.passed===true&&sourceResult.cleanupUnknown===false;
  }catch(error){passed=false;failure=error;}
  finally{if(item)try{const c=assertSourceColdSupervisor(await inspect(item.id??item.name),item);item.id=c.Id;
    launchDiagnostic=sourceColdLaunchDiagnostic(failure,c.State);
    if(c.State.Running)await commands.run(['kill','--signal','SIGKILL',c.Id]);
    await attach?.stopAndWait();const final=assertSourceColdSupervisor(await inspect(c.Id),item);check(!final.State.Running);await commands.run(['rm',c.Id]);
    check(await commands.run(['container','ls','--all','--no-trunc','--filter','id='+c.Id,'--format','{{.ID}}'],{limit:128})==='');
  }catch{cleanupUnknown=true;try{await attach?.stopAndWait();}catch{}}}
  return{schema:'soty.source-cold-supervisor-receipt.v1',passed:passed&&!cleanupUnknown,phase,cleanupUnknown,
    launchDiagnostic:launchDiagnostic??sourceColdLaunchDiagnostic(failure),
    ...(sourceResult?{sourcePassed:sourceResult.passed===true,sourceCleanupUnknown:sourceResult.cleanupUnknown===true,sourcePhase:['seed','archive','dry_inspect','restore','compare','entry','negatives'].includes(sourceResult.phase)?sourceResult.phase:'unknown'}:{}),
    authenticationProved:false,models:false,productionReady:false};
}
if(process.argv[1]&&pathToFileURL(resolve(process.argv[1])).href===import.meta.url){try{check(process.argv.length===3);const result=await launchSourceColdSupervisor(resolve(process.argv[2]));console.log(JSON.stringify(result));if(!result.passed)process.exitCode=1;}
catch{console.log(JSON.stringify({schema:'soty.source-cold-supervisor-receipt.v1',passed:false,phase:'preflight',cleanupUnknown:true,productionReady:false}));process.exitCode=1;}}
