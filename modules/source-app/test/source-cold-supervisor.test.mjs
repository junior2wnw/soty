import test from 'node:test';
import assert from 'node:assert/strict';
import {assertSourceColdSupervisor} from '../install/cold-supervisor.mjs';
import {SOURCE_COLD_PROFILE as profile} from '../install/cold-profile.mjs';
test('Node24 cold supervisor is fixed image/CLI/socketGID and exact env/mounts; helpers cannot inherit its socket',()=>{
  const spec={name:'own',nonce:'a'.repeat(32),directory:'/own/packet',expectedEnv:['NODE_ENV=production'],argv:['/own/packet/guardian.mjs','/own/packet/operator.json'],
    tmpfs:{'/data':'fixed','/tmp':'fixed'},mounts:[{source:'/own/public',target:'/own/packet',rw:true},{source:'/own/docker',target:'/usr/bin/docker',rw:false},{source:'/own/socket',target:'/run/soty-docker.sock',rw:false}]};
  const h={ReadonlyRootfs:true,NetworkMode:'none',Memory:536870912,MemorySwap:536870912,PidsLimit:128,NanoCpus:1000000000,CpuPeriod:0,CpuQuota:0,CpuShares:0,CpusetCpus:'',CpusetMems:'',Privileged:false,
    GroupAdd:['1001'],CapDrop:['ALL'],CapAdd:null,SecurityOpt:['no-new-privileges'],Devices:null,DeviceRequests:null,DeviceCgroupRules:null,Binds:null,VolumesFrom:null,PortBindings:{},PublishAllPorts:false,
    PidMode:'',IpcMode:'private',UTSMode:'',RestartPolicy:{Name:'no',MaximumRetryCount:0},LogConfig:{Type:'none',Config:{}},Ulimits:[{Name:'core',Soft:0,Hard:0}],Tmpfs:spec.tmpfs};
  const c={Id:'b'.repeat(64),Name:'/own',Image:profile.supervisorImage,Config:{User:'1000:1000',Labels:{'io.soty.source.cold-supervisor':spec.nonce},WorkingDir:spec.directory,Env:spec.expectedEnv,Entrypoint:['/usr/local/bin/node'],Cmd:spec.argv},HostConfig:h,
    Mounts:spec.mounts.map(m=>({Type:'bind',Source:m.source,Destination:m.target,RW:m.rw,Propagation:'rprivate'}))};
  assertSourceColdSupervisor([c],spec);
  for(const delta of [{Image:profile.imageId},{Config:{...c.Config,Env:['CALLER=true']}},{Config:{...c.Config,WorkingDir:'/'}},
    {Mounts:[...c.Mounts,{Type:'volume',Name:'anonymous',Destination:'/data',RW:true}]},
    ...Object.entries({GroupAdd:['0','1001'],NetworkMode:'host',Privileged:true,Devices:[{}],Tmpfs:{'/tmp':'unbounded'},LogConfig:{Type:'json-file'},NanoCpus:0}).map(([key,value])=>({HostConfig:{...h,[key]:value}}))])
    assert.throws(()=>assertSourceColdSupervisor([{...c,...delta}],spec));
});
