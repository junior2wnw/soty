import test from 'node:test';
import assert from 'node:assert/strict';
import {assertSourceColdContainer} from '../install/cold-runner.mjs';
test('physical Source cold guard refuses foreign image/volume/socket/device/command/writer scope before START',()=>{
  const item={id:'a'.repeat(64),name:'own',image:'sha256:'+'b'.repeat(64),nonce:'c'.repeat(32),volume:'own-volume',target:'/data',rw:true,packet:'/own/packet',argv:['/probe/fixture.mjs','seed'],expectedEnv:['NODE_ENV=production'],tmpfs:{'/tmp':'fixed-tmp','/data':'fixed-data'}};
  const host={ReadonlyRootfs:true,NetworkMode:'none',Privileged:false,Memory:268435456,MemorySwap:268435456,PidsLimit:64,
    NanoCpus:1000000000,CpuPeriod:0,CpuQuota:0,CpuShares:0,CpusetCpus:'',CpusetMems:'',CapDrop:['ALL'],CapAdd:null,SecurityOpt:['no-new-privileges'],
    Devices:null,DeviceRequests:null,DeviceCgroupRules:null,Binds:null,VolumesFrom:null,PortBindings:{},PublishAllPorts:false,PidMode:'',IpcMode:'private',UTSMode:'',
    RestartPolicy:{Name:'no',MaximumRetryCount:0},LogConfig:{Type:'none',Config:{}},Ulimits:[{Name:'core',Soft:0,Hard:0}],GroupAdd:null,Tmpfs:item.tmpfs,Mounts:[{Type:'volume',Source:item.volume,Target:item.target}]};
  const value={Id:item.id,Name:'/own',Image:item.image,Config:{Labels:{'io.soty.source.cold':item.nonce},User:'1000:1000',WorkingDir:'/app/source-app',Env:item.expectedEnv,Entrypoint:['/usr/local/bin/node'],Cmd:item.argv},HostConfig:host,
    Mounts:[{Type:'volume',Name:item.volume,Destination:'/data',RW:true},{Type:'bind',Source:item.packet,Destination:'/probe',RW:false,Propagation:'rprivate'}]};
  assertSourceColdContainer(item,[value]);
  const dockerHost={...host,Tmpfs:{'/data':'fixed-data','/tmp':'fixed-tmp'},Ulimits:[{Name:'core',Hard:0,Soft:0}]};
  assertSourceColdContainer(item,[{...value,HostConfig:dockerHost}]);
  assert.throws(()=>assertSourceColdContainer(item,[{...value,HostConfig:{...dockerHost,Tmpfs:{...dockerHost.Tmpfs,'/extra':'fixed'}}}]));
  assert.throws(()=>assertSourceColdContainer(item,[{...value,HostConfig:{...dockerHost,Ulimits:[{Name:'core',Hard:1,Soft:0}]}}]));
  assert.throws(()=>assertSourceColdContainer(item,[{...value,HostConfig:{...dockerHost,Ulimits:[{Name:'core',Hard:0,Soft:0,Extra:0}]}}]));
  assert.throws(()=>assertSourceColdContainer(item,[{...value,Config:{...value.Config,Cmd:[...item.argv].reverse()}}]));
  assert.throws(()=>assertSourceColdContainer({...item,expectedEnv:['A=1','B=2']},[{...value,Config:{...value.Config,Env:['B=2','A=1']}}]));
  for(const delta of [{Image:'sha256:'+'d'.repeat(64)},{Name:'/foreign'},{Id:'e'.repeat(64)},
    {Config:{...value.Config,Cmd:['/bin/sh']}},{Config:{...value.Config,Env:['NODE_ENV=caller']}},{Config:{...value.Config,WorkingDir:'/foreign'}},
    {Mounts:[...value.Mounts,{Type:'tmpfs',Destination:'/extra',RW:true}]},{Mounts:[...value.Mounts,{Type:'volume',Name:'anonymous',Destination:'/more',RW:true}]},
    {Mounts:value.Mounts.map(m=>({...m,Name:'foreign'}))},...Object.entries({NetworkMode:'host',Privileged:true,Memory:536870912,NanoCpus:0,
      Devices:[{}],CapAdd:['SYS_ADMIN'],PortBindings:{'8080/tcp':[{}]},PidMode:'host',LogConfig:{Type:'json-file'},VolumesFrom:['foreign'],GroupAdd:['0'],Tmpfs:{'/extra':'unbounded'},
      Mounts:[{Type:'volume',VolumeOptions:{Subpath:'foreign'}}]}).map(([key,field])=>({HostConfig:{...host,[key]:field}}))])
    assert.throws(()=>assertSourceColdContainer(item,[{...value,...delta}]));
  assertSourceColdContainer({...item,id:null},[value],'unknown CREATE permits only exact knownname/spec recovery for cleanup');
});
