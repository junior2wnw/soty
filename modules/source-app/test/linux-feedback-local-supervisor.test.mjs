import test from 'node:test';
import assert from 'node:assert/strict';
import {assertSupervisorContainer} from '../../../scripts/fixtures/linux-feedback-local-supervisor.mjs';
import {LOCAL_LINUX_FEEDBACK_PLACEMENT as placement} from '../server/linux-feedback-local-placement.mjs';

test('NEW local supervisor guard enforces exact socket-only mount/GID/runtime; import never starts the local daemon',()=>{
  const spec={name:'own',image:'sha256:'+'1'.repeat(64),label:'2'.repeat(64),command:['/probe/fixture.mjs'],groups:['1001'],memory:536870912,pids:128,
    tmpfs:{'/tmp':'bounded','/data':'bounded'},mounts:[{source:'/public',target:'/probe',rw:false},{source:placement.socketHost,target:placement.socketContainer,rw:false}]};
  const actual={id:'3'.repeat(64),name:'/own',image:spec.image,labels:{'io.soty.feedback.supervisor':spec.label},user:'1000:1000',workdir:'/probe',
    entrypoint:['/usr/local/bin/node'],cmd:spec.command,groups:['1001'],readonly:true,network:'none',memory:spec.memory,swap:spec.memory,pids:spec.pids,
    nanoCpus:1000000000,cpuPeriod:0,cpuQuota:0,cpuShares:0,cpusetCpus:'',cpusetMems:'',ulimits:[{Name:'core',Soft:0,Hard:0}],ports:{},publishPorts:false,
    pidMode:'',ipcMode:'private',utsMode:'',caps:['ALL'],capAdd:null,security:['no-new-privileges'],privileged:false,devices:null,requests:null,rules:null,
    log:{Type:'none',Config:{}},restart:{Name:'no',MaximumRetryCount:0},tmpfs:{'/data':'bounded','/tmp':'bounded'},
    mounts:spec.mounts.map(m=>({Type:'bind',Source:m.source,Destination:m.target,RW:m.rw,Propagation:'rprivate'})),tmpdir:true,state:'created'};
  assertSupervisorContainer(actual,spec);
  for(const delta of [{image:'sha256:'+'4'.repeat(64)},{groups:['0','1001']},{groups:null},{network:'host'},{privileged:true},{devices:[{}]},
    {memory:1073741824},{nanoCpus:0},{ulimits:[]},{tmpdir:false},{log:{Type:'json-file',Config:{}}},{cmd:['/bin/sh']},
    {mounts:[...actual.mounts,{Type:'bind',Source:'/entire-lab',Destination:'/lab',RW:true,Propagation:'rprivate'}]},
    {mounts:actual.mounts.map(m=>({...m,RW:true}))}])assert.throws(()=>assertSupervisorContainer({...actual,...delta},spec));
  assertSupervisorContainer({...actual,groups:null},{...spec,groups:[]});
});
