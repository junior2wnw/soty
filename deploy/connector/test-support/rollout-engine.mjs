import test from 'node:test';
import {mkdtemp,writeFile,rm,realpath} from 'node:fs/promises';
import {tmpdir} from 'node:os';
import path from 'node:path';
import {createHash} from 'node:crypto';
import {readApprovedPolicy,policyTarget,policySetting} from '../application-policy.mjs';
import assert from 'node:assert/strict';
import {Rollout,createConfig,preservationHash} from '../rollout.mjs';
import {SafeError} from '../docker-api.mjs';
import {productionMaintenance,modelReadiness} from '../runtime.mjs';
import {storageReaderLabel,currentStorageReaders} from '../storage-guard.mjs';
export const modelProxies={agentModelProxy:{ready:true,model:'DeepSeek-V4-Flash-0731',transport:'server-proxy'},applicationModelProxy:{ready:true,model:'DeepSeek-V4-Flash-0731',transport:'application-token-server-proxy',path:'/api/inference/v1/chat/completions'}};
export const args={originalId:'a'.repeat(64),originalImage:'sha256:'+'d'.repeat(64),candidateImage:'sha256:'+'c'.repeat(64),storageProbeImage:'sha256:'+'d'.repeat(64),revision:'e'.repeat(40),transaction:'f'.repeat(20)};
export const original=()=>({Id:args.originalId,Image:args.originalImage,Name:'/soty-online-chat',State:{Running:true,Status:'running'},Config:{Image:args.originalImage,Env:['DATA_DIR=/data','TOKEN=synthetic-sensitive','SOTY_CODEX_SESSION=preserved','SOTY_TRAFFIC_TARGET=unchanged'],Labels:{owner:'original'},Hostname:'existing-host',User:'123:456',Cmd:['node','server/index.js'],WorkingDir:'/app',Volumes:{'/data':{}},Healthcheck:{Test:['CMD','node','probe.js']}},HostConfig:{Binds:['/synthetic/tokens:/run/tokens:ro'],Memory:1073741824,NanoCpus:1500000000,PidsLimit:180,PortBindings:{'8080/tcp':[{HostIp:'127.0.0.1',HostPort:'18182'}]},NetworkMode:'soty',RestartPolicy:{Name:'unless-stopped'},ReadonlyRootfs:false,CapDrop:['NET_RAW'],SecurityOpt:['no-new-privileges']},Mounts:[{Type:'volume',Name:'synthetic-data',Source:'/docker/volumes/synthetic-data',Destination:'/data',RW:true}],NetworkSettings:{Networks:{soty:{IPAMConfig:null,Aliases:['preserved-alias'],DriverOpts:{},NetworkID:'runtime-only',IPAddress:'172.28.0.3'}}}});
export function fixture(fault={}) {
 const map=new Map([[args.originalId,original()],['sentinel',{Id:'sentinel',State:{Running:true},Name:'/independent-task'}]]),events=[],records=[];let n=0,maintenance=false,statusCalls=0,migrated=false;
 const lookup=id=>map.get(id)||[...map.values()].find(c=>c.Name==='/'+id);
 function fail(op,when){if(fault.op===op&&fault.when===when&&!fault.used){fault.used=true;throw new SafeError(when==='after'?'engine_response_ambiguous':'injected_failure');}}
 const engine={
 async inspect(id){const c=lookup(id);if(!c)throw new SafeError('engine_http_404');return structuredClone(c);},
 async image(key){return {Id:key,Config:{Labels:{'org.opencontainers.image.revision':args.revision,[storageReaderLabel]:currentStorageReaders}}};},
 async create(name,body){events.push('create:'+name);fail('create','before');const {HostConfig,NetworkingConfig,...Config}=structuredClone(body);Config.Labels={[storageReaderLabel]:currentStorageReaders,...Config.Labels};const Id=(++n).toString(16).padStart(64,'0');const Mounts=(HostConfig.Mounts||[]).map(m=>({Type:m.Type,Name:m.Type==='volume'?m.Source:undefined,Source:m.Type==='volume'?'/docker/volumes/'+m.Source:m.Source,Destination:m.Target,RW:!m.ReadOnly}));map.set(Id,{Id,Image:body.Image,Name:'/'+name,Config,HostConfig,Mounts,NetworkSettings:{Networks:NetworkingConfig?.EndpointsConfig||{}},State:{Running:false,Status:'created'}});fail('create','after');return {Id};},
 async request(method,route,body){if(method==='GET'&&route.startsWith('/volumes/')){const Name=decodeURIComponent(route.slice('/volumes/'.length));const mount=[...map.values()].flatMap(c=>c.Mounts||[]).find(m=>m.Name===Name);return {Name,Driver:'local',Scope:'local',Options:null,Mountpoint:mount?.Source};}if(method==='GET'&&route==='/containers/json')return structuredClone([...map.values()].filter(c=>c.State.Running));const match=route.match(/^\/containers\/([a-f0-9]{64})\/update$/);assert.equal(method,'POST');assert.ok(match);map.get(match[1]).HostConfig.RestartPolicy=structuredClone(body.RestartPolicy);events.push('policy:'+match[1]);return {};},
 async stop(id){events.push('stop:'+id);fail(id===args.originalId?'stop-old':'stop-candidate','before');Object.assign(map.get(id).State,{Running:false,Status:'exited'});fail(id===args.originalId?'stop-old':'stop-candidate','after');},
 async rename(id,name){const op=id===args.originalId?'rename-old':'rename-candidate';events.push(op);fail(op,'before');if([...map.values()].some(c=>c.Id!==id&&c.Name==='/'+name))throw new SafeError('name_conflict');map.get(id).Name='/'+name;fail(op,'after');},
 async start(id){const c=map.get(id),op=id===args.originalId?'start-old':'start-candidate';events.push(op);fail(op,'before');Object.assign(c.State,{Running:true,Status:'running'});if(c.Config.Labels?.['io.soty.connector-rollout.helper'])Object.assign(c.State,{Running:false,Status:'exited',ExitCode:0});fail(op,'after');},
 async remove(id){events.push('remove:'+id);map.delete(id);},
 async helperOutput(){return {ok:true,activeJobs:[],count:0,maintenance:false,schema:'legacy'};}
 };
 const helper=async verb=>{events.push('helper:'+verb);fail('helper-'+verb,'before');if(verb==='enter')maintenance=true;if(verb==='leave')maintenance=false;if(verb==='rollback'){if(fault.rollbackFailure)throw new SafeError('rollback_failed');migrated=false;}
 const raced=verb==='status'&&((++statusCalls===2&&fault.race)||fault.active);fail('helper-'+verb,'after');return {ok:true,count:raced?1:0,activeJobs:raced?[{id:'raced-job',status:'queued'}]:[],maintenance,schema:'synthetic'};};
 const ready=async kind=>{events.push('ready:'+kind);if(kind==='candidate'){migrated=true;fail('readiness','before');return {ok:true,storageReady:true,maintenance:true,schema:'soty.connector-storage-ready.v1',modelProxies,applicationPolicySha256:run.args.applicationPolicy?.sha256||null};}return {ok:true,modelProxies};};
 const run=new Rollout({engine,maintenance:helper,ready,storageProbe:async()=>({ok:true,schema:'soty.storage-format.v3',notes:'empty',capabilities:'empty',rooms:1,apps:'empty'}),record:async s=>records.push(s),attempts:2,sleep:async()=>{}});
 return {run,engine,map,events,records,get migrated(){return migrated;}};
}

