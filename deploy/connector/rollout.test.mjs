import test from 'node:test';
import {mkdtemp,writeFile,rm,realpath} from 'node:fs/promises';
import {tmpdir} from 'node:os';
import path from 'node:path';
import {createHash} from 'node:crypto';
import {readApprovedPolicy,policyTarget,policySetting} from './application-policy.mjs';
import assert from 'node:assert/strict';
import {Rollout,createConfig,preservationHash} from './rollout.mjs';
import {SafeError,DockerApi} from './docker-api.mjs';
import {productionMaintenance,modelReadiness} from './runtime.mjs';
const modelProxies={agentModelProxy:{ready:true,model:'DeepSeek-V4-Flash-0731',transport:'server-proxy'},applicationModelProxy:{ready:true,model:'DeepSeek-V4-Flash-0731',transport:'application-token-server-proxy',path:'/api/inference/v1/chat/completions'}};
const args={originalId:'a'.repeat(64),originalImage:'sha256:'+'d'.repeat(64),candidateImage:'sha256:'+'c'.repeat(64),revision:'e'.repeat(40),transaction:'f'.repeat(20)};
const authority=(kind='legacy',activeJobs=[])=>({schema:'soty.connector-authority.v1',kind,stateSha256:'1'.repeat(64),sourceSha256:kind==='legacy'?'2'.repeat(64):'3'.repeat(64),legacySha256:'2'.repeat(64),bytes:128,counts:{connectors:1,accessGrants:1,jobs:1,requests:0,events:2},statusCounts:activeJobs.length?{[activeJobs[0].status]:1}:{succeeded:1},activeJobs,temporaryFiles:[]});
const original=()=>({Id:args.originalId,Image:args.originalImage,Name:'/soty-online-chat',State:{Running:true,Status:'running'},Config:{Image:args.originalImage,Env:['TOKEN=synthetic-sensitive','SOTY_CODEX_SESSION=preserved','SOTY_TRAFFIC_TARGET=unchanged'],Labels:{owner:'original'},Hostname:'existing-host',User:'123:456',Cmd:['node','server/index.js'],WorkingDir:'/app',Volumes:{'/data':{}},Healthcheck:{Test:['CMD','node','probe.js']}},HostConfig:{Binds:['/synthetic/tokens:/run/tokens:ro'],Memory:1073741824,NanoCpus:1500000000,PidsLimit:180,PortBindings:{'8080/tcp':[{HostIp:'127.0.0.1',HostPort:'18182'}]},NetworkMode:'soty',RestartPolicy:{Name:'unless-stopped'},ReadonlyRootfs:false,CapDrop:['NET_RAW'],SecurityOpt:['no-new-privileges']},Mounts:[{Type:'volume',Name:'synthetic-data',Destination:'/data',RW:true}],NetworkSettings:{Networks:{soty:{IPAMConfig:null,Aliases:['preserved-alias'],DriverOpts:{},NetworkID:'runtime-only',IPAddress:'172.28.0.3'}}}});
function fixture(fault={}) {
 const map=new Map([[args.originalId,original()],['sentinel',{Id:'sentinel',State:{Running:true},Name:'/independent-task'}]]),events=[],records=[];let n=0,maintenance=false,statusCalls=0,migrated=false;
 const lookup=id=>map.get(id)||[...map.values()].find(c=>c.Name==='/'+id);
 function fail(op,when){if(fault.op===op&&fault.when===when&&!fault.used){fault.used=true;throw new SafeError(when==='after'?'engine_response_ambiguous':'injected_failure');}}
 const engine={
 async inspect(id){const c=lookup(id);if(!c)throw new SafeError('engine_http_404');return structuredClone(c);},
 async image(){return {Id:args.candidateImage,Config:{Labels:{'org.opencontainers.image.revision':args.revision}}};},
 async create(name,body){events.push('create:'+name);fail('create','before');const {HostConfig,NetworkingConfig,...Config}=structuredClone(body);const Id=(++n).toString(16).padStart(64,'0');map.set(Id,{Id,Image:body.Image,Name:'/'+name,Config,HostConfig,Mounts:[],NetworkSettings:{Networks:NetworkingConfig?.EndpointsConfig||{}},State:{Running:false,Status:'created'}});fail('create','after');return {Id};},
 async stop(id){events.push('stop:'+id);fail(id===args.originalId?'stop-old':'stop-candidate','before');Object.assign(map.get(id).State,{Running:false,Status:'exited'});fail(id===args.originalId?'stop-old':'stop-candidate','after');},
 async rename(id,name){const op=id===args.originalId?'rename-old':'rename-candidate';events.push(op);fail(op,'before');if([...map.values()].some(c=>c.Id!==id&&c.Name==='/'+name))throw new SafeError('name_conflict');map.get(id).Name='/'+name;fail(op,'after');},
 async start(id){const c=map.get(id),op=id===args.originalId?'start-old':'start-candidate';events.push(op);fail(op,'before');Object.assign(c.State,{Running:true,Status:'running'});if(c.Config.Labels?.['io.soty.connector-rollout.helper'])Object.assign(c.State,{Running:false,Status:'exited',ExitCode:0});fail(op,'after');},
 async remove(id){events.push('remove:'+id);map.delete(id);},
 async helperOutput(){return {ok:true,activeJobs:[],count:0,maintenance:false,schema:'legacy'};}
 };
 const helper=async verb=>{events.push('helper:'+verb);fail('helper-'+verb,'before');if(verb==='enter')maintenance=true;if(verb==='leave')maintenance=false;if(verb==='rollback'){if(fault.rollbackFailure)throw new SafeError('rollback_failed');migrated=false;}
 const raced=(verb==='snapshot'&&fault.race)||(verb==='status'&&fault.active);const activeJobs=raced?[{id:'raced-job',status:fault.raceStatus||'queued'}]:[];fail('helper-'+verb,'after');return {ok:true,count:activeJobs.length,activeJobs,maintenance,schema:'synthetic',...(['snapshot','enter','verify'].includes(verb)?{authority:authority(migrated?'sqlite':'legacy',activeJobs)}:{})};};
 const ready=async kind=>{events.push('ready:'+kind);if(kind==='candidate'){migrated=true;fail('readiness','before');return {ok:true,storageReady:true,maintenance:true,schema:'soty.connector-storage-ready.v1',modelProxies,applicationPolicySha256:run.args.applicationPolicy?.sha256||null};}return {ok:true,modelProxies};};
 const run=new Rollout({engine,maintenance:helper,ready,record:async s=>records.push(s),attempts:2,sleep:async()=>{}});
 return {run,engine,map,events,records,get migrated(){return migrated;}};
}
test('configuration and anonymous volume preserve exact protected values without journal disclosure',async()=>{const f=fixture();await f.run.prepare(args);const c=f.run.config;assert.deepEqual(c.Env,original().Config.Env);assert.deepEqual(c.HostConfig.PortBindings,original().HostConfig.PortBindings);assert.equal(c.HostConfig.Memory,1073741824);assert.equal(c.HostConfig.Mounts[0].Source,'synthetic-data');assert.deepEqual(c.NetworkingConfig.EndpointsConfig.soty.Aliases,['preserved-alias']);assert.doesNotMatch(JSON.stringify(f.records),/synthetic-sensitive|SOTY_CODEX|SOTY_TRAFFIC|\/synthetic\/tokens/);assert.equal(f.run.state.configurationSha256,preservationHash(c));});
test('normal success creates before stop and offline check before migration/admission',async()=>{const f=fixture();await f.run.prepare(args);await f.run.promote();assert.equal(f.run.state.phase,'committed');assert.ok(f.events.find(e=>e.startsWith('create:')));assert.ok(f.events.indexOf('helper:enter')>f.events.indexOf('stop:'+args.originalId));assert.ok(f.events.indexOf('helper:leave')>f.events.indexOf('ready:candidate'));assert.equal(f.map.get(args.originalId).State.Running,false);assert.equal(f.map.get('sentinel').State.Running,true);});
for(const op of ['create','stop-old','rename-old','rename-candidate','start-candidate'])test('applied but dropped '+op+' is reconciled without duplicate',async()=>{const f=fixture({op,when:'after'});await f.run.prepare(args);await f.run.promote();assert.equal(f.run.state.phase,'committed');assert.equal([...f.map.values()].filter(c=>c.Image===args.candidateImage).length,1);assert.equal(f.map.get('sentinel').State.Running,true);});
for(const op of ['rename-old','rename-candidate','start-candidate','readiness'])test('failure '+op+' restores same original ID',async()=>{const f=fixture({op,when:'before'});await f.run.prepare(args);await assert.rejects(f.run.promote());assert.equal(f.run.state.phase,'restored');assert.equal(f.map.get(args.originalId).Name,'/soty-online-chat');assert.equal(f.map.get(args.originalId).State.Running,true);assert.equal(f.map.get('sentinel').State.Running,true);assert.ok(f.events.indexOf('helper:leave')<f.events.indexOf('start-old'));});
for(const raceStatus of ['queued','leased','running','unknown'])test('offline '+raceStatus+' race is retained without old restart or migration',async()=>{const f=fixture({race:true,raceStatus});await f.run.prepare(args);await assert.rejects(f.run.promote(),/recovery_required/);assert.equal(f.run.state.phase,'recovery_required');assert.equal(f.map.get(args.originalId).State.Running,false);assert.ok(!f.events.includes('start-old'));assert.ok(!f.events.includes('helper:enter'));assert.ok(!f.events.includes('helper:rollback'));assert.equal(f.migrated,false);});
test('restore failure is explicit, never falsely restored',async()=>{const f=fixture({op:'readiness',when:'before',rollbackFailure:true});await f.run.prepare(args);await assert.rejects(f.run.promote(),/recovery_required/);assert.equal(f.run.state.phase,'recovery_required');assert.equal(f.map.get(args.originalId).State.Running,false);});
test('dropped leave reconciles marker, never rollback after reopening',async()=>{const f=fixture({op:'helper-leave',when:'after'});await f.run.prepare(args);await f.run.promote();assert.equal(f.run.state.phase,'committed');assert.ok(!f.events.includes('helper:rollback'));});
test('helper uses same mounts and Env in memory but no ports/network/capabilities',async()=>{const f=fixture();await f.run.prepare(args);const maint=productionMaintenance(f.engine,{sleep:async()=>{},maxPolls:2});const created=[];const oldCreate=f.engine.create;f.engine.create=async(n,c)=>{created.push(c);return oldCreate(n,c);};await maint('status',f.run);const c=created[0];assert.deepEqual(c.Env,original().Config.Env);assert.equal(c.HostConfig.NetworkMode,'none');assert.equal(c.HostConfig.PortBindings,undefined);assert.equal(c.HostConfig.Memory,805306368);assert.equal(c.HostConfig.Mounts[0].Source,'synthetic-data');assert.deepEqual(c.Cmd,['server/connector-maintenance.js','status']);});

test('preexisting active jobs abort without stopping original',async()=>{const f=fixture({active:true});await f.run.prepare(args);await assert.rejects(f.run.promote(),/precheck_not_quiescent/);assert.equal(f.run.state.phase,'aborted');assert.ok(!f.events.some(e=>e.startsWith('stop:')));});
test('unknown status fails closed before stop',async()=>{const f=fixture();await f.run.prepare(args);f.run.maintenance=async()=>({ok:false});await assert.rejects(f.run.promote(),/maintenance_status_invalid/);assert.equal(f.run.state.phase,'aborted');});
test('stop error with confirmed old still running is safe abort',async()=>{const f=fixture({op:'stop-old',when:'before'});await f.run.prepare(args);await assert.rejects(f.run.promote());assert.equal(f.run.state.phase,'aborted');assert.equal(f.map.get(args.originalId).State.Running,true);});
test('unresolved stop does not claim old restored while a delayed operation can still stop it',async()=>{const f=fixture();await f.run.prepare(args);f.engine.stop=async()=>{throw new SafeError('engine_response_ambiguous');};await assert.rejects(f.run.promote(),/recovery_required/);assert.equal(f.run.state.phase,'recovery_required');});
test('candidate revision mismatch makes no container mutation',async()=>{const f=fixture();f.engine.image=async()=>({Id:args.candidateImage,Config:{Labels:{}}});await assert.rejects(f.run.prepare(args),/candidate_revision_mismatch/);assert.ok(!f.events.some(e=>e.startsWith('create:')));});
test('configuration tamper refuses prepare without touching original',async()=>{const f=fixture();const create=f.engine.create;f.engine.create=async(n,c)=>create(n,{...c,Env:['tampered']});await assert.rejects(f.run.prepare(args),/candidate_configuration_mismatch/);assert.equal(f.map.get(args.originalId).State.Running,true);});
test('unresolved maintenance helper after stop retains recovery barrier',async()=>{const f=fixture();await f.run.prepare(args);const helper=f.run.maintenance;f.run.maintenance=async(v)=>{if(v==='enter')throw new SafeError('maintenance_helper_unresolved');return helper(v);};await assert.rejects(f.run.promote(),/maintenance_helper_unresolved/);assert.equal(f.run.state.phase,'recovery_required');assert.equal(f.map.get(args.originalId).State.Running,false);assert.ok(!f.events.includes('helper:rollback'));});

test('applied but dropped enter is reconciled from offline marker before candidate start',async()=>{const f=fixture({op:'helper-enter',when:'after'});await f.run.prepare(args);await f.run.promote();assert.equal(f.run.state.phase,'committed');assert.equal(f.events.filter(e=>e==='helper:enter').length,1);});


test('revision label is exact and exempt only from preservation delta',async()=>{const f=fixture();f.map.get(args.originalId).Config.Labels['org.opencontainers.image.revision']='b'.repeat(40);await f.run.prepare(args);assert.equal(f.run.candidate.Config.Labels['org.opencontainers.image.revision'],args.revision);f.map.get(f.run.candidate.Id).Config.Labels['org.opencontainers.image.revision']='b'.repeat(40);await assert.rejects(f.run.promote(),/candidate_ownership_mismatch/);assert.ok(!f.events.some(e=>e.startsWith('stop:')));});
test('candidate protected configuration drift fails before original stop',async()=>{const f=fixture();await f.run.prepare(args);f.map.get(f.run.candidate.Id).Config.Env.push('UNREVIEWED=1');await assert.rejects(f.run.promote(),/candidate_configuration_mismatch/);assert.ok(!f.events.some(e=>e.startsWith('stop:')));});
test('candidate application readiness mismatch restores before admission',async()=>{const f=fixture();await f.run.prepare(args);const ready=f.run.ready;f.run.ready=async kind=>{const r=await ready(kind);return kind==='candidate'?{...r,modelProxies:{...modelProxies,applicationModelProxy:{...modelProxies.applicationModelProxy,ready:false}}}:r;};await assert.rejects(f.run.promote(),/candidate_model_readiness_changed/);assert.equal(f.run.state.phase,'restored');});
test('health ok alone cannot hide missing or broken model proxy configuration',()=>{assert.throws(()=>modelReadiness({ok:true}),/model_readiness_failed/);assert.throws(()=>modelReadiness({...modelProxies,applicationModelProxy:{...modelProxies.applicationModelProxy,ready:false}}),/model_readiness_failed/);assert.deepEqual(modelReadiness(modelProxies),modelProxies);});

async function policyFixture(fn){const dir=await mkdtemp(path.join(await realpath(tmpdir()),'soty-reviewed-policy-'));try{const source=path.join(dir,'policy.json');const value={schema:'soty.application-model-policy.v1',applications:[{id:'kvartalufa',allowedModels:['deepseek-ai/DeepSeek-V4-Flash-0731','MiniMaxAI/MiniMax-M2.7']}]};const bytes=JSON.stringify(value);await writeFile(source,bytes);const sha256=createHash('sha256').update(bytes).digest('hex');await fn({source,sha256,value,policy:await readApprovedPolicy(source,sha256)});}finally{await rm(dir,{recursive:true,force:true});}}
test('reviewed application rule adds only one pathname env and one readonly nonsecret mount',async()=>policyFixture(async({policy})=>{const f=fixture();await f.run.prepare({...args,applicationPolicy:policy});const c=f.run.candidate;assert.deepEqual(c.Config.Env,original().Config.Env.concat(`${policySetting}=${policyTarget}`));assert.deepEqual(c.HostConfig.Mounts.at(-1),{Type:'bind',Source:policy.source,Target:policyTarget,ReadOnly:true});assert.equal(f.run.state.applicationPolicySha256,policy.sha256);await f.run.promote();assert.equal(f.run.state.phase,'committed');assert.deepEqual(f.map.get(args.originalId).Config.Env,original().Config.Env);}));
test('changed reviewed policy hash fails before original stop',async()=>policyFixture(async({source,policy})=>{const f=fixture();await f.run.prepare({...args,applicationPolicy:policy});await writeFile(source,'{}');await assert.rejects(f.run.promote(),/policy_hash_mismatch/);assert.ok(!f.events.some(e=>e.startsWith('stop:')));}));
test('policy for another application is rejected even with its exact byte hash',async()=>policyFixture(async({source,value})=>{value.applications[0].id='other';const bytes=JSON.stringify(value);await writeFile(source,bytes);await assert.rejects(readApprovedPolicy(source,createHash('sha256').update(bytes).digest('hex')),/policy_scope_mismatch/);}));
test('policy downgrade before admission restores same original without added env or mount',async()=>policyFixture(async({policy})=>{const f=fixture({op:'readiness',when:'before'});await f.run.prepare({...args,applicationPolicy:policy});await assert.rejects(f.run.promote());assert.equal(f.run.state.phase,'restored');assert.deepEqual(f.map.get(args.originalId).Config.Env,original().Config.Env);assert.equal(f.map.get(args.originalId).HostConfig.Mounts,undefined);}));

test('proven Docker null OOM default and mirrored primary MAC normalize without hiding drift',async()=>{const f=fixture();const old=f.map.get(args.originalId);old.HostConfig.OomKillDisable=null;old.NetworkSettings.Networks.soty.MacAddress='12:58:0b:53:88:ec';const create=f.engine.create;f.engine.create=async(n,c)=>create(n,{...c,MacAddress:c.NetworkingConfig.EndpointsConfig.soty.MacAddress,HostConfig:{...c.HostConfig,OomKillDisable:false}});await f.run.prepare(args);await f.run.promote();assert.equal(f.run.state.phase,'committed');});
test('nondefault OOM or disagreeing MAC drift is rejected before stop',async()=>{for(const field of ['oom','mac']){const f=fixture();f.map.get(args.originalId).NetworkSettings.Networks.soty.MacAddress='12:58:0b:53:88:ec';await f.run.prepare(args);const c=f.map.get(f.run.candidate.Id);if(field==='oom')c.HostConfig.OomKillDisable=true;else c.Config.MacAddress='12:58:0b:53:88:ff';await assert.rejects(f.run.promote(),/candidate_configuration_mismatch/);assert.ok(!f.events.some(e=>e.startsWith('stop:')));}});
test('late policy path hash drift after startup restores original before leave',async()=>policyFixture(async({source,policy})=>{const f=fixture();await f.run.prepare({...args,applicationPolicy:policy});const ready=f.run.ready;f.run.ready=async kind=>{const result=await ready(kind);if(kind==='candidate')await writeFile(source,'{}');return result;};await assert.rejects(f.run.promote(),/policy_hash_mismatch/);assert.equal(f.run.state.phase,'restored');}));
test('startup-loaded policy digest must match receipt even when host path is correct',async()=>policyFixture(async({policy})=>{const f=fixture();await f.run.prepare({...args,applicationPolicy:policy});const ready=f.run.ready;f.run.ready=async kind=>{const result=await ready(kind);return kind==='candidate'?{...result,applicationPolicySha256:'0'.repeat(64)}:result;};await assert.rejects(f.run.promote(),/candidate_loaded_policy_mismatch/);assert.equal(f.run.state.phase,'restored');}));

test('late helper CREATE after repeated404 is reconciled once by exact name',async()=>{
 const f=fixture();await f.run.prepare(args);const create=f.engine.create;let pending,creates=0,polls=0;
 f.engine.create=async(name,body)=>{creates++;pending={name,body};throw new SafeError('engine_response_ambiguous');};
 const maint=productionMaintenance(f.engine,{maxPolls:5,sleep:async()=>{if(++polls===3)await create(pending.name,pending.body);}});
 await maint('enter',f.run);
 assert.equal(creates,1);assert.equal(polls,3);
 assert.equal(f.events.filter(e=>e==='start-candidate').length,1);
 assert.equal(f.run.state.maintenanceHelper.state,'removed');
 assert.equal(f.run.state.maintenanceHelper.createCode,'engine_response_ambiguous');
 assert.doesNotMatch(JSON.stringify(f.records),/synthetic-sensitive|SOTY_CODEX|SOTY_TRAFFIC/);
});
test('unresolved helper CREATE retains exact pending name and never starts or retries',async()=>{
 const f=fixture();await f.run.prepare(args);let creates=0;
 f.engine.create=async()=>{creates++;throw new SafeError('engine_response_ambiguous');};
 const maint=productionMaintenance(f.engine,{maxPolls:3,sleep:async()=>{}});
 await assert.rejects(maint('enter',f.run),/maintenance_helper_unresolved/);
 assert.equal(creates,1);assert.equal(f.run.state.maintenanceHelper.state,'create_unresolved');
 assert.equal(f.run.state.maintenanceHelper.name,`soty-connector-helper-${args.transaction}-1`);
 assert.ok(!f.events.some(e=>e.startsWith('start:')||e==='start-candidate'));
});
test('wrong helper identity after delayed CREATE is never started',async()=>{
 const f=fixture();await f.run.prepare(args);const create=f.engine.create;
 f.engine.create=async(name,body)=>create(name,{...body,Image:args.originalImage});
 await assert.rejects(productionMaintenance(f.engine,{maxPolls:2,sleep:async()=>{}})('enter',f.run),/maintenance_helper_identity/);
 assert.ok(!f.events.includes('start-candidate'));
});
test('retained previously executed helper cannot be restarted on CREATE conflict',async()=>{
 const f=fixture();await f.run.prepare(args);const create=f.engine.create;
 f.engine.create=async(name,body)=>{const c=await create(name,body);Object.assign(f.map.get(c.Id).State,{Status:'exited',ExitCode:0});throw new SafeError('engine_http_409');};
 await assert.rejects(productionMaintenance(f.engine,{maxPolls:2,sleep:async()=>{}})('rollback',f.run),/maintenance_helper_unresolved/);
 assert.equal(f.run.state.maintenanceHelper.state,'prior_execution_unresolved');
 assert.ok(!f.events.includes('start-candidate'));
});
test('ambiguous helper START with no terminal proof remains fenced and retained',async()=>{
 const f=fixture();await f.run.prepare(args);let starts=0;
 f.engine.start=async()=>{starts++;throw new SafeError('engine_response_ambiguous');};
 await assert.rejects(productionMaintenance(f.engine,{maxPolls:2,sleep:async()=>{}})('enter',f.run),/maintenance_helper_unresolved/);
 assert.equal(starts,1);assert.equal(f.run.state.maintenanceHelper.state,'starting');
 assert.ok(f.map.has(f.run.state.maintenanceHelper.id));
});
test('unresolved leave helper never starts a concurrent status helper or downgrade',async()=>{
 const f=fixture();await f.run.prepare(args);const helper=f.run.maintenance;let leaving=false;
 f.run.maintenance=async verb=>{if(verb==='leave'){leaving=true;throw new SafeError('maintenance_helper_unresolved');}assert.equal(leaving,false);return helper(verb);};
 await assert.rejects(f.run.promote(),/maintenance_helper_unresolved/);
 assert.equal(f.run.state.phase,'recovery_required');assert.ok(!f.events.includes('helper:rollback'));
});

test('legacy pending-write diagnostic is not an admission prerequisite after proven stopped snapshot',async()=>{
 const f=fixture();await f.run.prepare(args);f.run.state.legacyNoPendingWritesObserved=false;
 await f.run.promote();assert.equal(f.run.state.phase,'committed');
 assert.ok(f.events.indexOf('helper:snapshot')>f.events.indexOf('stop:'+args.originalId));
 assert.equal(f.run.state.offlineAuthority.stateSha256,f.run.state.migratedAuthority.stateSha256);
});
test('malformed offline snapshot retains stopped original and does not enter or restore',async()=>{
 const f=fixture();await f.run.prepare(args);const helper=f.run.maintenance;
 f.run.maintenance=async verb=>verb==='snapshot'?{ok:false}:helper(verb);
 await assert.rejects(f.run.promote(),/recovery_required/);assert.equal(f.map.get(args.originalId).State.Running,false);
 assert.ok(!f.events.includes('start-old'));assert.ok(!f.events.includes('helper:enter'));
});
test('candidate source linkage mismatch prevents admission and verifies full legacy restoration',async()=>{
 const f=fixture();await f.run.prepare(args);const helper=f.run.maintenance;
 f.run.maintenance=async verb=>{const s=await helper(verb);if(verb==='verify'&&s.authority?.kind==='sqlite')s.authority.legacySha256='5'.repeat(64);return s;};
 await assert.rejects(f.run.promote(),/import_authority_mismatch/);assert.equal(f.run.state.phase,'restored');
 assert.equal(f.events.filter(v=>v==='helper:leave').length,1);assert.ok(f.events.indexOf('helper:leave')<f.events.indexOf('start-old'));
});
test('changed full state cannot be disguised by successful rollback status',async()=>{
 const f=fixture({op:'readiness',when:'before'});await f.run.prepare(args);const helper=f.run.maintenance;let snapshots=0;
 f.run.maintenance=async verb=>{const s=await helper(verb);if(verb==='snapshot'&&++snapshots===2)s.authority.stateSha256='6'.repeat(64);return s;};
 await assert.rejects(f.run.promote(),/recovery_required/);assert.equal(f.run.state.phase,'recovery_required');
 assert.equal(f.map.get(args.originalId).State.Running,false);assert.ok(!f.events.includes('start-old'));assert.ok(!f.events.includes('helper:leave'));
});
test('unresolved verification helper retains candidate maintenance and never starts rollback helper',async()=>{
 const f=fixture();await f.run.prepare(args);const helper=f.run.maintenance;
 f.run.maintenance=async verb=>{if(verb==='verify')throw new SafeError('maintenance_helper_unresolved');return helper(verb);};
 await assert.rejects(f.run.promote(),/maintenance_helper_unresolved/);assert.equal(f.run.state.phase,'recovery_required');
 assert.ok(!f.events.includes('helper:rollback'));assert.ok(!f.events.includes('helper:leave'));
});
test('legacy restoration never restarts PID1 over retained unacknowledged next evidence',async()=>{
 const f=fixture({op:'readiness',when:'before'});await f.run.prepare(args);const helper=f.run.maintenance;
 f.run.maintenance=async verb=>{const s=await helper(verb);if(s.authority)s.authority.temporaryFiles=[{name:'connector-store.json.1.next',bytes:123,sha256:'7'.repeat(64)}];return s;};
 await assert.rejects(f.run.promote(),/recovery_required/);assert.equal(f.run.state.phase,'recovery_required');
 assert.equal(f.map.get(args.originalId).State.Running,false);assert.ok(!f.events.includes('start-old'));assert.ok(!f.events.includes('helper:rollback'));assert.ok(!f.events.includes('helper:leave'));
});

async function stoppedFixture(extraArgs={}) {
 const f=fixture();f.engine.helpers=async()=>[];
 const old=f.map.get(args.originalId);old.NetworkSettings.Networks.soty.MacAddress='ee:c6:bd:b0:d3:f3';
 await f.run.prepare({...args,...extraArgs});const prepared=structuredClone(f.run.state);
 Object.assign(old.State,{Running:false,Status:'exited',FinishedAt:'2026-09-08T21:21:29.035097821Z',ExitCode:137,OOMKilled:false});old.NetworkSettings.Networks.soty.MacAddress='';
 f.map.get(f.run.candidate.Id).State.StartedAt='0001-01-01T00:00:00Z';
 const binding={failedJournalSha256:'8'.repeat(64),preparedJournalSha256:'9'.repeat(64)};
 const failed={...prepared,phase:'recovery_required',failureCode:'offline_authority_unresolved',maintenanceHelper:{name:`soty-connector-helper-${args.transaction}-1`,id:'7'.repeat(64),verb:'status',state:'removed'}};
 const approval={...prepared,...binding,schema:'soty.controller-stop-resume.v1',approved:true,migrationContract:'soty.stopped-legacy-snapshot.v1',originalPolicySha256:null,originalStop:{finishedAt:old.State.FinishedAt,exitCode:137},expectedAuthority:authority()};
 f.events.length=0;
 return {...f,failed,prepared,binding,approval,resume:()=>f.run.resumeAfterStop({...args,...extraArgs},failed,prepared,approval,binding)};
}
test('explicit stopped resume performs no original readiness/STOP and preserves failed receipt',async()=>{
 const f=await stoppedFixture();const prior=structuredClone(f.failed);await f.resume();assert.equal(f.run.state.phase,'committed');assert.deepEqual(f.failed,prior);
 assert.ok(!f.events.includes('ready:original'));assert.ok(!f.events.some(e=>e.startsWith('stop:')));assert.equal(f.events.filter(e=>e==='helper:snapshot').length,1);assert.ok(f.events.indexOf('helper:snapshot')<f.events.indexOf('helper:enter'));assert.equal(f.run.state.resumeFrom.failedJournalSha256,f.binding.failedJournalSha256);
});
for(const field of ['phase','helper','candidateStarted','finishedAt','env','mac','fingerprint','binding','policy','pendingHelper'])test('resume rejects '+field+' without any write helper/start/STOP',async()=>{
 const f=await stoppedFixture();const old=f.map.get(args.originalId),candidate=f.map.get(f.approval.candidateId);
 if(field==='phase')f.failed.phase='prepared';if(field==='helper')f.failed.maintenanceHelper.verb='enter';if(field==='candidateStarted')candidate.State.StartedAt='2026-09-08T20:00:00Z';if(field==='finishedAt')old.State.FinishedAt='2026-09-08T21:21:30Z';if(field==='env')old.Config.Env.push('DRIFT=1');if(field==='mac')old.NetworkSettings.Networks.soty.MacAddress='ee:c6:bd:b0:d3:ff';if(field==='fingerprint')candidate.HostConfig.Memory++;if(field==='binding')f.approval.failedJournalSha256='0'.repeat(64);if(field==='policy')f.approval.applicationPolicySha256='0'.repeat(64);if(field==='pendingHelper')f.engine.helpers=async()=>[{Id:'stray'}];
 await assert.rejects(f.resume());assert.ok(!f.events.some(e=>e.startsWith('helper:')||e.startsWith('stop:')||e.startsWith('start-')||e.startsWith('rename-')));
});
for(const status of ['queued','leased','running','unknown','interrupted'])test('resume fresh '+status+' canonical snapshot stays fenced without old restart',async()=>{
 const f=await stoppedFixture();const helper=f.run.maintenance;f.run.maintenance=async(v,r)=>v==='snapshot'?{ok:true,count:1,activeJobs:[{id:'race',status}],maintenance:false,authority:authority('legacy',[{id:'race',status}])}:helper(v,r);
 await assert.rejects(f.resume(),/recovery_required/);assert.ok(!f.events.includes('helper:enter'));assert.ok(!f.events.includes('start-old'));assert.equal(f.run.state.failureCode,'offline_authority_unresolved');
});
for(const defect of ['source','state','maintenance','sqlite','next'])test('resume stale '+defect+' authority cannot migrate or restore old',async()=>{
 const f=await stoppedFixture();const helper=f.run.maintenance;f.run.maintenance=async(v,r)=>{const out=await helper(v,r);if(v==='snapshot'){if(defect==='source')out.authority.legacySha256=out.authority.sourceSha256='0'.repeat(64);if(defect==='state')out.authority.stateSha256='0'.repeat(64);if(defect==='maintenance')out.maintenance=true;if(defect==='sqlite')out.authority.kind='sqlite';if(defect==='next')out.authority.temporaryFiles=[{name:'connector-store.json.1.next',bytes:3,sha256:'a'.repeat(64)}];}return out;};
 await assert.rejects(f.resume(),/recovery_required/);assert.ok(!f.events.includes('helper:enter'));assert.ok(!f.events.includes('start-old'));
});
test('resume actual approved policy still checks startup digest',async()=>policyFixture(async({policy})=>{
 const f=await stoppedFixture({applicationPolicy:policy});const ready=f.run.ready;f.run.ready=async kind=>{const out=await ready(kind);return kind==='candidate'?{...out,applicationPolicySha256:'0'.repeat(64)}:out;};await assert.rejects(f.resume(),/candidate_loaded_policy_mismatch/);assert.equal(f.run.state.phase,'restored');assert.ok(!f.events.includes('stop:'+args.originalId));
}));
test('resume retained next blocks automatic restoration and keeps frozen candidate',async()=>{
 const f=await stoppedFixture();const next={name:'connector-store.json.1.next',bytes:3,sha256:'a'.repeat(64)};f.approval.expectedAuthority.temporaryFiles=[next];const helper=f.run.maintenance;f.run.maintenance=async(v,r)=>{const out=await helper(v,r);if(out.authority)out.authority.temporaryFiles=[next];return out;};const ready=f.run.ready;f.run.ready=async kind=>{const out=await ready(kind);if(kind==='candidate')throw new SafeError('synthetic_readiness');return out;};await assert.rejects(f.resume(),/recovery_required/);assert.equal(f.run.state.failureCode,'legacy_restart_would_overwrite_unacknowledged_evidence');assert.ok(!f.events.includes('start-old'));assert.ok(!f.events.includes('helper:rollback'));assert.equal(f.map.get(f.approval.candidateId).State.Running,true);
});
test('resumed helper sequence starts after recorded status helper',async()=>{
 const f=await stoppedFixture();f.run.state=f.failed;await productionMaintenance(f.engine,{sleep:async()=>{}})('snapshot',f.run);assert.equal(f.run.state.maintenanceHelper.name,`soty-connector-helper-${args.transaction}-2`);
});
test('standard STOP gets bounded grace margin without increasing other API deadlines',async()=>{
 const api=new DockerApi();const calls=[];api.request=async(...a)=>calls.push(a);await api.stop(args.originalId);await api.inspect(args.originalId);assert.equal(calls[0][1],`/containers/${args.originalId}/stop?t=10`);assert.equal(calls[0][4],20000);assert.equal(calls[1][4],undefined);assert.equal(api.timeoutMs,10000);
});
test('late observed original STOP reconciles beyond four polls without second STOP',async()=>{
 const f=fixture();await f.run.prepare(args);let stopCalls=0,polls=0;f.engine.stop=async()=>{stopCalls++;throw new SafeError('engine_response_ambiguous');};const inspect=f.engine.inspect;f.engine.inspect=async id=>{if(id===args.originalId&&stopCalls&&++polls===6)Object.assign(f.map.get(id).State,{Running:false,Status:'exited'});return inspect(id);};await f.run.promote();assert.equal(f.run.state.phase,'committed');assert.equal(stopCalls,1);assert.ok(polls>=6);
});
