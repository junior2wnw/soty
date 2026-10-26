import test from 'node:test';
import assert from 'node:assert/strict';
import {mkdtemp,writeFile,readFile,rm,realpath,chmod,symlink} from 'node:fs/promises';
import {tmpdir} from 'node:os';
import path from 'node:path';
import {pathToFileURL,fileURLToPath} from 'node:url';
import {spawn} from 'node:child_process';
import {runConnectorCli} from './cli.mjs';
import {dockerHttpFixture} from './test-support/docker-http.mjs';
import {args} from './test-support/rollout-engine.mjs';
const cli=fileURLToPath(new URL('./cli.mjs',import.meta.url));
const tx='f'.repeat(32),key='A'.repeat(64);
async function setup(t){
 const root=await mkdtemp(path.join(await realpath(tmpdir()),'soty-universal-policy-')),f=await dockerHttpFixture(root);
 t.after(async()=>{await f.close();assert.equal(path.dirname(root),await realpath(tmpdir()));assert.match(path.basename(root),/^soty-universal-policy-/);await rm(root,{recursive:true,force:true,maxRetries:3})});
 const original=f.map.get(args.originalId);original.RestartCount=0;Object.assign(original.State,{Pid:1234,StartedAt:'2026-10-25T00:00:00.000000000Z',FinishedAt:'0001-01-01T00:00:00Z',ExitCode:0,OOMKilled:false,Paused:false,Restarting:false});
 const stop=f.engine.stop;f.engine.stop=async id=>{await stop(id);Object.assign(f.map.get(id).State,{Pid:0,ExitCode:0,FinishedAt:'2026-10-25T00:00:02.000000000Z'})};
 const start=f.engine.start;f.engine.start=async id=>{await start(id);const c=f.map.get(id);if(c.State.Running)Object.assign(c.State,{Pid:1235,StartedAt:'2026-10-25T00:00:03.000000000Z',Paused:false,Restarting:false,OOMKilled:false})};
 const journal=path.join(root,'journal.json'),approval=path.join(root,'approval.json'),planFile=path.join(root,'backup.json'),marker=path.join(root,'called.json');
 const flags=['--journal',journal,'--original-id',args.originalId,'--original-image',args.originalImage,'--candidate-image',args.candidateImage,'--storage-probe-image',args.storageProbeImage,'--revision',args.revision,'--transaction',tx,'--health-origin',f.healthOrigin,'--rollback-mode','latest-data'];
 const prepared=await runConnectorCli(['prepare',...flags,'--docker-socket',f.socket]);assert.equal(prepared.ok,true,prepared.code);
 await writeFile(approval,JSON.stringify({...prepared,approved:true,noPendingConnectorJobsObserved:true,fullStoppedWriterBackupRequired:true}),{mode:0o600});
 const plan={operation:'backup',containerId:args.originalId,image:args.storageProbeImage,operationId:tx,directory:path.join(root,'backup-output')};await writeFile(planFile,JSON.stringify(plan),{mode:0o600});
 const api=pathToFileURL(fileURLToPath(new URL('./docker-api.mjs',import.meta.url))).href,gpg=pathToFileURL(fileURLToPath(new URL('../connect/gpg-release-backup.mjs',import.meta.url))).href;
 async function child({fail=false,input=key+'\n'}={}){
  const preload=path.join(root,fail?'failure.mjs':'success.mjs');
  await writeFile(preload,`import {mock} from 'node:test';import assert from 'node:assert/strict';import fs from 'node:fs';const api=await import(${JSON.stringify(api+'?original')});const gpg=await import(${JSON.stringify(gpg+'?readers')});let held;mock.module(${JSON.stringify(api)},{namedExports:{...api,DockerApi:class extends api.DockerApi{constructor(options={}){super({...options,socketPath:${JSON.stringify(f.socket)}})}}}});mock.module(${JSON.stringify(gpg)},{namedExports:{...gpg,backupStopped:async(options,engine)=>{const old=await engine.inspect(options.containerId);assert.equal(old.State.Running,false);assert.equal(old.State.Pid,0);assert.equal(old.HostConfig.RestartPolicy.Name,'no');assert.equal(options.operationId,${JSON.stringify(tx)});assert.equal(options.key.toString(),${JSON.stringify(key)});held=options.key;fs.writeFileSync(${JSON.stringify(marker)},JSON.stringify({stopped:true,restartNo:true,keyMatched:true}));if(${fail})throw Error('synthetic backup failure');const a={sha256:'1'.repeat(64),bytes:1,entries:1,fileBytes:1,inventorySha256:'2'.repeat(64)};return {ok:true,encrypted:true,operationId:options.operationId,sourceCheckpointSha256:'3'.repeat(64),volume:a,configuration:a}}}});process.on('beforeExit',()=>{if(held){assert(held.every(b=>b===0));fs.writeFileSync(${JSON.stringify(marker)},JSON.stringify({stopped:true,restartNo:true,keyMatched:true,keyWiped:true}))}});`);
  return await new Promise((resolve,reject)=>{const c=spawn(process.execPath,['--experimental-test-module-mocks','--import',preload,cli,'promote',...flags,'--reviewed-receipt',approval,'--backup-plan',planFile],{stdio:['pipe','pipe','pipe']});let stdout='',stderr='';c.stdout.on('data',b=>stdout+=b);c.stderr.on('data',b=>stderr+=b);c.once('error',reject);c.once('close',code=>resolve({code,stdout,stderr,value:JSON.parse(stdout)}));c.stdin.end(input)});
 }
 return {root,f,flags,approval,prepared,plan,planFile,marker,child};
}
test('backup-plan is never accepted on prepare or legacy promotion',async()=>{
 for(const action of ['prepare','promote']){const r=await runConnectorCli([action,'--journal','unused','--backup-plan','not-opened']);assert.equal(r.code,'backup_plan_not_applicable')}
});
test('shell backup plan refuses malformed input before any lifecycle effect',{skip:process.platform!=='linux'},async t=>{
 const x=await setup(t),before=x.f.events.length;
 for(const mutate of [p=>p.operation='verify',p=>p.containerId='b'.repeat(64),p=>p.operationId='0'.repeat(32),p=>p.image='latest',p=>p.directory='relative',p=>p.cap={wallMs:0},p=>p.cap={unknown:1},p=>p.key='not-allowed']){
  const p=structuredClone(x.plan);mutate(p);await writeFile(x.planFile,JSON.stringify(p));const r=await x.child();assert.equal(r.value.code,'backup_plan_invalid');assert(!r.stdout.includes(key));assert.equal(x.f.events.length,before);
 }
 await writeFile(x.planFile,JSON.stringify(x.plan));await chmod(x.planFile,0o644);assert.equal((await x.child()).value.code,'backup_plan_invalid');assert.equal(x.f.events.length,before);
 await chmod(x.planFile,0o600);const target=path.join(x.root,'linked.json');await writeFile(target,JSON.stringify(x.plan),{mode:0o600});await rm(x.planFile);await symlink(target,x.planFile);assert.equal((await x.child()).value.code,'backup_plan_invalid');assert.equal(x.f.events.length,before);
});
test('invalid key, unsupported socket and conflicting callback refuse before STOP',{skip:process.platform!=='linux'},async t=>{
 const x=await setup(t),before=x.f.events.length;for(const input of ['bad\n',key,key+'X\n']){const r=await x.child({input});assert.equal(r.value.code,'backup_key_invalid');assert.equal(x.f.events.length,before);assert(!r.stdout.includes(input.trim()))}
 const flags=['promote',...x.flags,'--reviewed-receipt',x.approval,'--backup-plan',x.planFile];assert.equal((await runConnectorCli([...flags,'--docker-socket',x.f.socket])).code,'backup_socket_unsupported');assert.equal((await runConnectorCli(flags,{fullEncryptedBackup:async()=>{throw Error('not called')}})).code,'backup_plan_not_applicable');assert.equal(x.f.events.length,before);
});
for(const fail of [false,true])test(`shell wires backup only after stopped writer; ${fail?'same latest DATA recovery':'commit'} and key wipe`,{skip:process.platform!=='linux'},async t=>{
 const x=await setup(t),r=await x.child({fail});assert.equal(r.value.ok,!fail,r.stdout);assert.equal(r.code,fail?1:0);assert.equal(JSON.parse(await readFile(x.marker)).keyWiped,true);const journal=JSON.parse(await readFile(path.join(x.root,'journal.json')));assert.equal(journal.phase,fail?'restored':'committed');assert.equal(x.f.map.get(args.originalId).State.Running,fail);assert.equal(x.f.events.filter(v=>v==='stop:'+args.originalId).length,1);assert(!r.stdout.includes(key));assert(!r.stdout.includes('synthetic-sensitive'));assert(!r.stderr.includes(key));
});
