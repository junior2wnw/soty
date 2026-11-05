import test from 'node:test';
import assert from 'node:assert/strict';
import {createServer} from 'node:http';
import {createHash,randomBytes} from 'node:crypto';
import {mkdtempSync,rmSync} from 'node:fs';
import {tmpdir} from 'node:os';
import {join,resolve,dirname} from 'node:path';
import WebSocket from 'ws';
import {createAppsService} from '../../modules/apps/server/index.mjs';
import {createWorldService} from '../../modules/world/server/index.mjs';
import {createFieldPersistence} from './field-persistence.ts';
import {createAppFieldPlacementController} from './app-field-placement.mjs';
import {createAppConnectJournal,createAppConnectFlow,parseAppConnectAddress} from './app-connect-flow.mjs';
import {createAppConnectJournal as createLegacyJournal} from './test-fixtures/app-connect-journal-v2.mjs';

const actor={accountId:'creator-connection-owner',deviceId:'creator-browser-device',label:'Creator fixture'},host={linkId:'creator-link',hostDeviceId:'creator-host',connectorId:'creator-connector'};
const copy=value=>structuredClone(value),error=code=>Object.assign(new Error(code),{code});
const input={name:'Настоящий проект',address:'localhost:3000/gallery?q=hello',...{hostDeviceId:host.hostDeviceId,connectorId:host.connectorId},audience:'private'};
const until=async check=>{const end=Date.now()+3000;while(!check()){assert.ok(Date.now()<end,'fixture did not become ready');await new Promise(resolve=>setTimeout(resolve,5));}};
function local(){const values=new Map(),queues=new Map();let fail=false;return {storage:{getItem:key=>values.get(key)??null,setItem(key,value){if(fail)throw error('test_storage_full');values.set(key,value);}},locks:{request(name,work){const job=(queues.get(name)||Promise.resolve()).catch(()=>{}).then(work);queues.set(name,job.catch(()=>{}));return job;}},fail(value){fail=value;},values};}
async function fixture(t){
  const base=resolve(tmpdir()),directory=mkdtempSync(join(base,'soty-app-connect-')),sockets=new Set(),frames=[],token=randomBytes(32).toString('base64url');let apps,connector,current=true;
  const server=createServer((req,res)=>{if(!apps?.handleRequest(req,res)){res.writeHead(404);res.end();}});server.on('connection',socket=>{sockets.add(socket);socket.on('close',()=>sockets.delete(socket));});server.on('upgrade',(req,socket,head)=>{if(!apps?.handleUpgrade(req,socket,head))socket.destroy();});
  await new Promise(resolve=>server.listen(0,'127.0.0.1',resolve));const origin=`http://127.0.0.1:${server.address().port}`;
  apps=createAppsService({databasePath:join(directory,'apps.sqlite'),shellOrigins:[origin],appOriginTemplate:`http://{appId}.legacy.localhost:${server.address().port}`,namedAppZone:`http://apps.localhost:${server.address().port}`,allowAudienceMigration:true,isGroupAdmin:(accountId,id)=>accountId===actor.accountId&&id==='group-project',actorActive:value=>value?.accountId===actor.accountId&&value?.deviceId===actor.deviceId,authenticateConnector:async value=>value.token===token&&value.linkId===host.linkId&&value.deviceId===host.hostDeviceId&&value.connectorId===host.connectorId});
  const world=createWorldService({databasePath:join(directory,'world.sqlite'),projectId:'creator-connection'});
  const controllers=[];t.after(async()=>{controllers.forEach(c=>c.dispose());connector?.terminate();apps.close();world.close();for(const socket of sockets)socket.destroy();await new Promise(resolve=>server.close(resolve));assert.equal(dirname(resolve(directory)),base);assert.match(directory.split(/[\\/]/u).at(-1),/^soty-app-connect-/u);rmSync(directory,{recursive:true,force:true});});
  world.execute({actor,op:'world.profile.get'});
  connector=new WebSocket(origin.replace('http:','ws:')+'/api/apps/channel');connector.on('error',()=>{});connector.on('message',bytes=>frames.push(JSON.parse(bytes.toString())));await new Promise((resolve,reject)=>{connector.once('open',resolve);connector.once('error',reject);});
  connector.send(JSON.stringify({type:'auth',schema:'soty.apps-channel.v1',...host,token,name:'Creator fixture'}));await until(()=>frames.some(frame=>frame.type==='ready'));
  const claimCode=randomBytes(32).toString('base64url');connector.send(JSON.stringify({type:'claim',claimDigest:createHash('sha256').update(claimCode).digest('hex')}));await until(()=>frames.some(frame=>frame.type==='claim-ready'));apps.execute({op:'apps.claim',args:{hostDeviceId:host.hostDeviceId,connectorId:host.connectorId,claimCode},actor});
  const own=local(),calls=[];let fieldLocal=null,failPlacement=false,loseRegistration=false,losePublication=false,denyPublication=false,registerGate=null;
  const api={async request(op,args){calls.push({op,args:copy(args)});if(op==='apps.publication.update'&&denyPublication)throw error('test_publication_denied');if(op==='world.field.put'&&failPlacement)throw error('network_offline');const result=op.startsWith('world.')?world.execute({op,args,actor}):apps.execute({op,args,actor});if(op==='apps.register'&&registerGate)await registerGate();if(op==='apps.register'&&loseRegistration){loseRegistration=false;throw error('network_lost_ack');}if(op==='apps.publication.update'&&losePublication){losePublication=false;throw error('network_lost_ack');}return result;}};
  const journal=()=>createAppConnectJournal({accountId:actor.accountId,...own});
  const create=()=>createAppConnectFlow({api,accountId:actor.accountId,journal:journal(),...own,isCurrent:()=>current,createPlacement(appId){const persistence=createFieldPersistence({api,accountId:actor.accountId,isCurrent:()=>current,store:{async read(){return copy(fieldLocal);},async write(value,expected){if((fieldLocal?.localRevision??0)!==expected)throw error('field_local_revision_conflict');fieldLocal=copy(value);},subscribe(){return()=>{};}}});const controller=createAppFieldPlacementController({persistence,appId,accountId:actor.accountId,isCurrent:()=>current});controllers.push(controller);return controller;}});
  const inspect=appId=>apps.execute({actor,op:'apps.inspect',args:{appId}}),field=()=>world.execute({actor,op:'world.field.get',args:{expectedAccountId:actor.accountId}});
  return {create,journal,calls,inspect,field,own,api,apps,setCurrent:value=>{current=value;},loseRegistration:()=>{loseRegistration=true;},losePublication:()=>{losePublication=true;},failPlacement:value=>{failPlacement=value;},denyPublication:value=>{denyPublication=value;},gateRegister:gate=>{registerGate=gate;}};
}
const placementProof=(field,id)=>assert.equal(field.document.shortcuts.filter(s=>s.entity.kind==='app'&&s.entity.id===id).length,1,'registered app must have exactly one actual field shortcut');

test('address contract distinguishes literal self-IP intent from loopback and rejects normalized aliases/protocol/secrets',()=>{
  assert.deepEqual(parseAppConnectAddress('3000'),{port:3000,entryPath:'/',address:'http://127.0.0.1:3000/'});
  assert.equal(parseAppConnectAddress('localhost:3000/a?x=1').entryPath,'/a?x=1');
  assert.deepEqual(parseAppConnectAddress('192.168.1.2:3000/a'),{port:3000,entryPath:'/a',address:'http://192.168.1.2:3000/a',profile:'soty.relay-self-ip.v1',endpoint:{scope:'connector-self',scheme:'http',family:'IPv4',address:'192.168.1.2'}});
  for(const address of ['example.com:3000','[::1]:3000','127.1:3000','2130706433:3000','0x7f000001:3000','010.0.0.1:3000','%31%32%37.0.0.1:3000','169.254.169.254:3000'])assert.throws(()=>parseAppConnectAddress(address),{code:'app_connect_source_host_unsupported'});
  assert.throws(()=>parseAppConnectAddress('https://localhost:3000'),{code:'app_connect_source_protocol_unsupported'});
  for(const address of ['localhost:49424','localhost:80','localhost:70000','localhost:3000/#x','localhost:3000/_soty/entry','user:pass@localhost:3000','localhost:3000/?token=secret'])assert.throws(()=>parseAppConnectAddress(address));
});
test('one action registers, applies private audience and durably places the actual app; repeat/remount adds nothing',async t=>{
  const f=await fixture(t),flow=f.create(),result=await flow.run(input);placementProof(f.field(),result.app.id);assert.equal(f.inspect(result.app.id).publication.audience,'private');
  const before=f.calls.filter(c=>c.op==='apps.register').length;await f.create().run(input);placementProof(f.field(),result.app.id);assert.equal(f.calls.filter(c=>c.op==='apps.register').length,before);
});
test('Window registration returns a durable selection intent instead of creating an unrelated app shortcut',async t=>{
  const f=await fixture(t),selection={...input,audience:'public',resultKind:'selection'},flow=f.create(),result=await flow.run(selection);
  assert.equal(f.field().revision,0,'choosing an app for a widget must not create a plain app placement');
  assert.equal(result.resultKind,'selection');assert.equal(result.shortcutId,undefined);assert.equal(result.contextId,undefined);
  assert.equal(f.inspect(result.app.id).publication.audience,'public');assert.equal(f.journal().read().schema,3);assert.equal(f.journal().read().complete,false,'the UI must confirm the exact fresh chooser handoff');
  const registrations=f.calls.filter(call=>call.op==='apps.register').length,reopened=f.create(),same=await reopened.run(selection);
  assert.equal(same.app.id,result.app.id);assert.equal(f.calls.filter(call=>call.op==='apps.register').length,registrations);
  await reopened.confirmSelection(same);assert.equal(f.journal().read().complete,true);assert.equal(f.journal().read().shortcutId,undefined);assert.equal(f.field().revision,0);
});
test('a selection outcome survives a lost publication ACK and refuses a changed outcome before handoff',async t=>{
  const f=await fixture(t),selection={...input,audience:'link',resultKind:'selection'};f.losePublication();
  await assert.rejects(f.create().run(selection),{code:'network_lost_ack'});const pending=f.journal().read(),appId=pending.appId;
  assert.equal(f.field().revision,0);assert.equal(pending.intent.resultKind,'selection');
  await assert.rejects(f.create().run(input),{code:'app_connect_in_progress'});
  const reopened=f.create(),result=await reopened.run(selection);assert.equal(result.app.id,appId);assert.equal(f.field().revision,0);
  f.setCurrent(false);await assert.rejects(reopened.confirmSelection(result),{code:'field_account_changed'});assert.equal(f.journal().read().complete,false);
});
test('schema3 is explicit, old journals round-trip unchanged, and a stale reader cannot repair or consume selection state',async()=>{
  for(const legacyInput of [input,{...input,address:'192.168.1.8:3000/path'}]){
    const disk=local(),old=createLegacyJournal({accountId:actor.accountId,...disk});await old.begin(legacyInput);
    const before=[...disk.values];assert.deepEqual(createAppConnectJournal({accountId:actor.accountId,...disk}).read(),old.read());assert.deepEqual([...disk.values],before);
  }
  const disk=local(),journal=createAppConnectJournal({accountId:actor.accountId,...disk});const pending=await journal.begin({...input,resultKind:'selection'});
  assert.equal(pending.schema,3);const bytes=[...disk.values];assert.throws(()=>createLegacyJournal({accountId:actor.accountId,...disk}).read(),{code:'app_connect_journal_corrupt'});assert.deepEqual([...disk.values],bytes);
  const [key,text]=bytes[0],future=JSON.parse(text);future.schema=4;disk.values.set(key,JSON.stringify(future));const futureBytes=[...disk.values];
  assert.throws(()=>journal.read(),{code:'app_connect_journal_corrupt'});assert.deepEqual([...disk.values],futureBytes);
});
test('public and link choices produce real publication receipts and a shortcut, never just grants',async t=>{
  const f=await fixture(t);for(const [index,audience]of['public','link'].entries()){const result=await f.create().run({...input,address:`localhost:${3000+index}`,audience});const inspected=f.inspect(result.app.id);assert.equal(inspected.publication.audience,audience);assert.equal(inspected.publication.launchPolicy,'anyone');assert.equal(inspected.publication.listed,audience==='public');assert.equal(inspected.publication.activeDomainIds.length,1);placementProof(f.field(),result.app.id);}
});
test('lost registration ACK is reconciled by real list/inspection, with no second registration',async t=>{
  const f=await fixture(t);f.loseRegistration();await assert.rejects(f.create().run(input),{code:'network_lost_ack'});assert.equal(f.journal().read().appId,undefined);
  const result=await f.create().run(input);placementProof(f.field(),result.app.id);assert.equal(f.calls.filter(c=>c.op==='apps.register').length,1);assert.ok(f.calls.some(c=>c.op==='apps.list'));
});
test('lost publication ACK reuses exact durable request ID after remount, preserving the registered app',async t=>{
  const f=await fixture(t);f.losePublication();const wanted={...input,audience:'public'};await assert.rejects(f.create().run(wanted),{code:'network_lost_ack'});const result=await f.create().run(wanted);
  const publication=f.calls.filter(c=>c.op==='apps.publication.update');assert.equal(publication.length,2);assert.deepEqual(publication[0].args,publication[1].args);assert.equal(f.calls.filter(c=>c.op==='apps.register').length,1);placementProof(f.field(),result.app.id);
});
test('failed publication remains unfinished and retry neither registers twice nor claims earlier success',async t=>{
  const f=await fixture(t);f.denyPublication(true);await assert.rejects(f.create().run({...input,audience:'public'}),{code:'test_publication_denied'});const id=f.journal().read().appId;assert.equal(f.journal().read().complete,false);assert.equal(f.inspect(id).publication.listed,false);assert.equal(f.field().document.shortcuts.some(s=>s.entity.id===id),false);
  f.denyPublication(false);const result=await f.create().run({...input,audience:'public'});placementProof(f.field(),result.app.id);assert.equal(f.calls.filter(c=>c.op==='apps.register').length,1);
});
test('failed placement retains app/publication and retries the original field outbox after remount',async t=>{
  const f=await fixture(t);f.failPlacement(true);await assert.rejects(f.create().run(input),{code:'app_connect_field_pending'});assert.equal(f.journal().read().complete,false);const id=f.journal().read().appId;assert.ok(id);const first=f.calls.find(c=>c.op==='world.field.put').args.requestId;
  f.failPlacement(false);const result=await f.create().run(input);placementProof(f.field(),result.app.id);assert.equal(f.calls.filter(c=>c.op==='apps.register').length,1);assert.equal(f.calls.filter(c=>c.op==='world.field.put')[1].args.requestId,first);
});
test('account change while registration is pending settles only its old journal and performs no publication/placement',async t=>{
  const f=await fixture(t);let release,entered;const started=new Promise(resolve=>{entered=resolve;});f.gateRegister(()=>{entered();return new Promise(resolve=>{release=resolve;});});
  const operation=f.create().run({...input,audience:'public'});await started;f.setCurrent(false);release();await assert.rejects(operation,{code:'field_account_changed'});assert.ok(f.journal().read().appId);assert.equal(f.calls.some(c=>c.op==='apps.publication.update'||c.op==='world.field.put'),false);
});
test('storage failure and incompatible concurrent inputs stop before any registration',async t=>{
  const f=await fixture(t);f.own.fail(true);await assert.rejects(f.create().run(input),{code:'app_connect_storage_unavailable'});assert.equal(f.calls.length,0);f.own.fail(false);await f.journal().begin(input);await assert.rejects(f.create().run({...input,address:'localhost:4000'}),{code:'app_connect_in_progress'});assert.equal(f.calls.length,0);
});
test('same controller double submit shares one flight and registry tuple stays idempotent',async t=>{
  const f=await fixture(t),flow=f.create(),a=flow.run(input),b=flow.run(input);assert.equal(a,b);const result=await a;placementProof(f.field(),result.app.id);assert.equal(f.calls.filter(c=>c.op==='apps.register').length,1);
});
test('negative controls detect the old register-only path missing both placement and actual public policy',async t=>{
  const f=await fixture(t),registered=await f.api.request('apps.register',{expectedAccountId:actor.accountId,hostDeviceId:host.hostDeviceId,connectorId:host.connectorId,name:input.name,port:3000,entryPath:'/gallery?q=hello',grants:{accountIds:[],communityIds:[]}});
  assert.throws(()=>placementProof(f.field(),registered.app.id),/actual field shortcut/);assert.throws(()=>assert.equal(f.inspect(registered.app.id).publication.listed,true));
  const result=await f.create().run({...input,audience:'public'});placementProof(f.field(),result.app.id);assert.equal(f.inspect(result.app.id).publication.listed,true);
});
test('community entry preserves its real group grant/manual policy while explicit Only me removes that grant',async t=>{
  const f=await fixture(t),group=await f.create().run({...input,audience:'community',communityId:'group-project'}),snapshot=f.inspect(group.app.id);
  assert.deepEqual(snapshot.app.grants.communityIds,['group-project']);assert.equal(snapshot.publication.audience,null);assert.equal(snapshot.publication.launchPolicy,'restricted');assert.equal(snapshot.publication.listed,false);placementProof(f.field(),group.app.id);
  const own=await f.create().run({...input,address:'localhost:3001',audience:'private',communityId:'group-project'});assert.deepEqual(f.inspect(own.app.id).app.grants.communityIds,[]);assert.equal(f.inspect(own.app.id).publication.audience,'private');
});
test('malformed registration ACK never claims a shortcut or publication; real registry identity is recovered on retry',async t=>{
  const f=await fixture(t),request=f.api.request;let corrupt=true;
  f.api.request=async(op,args)=>{const response=await request(op,args);return op==='apps.register'&&corrupt?{app:{id:'invalid-app',name:input.name}}:response;};
  await assert.rejects(f.create().run({...input,audience:'public'}),{code:'app_connect_invalid_registration'});assert.equal(f.journal().read().complete,false);assert.equal(f.calls.some(c=>c.op==='apps.publication.update'||c.op==='world.field.put'),false);
  corrupt=false;const result=await f.create().run({...input,audience:'public'});placementProof(f.field(),result.app.id);assert.equal(f.calls.filter(c=>c.op==='apps.register').length,1);
});
