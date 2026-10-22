import test from 'node:test';
import assert from 'node:assert/strict';
import { createServer } from 'node:http';
import { createECDH,generateKeyPairSync,randomBytes,sign } from 'node:crypto';
import { mkdtemp,rm } from 'node:fs/promises';
import { existsSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { join,dirname,resolve } from 'node:path';
import { createRequire } from 'node:module';
import { EventEmitter } from 'node:events';
import { DatabaseSync } from 'node:sqlite';
import webPush from 'web-push';
import { createConnectService,digestArgs } from '../../modules/connect/server/index.mjs';
import { createConnectHandler } from '../../modules/connect/server/http.mjs';
import { createWorldService } from '../../modules/world/server/index.mjs';
import { createPushService,validatePushSubscription } from '../push-service.js';
import { createPushTransport } from '../push-transport.js';
import { createPushStore } from '../push-store.js';

const ece=createRequire(import.meta.resolve('web-push'))('http_ece');
const event={kind:'room',conversationId:'room_owned',messageId:'message_owned'};
const defer=()=>{let resolve,reject;const promise=new Promise((yes,no)=>{resolve=yes;reject=no;});return{promise,resolve,reject};};
function identity(){const pair=generateKeyPairSync('ec',{namedCurve:'prime256v1'});return{key:pair.privateKey,jwk:pair.publicKey.export({format:'jwk'})};}
function subscription(){const dh=createECDH('prime256v1');dh.generateKeys();const auth=randomBytes(16).toString('base64url');
  return{dh,auth,value:{endpoint:'https://fcm.googleapis.com/fcm/send/'+randomBytes(24).toString('base64url'),expirationTime:null,keys:{p256dh:dh.getPublicKey().toString('base64url'),auth}}};}
async function fixture(t,{enabled=true,send,actualWorld=false}={}){
  const directory=await mkdtemp(join(tmpdir(),'soty-push-owned-')),pushFile=join(directory,'push','push.sqlite'),configuration={enabled,...webPush.generateVAPIDKeys(),subject:'mailto:fixture@example.invalid'};
  let at=1000,connect,push,world,allowed=true,muted=false,accounts=[],pushPath=pushFile;const delivered=[],listeners=new Set();
  const server=createServer((req,res)=>void createConnectHandler(connect)(req,res));await new Promise(done=>server.listen(0,'127.0.0.1',done));
  const origin='http://127.0.0.1:'+server.address().port;
  const create=()=>{
    if(actualWorld)world=createWorldService({databasePath:join(directory,'world.sqlite'),projectId:'soty',clock:()=>at,
      withCurrentContacts:(actor,callback)=>connect.withCurrentContacts(actor,callback)});
    push=createPushService({databasePath:pushPath,configuration,getConnect:()=>connect,clock:()=>at,
      recipientsFor:(value,page)=>actualWorld?world.notificationRecipients(value,page):accounts.slice().sort().filter(account=>account>page.afterAccountId).slice(0,page.limit),
      authorizeDelivery:(actor,value)=>actualWorld?world.canNotify(actor,value):allowed&&!muted?(value.kind==='room'?{kind:'room',communityId:value.conversationId}:{kind:'direct',threadId:value.conversationId,peerAccountId:'peer_owned',relationshipId:'relation_owned'}):null,
      authorizeOpen:(actor,value)=>actualWorld?world.notificationTarget(actor,value):allowed?(value.kind==='room'?{kind:'room',communityId:value.conversationId}:{kind:'direct',threadId:value.conversationId,peerAccountId:'peer_owned',relationshipId:'relation_owned'}):null,
      sendNotification:async(sub,payload)=>{delivered.push({subscription:sub,payload:JSON.parse(payload)});return send?.(sub,payload);}});
    connect=createConnectService({databasePath:join(directory,'connect.sqlite'),projectId:'soty',allowedOrigins:[origin],extensions:[...(actualWorld?[world]:[]),push],
      ...(actualWorld?{canRequestContact:(left,right)=>world.canRequestContact(left,right)}:{})});
    push.attach({subscribeMessages:listener=>actualWorld?world.subscribeMessages(listener):(listeners.add(listener),()=>listeners.delete(listener))});
  };
  create();
  const post=body=>fetch(origin+'/api/connect/rpc',{method:'POST',headers:{Origin:origin,'content-type':'application/json'},body:JSON.stringify({protocol:1,...body})}).then(res=>res.json());
  const rpc=async(actor,op,args={})=>{const challenge=await post({op:'challenge',args:{operation:op,digest:digestArgs(args)}});assert.equal(challenge.ok,true);
    return post({op,args,proof:{challengeId:challenge.challengeId,publicJwk:actor.jwk,signature:sign('sha256',Buffer.from(challenge.message),{key:actor.key,dsaEncoding:'ieee-p1363'}).toString('base64url')}});};
  const alice=identity(),bob=identity();for(const actor of [alice,bob]){const result=await rpc(actor,'bootstrap',{label:'Isolated push fixture',encryptionPublicJwk:actor.jwk});assert.equal(result.ok,true);Object.assign(actor,result);}
  accounts=[alice.accountId];
  const own=(actor,op,args={})=>rpc(actor,op,{expectedAccountId:actor.accountId,...args});
  t.after(async()=>{await push.close();world?.close();connect.close();server.closeAllConnections();await new Promise(done=>server.close(done));
    assert.equal(dirname(resolve(directory)),resolve(tmpdir()));assert.match(directory,/soty-push-owned-/u);await rm(directory,{recursive:true,force:true,maxRetries:3});});
  return{alice,bob,rpc,own,configuration,directory,pushFile,delivered,get push(){return push;},post,
    emit(value=event){for(const listener of listeners)listener(value);},at(value){at=value;},allow(value){allowed=value;},mute(value){muted=value;},recipients(values){accounts=values;},
    async restart(path=pushFile){await push.close();world?.close();connect.close();pushPath=path;create();},
  };
}

test('default OFF opens no push database, advertises no key and cannot dispatch or accept a subscription',async t=>{
  const f=await fixture(t,{enabled:false});assert.equal(existsSync(f.pushFile),false);
  const status=await f.own(f.alice,'notifications.status');assert.equal(status.configured,false);assert.equal(status.publicKey,undefined);
  assert.equal((await f.own(f.alice,'notifications.subscribe',{subscription:subscription().value})).error.code,'push_unavailable');
  f.emit();await f.push.drain();assert.equal(f.delivered.length,0);assert.equal(existsSync(f.pushFile),false);
});

test('missing open authority stays OFF and mismatched VAPID key pairs fail before opening storage',async t=>{
  const f=await fixture(t,{enabled:false}),configuration={...f.configuration,enabled:true},ports={recipientsFor:()=>[],authorizeDelivery:()=>null,authorizeOpen:()=>null};
  const missing=createPushService({databasePath:f.pushFile,configuration,recipientsFor:ports.recipientsFor,authorizeDelivery:ports.authorizeDelivery});
  assert.equal(missing.status().configured,false);await missing.close();assert.equal(existsSync(f.pushFile),false);
  assert.throws(()=>createPushService({databasePath:f.pushFile,configuration:{...configuration,privateKey:webPush.generateVAPIDKeys().privateKey},...ports}),{code:'push_configuration_invalid'});
  assert.equal(existsSync(f.pushFile),false);
});

test('signed HTTP subscription is device/account owned and no endpoint/key enters a public result',async t=>{
  const f=await fixture(t),sub=subscription().value;
  const anonymous=await f.post({op:'notifications.subscribe',args:{expectedAccountId:f.alice.accountId,subscription:sub}});assert.equal(anonymous.ok,false);
  assert.equal((await f.own(f.alice,'notifications.subscribe',{subscription:sub})).enabled,true);
  const status=await f.own(f.alice,'notifications.status');assert.equal(status.enabled,true);assert.equal(status.publicKey,f.configuration.publicKey);
  for(const value of [sub.endpoint,sub.keys.auth,sub.keys.p256dh,f.configuration.privateKey])assert.equal(JSON.stringify(status).includes(value),false);
  assert.equal((await f.own(f.bob,'notifications.status')).enabled,false);
  assert.equal((await f.own(f.bob,'notifications.subscribe',{subscription:sub})).error.code,'push_subscription_bound');
  assert.equal((await f.rpc(f.bob,'notifications.unsubscribe',{expectedAccountId:f.alice.accountId})).ok,false);
  assert.equal((await f.own(f.alice,'notifications.status')).enabled,true);
});

test('untrusted endpoints cannot reach private hosts, credentials, fragments, redirects or arbitrary providers',async t=>{
  const f=await fixture(t),sub=subscription().value;
  const endpoints=['https://127.0.0.1/private','https://[::1]/','https://169.254.169.254/latest','https://internal.example.invalid/',
    'http://fcm.googleapis.com/x','https://fcm.googleapis.com.evil.invalid/x','https://user:password@fcm.googleapis.com/x','https://fcm.googleapis.com/x#fragment'];
  for(const endpoint of endpoints){const result=await f.own(f.alice,'notifications.subscribe',{subscription:{...sub,endpoint}});assert.equal(result.ok,false);}
  assert.equal((await f.own(f.alice,'notifications.status')).enabled,false);assert.equal(f.delivered.length,0);
  assert.throws(()=>validatePushSubscription({...sub,keys:{...sub.keys,auth:'invalid'}},1000),{code:'push_invalid'});
});

test('post-commit bus schedules without waiting for transport and opaque opens need fresh signed authority',async t=>{
  const entered=defer(),release=defer();t.after(()=>release.resolve());const f=await fixture(t,{send:async()=>{entered.resolve();await release.promise;}});
  await f.own(f.alice,'notifications.subscribe',{subscription:subscription().value});f.emit();const draining=f.push.drain();await entered.promise;
  assert.equal((await f.own(f.alice,'notifications.status')).enabled,true); // transport pending, signed ACK path is not blocked
  const payload=f.delivered[0].payload;assert.deepEqual(Object.keys(payload).sort(),['bindingId','notificationId','schema']);
  assert.equal((await f.own(f.bob,'notifications.open',{notificationId:payload.notificationId})).ok,false);
  const opened=await f.own(f.alice,'notifications.open',{notificationId:payload.notificationId});assert.deepEqual(opened.target,{kind:'room',communityId:event.conversationId});
  f.allow(false);assert.equal((await f.own(f.alice,'notifications.open',{notificationId:payload.notificationId})).ok,false);
  release.resolve();await draining;f.emit({messageId:event.messageId,conversationId:event.conversationId,kind:event.kind});await f.push.drain();assert.equal(f.delivered.length,1);
});

test('every retry rechecks the current host authority and dead subscriptions are retired',async t=>{
  const retry=await fixture(t,{send:async()=>{throw Object.assign(new Error('synthetic retryable'),{statusCode:503});}});
  await retry.own(retry.alice,'notifications.subscribe',{subscription:subscription().value});retry.emit();await retry.push.drain();assert.equal(retry.delivered.length,1);
  retry.allow(false);retry.at(7000);await retry.push.drain();assert.equal(retry.delivered.length,1);
  const f=await fixture(t,{send:async()=>{throw Object.assign(new Error('synthetic expired'),{statusCode:410});}});
  await f.own(f.alice,'notifications.subscribe',{subscription:subscription().value});f.emit();await f.push.drain();assert.equal((await f.own(f.alice,'notifications.status')).enabled,false);
});

test('read/mute delivery preferences do not revoke an explicit click on a still-authorized dialogue',async t=>{
  const f=await fixture(t);await f.own(f.alice,'notifications.subscribe',{subscription:subscription().value});f.emit();await f.push.drain();const id=f.delivered[0].payload.notificationId;
  f.mute(true);f.emit({...event,messageId:'new_muted_message'});await f.push.drain();assert.equal(f.delivered.length,1);
  assert.equal((await f.own(f.alice,'notifications.open',{notificationId:id})).ok,true);
  f.allow(false);assert.equal((await f.own(f.alice,'notifications.open',{notificationId:id})).ok,false);
});

test('actual World room commit reaches only a subscribed member; read/mute and leave are checked through signed HTTP',async t=>{
  const f=await fixture(t,{actualWorld:true}),good=value=>{assert.equal(value.ok,true,value.error?.code);return value;};
  const room=good(await f.rpc(f.alice,'world.community.create',{requestId:'push_real_room',name:'Disposable push room'})).community;
  good(await f.rpc(f.bob,'world.membership.join',{communityId:room.communityId}));
  for(const actor of [f.alice,f.bob])good(await f.own(actor,'notifications.subscribe',{subscription:subscription().value}));
  const args={communityId:room.communityId,clientId:'push_real_room_one',text:'Never put this private room text in a notification'};
  const sent=good(await f.rpc(f.alice,'world.chat.send',args)).message;await f.push.drain();assert.equal(f.delivered.length,1);
  const notificationId=f.delivered[0].payload.notificationId;
  assert.equal(JSON.stringify(f.delivered[0].payload).includes(args.text),false);
  assert.equal((await f.own(f.alice,'notifications.open',{notificationId})).ok,false);
  good(await f.rpc(f.bob,'world.chat.read',{communityId:room.communityId,throughSeq:sent.seq}));
  assert.deepEqual(good(await f.own(f.bob,'notifications.open',{notificationId})).target,{kind:'room',communityId:room.communityId});
  good(await f.rpc(f.alice,'world.chat.send',args));await f.push.drain();assert.equal(f.delivered.length,1); // receipt replay publishes no event
  good(await f.rpc(f.bob,'world.membership.preferences',{communityId:room.communityId,muted:true}));
  good(await f.rpc(f.alice,'world.chat.send',{...args,clientId:'push_real_room_muted'}));await f.push.drain();assert.equal(f.delivered.length,1);
  assert.equal((await f.own(f.bob,'notifications.open',{notificationId})).ok,true);
  good(await f.rpc(f.bob,'world.membership.leave',{communityId:room.communityId}));
  assert.equal((await f.own(f.bob,'notifications.open',{notificationId})).ok,false);
  good(await f.rpc(f.alice,'world.chat.send',{...args,clientId:'push_real_room_left'}));await f.push.drain();assert.equal(f.delivered.length,1);
});

test('actual World direct delivery and click use the current Connect relationship; blocking cancels a durable retry',async t=>{
  const f=await fixture(t,{actualWorld:true,send:async()=>{throw Object.assign(new Error('synthetic provider unavailable'),{statusCode:503});}});
  const good=value=>{assert.equal(value.ok,true,value.error?.code);return value;};
  const card=good(await f.rpc(f.bob,'card.get')),request=good(await f.rpc(f.alice,'contacts.request',{cardId:card.cardId}));
  const relationshipId=good(await f.rpc(f.bob,'contacts.accept',{requestId:request.requestId})).relationshipId;
  const thread=good(await f.own(f.alice,'world.direct.open',{peerAccountId:f.bob.accountId,expectedRelationshipId:relationshipId,requestId:'push_real_dm'})).current;
  for(const actor of [f.alice,f.bob])good(await f.own(actor,'notifications.subscribe',{subscription:subscription().value}));
  const scope={threadId:thread.threadId,expectedRelationshipId:relationshipId};
  good(await f.own(f.alice,'world.direct.chat.send',{...scope,clientId:'push_real_dm_one',text:'Disposable private direct message'}));
  await f.push.drain();assert.equal(f.delivered.length,1);const notificationId=f.delivered[0].payload.notificationId;
  good(await f.own(f.bob,'world.direct.preferences',{...scope,muted:true}));
  assert.deepEqual(good(await f.own(f.bob,'notifications.open',{notificationId})).target,
    {kind:'direct',threadId:thread.threadId,peerAccountId:f.alice.accountId,relationshipId});
  assert.equal((await f.own(f.alice,'notifications.open',{notificationId})).ok,false);
  good(await f.rpc(f.bob,'contacts.block',{peerAccountId:f.alice.accountId}));f.at(7000);await f.push.drain();
  assert.equal(f.delivered.length,1);assert.equal((await f.own(f.bob,'notifications.open',{notificationId})).ok,false);
});

test('a late 410 for an old subscription cannot delete a replacement on the same device',async t=>{
  const entered=defer(),release=defer();t.after(()=>release.resolve());const f=await fixture(t,{send:async()=>{entered.resolve();await release.promise;throw Object.assign(new Error('synthetic old endpoint'),{statusCode:410});}});
  await f.own(f.alice,'notifications.subscribe',{subscription:subscription().value});f.emit();const draining=f.push.drain();await entered.promise;
  await f.own(f.alice,'notifications.subscribe',{subscription:subscription().value});release.resolve();await draining;
  assert.equal((await f.own(f.alice,'notifications.status')).enabled,true);
});

test('device revocation removes its durable subscription and prevents a queued retry',async t=>{
  const f=await fixture(t,{send:async()=>{throw Object.assign(new Error('synthetic outage'),{statusCode:503});}}),second=identity();
  const enrollment=await f.rpc(second,'enrollment.start',{label:'Isolated second key',encryptionPublicJwk:second.jwk});assert.equal(enrollment.ok,true);
  assert.equal((await f.rpc(f.alice,'enrollment.approve',{requestId:enrollment.requestId,wrappedKey:{schema:'synthetic.encrypted',ciphertext:'opaque'}})).ok,true);
  Object.assign(second,await f.rpc(second,'enrollment.finish',{requestId:enrollment.requestId,expectedAccountId:f.alice.accountId}));
  await f.own(f.alice,'notifications.subscribe',{subscription:subscription().value});f.emit();await f.push.drain();assert.equal(f.delivered.length,1);
  assert.equal((await f.rpc(second,'device.revoke',{deviceId:f.alice.deviceId})).ok,true);f.at(7000);await f.push.drain();assert.equal(f.delivered.length,1);
});

test('retries stop after four attempts and a duplicate event never restarts failed delivery',async t=>{
  const f=await fixture(t,{send:async()=>{throw Object.assign(new Error('synthetic retryable'),{statusCode:429});}});
  await f.own(f.alice,'notifications.subscribe',{subscription:subscription().value});f.emit();await f.push.drain();
  for(const at of [7000,18000,39000,1_000_000]){f.at(at);await f.push.drain();}
  assert.equal(f.delivered.length,4);f.emit();await f.push.drain();assert.equal(f.delivered.length,4);
});

test('pending event intake is bounded before fanout and processing runs in bounded batches',async t=>{
  const f=await fixture(t);await f.own(f.alice,'notifications.subscribe',{subscription:subscription().value});
  for(let index=0;index<150;index++)f.emit({...event,messageId:'bounded_'+index});
  await f.push.drain();assert.equal(f.delivered.length,4);
  for(let index=0;index<32;index++)await f.push.drain();assert.equal(f.delivered.length,128);
  assert.equal(f.push.status().counters.intakeDropped,22);
});

test('recipient keyset pagination reaches a subscriber after 10k accounts and resumes across a service restart',async t=>{
  const f=await fixture(t);f.recipients([...Array.from({length:10_240},(_,index)=>'0_recipient_'+String(index).padStart(5,'0')),f.alice.accountId]);
  await f.own(f.alice,'notifications.subscribe',{subscription:subscription().value});f.emit();
  for(let index=0;index<20;index++)await f.push.drain();assert.equal(f.delivered.length,0);
  await f.restart();for(let index=0;index<24;index++)await f.push.drain();assert.equal(f.delivered.length,1);
  f.emit();await f.push.drain();assert.equal(f.delivered.length,1);
});

test('a full durable queue preserves its fanout cursor; restart and replay deliver the entire remaining device page once',async t=>{
  const directory=await mkdtemp(join(tmpdir(),'soty-push-owned-')),file=join(directory,'queue.sqlite');let store=createPushStore({databasePath:file,projectId:'soty'});
  t.after(async()=>{store.close();assert.equal(dirname(resolve(directory)),resolve(tmpdir()));assert.match(directory,/soty-push-owned-/u);await rm(directory,{recursive:true,force:true,maxRetries:3});});
  const sub=subscription().value,actor={accountId:'account_queue',deviceId:'device_queue'};
  for(let index=0;index<4200;index++)store.put({...actor,deviceId:'device_'+index},{...sub,endpoint:sub.endpoint+'_'+index},'test-key',1000);
  const [fanoutId]=store.stageEvents([event],1000);store.fanoutPage(fanoutId,[actor.accountId]);
  for(let index=0;index<17;index++)store.enqueueFanout(fanoutId,'test-key',1000);
  const before=store.fanouts(1000)[0];assert.ok(before.subscription_cursor);assert.equal(before.state,'pending');
  store.close();store=createPushStore({databasePath:file,projectId:'soty'});
  assert.equal(store.fanouts(1000)[0].subscription_cursor,before.subscription_cursor);assert.deepEqual(store.stageEvents([event],1000),[fanoutId]);
  for(let index=0;index<13;index++){
    for(const job of store.due(1000))store.settle(job.id,'sent',1,1000);
    store.enqueueFanout(fanoutId,'test-key',1000);
  }
  assert.equal(store.fanouts(1000).length,0);
  const inspect=new DatabaseSync(file,{readOnly:true});try{
    assert.equal(inspect.prepare('SELECT count(*) AS n FROM push_jobs').get().n,4200);
    assert.equal(inspect.prepare('SELECT count(DISTINCT subscription_id) AS n FROM push_jobs').get().n,4200);
    assert.equal(inspect.prepare("SELECT count(*) AS n FROM push_jobs WHERE state='pending'").get().n,4096);
  }finally{inspect.close();}
});

test('durable queue survives close, actual SQLite backup/restore and a fresh Connect service',async t=>{
  let fail=true;const f=await fixture(t,{send:async()=>{if(fail)throw Object.assign(new Error('synthetic outage'),{statusCode:503});}});
  await f.own(f.alice,'notifications.subscribe',{subscription:subscription().value});f.emit();await f.push.drain();assert.equal(f.delivered.length,1);
  const snapshot=join(f.directory,'snapshot','push.sqlite');await f.push.backupTo(snapshot);assert.equal(existsSync(snapshot),true);
  await f.restart(snapshot);f.at(7000);fail=false;await f.push.drain();assert.equal(f.delivered.length,2);
  assert.equal(f.delivered[1].payload.notificationId,f.delivered[0].payload.notificationId);assert.equal((await f.own(f.alice,'notifications.status')).enabled,true);
  f.emit();await f.push.drain();assert.equal(f.delivered.length,2);
});

test('graceful close persists staged committed events without starting a new network send',async t=>{
  const f=await fixture(t);await f.own(f.alice,'notifications.subscribe',{subscription:subscription().value});f.emit();
  await f.restart();assert.equal(f.delivered.length,0);await f.push.drain();assert.equal(f.delivered.length,1);
});

test('a foreign/future push database is rejected without rewriting its schema',async t=>{
  const f=await fixture(t),file=join(f.directory,'future.sqlite'),db=new DatabaseSync(file);db.exec('PRAGMA user_version=99');db.close();
  assert.throws(()=>createPushStore({databasePath:file,projectId:'soty'}),{code:'push_store_invalid'});
  const preserved=new DatabaseSync(file);assert.equal(preserved.prepare('PRAGMA user_version').get().user_version,99);preserved.close();
  assert.throws(()=>createPushStore({databasePath:f.pushFile,projectId:'another-project'}),{code:'push_store_invalid'});
});

test('mature web-push encryption decrypts with receiver keys, uses VAPID and includes no message preview',async t=>{
  const sub=subscription(),keys=webPush.generateVAPIDKeys(),payload={schema:'soty.push.v1',notificationId:randomBytes(24).toString('base64url'),bindingId:'a'.repeat(64)};
  let details;const send=createPushTransport({...keys,subject:'mailto:fixture@example.invalid'},{requestImpl(endpoint,options,callback){
    const request=new EventEmitter();request.destroy=()=>{};request.end=body=>{details={endpoint,...options,body};queueMicrotask(()=>{const response=new EventEmitter();response.statusCode=201;response.resume=()=>{};callback(response);response.emit('end');});};return request;
  }});await send(sub.value,JSON.stringify(payload));
  assert.equal(details.headers['Content-Encoding'],'aes128gcm');assert.equal(details.headers.TTL,300);assert.match(details.headers.Authorization,/^vapid /u);
  const decoded=ece.decrypt(details.body,{version:'aes128gcm',privateKey:sub.dh,authSecret:sub.auth});assert.deepEqual(JSON.parse(decoded.toString()),payload);
  assert.equal(details.body.includes(Buffer.from(payload.notificationId)),false);
});

test('the HTTP boundary never follows redirects and enforces an absolute deadline with safe errors',async()=>{
  const keys={...webPush.generateVAPIDKeys(),subject:'mailto:fixture@example.invalid'},sub=subscription().value;let requests=0,destroyed=0;
  const redirect=createPushTransport(keys,{requestImpl(_endpoint,_options,callback){requests++;const request=new EventEmitter();request.destroy=()=>{destroyed++;};request.end=()=>queueMicrotask(()=>{
    const response=new EventEmitter();response.statusCode=302;response.headers={location:'https://127.0.0.1/private'};response.resume=()=>{};callback(response);response.emit('end');});return request;}});
  await assert.rejects(redirect(sub,'opaque'),{code:'push_transport_failed',statusCode:302});assert.equal(requests,1);
  const timeout=createPushTransport(keys,{timeoutMs:10,requestImpl(){const request=new EventEmitter();request.end=()=>{};request.destroy=()=>{destroyed++;};return request;}});
  await assert.rejects(timeout(sub,'opaque'),{code:'push_transport_failed'});assert.equal(destroyed,1);
});
