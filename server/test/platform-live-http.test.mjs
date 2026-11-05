import test from 'node:test';
import assert from 'node:assert/strict';
import {createServer} from 'node:http';
import {randomBytes,createHash} from 'node:crypto';
import {mkdtemp,mkdir,writeFile,rm} from 'node:fs/promises';
import {join} from 'node:path';
import {tmpdir} from 'node:os';
import WebSocket from 'ws';
import {createHttpApp} from '../http-app.js';
import {createClientWithStorage} from '../../modules/connect/browser/client.mjs';
import {pngFixture,asDataUrl} from '../../modules/world/test/avatar-fixtures.mjs';

const until=async fn=>{const end=Date.now()+5000;while(!fn()){assert.ok(Date.now()<end,'fixture condition timed out');await new Promise(resolve=>setTimeout(resolve,5));}};
function identityStore(){let value=null;return {async read(){return structuredClone(value);},async claim(next){value??=structuredClone(next);return structuredClone(value);},async compareAndSwap(revision,next){assert.equal(value.localRevision,revision);value=structuredClone(next);return structuredClone(value);}};}
async function environment(t){
  const directory=await mkdtemp(join(tmpdir(),'soty-live-http-')),dataDir=join(directory,'data'),dist=join(directory,'dist');await mkdir(dist);await writeFile(join(dist,'index.html'),'<!doctype html><title>Live transport acceptance</title>');
  let app,serial=0;const sockets=new Set(),clients=[],waitControllers=new Map();
  const server=createServer((req,res)=>app?app(req,res):res.writeHead(503).end());server.on('connection',socket=>{sockets.add(socket);socket.once('close',()=>sockets.delete(socket));});
  await new Promise(resolve=>server.listen(0,'127.0.0.1',resolve));const port=server.address().port,origin=`http://127.0.0.1:${port}`;
  const open=()=>createHttpApp(dist,{dataDir,connectOrigins:[origin],appOriginTemplate:`http://{appId}.legacy.localhost:${port}`,namedAppZone:`http://named.localhost:${port}`});app=open();
  server.on('upgrade',(req,socket,head)=>{if(!app.locals.appsService.handleUpgrade(req,socket,head))socket.destroy();});
  t.after(async()=>{clients.forEach(client=>client.dispose());await app.locals.closeServices();const stopped=new Promise(resolve=>server.close(resolve));sockets.forEach(socket=>socket.destroy());await stopped;await rm(directory,{recursive:true,force:true,maxRetries:3,retryDelay:50});});
  async function person(label){const client=createClientWithStorage({projectId:'soty',endpoint:`${origin}/api/connect/rpc`,fetch:(url,options)=>{
    const input=JSON.parse(options.body);let signal=options.signal;
    if(input.op==='platform.live.wait'){const controller=new AbortController();waitControllers.set(label,controller);signal=AbortSignal.any([signal,controller.signal]);}
    return fetch(url,{...options,signal,headers:{...options.headers,origin}});
  }},identityStore());clients.push(client);const account=await client.bootstrap(label);await client.extension('world.profile.get',{}, {expectedAccountId:account.accountId});return {client,account,label};}
  const owner=await person('Author'),reader=await person('Reader'),outside=await person('Outside');
  const call=(who,op,args={})=>who.client.extension(op,args,{expectedAccountId:who.account.accountId});
  const wait=(who,args={})=>call(who,'platform.live.wait',{expectedAccountId:who.account.accountId,...args});
  const identity={linkId:'live_test_link_0123456789012345678901234567',hostDeviceId:'live_host',connectorId:'live_connector'},token=randomBytes(32).toString('base64url'),claimCode=randomBytes(32).toString('base64url');
  const registered=await fetch(`${origin}/api/connectors/register`,{method:'POST',headers:{'content-type':'application/json',authorization:`Bearer ${token}`},body:JSON.stringify({linkId:identity.linkId,deviceId:identity.hostDeviceId,connectorId:identity.connectorId,scope:'Dev',protocol:2,capabilities:['apps']})});assert.equal((await registered.json()).ok,true);
  const ws=new WebSocket(`${origin.replace('http:','ws:')}/api/apps/channel`),frames=[];ws.on('message',raw=>frames.push(JSON.parse(raw.toString())));ws.on('error',()=>{});await until(()=>ws.readyState===WebSocket.OPEN);
  ws.send(JSON.stringify({type:'auth',schema:'soty.apps-channel.v1',...identity,token,name:'Local test host'}));await until(()=>frames.some(frame=>frame.type==='ready'));
  ws.send(JSON.stringify({type:'claim',claimDigest:createHash('sha256').update(claimCode).digest('hex')}));await until(()=>frames.some(frame=>frame.type==='claim-ready'));
  const ids={hostDeviceId:identity.hostDeviceId,connectorId:identity.connectorId};await call(owner,'apps.claim',{...ids,claimCode});ws.close();await until(()=>ws.readyState===WebSocket.CLOSED);
  const create=async(grants={})=>(await call(owner,'apps.register',{...ids,name:`Live app ${++serial}`,port:31000+serial,grants})).app;
  async function publish(appId,listed){let current=await call(owner,'apps.inspect',{appId});
    const address=await call(owner,'apps.domains.claim',{appId,slug:`live-${++serial}`,requestId:`claim_${serial}`,expectedDomainsRevision:current.addresses.revision});
    current=await call(owner,'apps.inspect',{appId});return call(owner,'apps.publication.update',{appId,requestId:`publish_${++serial}`,expectedPolicyEpoch:current.publication.policyEpoch,expectedTargetRevision:current.source.revision,activeDomainIds:[address.receipt.domainId],launchPolicy:'anyone',listed,
      exposureAck:{scope:'whole-port',targetRevision:current.source.revision,targetDigest:current.source.digest,profile:current.source.profile}});
  }
  return {get app(){return app;},owner,reader,outside,call,wait,create,publish,cancelWait(who){waitControllers.get(who.label).abort();},async restart(){await app.locals.closeServices();app=open();}};
}

test('real signed HTTP: an open wait does not block registration, authorized readers wake and private outsiders do not',{timeout:20000},async t=>{
  const f=await environment(t),a=await f.wait(f.owner,{waitMs:0}),b=await f.wait(f.reader,{waitMs:0}),c=await f.wait(f.outside,{waitMs:0});
  const ownerWait=f.wait(f.owner,{cursor:a.cursor,waitMs:4000}),readerWait=f.wait(f.reader,{cursor:b.cursor,waitMs:4000});
  await until(()=>f.app.locals.platformLive.stats().waiting===2);
  const app=await f.create({accountIds:[f.reader.account.accountId]});
  assert.equal((await ownerWait).changed,true,'same-client registration must complete before wait timeout');const current=await readerWait;assert.equal(current.changed,true);
  assert.deepEqual(Object.keys(current).sort(),['changed','cursor','ok','reset','schema']);
  assert.equal((await f.wait(f.outside,{cursor:c.cursor,waitMs:0})).changed,false,'private registration must not wake outsiders');
  const resolved=await f.call(f.reader,'apps.directory.resolve',{expectedAccountId:f.reader.account.accountId,appIds:[app.id]});assert.equal(resolved.items[0].available,true);
  const watched=await f.wait(f.reader,{cursor:current.cursor,waitMs:0,watch:[{kind:'app',id:app.id}]});
  const removal=f.wait(f.reader,{cursor:watched.cursor,waitMs:4000});await until(()=>f.app.locals.platformLive.stats().waiting===1);
  await f.call(f.owner,'apps.update',{appId:app.id,grants:{accountIds:[],communityIds:[]}});const removed=await removal;assert.equal(removed.changed,true);
  assert.equal((await f.call(f.reader,'apps.directory.resolve',{expectedAccountId:f.reader.account.accountId,appIds:[app.id]})).items[0].available,false);
  await f.call(f.owner,'apps.update',{appId:app.id,name:'Now private'});
  assert.equal((await f.wait(f.reader,{cursor:removed.cursor,waitMs:0})).changed,false,'former reader gets no later private activity');
  await assert.rejects(f.wait(f.outside,{cursor:removed.cursor,waitMs:0}),{code:'live_cursor_invalid'});
  await f.restart();assert.equal((await f.wait(f.owner,{cursor:a.cursor,waitMs:0})).reset,true);
});

test('real signed HTTP: profile/avatar commits wake permitted viewers; failed CAS never emits',{timeout:20000},async t=>{
  const f=await environment(t),profile=(await f.call(f.owner,'world.profile.get')).profile;
  await f.call(f.owner,'world.profile.update',{expectedRevision:profile.revision,audience:'public'});
  const state=await f.wait(f.reader,{waitMs:0,watch:[{kind:'person',id:f.owner.account.accountId}]}),waiting=f.wait(f.reader,{cursor:state.cursor,waitMs:4000});await until(()=>f.app.locals.platformLive.stats().waiting===1);
  const current=(await f.call(f.owner,'world.profile.get')).profile,image=asDataUrl(pngFixture());
  await f.call(f.owner,'world.profile.avatar.set',{expectedRevision:current.revision,avatarUrl:image,thumbnailUrl:image});const after=await waiting;assert.equal(after.changed,true);
  const avatars=await f.call(f.reader,'world.profile.avatars',{profileIds:[f.owner.account.accountId]});assert.ok(avatars.avatars[0].avatarRevision>current.revision);
  await assert.rejects(f.call(f.owner,'world.profile.avatar.set',{expectedRevision:current.revision,avatarUrl:image,thumbnailUrl:image}));
  assert.equal((await f.wait(f.reader,{cursor:after.cursor,waitMs:0})).changed,false,'failed durable write cannot produce a live success hint');
});

test('real signed HTTP: app cover drafts stay private, commit updates authorized readers, public listing wakes discovery',{timeout:20000},async t=>{
  const f=await environment(t),app=await f.create({accountIds:[f.reader.account.accountId]}),scope={expectedAccountId:f.owner.account.accountId,appId:app.id};
  const initial=await f.call(f.owner,'apps.cover.status',scope),state=await f.wait(f.reader,{waitMs:0,watch:[{kind:'app',id:app.id}]}),outside=await f.wait(f.outside,{waitMs:0});
  const id=()=>`cover1.${initial.serverTime}.${randomBytes(16).toString('hex')}`;
  const prepared=await f.call(f.owner,'apps.cover.prepare',{...scope,requestId:id(),expectedTargetRevision:initial.scope.targetRevision,expectedPolicyEpoch:initial.scope.policyEpoch,expectedCoverRevision:initial.scope.coverRevision,mime:'image/png',bytesBase64:pngFixture().toString('base64')});
  assert.equal((await f.wait(f.reader,{cursor:state.cursor,waitMs:0})).changed,false,'an uncommitted cover draft must not announce a visual change');
  const waiting=f.wait(f.reader,{cursor:state.cursor,waitMs:4000});await until(()=>f.app.locals.platformLive.stats().waiting===1);
  await f.call(f.owner,'apps.cover.commit',{...scope,requestId:id(),draftId:prepared.draft.id});assert.equal((await waiting).changed,true);
  const image=await f.call(f.reader,'apps.cover.read',{expectedAccountId:f.reader.account.accountId,appId:app.id});assert.equal(image.current.mode,'custom');assert.ok(image.image.base64.length>10);
  assert.equal((await f.wait(f.outside,{cursor:outside.cursor,waitMs:0})).changed,false);
  const discovery=f.wait(f.outside,{cursor:outside.cursor,waitMs:4000});await until(()=>f.app.locals.platformLive.stats().waiting===1);await f.publish(app.id,true);
  assert.equal((await discovery).changed,true,'explicit listed public publication should wake discovery without reload');
});

test('real HTTP disconnect cancels a signed wait and frees capacity promptly',{timeout:20000},async t=>{
  const f=await environment(t),state=await f.wait(f.owner,{waitMs:0}),pending=f.wait(f.owner,{cursor:state.cursor,waitMs:15000});
  const failed=assert.rejects(pending);await until(()=>f.app.locals.platformLive.stats().waiting===1);f.cancelWait(f.owner);await failed;
  await until(()=>f.app.locals.platformLive.stats().waiting===0);
  assert.equal((await f.wait(f.owner,{cursor:state.cursor,waitMs:0})).changed,false);
});

for(const removal of ['block','remove'])test(`real signed HTTP: contact accept then ${removal} updates friends-based app/profile authority without outsider hints`,{timeout:20000},async t=>{
  const f=await environment(t),profile=(await f.call(f.owner,'world.profile.get')).profile;
  await f.call(f.owner,'world.profile.update',{expectedRevision:profile.revision,audience:'friends'});
  const app=await f.create(),inspection=await f.call(f.owner,'apps.inspect',{appId:app.id});
  await f.call(f.owner,'apps.publication.update',{appId:app.id,requestId:`friends_${removal}`,expectedPolicyEpoch:inspection.publication.policyEpoch,expectedTargetRevision:inspection.source.revision,activeDomainIds:[],launchPolicy:'restricted',listed:false,audience:'friends'});
  const outsider=await f.wait(f.outside,{waitMs:0}),card=await f.reader.client.card(),request=await f.owner.client.requestContact(card.cardId);
  const before=await f.wait(f.owner,{waitMs:0}),acceptance=f.wait(f.owner,{cursor:before.cursor,waitMs:4000});await until(()=>f.app.locals.platformLive.stats().waiting===1);
  const accepted=await f.reader.client.acceptContact(request.requestId);assert.equal((await acceptance).changed,true);
  const appRef={kind:'app',id:app.id},personRef={kind:'person',id:f.owner.account.accountId};
  const resolveApp=()=>f.call(f.reader,'apps.directory.resolve',{expectedAccountId:f.reader.account.accountId,appIds:[app.id]});
  const resolvePerson=()=>f.call(f.reader,'world.directory.resolve',{expectedAccountId:f.reader.account.accountId,entities:[personRef]});
  assert.equal((await resolveApp()).items[0].available,true);assert.equal((await resolvePerson()).items[0].available,true);
  const state=await f.wait(f.reader,{waitMs:0,watch:[appRef,personRef]}),waiting=f.wait(f.reader,{cursor:state.cursor,waitMs:4000});await until(()=>f.app.locals.platformLive.stats().waiting===1);
  if(removal==='block')await f.owner.client.blockContact(f.reader.account.accountId);else await f.owner.client.removeContact(accepted.relationshipId);
  const removed=await waiting;assert.equal(removed.changed,true);assert.equal((await resolveApp()).items[0].available,false);assert.equal((await resolvePerson()).items[0].available,false);
  assert.equal((await f.wait(f.outside,{cursor:outsider.cursor,waitMs:0})).changed,false);
  await f.call(f.owner,'apps.update',{appId:app.id,name:'Private after friendship ended'});
  const current=(await f.call(f.owner,'world.profile.get')).profile;await f.call(f.owner,'world.profile.update',{expectedRevision:current.revision,displayName:'Private author'});
  assert.equal((await f.wait(f.reader,{cursor:removed.cursor,waitMs:0})).changed,false,'ended friendship cannot observe later private app/profile changes');
});
