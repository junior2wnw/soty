import test from 'node:test';
import assert from 'node:assert/strict';
import { join } from 'node:path';
import WebSocket, { WebSocketServer } from 'ws';
import { nativeHttpFixture, nativeIdentity, good } from './support/native-capability-http.mjs';
import { createAppConnectFlow, createAppConnectJournal } from '../../src/world/app-connect-flow.mjs';
import { createFieldPersistence } from '../../src/world/field-persistence.ts';
import { createAppFieldPlacementController } from '../../src/world/app-field-placement.mjs';
import {createAppSourceState} from '../../src/world/app-source-state.mjs';
import {rootSelfIpFixture as fixture,rawHttp,until} from './support/self-ip-root-fixture.mjs';

test('signed Root → real connector self-IP HTTP/WS → audience/canvas receipt → exact replay/restart', {timeout:45000},async t=>{
  const f=await fixture(t),capability=await f.http('/api/apps/runtime-capabilities');
  assert.equal(capability.status,200);assert.equal(capability.headers['cache-control'],'no-store');assert.ok(capability.body.runtimeProfiles.includes('soty.relay-self-ip.v1'));
  const storageValues=new Map(),storage={getItem:key=>storageValues.get(key)??null,setItem:(key,value)=>storageValues.set(key,value)},locks={request:async(_key,callback)=>callback()};
  let fieldLocal=null,lostAck=true,current=true,failPlacement=false;const calls=[];
  const api={async request(op,args){calls.push({op,args:structuredClone(args)});if(op==='world.field.put'&&failPlacement)throw Object.assign(Error('offline'),{code:'network_offline'});const result=good(await f.call(f.owner,op,args));if(op==='apps.registerEndpoint'&&lostAck){lostAck=false;throw Object.assign(Error('lost acknowledgement'),{code:'network_lost_ack'});}return result;}};
  const journal=()=>createAppConnectJournal({accountId:f.account.accountId,storage,locks});
  const flow=()=>createAppConnectFlow({api,accountId:f.account.accountId,journal:journal(),storage,locks,isCurrent:()=>current,
    createPlacement(appId){const persistence=createFieldPersistence({api,accountId:f.account.accountId,isCurrent:()=>current,store:{async read(){return structuredClone(fieldLocal);},async write(value,expected){assert.equal(fieldLocal?.localRevision??0,expected);fieldLocal=structuredClone(value);},subscribe(){return()=>{};}}});return createAppFieldPlacementController({persistence,appId,accountId:f.account.accountId,isCurrent:()=>current});}});
  const input={name:'Настоящее IP приложение',address:`${f.source.address}:${f.source.port}/identity`,hostDeviceId:f.identity.hostDeviceId,connectorId:f.identity.connectorId,audience:'public'};
  await assert.rejects(flow().run(input),{code:'network_lost_ack'});assert.equal(journal().read().schema,2);assert.equal(journal().read().appId,undefined);
  const one=good(await f.call(f.owner,'apps.list'));assert.equal(one.apps.length,1);const id=one.apps[0].id;
  const initialUniversal=good(await f.ownerCall('apps.universal.get',{appId:id}));assert.ok(initialUniversal.registration.descriptor.feedback,'registration uses the real automatic universal feedback path before publication changes authority');assert.equal(initialUniversal.registration.feedback.state,'ready');
  failPlacement=true;await assert.rejects(flow().run(input),{code:'app_connect_field_pending'});
  assert.equal(good(await f.call(f.owner,'apps.inspect',{appId:id})).publication.listed,true);assert.equal(journal().read().complete,false);
  failPlacement=false;const result=await flow().run(input);assert.equal(result.app.id,id);
  const field=good(await f.ownerCall('world.field.get'));assert.equal(field.document.shortcuts.filter(value=>value.entity.kind==='app'&&value.entity.id===id).length,1);
  const snapshot=good(await f.call(f.owner,'apps.inspect',{appId:id}));assert.equal(snapshot.source.endpoint.address,f.source.address);assert.equal(snapshot.source.profile,'soty.relay-self-ip.v1');assert.equal(snapshot.source.requiredBindingVersion,2);assert.equal(snapshot.publication.audience,'public');assert.equal(snapshot.publication.listed,true);
  assert.ok(good(await f.ownerCall('apps.cover.status',{appId:id})),'cover Source consumer accepts the complete endpoint-bound canonical target');
  const publicUniversal=good(await f.ownerCall('apps.universal.get',{appId:id})).registration;assert.equal(publicUniversal.state,'ready');assert.equal(publicUniversal.feedback.installationId,initialUniversal.registration.feedback.installationId);
  assert.deepEqual({...publicUniversal.descriptor,app:{...publicUniversal.descriptor.app,visibility:'private'}},initialUniversal.registration.descriptor,'all accepted descriptor/module fields survive authorized publication');
  const feedback=good(await f.call(f.owner,'apps.feedback.submit',{appId:id,installationId:publicUniversal.feedback.installationId,requestId:'creator-public-feedback-001',body:'Actual creator → public → feedback path',attachments:[]}));assert.ok(feedback.receipt.ticketId);
  const session=await f.session(id),body=await rawHttp(f.server.address().port,'/identity',session);assert.equal(body.text,'SELF_IP_ONLY');assert.equal(f.source.seen.filter(value=>value.marker==='LOOPBACK_ONLY').length,0);
  assert.ok(f.source.seen.some(value=>value.marker==='SELF_IP_ONLY'&&value.host===`${f.source.address}:${f.source.port}`));
  const ws=new WebSocket(`ws://127.0.0.1:${f.server.address().port}/live`,{headers:{host:session.host,origin:session.origin,cookie:session.cookie}});ws.on('error',()=>{});t.after(()=>ws.terminate());
  const message=await new Promise((resolve,reject)=>{ws.once('message',bytes=>resolve(bytes.toString()));ws.once('error',reject);});assert.equal(message,'SELF_IP_WS');ws.close();
  const redirect=await rawHttp(f.server.address().port,'/redirect',session);assert.equal(redirect.status,502);assert.equal(f.source.seen.filter(value=>value.marker==='LOOPBACK_ONLY').length,0);
  const registrationCalls=calls.filter(value=>value.op==='apps.registerEndpoint');assert.equal(registrationCalls.length,2);assert.equal(registrationCalls[0].args.requestId,registrationCalls[1].args.requestId);
  assert.equal(f.sql(join(f.directory,'apps','registry.sqlite'),db=>db.prepare('SELECT count(*) AS n FROM app_endpoint_registration_receipts').get().n),1);
  await f.restart({enabled:true});await f.inventory();const reopened=good(await f.call(f.owner,'apps.inspect',{appId:id}));assert.equal(reopened.source.endpoint.address,f.source.address);assert.equal(reopened.publication.listed,true);
  assert.equal((await rawHttp(f.server.address().port,'/identity',await f.session(id))).text,'SELF_IP_ONLY');
  const replay=await f.call(f.owner,'apps.registerEndpoint',{...registrationCalls[0].args,inventoryWitness:{channelId:'stale',revision:1,digest:'0'.repeat(64)}});assert.equal(good(replay).app.id,id,'durable exact receipt precedes obsolete inventory witness');
  const changed=await f.call(f.owner,'apps.registerEndpoint',{...registrationCalls[0].args,name:'Different payload'});assert.equal(changed.ok,false);assert.equal(changed.error.code,'apps_endpoint_request_conflict');
  const legacy=await f.call(f.owner,'apps.register',{hostDeviceId:f.identity.hostDeviceId,connectorId:f.identity.connectorId,name:input.name,port:f.source.port,entryPath:'/identity',grants:{accountIds:[],communityIds:[]}});assert.equal(legacy.ok,false);assert.equal(legacy.error.code,'app_port_already_registered','same port never aliases self-IP app as localhost');
});

test('current self-IP admission rejects unrelated devices/nonself/old connectors before source dial', {timeout:25000},async t=>{
  const f=await fixture(t),args=await f.args(),before=f.source.seen.length;
  const outsider=await f.call(f.other,'apps.registerEndpoint',args);assert.equal(outsider.ok,false);assert.equal(outsider.error.code,'apps_device_not_owned');
  for(const address of ['203.0.113.9','169.254.169.254','127.0.0.1','127.1','2130706433','0x7f000001','010.0.0.1','[::1]','example.com','0.0.0.0']){
    const rejected=await f.call(f.owner,'apps.registerEndpoint',{...args,source:{...args.source,endpoint:{...args.source.endpoint,address}}});assert.equal(rejected.ok,false,address);
  }
  assert.equal(f.source.seen.length,before);assert.equal(good(await f.call(f.owner,'apps.list')).apps.length,0);
  f.removeInterfaces();const rejected=await f.call(f.owner,'apps.registerEndpoint',args);assert.equal(rejected.ok,false);assert.ok(['apps_endpoint_interface_unavailable','app_source_unreachable'].includes(rejected.error.code));assert.equal(good(await f.call(f.owner,'apps.list')).apps.length,0);
  f.restoreInterfaces();const ready=good(await f.call(f.owner,'apps.registerEndpoint',args));assert.equal(ready.app.endpoint.address,f.source.address);
});

test('negative control: deliberately ignored host fails the same real registration gate and commits no fake app', {timeout:20000},async t=>{
  const f=await fixture(t,{brokenDial:true}),args=await f.args();const response=await f.call(f.owner,'apps.registerEndpoint',args);
  assert.equal(response.ok,false);assert.equal(response.error.code,'app_source_unreachable');assert.ok(f.source.accepted.some(value=>value.marker==='LOOPBACK_WS'&&value.address==='127.0.0.1'),'broken control really opened the wrong TCP listener; the remote-address guard rejects before HTTP bytes');
  assert.equal(f.source.seen.filter(value=>value.marker==='SELF_IP_ONLY').length,0);assert.equal(good(await f.call(f.owner,'apps.list')).apps.length,0);
  assert.equal(f.sql(join(f.directory,'apps','registry.sqlite'),db=>db.prepare('SELECT count(*) AS n FROM app_endpoint_registration_receipts').get().n),0);
});

test('new Root plus legacy connector preserves loopback and explicitly refuses IP registration', {timeout:20000},async t=>{
  const f=await fixture(t,{legacyConnector:true}),args=await f.args();const rejected=await f.call(f.owner,'apps.registerEndpoint',args);assert.equal(rejected.ok,false);assert.equal(rejected.error.code,'apps_endpoint_connector_upgrade_required');
  const registered=good(await f.call(f.owner,'apps.register',{hostDeviceId:f.identity.hostDeviceId,connectorId:f.identity.connectorId,name:'Legacy loopback',port:f.source.port,entryPath:'/identity',grants:{accountIds:[],communityIds:[]}}));
  assert.equal(registered.app.endpoint,undefined);assert.equal(good(await f.call(f.owner,'apps.inspect',{appId:registered.app.id})).source.profile,'soty.relay-restricted.v1');
});

test('new connector preflight against a Root without the feature retains loopback without advertising unsupported IP profile', {timeout:20000},async t=>{
  const f=await fixture(t,{oldRoot:true}),args=await f.args();
  const devices=good(await f.ownerCall('apps.devices'));assert.equal(devices.devices[0].sourceEndpoints,undefined);
  const rejected=await f.call(f.owner,'apps.registerEndpoint',args);assert.equal(rejected.ok,false);assert.equal(rejected.error.code,'apps_endpoint_connector_upgrade_required');
  const registered=good(await f.call(f.owner,'apps.register',{hostDeviceId:f.identity.hostDeviceId,connectorId:f.identity.connectorId,name:'Older Root localhost',port:f.source.port,entryPath:'/identity',grants:{accountIds:[],communityIds:[]}}));
  assert.equal((await rawHttp(f.server.address().port,'/identity',await f.session(registered.app.id))).text,'LOOPBACK_ONLY');
});

test('self-IP source promotion and retained rollback preserve endpoint pins through real prepare/commit/runtime', {timeout:25000},async t=>{
  const f=await fixture(t),registered=good(await f.call(f.owner,'apps.registerEndpoint',await f.args())),appId=registered.app.id;
  const original=good(await f.ownerCall('apps.inspect',{appId})),priorSession=await f.session(appId);
  const sourceModel=createAppSourceState({accountId:f.account.accountId,appId,snapshot:original}),sourceCalls=[];
  const sourceApi={async request(op,args){sourceCalls.push({op,args});return good(await f.call(f.owner,op,args));}};
  sourceModel.patch({entryPath:'/identity?revision=2',endpointAddress:'192.'});assert.equal(sourceModel.read().draft.endpointAddress,'192.');
  await assert.rejects(sourceModel.prepare({api:sourceApi,isCurrent:()=>true}),{code:'app_source_invalid_target'});assert.equal(sourceCalls.length,0,'invalid editable draft never reaches Root');
  sourceModel.patch({endpointAddress:f.source.address});assert.equal((await sourceModel.prepare({api:sourceApi,isCurrent:()=>true})).status,'ready');
  const prepared=sourceModel.read().preparation;assert.deepEqual(sourceCalls[0].args.source.endpoint,original.source.endpoint,'the existing Hosting form forwards its exact IP, never drops it');
  assert.equal(prepared.target.endpoint.address,f.source.address);assert.notEqual(prepared.target.digest,original.source.digest);
  good(await f.ownerCall('apps.source.promote',{appId,requestId:'self-ip-source-promote-real-001',preparationId:prepared.preparationId,expectedPolicyEpoch:prepared.expectedPolicyEpoch,
    expectedTargetRevision:prepared.expectedTargetRevision,launchPolicy:'restricted',listed:false}));
  assert.equal((await rawHttp(f.server.address().port,'/identity',priorSession)).status,403);
  assert.equal((await rawHttp(f.server.address().port,'/identity',await f.session(appId))).text,'SELF_IP_ONLY');
  const current=good(await f.ownerCall('apps.inspect',{appId})),retained=good(await f.ownerCall('apps.source.prepare',{appId,expectedPolicyEpoch:current.publication.policyEpoch,expectedTargetRevision:current.source.revision,targetRevision:1}));
  assert.equal(retained.target.digest,original.source.digest);assert.deepEqual(retained.target.endpoint,original.source.endpoint);
  good(await f.ownerCall('apps.source.promote',{appId,requestId:'self-ip-source-rollback-real-001',preparationId:retained.preparationId,expectedPolicyEpoch:retained.expectedPolicyEpoch,expectedTargetRevision:retained.expectedTargetRevision,launchPolicy:'restricted',listed:false}));
  assert.equal(good(await f.ownerCall('apps.inspect',{appId})).source.digest,original.source.digest);
});

test('ordinary widget Root-private session barrier stops a held HTTP response/WS/ticket at protect/relock/TTL while main app stays usable', {timeout:30000},async t=>{
  const f=await fixture(t),appId=good(await f.call(f.owner,'apps.registerEndpoint',await f.args())).app.id;
  const widget=good(await f.ownerCall('canvas.widgets.create',{requestId:'runtime-barrier-widget-001',appId})).current;
  const object=good(await f.ownerCall('canvas.objects.create',{requestId:'runtime-barrier-object-001',binding:{providerId:'soty.apps',ref:widget.widgetId},presentation:{mask:{shape:'rect'}},
    visibility:{audience:'private',accountIds:[],communityIds:[]},hidden:{owner:false,others:false},protection:{kind:'none'}})).current;
  const extra={...widget.entry,instanceId:widget.widgetId},session=await f.session(appId,extra),main=await f.session(appId);
  const priorTicket=good(await f.ownerCall('apps.launch',{appId,...extra}));
  const ws=new WebSocket(`ws://127.0.0.1:${f.server.address().port}/live`,{headers:{host:session.host,origin:session.origin,cookie:session.cookie}});ws.on('error',()=>{});t.after(()=>ws.terminate());
  assert.equal(await new Promise((resolve,reject)=>{ws.once('message',bytes=>resolve(bytes.toString()));ws.once('error',reject);}), 'SELF_IP_WS');
  const closed=new Promise(resolve=>ws.once('close',resolve));f.source.holdGet();const held=rawHttp(f.server.address().port,'/hold',session);
  await until(()=>f.source.seen.some(value=>value.path==='/hold'),'held network request actually reached Source before lock');
  const password='Isolated-widget-password-734';
  good(await f.ownerCall('canvas.vault.protect',{requestId:'runtime-barrier-protect-001',objectId:object.objectId,expectedObjectRevision:object.revision,password}));
  await Promise.race([closed,new Promise((_,reject)=>setTimeout(()=>reject(Error('already-open widget WS survived protect')),1500))]);
  f.source.release();const stopped=await held;assert.equal(stopped.status,403);assert.equal(stopped.text.includes('SECRET_HELD_AFTER_LOCK'),false);
  assert.equal((await rawHttp(f.server.address().port,'/identity',session)).status,403);
  const prior=new URL(priorTicket.launchUrl);assert.equal((await rawHttp(f.server.address().port,'/_soty/session',{host:prior.host,origin:prior.origin,method:'POST',body:{ticket:prior.hash.slice(1)}})).status,403);
  assert.equal((await rawHttp(f.server.address().port,'/identity',main)).text,'SELF_IP_ONLY','locking an object does not revoke independent main-app authority');
  good(await f.ownerCall('canvas.vault.unlock',{objectId:object.objectId,password,ttlMs:1000}));
  const unlocked=await f.session(appId,extra);assert.equal((await rawHttp(f.server.address().port,'/identity',unlocked)).text,'SELF_IP_ONLY');
  await new Promise(resolve=>setTimeout(resolve,1100));assert.equal((await rawHttp(f.server.address().port,'/identity',unlocked)).status,403,'runtime rechecks expired permit even without a launch');
  good(await f.ownerCall('canvas.vault.unlock',{objectId:object.objectId,password}));const again=await f.session(appId,extra);
  good(await f.ownerCall('canvas.vault.relock',{requestId:'runtime-barrier-relock-001',objectId:object.objectId,scope:'device'}));assert.equal((await rawHttp(f.server.address().port,'/identity',again)).status,403);
});
