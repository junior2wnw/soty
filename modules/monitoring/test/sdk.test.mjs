import test from 'node:test';
import assert from 'node:assert/strict';
import {createMonitoringClient} from '../sdk.mjs';
import {createMonitoringService} from '../server/index.mjs';
const actor=Object.freeze({accountId:'sdk-owner',deviceId:'sdk-device'}),scope={kind:'app',appId:'app-'+'c'.repeat(32)};
function fixture(t){const values=new Map(),storage={getItem:key=>values.get(key)??null,setItem:(key,value)=>values.set(key,value),removeItem:key=>values.delete(key)};const service=createMonitoringService({databasePath:':memory:',projectId:'soty',mode:'writer',definitions:[{id:'http.requests',kind:'counter',unit:'count'}],currentActor:value=>value===actor,withAuthority:(_request,callback)=>callback(),clock:()=>60000000});t.after(()=>service.close());let current=true,account=actor.accountId,lost=false,held=null,calls=[];
 const options={scope,storage,isCurrent:()=>current,currentAccountId:()=>account,async request(op,args){calls.push({op,args});const result=service.execute({op,args,actor});if(held)await held;if(lost){lost=false;throw Object.assign(new Error('network'),{code:'network'});}return{ok:true,...result};}};
 return{values,storage,service,options,get calls(){return calls;},create:()=>createMonitoringClient(options),lose(){lost=true;},hold(value){held=value;},switch(value){account=value;},stop(){current=false;}};}
test('browser SDK consumes actual module DTO with null gaps and no guessed app IDs',async t=>{
 const f=fixture(t),client=f.create();assert.equal((await client.describe()).metrics[0].current,null);f.service.observe({scope,metricId:'http.requests',value:0,observedAt:60000000});const series=await client.query({metricIds:['http.requests'],from:59940000,to:60060000,maxPoints:2});assert.deepEqual(series.series[0].points.map(point=>point.value),[null,0]);
});
test('lost config ACK journal survives new client; retry uses identical ID/intent and clears only confirmed receipt',async t=>{
 const f=fixture(t),command={requestId:'same-configuration',expectedRevision:0,enabled:false};f.lose();await assert.rejects(f.create().setEnabled(command),{code:'network'});const reopened=f.create();assert.deepEqual(reopened.pending(),command);assert.equal((await reopened.retryPending()).replayed,true);assert.equal(reopened.pending(),null);assert.deepEqual(f.calls[0].args,f.calls[1].args);assert.equal((await reopened.describe()).config.revision,1);
});
test('known CAS conflict retains original command until explicit refresh/discard; no automatic replacement',async t=>{
 const f=fixture(t),client=f.create(),command={requestId:'old-draft',expectedRevision:0,enabled:false};await client.setEnabled({requestId:'other-current',expectedRevision:0,enabled:true});await assert.rejects(client.setEnabled(command),{code:'monitoring_revision_conflict'});assert.deepEqual(client.pending(),command);await assert.rejects(client.setEnabled({...command,requestId:'new-hidden-id'}),{code:'monitoring_pending_conflict'});assert.equal((await client.describe()).config.revision,1);client.discardPending('wrong-id');assert.deepEqual(client.pending(),command);client.discardPending(command.requestId);assert.equal(client.pending(),null);
});
test('failed durable journal prevents network mutation; late account/dispose reply cannot ACK/clear another owner',async t=>{
 const f=fixture(t),bad=createMonitoringClient({...f.options,storage:{...f.storage,setItem(){throw new Error('quota');}}});await assert.rejects(bad.setEnabled({requestId:'no-dispatch',expectedRevision:0,enabled:false}),{code:'monitoring_journal_unavailable'});assert.equal(f.calls.length,0);
 let release;f.hold(new Promise(resolve=>{release=resolve;}));const client=f.create(),command={requestId:'held-owner',expectedRevision:0,enabled:false},pending=client.setEnabled(command);await Promise.resolve();f.stop();release();await assert.rejects(pending,{code:'monitoring_account_changed'});assert.equal(f.values.size,1,'unknown ACK journal must remain');
});
test('malformed/wrong-scope server DTO is not accepted as current data',async()=>{
 const make=reply=>createMonitoringClient({scope,currentAccountId:()=>actor.accountId,request:async()=>reply});await assert.rejects(make({schema:'soty.monitoring.describe.v0'}).describe(),{code:'monitoring_invalid_response'});
 const minimal={schema:'soty.monitoring.config.v1',scope:{kind:'platform'},config:{enabled:false,revision:1,updatedAt:1},replayed:false};const values=new Map(),storage={getItem:key=>values.get(key)??null,setItem:(key,value)=>values.set(key,value),removeItem:key=>values.delete(key)};const client=createMonitoringClient({scope,currentAccountId:()=>actor.accountId,storage,request:async()=>minimal});await assert.rejects(client.setEnabled({requestId:'scope-swap',expectedRevision:0,enabled:false}),{code:'monitoring_invalid_response'});assert.ok(client.pending());
});
test('admin journal is separate from config, unknown is retained and retry defaults to inspect-only',async()=>{
 const rows=new Map(),storage={getItem:key=>rows.get(key)??null,setItem:(key,value)=>rows.set(key,value),removeItem:key=>rows.delete(key)};let committed=false,calls=[];
 const options={scope,currentAccountId:()=>actor.accountId,storage,async request(op,args){calls.push({op,args});return{schema:'soty.monitoring.admin.v1',actionId:args.actionId,state:committed?'committed':'unknown',revision:committed?2:null,replayed:calls.length>1};}};
 const first=createMonitoringClient(options),command={actionId:'app.listing',requestId:'admin-lost-ack',expectedRevision:1,input:{listed:false}};assert.equal((await first.invokeAction(command)).state,'unknown');assert.deepEqual(first.pendingAction(),command);assert.equal(first.pending(),null);
 const reopened=createMonitoringClient(options);await reopened.retryAction();assert.equal(calls[1].args.retry,false);assert.equal(calls[0].args.requestId,calls[1].args.requestId);assert.ok(reopened.pendingAction());committed=true;assert.equal((await reopened.retryAction({executeIfMissing:true})).state,'committed');assert.equal(reopened.pendingAction(),null);assert.equal(calls[2].args.retry,true);
});
