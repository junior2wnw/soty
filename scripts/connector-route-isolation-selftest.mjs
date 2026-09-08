#!/usr/bin/env node
import assert from 'node:assert/strict';
import {mkdtemp,rm} from 'node:fs/promises';
import {tmpdir} from 'node:os';
import path from 'node:path';
import {performance} from 'node:perf_hooks';
import {createServer} from 'node:http';
import express from 'express';
import {createConnectorStore} from '../server/connector-store.js';
import {attachConnectorApi} from '../server/connector-api.js';

const root=await mkdtemp(path.join(tmpdir(),'soty-route-isolation-'));
const keepAlive=setInterval(()=>{},250),stores=[],servers=[],checks=[];
const a={linkId:'a'.repeat(43),deviceId:'synthetic-a',connectorId:'synthetic:user:a',token:'a'.repeat(48)};
const b={linkId:'b'.repeat(43),deviceId:'synthetic-b',connectorId:'synthetic:user:b',token:'b'.repeat(48)};
const c={...b,linkId:a.linkId};
const pause=ms=>new Promise(r=>setTimeout(r,ms));
const registration=auth=>({...auth,scope:'CurrentUser',capabilities:['command'],agent:{id:'opencode',provider:'gonka',available:true}});
const input=(auth,id)=>({linkId:auth.linkId,deviceId:auth.deviceId,kind:'command',text:'echo synthetic',requestId:id});
async function fixture(name,api=false){
  let server,origin,store;
  const dataDir=path.join(root,name);
  if(api){const app=express();({store}=attachConnectorApi(app,{dataDir}));server=createServer(app);servers.push(server);await new Promise(r=>server.listen(0,'127.0.0.1',r));origin=`http://127.0.0.1:${server.address().port}`;}
  else store=createConnectorStore(dataDir);
  stores.push(store);
  for(const auth of [a,b,c])assert.equal((await store.register(registration(auth),auth.token)).ok,true);
  return {store,origin};
}
try{
  for(const [name,other] of [['different-link',b],['same-link-different-device',c]]){
    const {store}=await fixture(name);
    const start=performance.now();
    const [created,polled]=await Promise.all([store.createJob(input(a,name)),store.poll(other,1200)]);
    const elapsed=Math.round(performance.now()-start);
    assert.equal(created.ok,true);assert.equal(polled.ok,true);assert.equal(polled.jobs.length,0);
    assert.ok(elapsed>=1100,`${name} woke after ${elapsed}ms`);
    checks.push({name,requestedWaitMs:1200,elapsedMs:elapsed});
  }
  for(const target of ['matching-device','untargeted-link']){
    const {store}=await fixture(target);
    const lease=store.lease.bind(store);let enter,release;
    const entered=new Promise(r=>{enter=r;}),barrier=new Promise(r=>{release=r;});
    let first=true;store.lease=async auth=>{const v=await lease(auth);if(first){first=false;enter();await barrier;}return v;};
    const pending=store.poll(a,25000);await entered;
    const value=input(a,target);if(target==='untargeted-link')delete value.deviceId;
    const created=await store.createJob(value);const start=performance.now();release();
    const result=await Promise.race([pending,pause(1500).then(()=>{throw new Error(`${target} missed wake`);})]);
    assert.equal(result.jobs[0].id,created.job.id);
    assert.equal(store.events.eventNames().length,0);
    checks.push({name:`read-subscribe-race-${target}`,elapsedMs:Math.round(performance.now()-start)});
  }
  for(const [name,other] of [['http-different-link',b],['http-same-link-different-device',c]]){
    const {store,origin}=await fixture(name,true);
    const request=async(route,auth,body)=>{
      const res=await fetch(origin+route,{method:body?'POST':'GET',headers:{'x-soty-link-id':auth.linkId,'x-soty-device-id':auth.deviceId,'x-soty-connector-id':auth.connectorId,authorization:`Bearer ${auth.token}`,...(body?{'content-type':'application/json'}:{})},...(body?{body:JSON.stringify(body)}:{}),signal:AbortSignal.timeout(6000)});
      assert.ok(res.ok,`${route}: ${res.status}`);return await res.json();
    };
    // Hold the actual HTTP poll between its committed empty read and subscribe.
    const lease=store.lease.bind(store);let enter,release;
    const entered=new Promise(r=>{enter=r;}),barrier=new Promise(r=>{release=r;});let first=true;
    store.lease=async auth=>{const v=await lease(auth);if(first){first=false;enter();await barrier;}return v;};
    let settled=false;const pending=request('/api/connectors/poll?wait=1',other).then(v=>{settled=true;return v;});
    await entered;
    // Create uses owner authorization, not a connector bearer token.
    const create=async auth=>{
      const res=await fetch(origin+'/api/connectors/jobs',{method:'POST',headers:{'x-soty-link-id':auth.linkId,'content-type':'application/json'},body:JSON.stringify(input(auth,name+'-'+auth.deviceId)),signal:AbortSignal.timeout(4000)});
      assert.equal(res.status,201);return await res.json();
    };
    await create(a);release();await pause(1200);
    assert.equal(settled,false,`${name} returned for unrelated route`);
    const start=performance.now(),own=await create(other);
    const result=await Promise.race([pending,pause(1500).then(()=>{throw new Error('matching HTTP wake missing');})]);
    assert.equal(result.jobs[0].id,own.job.id);assert.equal(store.events.eventNames().length,0);
    checks.push({name,unrelatedWaitObservedMs:1200,matchingWakeMs:Math.round(performance.now()-start)});
  }
  const {store}=await fixture('controller-events');
  const created=await store.createJob(input(a,'event-watch'));
  const page=await store.getEvents(a.linkId,created.job.id);
  await store.createJob(input(c,'unrelated-event'));
  const start=performance.now();
  await store.waitForControllerChange(a.linkId,a.deviceId,1200,undefined,page.changeVersion);
  assert.ok(performance.now()-start>=1100,'event watch woke for another device');
  const version=(await store.getEvents(a.linkId,created.job.id)).changeVersion;
  await store.cancelJob(a.linkId,created.job.id);
  const matched=performance.now();
  await store.waitForControllerChange(a.linkId,a.deviceId,25000,undefined,version);
  assert.ok(performance.now()-matched<250,'event watch lost matching change');
  checks.push({name:'controller-event-watch-scoped-version-and-missed-wake',ok:true});
  console.log(JSON.stringify({ok:true,node:process.version,scope:'loopback synthetic; no commands executed',checks},null,2));
}finally{
  for(const server of servers){server.closeAllConnections();await new Promise(r=>server.close(r));}
  for(const store of stores)await store.close();
  clearInterval(keepAlive);await rm(root,{recursive:true,force:true});
}
