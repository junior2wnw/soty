#!/usr/bin/env node
import assert from 'node:assert/strict';
import { createHash } from 'node:crypto';
import { mkdtemp, readFile, writeFile } from 'node:fs/promises';
import { tmpdir } from 'node:os';
import path from 'node:path';
import { pathToFileURL } from 'node:url';
import ts from 'typescript';
import { createRequestJournal, reconcileCreate } from '../src/features/connector-request-journal.ts';
const key='soty:connector:create-journal:v1';
class MemoryStorage {
  values=new Map();
  getItem(key){return this.values.get(key)??null;}
  setItem(key,value){this.values.set(key,String(value));}
  removeItem(key){this.values.delete(key);}
}
let sequence=0;
const digest=async text=>createHash('sha256').update(text).digest('hex');
let queue=Promise.resolve();
const lock=operation=>{const result=queue.then(operation);queue=result.catch(()=>undefined);return result;};
const factory=storage=>createRequestJournal({storage,digest,randomId:()=>`synthetic-request-${++sequence}`,lock});
const storage=new MemoryStorage();
let journal=factory(storage);
const reservation=await journal.reserve({auth:'secret-owner',body:{context:'private-context',text:'task'}});
assert.equal(reservation.reused,false);
assert.doesNotMatch(storage.getItem(key),/secret-owner|private-context|task/);
journal=factory(storage);
const reused=await journal.reserve({body:{text:'task',context:'private-context'},auth:'secret-owner'});
assert.equal(reused.requestId,reservation.requestId);
assert.equal(reused.reused,true);
await journal.acknowledge(reused);
assert.notEqual((await journal.reserve({auth:'secret-owner',body:{context:'private-context',text:'task'}})).requestId,reservation.requestId);
const full=new MemoryStorage();const fullJournal=factory(full);
await Promise.all(Array.from({length:32},(_,i)=>fullJournal.reserve({i})));
const fullBefore=full.getItem(key);
await assert.rejects(fullJournal.reserve({i:33}),/full/);
assert.equal(full.getItem(key),fullBefore);
assert.equal((await fullJournal.reserve({i:0})).reused,true);
await assert.rejects(factory({getItem:()=>null,setItem:()=>{throw new Error('quota');}}).reserve({safe:true}),/quota/);
await assert.rejects(factory({getItem:()=>null,setItem:()=>{}}).reserve({safe:true}),/unconfirmed/);
await assert.rejects(factory({getItem:()=>'{',setItem:()=>{}}).reserve({safe:true}));
let attempts=0;
const recovered=await reconcileCreate(async()=>{if(++attempts===1)throw new Error('response dropped');return{ok:true,httpStatus:201,job:{id:'job_abc'}};});
assert.equal(attempts,2);assert.equal(recovered.response.job.id,'job_abc');assert.equal(recovered.ambiguous,true);
attempts=0;await reconcileCreate(async()=>{attempts++;return{ok:false,httpStatus:409};});assert.equal(attempts,1);
attempts=0;const malformed=await reconcileCreate(async()=>{attempts++;return{ok:true,httpStatus:201};});assert.equal(attempts,2);assert.equal(malformed.ambiguous,true);
const controller=new AbortController();attempts=0;
const cancelled=await reconcileCreate(async()=>{attempts++;controller.abort();throw new Error('lost after cancel');},controller.signal);
assert.equal(attempts,1);assert.equal(cancelled.ambiguous,true);

// Exercise the actual UI entry point with a storage/HTTP shim; no browser or provider is contacted.
const fixture=await mkdtemp(path.join(tmpdir(),'soty-ui-request-'));
const bundle=path.join(fixture,'connector.mjs');
const compiled=ts.transpileModule(await readFile('src/features/connector.ts','utf8'),{compilerOptions:{module:ts.ModuleKind.ESNext,target:ts.ScriptTarget.ES2022}}).outputText;
await writeFile(bundle,compiled.replace('./connector-request-journal',pathToFileURL(path.resolve('src/features/connector-request-journal.ts')).href));
const browserStorage=new MemoryStorage();browserStorage.setItem('soty:connector:link-id','l'.repeat(43));
const namedQueues=new Map();
const namedLock=(name,operation)=>{const result=(namedQueues.get(name)||Promise.resolve()).then(operation);namedQueues.set(name,result.catch(()=>undefined));return result;};
Object.defineProperty(globalThis,'navigator',{value:{locks:{request:namedLock}},configurable:true});
globalThis.localStorage=browserStorage;
globalThis.window={setTimeout,clearTimeout};
let ui=await import(pathToFileURL(bundle));
const input={deviceId:'synthetic-device',threadId:'synthetic-thread',input:{kind:'command',text:'private-command',context:'private-context'}};
let bodies=[];
let mode='drop';let overlapCalls=0;let releaseOverlap;const overlapGate=new Promise(resolve=>{releaseOverlap=resolve;});
globalThis.fetch=async(url,options)=>{
 if(url==='/api/connectors/jobs'){
  const body=JSON.parse(options.body);bodies.push(body);
  assert.ok(browserStorage.getItem(key).includes(body.requestId),'identity durable before POST');
  assert.doesNotMatch(browserStorage.getItem(key),/private-command|private-context/);
  if(mode==='overlap'){overlapCalls++;if(overlapCalls===1)await overlapGate;if(overlapCalls<=2)throw new TypeError('overlapped response loss');}
  if(mode==='drop')throw new TypeError('synthetic response loss');
  if(mode==='conflict')return new Response(JSON.stringify({ok:false,error:'job-request-conflict'}),{status:409});
  if(mode==='reject')return new Response(JSON.stringify({ok:false,error:'connector-access-denied'}),{status:403});
  return new Response(JSON.stringify({ok:true,job:{id:'job_abcdef',status:'succeeded'}}),{status:201});
 }
 if(String(url).includes('/events'))return new Response(JSON.stringify({ok:true,events:[],done:true,job:{status:'succeeded',result:{ok:true,exitCode:0,text:'known-native-result'}}}));
 throw new Error('Unexpected synthetic route');
};
const ambiguous=await ui.runConnectorJob(input);
assert.equal(ambiguous.ok,false);assert.equal(bodies.length,2);assert.equal(bodies[0].requestId,bodies[1].requestId);
const retainedId=bodies[0].requestId;assert.match(ambiguous.text,new RegExp(retainedId));
// Reload module while retaining browser storage: retry resolves the same server-side request.
ui=await import(pathToFileURL(bundle).href+'?reload=1');mode='ack';
const completed=await ui.runConnectorJob(input);assert.equal(completed.ok,true);assert.equal(completed.text,'known-native-result');assert.equal(bodies[2].requestId,retainedId);
assert.equal(JSON.parse(browserStorage.getItem(key)).entries.length,0);
await ui.runConnectorJob(input);assert.notEqual(bodies[3].requestId,retainedId);
mode='conflict';const count=bodies.length;await ui.runConnectorJob(input);assert.equal(bodies.length,count+1);const conflicted=bodies.at(-1).requestId;
await ui.runConnectorJob(input);assert.equal(bodies.at(-1).requestId,conflicted);assert.equal(JSON.parse(browserStorage.getItem(key)).entries.length,1);
mode='ack';await ui.runConnectorJob(input);
mode='reject';await ui.runConnectorJob(input);assert.equal(JSON.parse(browserStorage.getItem(key)).entries.length,0);
const originalSet=browserStorage.setItem.bind(browserStorage);browserStorage.setItem=(k,v)=>{if(k===key)throw new Error('quota');return originalSet(k,v);};
const beforeFailure=bodies.length;assert.equal((await ui.runConnectorJob(input)).ok,false);assert.equal(bodies.length,beforeFailure);
browserStorage.setItem=(k,v)=>{if(k==='soty:connector:pending-jobs:v1')throw new Error('pending quota');return originalSet(k,v);};
mode='ack';const trackingFailure=await ui.runConnectorJob(input);assert.equal(trackingFailure.ok,false);assert.match(trackingFailure.text,/job_abcdef/);const acceptedRequest=bodies.at(-1).requestId;assert.ok(browserStorage.getItem(key).includes(acceptedRequest));
browserStorage.setItem=originalSet;assert.equal((await ui.runConnectorJob(input)).ok,true);assert.equal(bodies.at(-1).requestId,acceptedRequest);
mode='overlap';const overlappedStart=bodies.length;const firstLogical=ui.runConnectorJob(input);const queuedRetry=ui.runConnectorJob(input);
for(let i=0;overlapCalls<1&&i<100;i++)await new Promise(resolve=>setTimeout(resolve,2));
assert.equal(overlapCalls,1);await new Promise(resolve=>setTimeout(resolve,20));assert.equal(overlapCalls,1,'queued same-fingerprint submission must not race current ACK');releaseOverlap();
const overlapping=await Promise.all([firstLogical,queuedRetry]);assert.equal(overlapping[0].ok,false);assert.equal(overlapping[1].ok,true);assert.equal(bodies.length,overlappedStart+3);assert.equal(new Set(bodies.slice(overlappedStart).map(body=>body.requestId)).size,1);
mode='ack';const acknowledgedStart=bodies.length;const separate=await Promise.all([ui.runConnectorJob(input),ui.runConnectorJob(input)]);assert.ok(separate.every(result=>result.ok));assert.notEqual(bodies[acknowledgedStart].requestId,bodies[acknowledgedStart+1].requestId,'two calls after definite ACK are distinct logical requests, not double-click dedupe');
console.log(JSON.stringify({ok:true,fixture,checks:['opaque journal before dispatch','stable canonical fingerprint and reload identity','ACK permits intentional new request','cross-request lock serialization','capacity32 retains unresolved entries','quota/silent-write/corruption fail closed','transport retries same key once','409 no blind retry','cancel after ambiguous retains uncertainty','actual UI dropped response and module reload reconcile same ID','actual UI pending/native history maintained','actual UI storage failure sends no request','malformed success remains ambiguous','accepted job tracking failure retains identity for recovery','overlapping failed submission retains key for queued retry until definite ACK','two acknowledged calls remain distinct logical requests']}));
