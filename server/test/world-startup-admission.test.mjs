import test from 'node:test';
import assert from 'node:assert/strict';
import {readFile} from 'node:fs/promises';
import {types} from 'node:util';
import {captureWorldAttachmentWrites,loadWorldConfiguration} from '../world-configuration.js';

// Drop-in server/test control: source packets use public fallback copies only
// when unchanged Native support files are absent from the four-file candidate.
async function publicText(local,fallback) {
  try{return await readFile(new URL(local,import.meta.url),'utf8');}
  catch(e){if(e.code!=='ENOENT')throw e;return readFile(new URL(fallback,import.meta.url),'utf8');}
}
let validation;
try{validation=await import('../../modules/capabilities/server/validation.mjs');}
catch(e){if(e.code!=='ERR_MODULE_NOT_FOUND')throw e;validation=await import('../../../peer-inputs/capability-validation.mjs');}
const {AccessError}=validation;
const http=await readFile(new URL('../http-app.js',import.meta.url),'utf8');
const native=await publicText('../native-source-handlers.js','../../../peer-inputs/native-source-handlers.js');
const nativeCapture=new Function('types','AccessError',native.slice(native.indexOf('const invalid =')).replace(/^export /gmu,'')+'\nreturn captureNativeSourceHandlers;')(types,AccessError);
const outerStart=http.indexOf('export function createHttpApp('),outerEnd=http.indexOf('\nfunction createHttpAppWithNativeSources(',outerStart);
assert(outerStart>=0&&outerEnd>outerStart);
const outer=http.slice(outerStart,outerEnd).replace(/^export /u,'');
const waiterStart=http.indexOf('export async function waitForRejectedHttpStart(');
assert(waiterStart>=0&&waiterStart<outerStart);
const waiter=http.slice(waiterStart,outerStart).replace(/^export /u,'');
function fixture(inner=(_dist,_options,_native,writes)=>writes) {
  let captures=0;const rejectedStarts=new WeakMap();
  const call=new Function('types','AccessError','captureWorldAttachmentWrites','captureNativeSourceHandlers','createHttpAppWithNativeSources','rejectedStarts','return ('+outer+');')(
    types,AccessError,captureWorldAttachmentWrites,options=>{captures++;return nativeCapture(options);},inner,rejectedStarts);
  const wait=new Function('rejectedStarts','return ('+waiter+');')(rejectedStarts);
  return {call:options=>call('PUBLIC-INERT',options),wait,captures:()=>captures};
}

test('World host flag is strict own bool/defaultOFF; ENV accepts only exact0/1 strings',()=>{
  const f=fixture();assert.equal(f.call(),false);assert.equal(f.call({worldAttachmentWrites:undefined}),false);
  for(const value of [false,true])assert.equal(f.call({worldAttachmentWrites:value}),value);
  for(const value of [null,0,1,'1','true',{},[]])assert.throws(()=>f.call({worldAttachmentWrites:value}),{code:'world_attachment_configuration_invalid'});
  for(const value of ['', '0','1'])assert.equal(loadWorldConfiguration({SOTY_WORLD_ATTACHMENT_WRITES:value}).worldAttachmentWrites,value==='1');
  for(const value of [null,undefined,0,1,'true',' 1'])assert.throws(()=>loadWorldConfiguration({SOTY_WORLD_ATTACHMENT_WRITES:value}),{code:'world_attachment_configuration_invalid'});
});

test('invalid top-level options keep Native error without traps/getters/capture',()=>{
  const f=fixture();let effects=0;
  const trap=()=>{effects++;throw new Error('PUBLIC_UNEXPECTED_TRAP');};
  const proxy=new Proxy({},{get:trap,getPrototypeOf:trap,getOwnPropertyDescriptor:trap});
  const revoked=Proxy.revocable({},{});revoked.revoke();class PublicClass{}
  const accessor=[];Object.defineProperty(accessor,'worldAttachmentWrites',{get:trap});
  for(const value of [null,0,true,'plain',()=>{},[],accessor,new PublicClass(),Object.create({}),proxy,revoked.proxy])
    assert.throws(()=>f.call(value),{code:'native_source_configuration_invalid'});
  assert.equal(effects,0);assert.equal(f.captures(),0);
});

test('World accessor refusal precedes Native capture; descriptor snapshot cannot change afterward',()=>{
  const f=fixture();let reads=0;const options={};
  Object.defineProperty(options,'worldAttachmentWrites',{get(){reads++;return reads>1;}});
  assert.throws(()=>f.call(options),{code:'world_attachment_configuration_invalid'});assert.equal(reads,0);assert.equal(f.captures(),0);
  const mutable={worldAttachmentWrites:false},next=fixture((_dist,actual,_native,writes)=>{actual.worldAttachmentWrites=true;return writes;});
  assert.equal(next.call(mutable),false);assert.equal(mutable.worldAttachmentWrites,true);
  const own=Object.create(null);Object.defineProperty(own,'worldAttachmentWrites',{value:true});assert.equal(f.call(own),true);
});

test('Native field Proxy still fails original Native descriptor guard with zero callbacks',()=>{
  const f=fixture();let effects=0;const proxy=new Proxy([],{get(){effects++;},getPrototypeOf(){effects++;}});
  assert.throws(()=>f.call({nativeSourceHandlers:proxy}),{code:'native_source_configuration_invalid'});
  assert.equal(f.captures(),1);assert.equal(effects,0);
});

test('accepted Native owner closes once after inner rejection and waiter waits for cleanup',async()=>{
  const failure=new AccessError('PUBLIC_INNER_REJECTED');const f=fixture(()=>{throw failure;});
  let closeCalls=0,release,settled=false;const gate=new Promise(resolve=>{release=resolve;});
  assert.throws(()=>f.call({nativeSourceHandlers:[{handle:()=>false,close(){closeCalls++;return gate;}}]}),e=>e===failure);
  const pending=f.wait(failure).then(()=>{settled=true;});await Promise.resolve();await Promise.resolve();
  assert.equal(settled,false);assert.equal(closeCalls,1);release();await pending;await f.wait(failure);assert.equal(closeCalls,1);
});
