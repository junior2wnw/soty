import test from 'node:test';
import assert from 'node:assert/strict';
import {Writable} from 'node:stream';
import {fixture} from '../../test/strict-sender-fixture.mjs';
import {sendAuthenticatedBackup} from './staged-test-adapter.mjs';
import {createReadinessBoundary} from './readiness-boundary.mjs';
const spec={transaction:'1234567890abcdef1234567890abcdef',nonce:'2'.repeat(32),image:'sha256:'+'3'.repeat(64),receiverSourceSha256:'4'.repeat(64)};
const warm=s=>({...s,inputBytes:0,verifiedBeforeStop:true});
const ready=e=>({transaction:e.transaction,nonce:e.nonce,expectedSha256:e.expectedSha256,expectedManifestSha256:e.expectedManifestSha256,...e.sourceWitness,inputBytes:0});
const delay=ms=>new Promise(r=>setTimeout(r,ms));
test('ready accessor is rejected without executing getter or starting sender body',async t=>{
 const f=await fixture(t),encrypted=await f.encrypt();let boundary,lease,cancelled=0,bytes=0;
 boundary=createReadinessBoundary({prepareBeforeStop:warm,bindAfterAuthentication:e=>{const value=ready(e);Object.defineProperty(value,'nonce',{enumerable:true,get(){cancelled++;boundary.cancel(lease);return e.nonce;}});return value;}});
 lease=await boundary.warmBeforeServingStop(spec);boundary.noteServingStopped(lease);boundary.captureLaterBackup(lease,{expectedSha256:encrypted.options.expectedSha256,expectedManifestSha256:encrypted.options.expectedManifestSha256,sourceWitness:encrypted.options.sourceWitness});
 const output=new Writable({write(chunk,_enc,cb){bytes+=chunk.length;cb();},autoDestroy:true,emitClose:true});
 let error;try{await sendAuthenticatedBackup({...encrypted.options,output,beforeBody:boundary.beforeBody(lease)});}catch(e){error=e.code;}
 assert.equal(cancelled,0);assert.equal(bytes,0,'accessor must not emit body');assert.equal(error,'restore_io_failed');assert.equal(output.closed,true);
});
test('warm accessor is rejected without executing getter or returning a warm lease',async()=>{
 const c=new AbortController();let calls=0;const boundary=createReadinessBoundary({prepareBeforeStop:s=>{const value=warm(s);Object.defineProperty(value,'verifiedBeforeStop',{enumerable:true,get(){calls++;c.abort();return true;}});return value;},bindAfterAuthentication:ready});
 await assert.rejects(boundary.warmBeforeServingStop(spec,{signal:c.signal}),{code:'readiness_warm_invalid'});assert.equal(calls,0);assert.equal(c.signal.aborted,false);
});
test('cancel never bind settles prompt; native sender output closes and late ready remains unread',async t=>{
 const f=await fixture(t),e=await f.encrypt();let release,entered;const entry=new Promise(r=>entered=r);let reads=0,bytes=0;
 const boundary=createReadinessBoundary({prepareBeforeStop:warm,bindAfterAuthentication:()=>{entered();return new Promise(r=>release=r);}}),lease=await boundary.warmBeforeServingStop(spec);
 boundary.noteServingStopped(lease);boundary.captureLaterBackup(lease,{expectedSha256:e.options.expectedSha256,expectedManifestSha256:e.options.expectedManifestSha256,sourceWitness:e.options.sourceWitness});
 const output=new Writable({write(chunk,_enc,cb){bytes+=chunk.length;cb();},autoDestroy:true,emitClose:true});
 const sent=sendAuthenticatedBackup({...e.options,output,beforeBody:boundary.beforeBody(lease)}).then(()=>({ok:true}),e=>({code:e.code}));
 await entry;boundary.cancel(lease);assert.deepEqual(await Promise.race([sent,delay(100).then(()=>({timeout:true}))]),{code:'restore_io_failed'});assert.equal(output.closed,true);assert.equal(bytes,0);
 release(new Proxy({}, {ownKeys(){reads++;throw Error('private');},get(_target,key){if(key==='then')return undefined;reads++;throw Error('private');}}));await delay(5);assert.equal(reads,0);
});
