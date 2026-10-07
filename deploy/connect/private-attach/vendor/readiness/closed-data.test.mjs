import test from 'node:test';
import assert from 'node:assert/strict';
import { Writable } from 'node:stream';
import { readFile } from 'node:fs/promises';
import { createHash } from 'node:crypto';
import { fixture } from '../../test/strict-sender-fixture.mjs';
import { createReadinessBoundary } from './readiness-boundary.mjs';
import { sendAuthenticatedBackup } from './staged-test-adapter.mjs';
const SPEC = Object.freeze({ transaction:'1234567890abcdef1234567890abcdef', nonce:'2'.repeat(32), image:'sha256:'+'3'.repeat(64), receiverSourceSha256:'4'.repeat(64) });
const warm = spec => ({ ...spec, inputBytes:0, verifiedBeforeStop:true });
const ready = expected => ({ transaction:expected.transaction,nonce:expected.nonce,
  expectedSha256:expected.expectedSha256,expectedManifestSha256:expected.expectedManifestSha256,
  ...expected.sourceWitness,inputBytes:0 });
const receipt = encrypted => ({expectedSha256:encrypted.options.expectedSha256,
  expectedManifestSha256:encrypted.options.expectedManifestSha256,sourceWitness:encrypted.options.sourceWitness});
const delay = ms => new Promise(resolve => setTimeout(resolve,ms));
const deferred = () => { let resolve,reject;const promise=new Promise((ok,bad)=>{resolve=ok;reject=bad;});return{promise,resolve,reject}; };
const sha = bytes => createHash('sha256').update(bytes).digest('hex');
function collected() {let bytes=0;const stream=new Writable({autoDestroy:true,emitClose:true,write(chunk,_enc,done){bytes+=chunk.length;done();}});return{stream,bytes:()=>bytes};}
async function run(t, bindAfterAuthentication, extra = {}) {
  const f=await fixture(t),encrypted=await f.encrypt();
  const boundary=createReadinessBoundary({prepareBeforeStop:warm,bindAfterAuthentication,...extra});
  const lease=await boundary.warmBeforeServingStop(SPEC);boundary.noteServingStopped(lease);boundary.captureLaterBackup(lease,receipt(encrypted));
  const output=collected();
  return{boundary,lease,output,encrypted,start:()=>sendAuthenticatedBackup({...encrypted.options,output:output.stream,beforeBody:boundary.beforeBody(lease)})};
}

test('all six warm accessors deny without executing any getter',async()=>{
  let calls=0;
  for(const field of ['transaction','nonce','image','receiverSourceSha256','inputBytes','verifiedBeforeStop']){
    const boundary=createReadinessBoundary({prepareBeforeStop:spec=>{
      const result=warm(spec);Object.defineProperty(result,field,{enumerable:true,get(){calls++;return result[field];}});return result;
    },bindAfterAuthentication:ready});
    await assert.rejects(boundary.warmBeforeServingStop(SPEC),{code:'readiness_warm_invalid'});
    await assert.rejects(boundary.warmBeforeServingStop(SPEC),{code:'readiness_capacity_exhausted'});
  }
  assert.equal(calls,0);
});

test('all eight ready accessors deny actual body and are never executed',async t=>{
  let calls=0;
  for(const field of ['transaction','nonce','expectedSha256','expectedManifestSha256','generationId','checkpointSha256','inventorySha256','inputBytes']){
    const f=await run(t,expected=>{const value=ready(expected);Object.defineProperty(value,field,{enumerable:true,get(){calls++;return expected[field];}});return value;});
    await assert.rejects(f.start(),{code:'restore_io_failed'});
    assert.equal(f.output.bytes(),0);assert.equal(f.output.stream.closed,true);
  }
  assert.equal(calls,0);
});

test('sync warm Proxy/symbol/hidden field/then getter deny without trapping or then assimilation',async()=>{
  let traps=0;
  for(const make of [
    spec=>new Proxy(warm(spec),{ownKeys(){traps++;throw Error('must_not_run');},get(){traps++;throw Error('must_not_run');}}),
    spec=>Object.assign(warm(spec),{[Symbol('hidden')]:true}),
    spec=>Object.defineProperty(warm(spec),'hidden',{value:true}),
    spec=>Object.defineProperty(warm(spec),'then',{enumerable:true,get(){traps++;throw Error('must_not_run');}}),
  ]){
    const boundary=createReadinessBoundary({prepareBeforeStop:make,bindAfterAuthentication:ready});
    await assert.rejects(boundary.warmBeforeServingStop(SPEC),{code:'readiness_warm_invalid'});
  }
  assert.equal(traps,0);
});

test('sync ready Proxy/symbol/hidden field/then getter deny actual body without traps',async t=>{
  let traps=0;
  for(const make of [
    expected=>new Proxy(ready(expected),{ownKeys(){traps++;throw Error('must_not_run');},get(){traps++;throw Error('must_not_run');}}),
    expected=>Object.assign(ready(expected),{[Symbol('hidden')]:true}),
    expected=>Object.defineProperty(ready(expected),'hidden',{value:true}),
    expected=>Object.defineProperty(ready(expected),'then',{enumerable:true,get(){traps++;throw Error('must_not_run');}}),
  ]){
    const f=await run(t,make);await assert.rejects(f.start(),{code:'restore_io_failed'});
    assert.equal(f.output.bytes(),0);assert.equal(f.output.stream.closed,true);
  }
  assert.equal(traps,0);
});

test('constructor/spec/control/sourceWitness getters and symbols never execute',async t=>{
  let reads=0;
  const input={prepareBeforeStop:warm,bindAfterAuthentication:ready};
  Object.defineProperty(input,'emit',{enumerable:true,get(){reads++;return()=>{};}});
  assert.throws(()=>createReadinessBoundary(input),{code:'readiness_port_invalid'});
  const boundary=createReadinessBoundary({prepareBeforeStop:warm,bindAfterAuthentication:ready});
  const spec={...SPEC};Object.defineProperty(spec,'nonce',{enumerable:true,get(){reads++;return SPEC.nonce;}});
  await assert.rejects(boundary.warmBeforeServingStop(spec),{code:'readiness_spec_invalid'});
  const control={};Object.defineProperty(control,'signal',{enumerable:true,get(){reads++;return new AbortController().signal;}});
  await assert.rejects(boundary.warmBeforeServingStop(SPEC,control),{code:'readiness_signal_invalid'});
  const f=await fixture(t),encrypted=await f.encrypt(),lease=await boundary.warmBeforeServingStop(SPEC);
  boundary.noteServingStopped(lease);
  const proof=receipt(encrypted);Object.defineProperty(proof.sourceWitness,'generationId',{enumerable:true,get(){reads++;return SPEC.transaction;}});
  assert.throws(()=>boundary.captureLaterBackup(lease,proof),{code:'readiness_backup_invalid'});
  assert.equal(reads,0);
});

test('non-string IDs cannot execute primitive coercion while validating bootstrap',async()=>{
  let reads=0,prepared=0;
  const trick={toString(){reads++;return SPEC.transaction;},[Symbol.toPrimitive](){reads++;return SPEC.transaction;}};
  const boundary=createReadinessBoundary({prepareBeforeStop:spec=>{prepared++;return warm(spec);},bindAfterAuthentication:ready});
  await assert.rejects(boundary.warmBeforeServingStop({...SPEC,transaction:trick}),{code:'readiness_spec_invalid'});
  assert.equal(reads,0);assert.equal(prepared,0);
});

test('native Promise shadow then getter is never read by port observation',async()=>{
  let reads=0;
  const boundary=createReadinessBoundary({prepareBeforeStop:spec=>{
    const promise=Promise.resolve(warm(spec));Object.defineProperty(promise,'then',{get(){reads++;throw Error('must_not_run');}});return promise;
  },bindAfterAuthentication:ready});
  const lease=await boundary.warmBeforeServingStop(SPEC);
  assert.ok(lease);assert.equal(reads,0);boundary.cancel(lease);
});

test('native Promise constructor accessor rejects without calling getter or returning a lease',async()=>{
  let reads=0;
  const boundary=createReadinessBoundary({prepareBeforeStop:spec=>{
    const promise=Promise.resolve(warm(spec));Object.defineProperty(promise,'constructor',{get(){reads++;return Promise;}});return promise;
  },bindAfterAuthentication:ready});
  await assert.rejects(boundary.warmBeforeServingStop(SPEC),{code:'readiness_port_failed'});
  assert.equal(reads,0);
  await assert.rejects(boundary.warmBeforeServingStop(SPEC),{code:'readiness_capacity_exhausted'});
});

test('cancellation from bound-read0 telemetry is fenced before actual body commit',async t=>{
  let current,cancels=0;
  const f=await run(t,ready,{emit:packet=>{if(packet.phase==='bound_read0_inside_sender_budget'){cancels++;current.boundary.cancel(current.lease);}}});current=f;
  await assert.rejects(f.start(),{code:'restore_io_failed'});
  assert.equal(cancels,1);assert.equal(f.output.bytes(),0);assert.equal(f.output.stream.closed,true);
});

test('reentrant cancellation from the post-ready trusted clock check cannot commit actual body',async t=>{
  let readyReturned=false,checks=0;
  const f=await run(t,expected=>{readyReturned=true;return ready(expected);});
  const callback=f.boundary.beforeBody(f.lease);
  await assert.rejects(sendAuthenticatedBackup({...f.encrypted.options,output:f.output.stream,beforeBody:fence=>callback({
    authenticated:fence.authenticated,check(){fence.check();checks++;if(readyReturned)f.boundary.cancel(f.lease);}
  })}),{code:'restore_io_failed'});
  assert.ok(checks>=2);assert.equal(f.output.bytes(),0);assert.equal(f.output.stream.closed,true);
});

test('revoked Proxy/inherited getter do not enter metadata reflection or become own fields',async()=>{
  let getters=0;
  const inherited=Object.create({get transaction(){getters++;return SPEC.transaction;}});
  Object.assign(inherited,{nonce:SPEC.nonce,image:SPEC.image,receiverSourceSha256:SPEC.receiverSourceSha256});
  const boundary=createReadinessBoundary({prepareBeforeStop:warm,bindAfterAuthentication:ready});
  await assert.rejects(boundary.warmBeforeServingStop(inherited),{code:'readiness_spec_invalid'});
  const pair=Proxy.revocable({...SPEC},{});pair.revoke();
  await assert.rejects(boundary.warmBeforeServingStop(pair.proxy),{code:'readiness_spec_invalid'});
  assert.equal(getters,0);
});

test('native abort from warm-read0 telemetry is fenced before publishing a lease',async()=>{
  const controller=new AbortController();let prepared=0;
  const boundary=createReadinessBoundary({prepareBeforeStop:spec=>{prepared++;return warm(spec);},bindAfterAuthentication:ready,
    emit:packet=>{if(packet.phase==='warm_read0_before_stop')controller.abort();}});
  await assert.rejects(boundary.warmBeforeServingStop(SPEC,{signal:controller.signal}),{code:'readiness_cancelled'});
  assert.equal(prepared,1);
  await assert.rejects(boundary.warmBeforeServingStop(SPEC),{code:'readiness_capacity_exhausted'});
});

test('sync warm reentry from emit and prepare cannot dispatch another actual port job',async()=>{
  for(const stage of ['emit','prepare']){
    let boundary,nested,prepared=0;
    boundary=createReadinessBoundary({prepareBeforeStop:spec=>{
      prepared++;if(stage==='prepare')nested=boundary.warmBeforeServingStop(SPEC).catch(error=>error.code);return warm(spec);
    },bindAfterAuthentication:ready,emit:packet=>{if(stage==='emit'&&packet.phase==='warming_before_stop')nested=boundary.warmBeforeServingStop(SPEC).catch(error=>error.code);}});
    const lease=await boundary.warmBeforeServingStop(SPEC);
    assert.equal(await nested,'readiness_capacity_exhausted');assert.equal(prepared,1);boundary.cancel(lease);
  }
});

test('128 native-cancel cycles invoke ONE unresolved job and deny127; late foreign result cannot free it',async()=>{
  let invoked=0,resolvePort,metadataReads=0;
  const boundary=createReadinessBoundary({prepareBeforeStop(){invoked++;return new Promise(resolve=>{resolvePort=resolve;});},bindAfterAuthentication:ready});
  const outcomes=[];
  for(let i=0;i<128;i++){
    const c=new AbortController(),pending=boundary.warmBeforeServingStop(SPEC,{signal:c.signal}).catch(error=>error.code);
    await Promise.resolve();c.abort();outcomes.push(await pending);
  }
  assert.equal(invoked,1);assert.equal(outcomes.filter(code=>code==='readiness_cancelled').length,1);
  assert.equal(outcomes.filter(code=>code==='readiness_capacity_exhausted').length,127);
  const foreign={...warm(SPEC),transaction:'9'.repeat(32)};
  Object.defineProperty(foreign,'verifiedBeforeStop',{enumerable:true,get(){metadataReads++;return true;}});
  resolvePort(foreign);await delay(5);
  assert.equal(metadataReads,0);
  await assert.rejects(boundary.warmBeforeServingStop(SPEC),{code:'readiness_capacity_exhausted'});
  assert.equal(invoked,1);
});

test('real prepare settlement success/failure/local cancel never resets single-use capacity',async()=>{
  for(const mode of ['success','throw','reject']){
    let invoked=0;
    const boundary=createReadinessBoundary({prepareBeforeStop:spec=>{
      invoked++;if(mode==='throw')throw Error('fixture');if(mode==='reject')return Promise.reject(Error('fixture'));return warm(spec);
    },bindAfterAuthentication:ready});
    if(mode==='success'){const lease=await boundary.warmBeforeServingStop(SPEC);boundary.cancel(lease);}
    else await assert.rejects(boundary.warmBeforeServingStop(SPEC),{code:'readiness_port_failed'});
    await assert.rejects(boundary.warmBeforeServingStop(SPEC),{code:'readiness_capacity_exhausted'});
    assert.equal(invoked,1);
  }
});

test('invalid input/pre-aborted no-port attempt cannot consume or dispatch the real slot',async()=>{
  let invoked=0;const c=new AbortController();c.abort();
  const boundary=createReadinessBoundary({prepareBeforeStop:spec=>{invoked++;return warm(spec);},bindAfterAuthentication:ready});
  await assert.rejects(boundary.warmBeforeServingStop({...SPEC,image:'bad'}),{code:'readiness_spec_invalid'});
  await assert.rejects(boundary.warmBeforeServingStop(SPEC,{signal:c.signal}),{code:'readiness_cancelled'});
  assert.equal(invoked,0);const lease=await boundary.warmBeforeServingStop(SPEC);assert.equal(invoked,1);boundary.cancel(lease);
});

test('current Root parser, sender and sink stay at the reviewed Core pins',async()=>{
  const pins=JSON.parse(await readFile(new URL('../../current-core-pins.json',import.meta.url),'utf8'));
  assert.equal(pins.currentCoreRevision,'90335314a663441d34148e8c312e3e4d42350eb7');
  assert.equal(pins.files.length,5);
  for(const file of pins.files) assert.equal(sha(await readFile(new URL('../../'+file.path,import.meta.url))),file.sha256);
  const adapter=await readFile(new URL('../../staged-sender-adapter.mjs',import.meta.url),'utf8');
  assert.doesNotMatch(adapter,/vendor\/readiness\/staged\/restore-backup/u);
});
