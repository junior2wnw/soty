import test from 'node:test';
import assert from 'node:assert/strict';
import {createHash} from 'node:crypto';
import {createCanvasVaultClient} from './canvas-vault-client.mjs';
import {createCanvasVaultLocalStore,CANVAS_VAULT_LOCAL_LIMITS} from './canvas-vault-local.mjs';
import {indexedNativePort} from '../../modules/notes/test/indexed-native-port.mjs';
import {vaultClientVariant} from './test-support/vault-bulk-client-variant.mjs';
const objectId='bulk_integrated_object',accountId='bulk_integrated_owner',password='Synthetic integrated reader password',CHUNK=512*1024;
const turn=()=>new Promise(resolve=>setImmediate(resolve));
const until=async predicate=>{for(let index=0;index<100;index++){if(predicate())return;await turn();}assert.fail('Expected bounded SDK read checkpoint');};
async function fixture(t,{factory=createCanvasVaultClient,size=CHUNK+71,holdAt=1}={}){
  let current=true,time=1800000000000,hold=true,release,held=false,allowed=false,reads=0;const timers=new Map();let timerId=0;
  const part=index=>Buffer.alloc(Math.min(CHUNK,size-index*CHUNK),index+1),chunks=Math.ceil(size/CHUNK),calls=[];
  const localKeyScope={schema:'soty.vault.local-scope.v1',kind:'source',id:'vault_bulk_fixture_source'};
  const api={async request(op,args){calls.push({op,args});assert.equal(args.expectedAccountId,accountId);
    if(op==='canvas.vault.status')return{objectId,ownerId:accountId,protection:{kind:'password'},state:'active',locked:!allowed,revision:1,expiresAt:allowed?time+300000:null,localKeyScope};
    if(op==='canvas.vault.unlock'){allowed=true;return{objectId,locked:false,revision:1,expiresAt:time+300000,localKeyScope};}
    if(op==='canvas.vault.relock'){allowed=false;return{objectId,locked:true};}
    assert.equal(op,'canvas.objects.content');assert.equal(args.objectId,objectId);assert.equal(args.part,'vault');reads++;
    if(args.index===holdAt&&hold){hold=false;held=true;await new Promise(resolve=>{release=resolve;});}
    if(!allowed)throw Object.assign(Error('canvas_access_denied'),{code:'canvas_access_denied',status:403});
    return{contentType:'application/octet-stream',bytesBase64:part(args.index).toString('base64url'),index:args.index,chunks,byteLength:size,revision:1};
  }};
  const store=createCanvasVaultLocalStore(accountId,'bulk_integration',{origin:'https://bulk-integration.example',indexedDB:indexedNativePort().indexedDB});
  const client=factory({api,accountId,projectId:'bulk_integration',isCurrent:()=>current,store,clock:()=>time,setTimer:(fn,delay)=>{const id=++timerId;timers.set(id,{fn,delay});return id;},clearTimer:id=>timers.delete(id)});
  t.after(()=>{release?.();client.dispose();store.close();});await client.unlock(objectId,password);
  return{client,calls,part,get held(){return held;},get reads(){return reads;},release(){release?.();},deny(){allowed=false;},changeAccount(){current=false;},expire(){time+=300001;for(const value of [...timers.values()])value.fn();}};
}

test('the proposed SDK reads above1MiB with unchanged local/create budgets; literal legacy control still rejects it',async t=>{
  const variant=await vaultClientVariant();t.after(()=>variant.dispose());const f=await fixture(t,{factory:variant.create,size:CHUNK*3+71,holdAt:-1}),value=await f.client.readContent(objectId),expected=createHash('sha256');for(let index=0;index<4;index++)expected.update(f.part(index));assert.equal(createHash('sha256').update(value.bytes).digest('hex'),expected.digest('hex'));value.bytes.fill(0);assert.equal(f.reads,4);assert.equal(CANVAS_VAULT_LOCAL_LIMITS.payloadBytes,1024*1024);
  const legacy=await vaultClientVariant({mode:'legacy'});t.after(()=>legacy.dispose());const old=await fixture(t,{factory:legacy.create,size:CHUNK*3+71,holdAt:-1});await assert.rejects(old.client.readContent(objectId),error=>error.code==='vault_wire_size');
});

async function interruptedRead(t,{factory,kind='relock'}={}){
  const f=await fixture(t,{factory});let settled=false;const reading=f.client.readContent(objectId).then(value=>{settled=true;return{value};},error=>{settled=true;return{error};});await until(()=>f.held);
  if(kind==='relock')await f.client.relock(objectId);else if(kind==='replace')await f.client.unlock(objectId,password);else if(kind==='expire')f.expire();else f.client.dispose();
  try{for(let index=0;index<15&&!settled;index++)await turn();assert.equal(settled,true,'A retired SDK session must cancel a held read before its transport releases plaintext');}
  finally{f.release();}
  const result=await reading;assert.equal(result.value,undefined);assert.equal(result.error.code,kind==='dispose'?'vault_account_changed':'canvas_object_locked');await turn();assert.equal(f.reads,2);
  if(kind==='replace'){assert.equal(f.client.isUnlocked(objectId),true,'An old request must not retire a replacement grant');const next=await f.client.readContent(objectId);assert.ok(next.bytes.length>0);next.bytes.fill(0);}
}
for(const kind of['relock','replace','expire','dispose'])test(`proposed SDK ${kind} retires a held read immediately and never hands out its prefix`,async t=>{
  const variant=await vaultClientVariant();t.after(()=>variant.dispose());await interruptedRead(t,{factory:variant.create,kind});
});
test('negative control: the original pending transport behavior fails the same immediate-retirement oracle',async t=>{
  const variant=await vaultClientVariant({mode:'legacy'});t.after(()=>variant.dispose());await assert.rejects(interruptedRead(t,{factory:variant.create}),/A retired SDK session must cancel a held read/);
});

test('caller abort cancels only its read, preserves the valid SDK grant and does not request another part',async t=>{
  const variant=await vaultClientVariant();t.after(()=>variant.dispose());const f=await fixture(t,{factory:variant.create}),abort=new AbortController();let settled=false;
  const reading=f.client.readContent(objectId,{signal:abort.signal}).then(value=>({value}),error=>{settled=true;return{error};});await until(()=>f.held);abort.abort();await until(()=>settled);const result=await reading;assert.equal(result.error.code,'vault_content_cancelled');assert.equal(f.client.isUnlocked(objectId),true);f.release();await turn();assert.equal(f.reads,2);
});
test('a current-account change or a Source denial never hands out buffered bytes',async t=>{
  const variant=await vaultClientVariant();t.after(()=>variant.dispose());
  for(const kind of['account','source']){const f=await fixture(t,{factory:variant.create}),reading=f.client.readContent(objectId).then(value=>({value}),error=>({error}));await until(()=>f.held);if(kind==='account')f.changeAccount();else f.deny();f.release();const result=await reading;assert.equal(result.value,undefined);assert.ok(result.error);assert.equal(f.reads,2);}
});

async function finalHandoff(t,mutate=value=>value){
  let abort,returned;const variant=await vaultClientVariant({mutate,onComplete:value=>{returned=value.bytes;abort.abort();}});t.after(()=>variant.dispose());const f=await fixture(t,{factory:variant.create,size:17,holdAt:-1});abort=new AbortController();
  await assert.rejects(f.client.readContent(objectId,{signal:abort.signal}),error=>error.code==='vault_content_cancelled','A cancellation after helper completion must still be checked before SDK returns bytes');assert.ok(returned.every(value=>value===0),'SDK clears the completed helper buffer if final handoff is rejected');assert.equal(f.client.isUnlocked(objectId),true);
}
test('SDK final handoff checks cancellation again after helper completion and clears the transferred buffer',t=>finalHandoff(t));
test('negative control: omitting the final SDK abort check fails the same post-helper cancellation oracle',async t=>{
  await assert.rejects(finalHandoff(t,source=>source.replace("vaultCheck(!readSignal.aborted,'vault_content_cancelled');",'')),/A cancellation after helper completion must still be checked/);
});
test('completed SDK reads remove their event listener rather than retaining an obsolete abort controller',async t=>{
  let signal;const variant=await vaultClientVariant({onBegin:options=>{signal=options.signal;}});t.after(()=>variant.dispose());const f=await fixture(t,{factory:variant.create,size:1,holdAt:-1}),value=await f.client.readContent(objectId);value.bytes.fill(0);assert.equal(signal.aborted,false);f.client.invalidate(objectId);assert.equal(signal.aborted,false,'Retired read listeners must be removed after successful completion');
});
test('canonical integrated SDK reads above1MiB and cancels a retired held read through the same port',async t=>{
  const f=await fixture(t,{size:CHUNK*3+71,holdAt:-1}),value=await f.client.readContent(objectId);assert.equal(value.bytes.length,CHUNK*3+71);value.bytes.fill(0);assert.equal(f.reads,4);
  await interruptedRead(t,{factory:createCanvasVaultClient});
});
