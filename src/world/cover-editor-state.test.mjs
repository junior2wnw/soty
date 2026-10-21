import test from 'node:test';
import assert from 'node:assert/strict';
import { createCoverApi, coverError } from './cover-api.mjs';
import { createCoverEditorState, createCoverJournal } from './cover-editor-state.mjs';
const now=1900000000000, identity={kind:'app',targetId:'app_owned',accountId:'account_owner'};
const storage=()=>{const data=new Map();return {getItem:k=>data.get(k)??null,setItem:(k,v)=>data.set(k,v),data};};
function fixture({disk=storage(),localNow=now}={}) {
  let head={revision:0,mode:'inherit',version:null},scope={storeInstanceId:'store_one',kind:'app',targetId:identity.targetId,ownerAccountId:identity.accountId,targetRevision:7,policyEpoch:4,coverRevision:0};
  let serial=0,active=true,disposedLocal=0,disposedImage=0,commits=0,dropCommit=false,holdPrepare=null,failRead=false,rejectCommit=false;const calls=[],receipts=new Map(),drafts=new Map(),versions=[];
  const api={async request(op,args){const method=op.split('.').at(-1);calls.push({method,args:structuredClone(args)});
    const current=()=>({...head});
    if(method==='status'){const receipt=receipts.get(args.requestId)??null;return {current:current(),scope:{...scope,coverRevision:head.revision},serverTime:now,receipt,draft:drafts.get(receipt?.draftId)??null};}
    if(method==='read'){if(failRead)throw coverError('read_unavailable');return {current:current(),versionId:head.version,image:null};}
    if(method==='versions')return {current:current(),versions:[...versions]};
    if(receipts.has(args.requestId))return {current:current(),receipt:receipts.get(args.requestId),replayed:true,...(method.startsWith('prepare')?{draft:drafts.get(receipts.get(args.requestId).draftId)}:{})};
    if(method==='prepare'||method==='prepareFromVersion'){
      assert.equal(args.expectedTargetRevision,scope.targetRevision);assert.equal(args.expectedPolicyEpoch,scope.policyEpoch);assert.equal(args.expectedCoverRevision,head.revision);
      const draft={id:'draft_'+(++serial),state:'ready',expiresAt:now+1800000},receipt={action:'prepared',draftId:draft.id};drafts.set(draft.id,draft);receipts.set(args.requestId,receipt);
      if(holdPrepare)await holdPrepare;return {current:current(),receipt,draft};
    }
    if(method==='cancel'){const draft=drafts.get(args.draftId);draft.state='cancelled';const receipt={action:'cancelled',draftId:draft.id};receipts.set(args.requestId,receipt);return {current:current(),receipt};}
    if(method==='commit'||method==='restore'||method==='remove'){
      if(method==='commit'&&rejectCommit)throw coverError('cover_draft_unavailable');
      commits++;head={revision:head.revision+1,mode:method==='remove'?args.mode:'custom',version:'version_'+(++serial)};
      versions.unshift({id:head.version,version:head.revision,mode:head.mode,createdAt:now,restorable:true});
      const receipt={action:'committed',targetId:identity.targetId,committedRevision:head.revision,versionId:head.version};receipts.set(args.requestId,receipt);
      if(dropCommit){dropCommit=false;throw coverError('network_lost');}return {current:current(),receipt};
    }
    throw Error('unexpected operation');
  }};
  const sdk=()=>createCoverApi({api,...identity,isCurrent:()=>active});
  const options={prepareFile:async()=>({url:'blob:local',width:100,height:80,mime:'image/png',byteCount:12,base64:()=> 'aW1hZ2UtcG5n',dispose(){disposedLocal++;}}),
    journal:createCoverJournal(disk,()=>localNow),isCurrent:()=>active,now:()=>localNow,random:bytes=>{bytes.fill(0);bytes[15]=++serial;return bytes;},
    makeImage:()=>({url:'blob:private',width:100,height:100,byteCount:10,dispose(){disposedImage++;}})};
  const create=extra=>createCoverEditorState({...options,sdk:sdk(),...extra});
  return {create,disk,calls,get commits(){return commits;},get disposedLocal(){return disposedLocal;},get disposedImage(){return disposedImage;},get scope(){return scope;},get head(){return head;},
    loseCommit(){dropCommit=true;},rejectCommit(){rejectCommit=true;},failReads(){failRead=true;},holdPrepare(value){holdPrepare=value;},switchAccount(){active=false;},changeScope(extra){scope={...scope,...extra};},
    laterNone(){head={revision:head.revision+1,mode:'none',version:'version_later'};}};
}
test('select/crop/cancel stays local; Save alone prepares then commits with actual CAS and server clock',async()=>{
  const f=fixture(),changed=[];const state=f.create({onCommitted:v=>changed.push(v)});await state.load();await state.choose({});state.setCrop({x:.2,y:.7,zoom:3});
  assert.equal(f.calls.some(v=>v.method==='prepare'),false);assert.equal(state.read().dirty,true);await state.discard();assert.equal(f.commits,0);assert.equal(f.disposedLocal,1);
  await state.choose({});state.setCrop({x:0,y:1,zoom:4});await state.save();assert.equal(f.commits,1);assert.equal(changed.length,1);assert.equal(state.read().dirty,false);
  const writes=f.calls.filter(v=>['prepare','commit'].includes(v.method));assert.deepEqual(writes.map(v=>v.method),['prepare','commit']);assert.deepEqual(writes[0].args.crop,{x:0,y:1,zoom:4});
  assert.equal(writes[0].args.expectedPolicyEpoch,4);assert.equal(writes[0].args.expectedTargetRevision,7);assert.match(writes[0].args.requestId,/^cover1\.1900000000000\.[a-f0-9]{32}$/);
  state.dispose();
});
test('a lost committed response replays the same intent and cannot rewind a later selected head',async()=>{
  const f=fixture(),state=f.create();await state.load();await state.choose({});f.loseCommit();await state.save();assert.equal(state.read().phase,'unknown');assert.equal(f.commits,1);
  const first=f.calls.find(v=>v.method==='commit');assert.ok(first);const journalText=Array.from(f.disk.data.values()).join('');assert.equal(journalText.includes('aW1hZ2UtcG5n'),false);assert.equal(journalText.includes('blob:'),false);assert.equal(journalText.includes('base64'),false);
  f.laterNone();await state.recheck();assert.equal(f.commits,1);assert.equal(state.read().current.mode,'none');assert.equal(state.read().pending,null);
  assert.deepEqual(f.calls.filter(v=>v.method==='commit').map(v=>v.args),[first.args,first.args]);state.dispose();
});
test('close/reopen recovers a commit without a second new intent; prepare recovery cannot publish an unseen preview',async()=>{
  const f=fixture(),first=f.create();await first.load();await first.choose({});f.loseCommit();await first.save();first.dispose();
  const reopened=f.create();await reopened.load();assert.equal(reopened.read().pending.method,'commit');await reopened.choose({});assert.equal(f.commits,1);await reopened.recheck();assert.equal(f.commits,1);assert.equal(reopened.read().pending,null);reopened.dispose();
  const g=fixture();let release;g.holdPrepare(new Promise(r=>release=r));const before=g.create();await before.load();await before.choose({});const saving=before.save();
  for(let n=0;n<8;n++)await Promise.resolve();before.dispose();release();await saving;assert.equal(g.commits,0);
  const recovery=g.create();await recovery.load();await recovery.recheck();assert.equal(recovery.read().canSave,false);assert.equal(recovery.read().phase,'unknown');await recovery.save();assert.equal(g.commits,0);await recovery.cancelPreparation();assert.equal(recovery.read().pending,null);recovery.dispose();
});
test('account generation at the prepare await retires local ownership and never sends commit',async()=>{
  const f=fixture();let release;f.holdPrepare(new Promise(r=>release=r));const state=f.create();await state.load();await state.choose({});const saving=state.save();
  for(let n=0;n<8;n++)await Promise.resolve();f.switchAccount();release();await saving;assert.equal(f.commits,0);state.dispose();assert.equal(f.disposedLocal,1);
  assert.equal(JSON.parse(Array.from(f.disk.data.values())[0])[0].method,'prepare');
});
test('CAS/Source changes require an explicit new choice, with no silent rebasing or preparation',async()=>{
  const f=fixture(),state=f.create();await state.load();await state.choose({});f.changeScope({policyEpoch:5});await state.save();assert.equal(state.read().phase,'conflict');assert.equal(f.calls.some(v=>v.method==='prepare'),false);assert.equal(state.read().dirty,true);state.dispose();
});
test('unavailable/corrupt/full recovery storage refuses new network writes',async()=>{
  for(const disk of [{getItem:()=>null,setItem(){throw Error('quota');}},{getItem:()=>'{bad',setItem(){}}]){
    const f=fixture({disk}),state=f.create();await state.load();await state.choose({});await state.save();assert.equal(f.calls.some(v=>['prepare','commit'].includes(v.method)),false);state.dispose();
  }
  const disk=storage(),journal=createCoverJournal(disk,()=>now),scope={storeInstanceId:'store',kind:'app',targetId:'app',ownerAccountId:'owner',targetRevision:1,policyEpoch:1,coverRevision:0};
  for(let n=0;n<8;n++)journal.save({schema:1,identity:{accountId:'owner',kind:'app',targetId:'app'+n},scope:{...scope,targetId:'app'+n},method:'commit',args:{requestId:`cover1.${now}.${n.toString(16).padStart(32,'0')}`,draftId:'draft'},savedAt:now});
  assert.throws(()=>journal.save({schema:1,identity:{accountId:'owner',kind:'app',targetId:'app9'},scope:{...scope,targetId:'app9'},method:'commit',args:{requestId:`cover1.${now}.${'f'.repeat(32)}`,draftId:'draft'},savedAt:now}),{code:'cover_journal_quota'});
});
test('a pending intent is fenced to its persisted store, including refresh/discard paths',async()=>{
  const f=fixture(),state=f.create();await state.load();await state.choose({});f.loseCommit();await state.save();state.dispose();f.changeScope({storeInstanceId:'different_store'});
  const next=f.create();await next.load();assert.equal(next.read().error,'cover_store_changed');await next.discard();await next.choose({});await next.recheck();assert.equal(f.commits,1);assert.equal(f.calls.filter(v=>v.method==='commit').length,1);next.dispose();
});
test('confirmed publication invalidates the field even when later readback fails',async()=>{
  const f=fixture(),changed=[];const state=f.create({onCommitted:v=>{changed.push(v);f.failReads();}});
  // A host callback can remove the mounted field without affecting the receipt.
  await state.load();await state.choose({});await state.save();assert.equal(changed.length,1);assert.equal(changed[0].current.revision,1);assert.equal(f.commits,1);assert.equal(state.read().phase,'error');assert.equal(state.read().pending,null);state.dispose();
});
test('new intent timestamps come from the real server even with a badly skewed device clock',async()=>{
  const f=fixture({localNow:123456}),state=f.create();await state.load();await state.choose({});await state.save();assert.equal(f.commits,1);
  for(const call of f.calls.filter(v=>['prepare','commit'].includes(v.method)))assert.equal(call.args.requestId.split('.')[1],String(now));state.dispose();
});
test('an expired rejected commit is resolved by exact replay plus no-receipt read, never by an automatic fresh intent',async()=>{
  const f=fixture(),state=f.create();await state.load();await state.choose({});f.rejectCommit();await state.save();assert.equal(state.read().pending.method,'commit');
  await state.recheck();assert.equal(state.read().pending,null);assert.equal(state.read().phase,'conflict');assert.equal(state.read().canSave,false);assert.equal(f.commits,0);
  state.setCrop({x:0,y:0,zoom:2});await state.save();assert.equal(f.calls.filter(v=>v.method==='prepare').length,1);assert.equal(f.commits,0);await state.discard();assert.equal(state.read().phase,'ready');state.dispose();
});
test('journal ownership is exact and images/accessor-shaped metadata cannot enter recovery storage',()=>{
  const disk=storage(),journal=createCoverJournal(disk,()=>now),value={schema:1,identity,scope:{storeInstanceId:'store_one',kind:'app',targetId:identity.targetId,ownerAccountId:identity.accountId,targetRevision:7,policyEpoch:4,coverRevision:0},
    method:'commit',args:{requestId:`cover1.${now}.${'a'.repeat(32)}`,draftId:'draft_real'},savedAt:now};
  journal.save(value);assert.equal(journal.read({...identity,accountId:'other_account'}),null);
  assert.throws(()=>journal.save({...value,args:{...value.args,base64:'private'}},value),{code:'cover_journal_invalid'});
  const tampered=JSON.parse(disk.getItem('soty.cover-intents.v1'));tampered[0].scope.ownerAccountId='other_account';disk.setItem('soty.cover-intents.v1',JSON.stringify(tampered));
  assert.throws(()=>journal.read(identity),{code:'cover_journal_invalid'});
});
