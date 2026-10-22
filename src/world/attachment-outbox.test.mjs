import test from 'node:test';
import assert from 'node:assert/strict';
import { createHash } from 'node:crypto';
import { File } from 'node:buffer';
import { DatabaseSync } from 'node:sqlite';
import { createAttachmentOutbox, downloadAttachment, ATTACHMENT_LIMITS as L } from './attachment-outbox.mjs';
import { createChatDraftStore } from './chat-state.mjs';
import { attachmentHttpFixture } from '../../modules/world/test/support/attachment-http-fixture.mjs';
const sha=b=>createHash('sha256').update(b).digest('hex');
const wait=async predicate=>{const end=Date.now()+10000;while(!predicate()){assert.ok(Date.now()<end,'OWN outbox deadline');await new Promise(r=>setTimeout(r,10));}};
function memoryDisk() {
  const data=new Map(),messages=new Map();let fail=false;
  const copy=v=>structuredClone(v);
  return {data,messages,setFailure:value=>{fail=value;},
    async list(prefix){return [...data].filter(([key])=>key.startsWith(prefix)).sort(([a],[b])=>a.localeCompare(b)).map(([,v])=>copy(v));},
    async put(row){if(fail)throw Object.assign(Error('own disk full'),{code:'attachment_local_storage'});data.set(row.key,copy(row));},
    async update(row){if(fail)throw Object.assign(Error('own disk full'),{code:'attachment_local_storage'});const old=data.get(row.key);if(!old)return null;const next={...row,order:old.order,...(old.status==='cancel-pending'?{status:'cancel-pending'}:{})};data.set(row.key,copy(next));return copy(next);},
    async beginCancel(prefix,id){if(fail)throw Object.assign(Error('own disk full'),{code:'attachment_local_storage'});if(messages.get(prefix)?.pending?.attachments.includes(id))throw Object.assign(Error('own pending intent'),{code:'attachment_send_unconfirmed'});const old=data.get(prefix+id);if(!old)return null;const next={...old,status:'cancel-pending',error:null};data.set(next.key,copy(next));return copy(next);},
    async scope(prefix){return {rows:[...data].filter(([key])=>key.startsWith(prefix)).sort(([a],[b])=>a.localeCompare(b)).map(([,v])=>copy(v)),message:copy(messages.get(prefix)??null)};},
    async claimMessage(prefix,proposal){if(fail)throw Object.assign(Error('own disk full'),{code:'attachment_local_storage'});let m=messages.get(prefix)??{kind:'message-intent.v1',pending:null,accepted:null};if(m.pending)return {state:'pending',intent:copy(m.pending)};if(m.accepted?.clientId===proposal.clientId)return {state:'accepted',intent:copy(m.accepted)};const all=[...data.values()].filter(row=>row.key.startsWith(prefix));if(!proposal.attachments.every(id=>all.some(row=>row.attachmentId===id&&row.status==='ready')))throw Object.assign(Error('own changed'),{code:'attachment_queue_changed'});m={...m,pending:copy(proposal)};messages.set(prefix,m);return {state:'pending',intent:copy(proposal)};},
    async acceptMessage(prefix,intent){if(fail)throw Object.assign(Error('own disk full'),{code:'attachment_local_storage'});const m=messages.get(prefix);if(m?.pending?.clientId===intent.clientId)messages.set(prefix,{...m,pending:null,accepted:copy(intent)});for(const id of intent.attachments)data.delete(prefix+id);},
    async remove(key){if(fail)throw Object.assign(Error('own disk full'),{code:'attachment_local_storage'});data.delete(key);},
    async removeBatch(keys){if(fail)throw Object.assign(Error('own disk full'),{code:'attachment_local_storage'});for(const key of keys)data.delete(key);},
    async addBatch(prefix,rows){if(fail)throw Object.assign(Error('own disk full'),{code:'attachment_local_storage'});const old=await this.list(prefix);if(old.length+rows.length>L.batch||[...old,...rows].reduce((n,r)=>n+r.size,0)>L.batchBytes)throw Object.assign(Error('own batch'),{code:'attachment_batch_limit'});const last=old.reduce((n,row)=>Math.max(n,row.order||0),0);rows.forEach((row,index)=>{row.order=last+index+1;data.set(row.key,copy(row));});},
  };
}
function wire() {
  const rows=new Map(),calls=[];
  return {calls,async request(op,args){
    calls.push({op,args:structuredClone(args)});const key=args.expectedAccountId+':'+args.communityId+':'+args.attachmentId;
    if(op==='world.attachment.begin') {if(!rows.has(key))rows.set(key,{...args,parts:[],state:'pending'});return {attachment:{attachmentId:args.attachmentId,state:rows.get(key).state}};}
    const row=rows.get(key);assert.ok(row,'OWN wire receives admitted scope');
    if(op==='world.attachment.chunk'){row.parts[args.index]=Buffer.from(args.data,'base64');return {accepted:true,index:args.index,sha256:args.sha256};}
    if(op==='world.attachment.complete'){row.state='complete';row.ref={attachmentId:row.attachmentId,name:row.name,size:row.size,chunks:Math.ceil(row.size/L.chunkBytes),manifestSha256:args.manifestSha256,fileSha256:sha(Buffer.concat(row.parts)),state:'complete'};return {attachment:row.ref};}
    if(op==='world.attachment.status')return {attachment:row.ref};
    if(op==='world.attachment.read'){const bytes=row.parts[args.index];return {index:args.index,size:bytes.length,sha256:sha(bytes),data:bytes.toString('base64')};}
    if(op==='world.attachment.cancel'){row.state='cancelled';return {cancelled:true};}
    throw Error('OWN wire unknown action');
  }};
}

test('signed direct outbox pins its relationship through reload and never shares room/account/new-contact drafts', async t => {
  const f = await attachmentHttpFixture(t), [alice, bob] = f.actors, disk = memoryDisk(), calls = [];
  const relationshipId = await f.contact(alice, bob);
  const opened = await f.call(alice, 'world.direct.open', { expectedAccountId: alice.accountId, peerAccountId: bob.accountId,
    expectedRelationshipId: relationshipId, requestId: 'direct_outbox_open' }); assert.equal(opened.ok, true);
  const threadId = opened.current.threadId, conversationScope = { threadId, expectedRelationshipId: relationshipId };
  const api = actor => ({ async request(op, args) {
    calls.push({ op, args: structuredClone(args) }); const result = await f.call(actor, op, args);
    if (!result.ok) throw Object.assign(Error('Signed disposable request refused'), { code: result.error.code }); return result;
  } });
  const options = { api: api(alice), accountId: alice.accountId, conversationKey: `direct:${threadId}:${relationshipId}`, conversationScope, disk };
  const source = Buffer.alloc(L.chunkBytes + 7, 53);
  let box = createAttachmentOutbox(options); await box.ready; await box.add([new File([source], 'Личный оригинал.bin')]); await wait(() => box.canSend());
  const ids = box.refs(); box.close(); box = createAttachmentOutbox(options); await box.ready; t.after(() => box.close());
  assert.deepEqual(box.refs(), ids); assert.equal((await box.localFile(ids[0])).blob.size, source.length);
  const unrelated = [
    createAttachmentOutbox({ api: api(alice), accountId: alice.accountId, communityId: f.group.communityId, disk }),
    createAttachmentOutbox({ ...options, api: api(bob), accountId: bob.accountId }),
    createAttachmentOutbox({ ...options, conversationKey: `direct:${threadId}:relationship_next`, conversationScope: { threadId, expectedRelationshipId: 'relationship_next' } }),
  ];
  t.after(() => unrelated.forEach(value => value.close()));
  for (const other of unrelated) { await other.ready; assert.deepEqual(other.snapshot(), []); }
  const intent = (await box.prepareSend({ clientId: 'direct_outbox_message', text: '', replyTo: null, attachments: ids })).intent;
  const accepted = await api(alice).request('world.direct.chat.send', { ...conversationScope, expectedAccountId: alice.accountId, ...intent });
  const file = await downloadAttachment({ api: api(bob), accountId: bob.accountId, conversationScope, attachment: accepted.message.attachments[0] });
  assert.deepEqual(Buffer.from(await file.blob.arrayBuffer()), source);
  assert.ok(calls.every(({ args }) => !Object.hasOwn(args, 'communityId') && args.threadId === threadId && args.expectedRelationshipId === relationshipId));
  await box.acknowledge(ids, intent); assert.equal(disk.data.size, 0);
});

test('actual signed HTTP: chunk ACK loss + IDB-model reopen retain binary/intent; second account downloads exact bytes',async t=>{
  const f=await attachmentHttpFixture(t),[a,b]=f.actors,disk=memoryDisk();let lost=true;
  const raw=Buffer.alloc(600001);for(let i=0;i<raw.length;i++)raw[i]=(i*73)%256;
  const real={request:async(op,args)=>{const r=await f.call(a,op,args);if(!r.ok)throw Object.assign(Error('OWN typed RPC failure'),{code:r.error.code});if(op==='world.attachment.chunk'&&lost){lost=false;throw Object.assign(Error('OWN ACK lost'),{code:'own_ack_lost'});}return r;}};
  const options={api:real,accountId:a.accountId,communityId:f.group.communityId,disk,makeId:()=> 'durable_http_attachment'};
  const first=createAttachmentOutbox(options);await first.ready;await first.add([new File([raw],'Снимок 🏡.bin')]);await wait(()=>first.snapshot()[0]?.status==='error');first.close();
  const reopened=createAttachmentOutbox(options);t.after(()=>reopened.close());await reopened.ready;await reopened.retry('durable_http_attachment');await wait(()=>reopened.canSend());
  const map=new Map(),local={getItem:k=>map.get(k),setItem:(k,v)=>map.set(k,v),removeItem:k=>map.delete(k)};
  const drafts=createChatDraftStore(local,()=> 'durable_message_identity');
  const pending=drafts.beginSend(a.accountId,f.group.communityId,'',null,reopened.refs());
  const args={...f.base(a),clientId:pending.clientId,text:'',attachments:pending.attachments};
  const accepted=await real.request('world.chat.send',args);const afterReload=createChatDraftStore(local,()=> 'must_not_replace_lost_ack');
  assert.equal(afterReload.beginSend(a.accountId,f.group.communityId,'',null,reopened.refs()).clientId,pending.clientId);
  assert.equal((await real.request('world.chat.send',args)).message.messageId,accepted.message.messageId);
  disk.setFailure(true);await assert.rejects(reopened.acknowledge(reopened.refs()));assert.equal(reopened.snapshot().length,1);assert.equal(disk.data.size,1);
  disk.setFailure(false);await reopened.acknowledge(reopened.refs());afterReload.acknowledge(a.accountId,f.group.communityId,pending.clientId);assert.equal(disk.data.size,0);
  const apiB={request:async(op,args)=>{const r=await f.call(b,op,args);if(!r.ok)throw Object.assign(Error('OWN typed RPC failure'),{code:r.error.code});return r;}};
  const download=await downloadAttachment({api:apiB,accountId:b.accountId,communityId:f.group.communityId,attachment:accepted.message.attachments[0]});
  assert.equal(sha(Buffer.from(await download.blob.arrayBuffer())),sha(raw));assert.equal(download.blob.type,'application/octet-stream');assert.equal(download.name,'Снимок 🏡.bin');
});

test('queue0/1/10/11, per-file/batch boundaries and storage denial occur before network admission',async()=>{
  const disk=memoryDisk(),api=wire();let id=0;const box=createAttachmentOutbox({api,accountId:'actor_a',communityId:'community_a',disk,makeId:()=>`file_${++id}`});await box.ready;
  assert.equal(box.canSend(),false);await assert.rejects(box.add([]),e=>e.code==='attachment_batch_limit');
  disk.setFailure(true);await assert.rejects(box.add([new File(['x'],'x.txt')]),e=>e.code==='attachment_local_storage');assert.equal(api.calls.length,0);disk.setFailure(false);
  await assert.rejects(box.add([new File([new Uint8Array(L.fileBytes+1)],'too-big')]),e=>e.code==='attachment_batch_limit');assert.equal(api.calls.length,0);
  await assert.rejects(box.add(Array.from({length:5},()=>new File([new Uint8Array(L.fileBytes)],'batch'))),e=>e.code==='attachment_batch_limit');assert.equal(api.calls.length,0);
  await box.add(Array.from({length:10},(_,i)=>new File([],`пустой-${i}.txt`)));await wait(()=>box.canSend());assert.equal(box.refs().length,10);
  const before=api.calls.length;await assert.rejects(box.add([new File([],'eleven')]),e=>e.code==='attachment_batch_limit');assert.equal(api.calls.length,before);box.close();
});

test('selection is durable before reads; source is sliced and never whole-file arrayBuffer-hashed',async()=>{
  const api=wire(),disk=memoryDisk(),reads=[];const original=disk.addBatch.bind(disk);let persisted=false;
  disk.addBatch=async(...args)=>{await original(...args);persisted=true;};
  const file=new File([new Uint8Array(L.chunkBytes+13)],'bounded.bin'),slice=Blob.prototype.slice,read=Blob.prototype.arrayBuffer;
  Blob.prototype.slice=function(a,b){if(this.size===file.size){assert.equal(persisted,true);reads.push(b-a);}return slice.call(this,a,b);};
  Blob.prototype.arrayBuffer=function(){if(this.size===file.size)throw Error('OWN whole source read prohibited');return read.call(this);};
  const box=createAttachmentOutbox({api,accountId:'actor_a',communityId:'community_a',disk});
  try{await box.ready;await box.add([file]);await wait(()=>box.canSend());assert.equal(reads.length,2);assert.ok(reads.every(n=>n<=L.chunkBytes));}
  finally{box.close();Blob.prototype.slice=slice;Blob.prototype.arrayBuffer=read;}
});

test('key-sorted storage preserves selection order across reopen, removal and a later batch; OFF does not upload',async()=>{
  const disk=memoryDisk(),api=wire();let ids=['att_zulu','att_alpha','att_middle','att_second','att_first'];
  const options={api,accountId:'actor_a',communityId:'community_a',disk,makeId:()=>ids.shift()};
  let box=createAttachmentOutbox(options);await box.ready;
  await box.add([new File(['z'],'z.txt'),new File(['a'],'a.txt'),new File(['m'],'m.txt')]);await wait(()=>box.canSend());
  assert.deepEqual(box.refs(),['att_zulu','att_alpha','att_middle']);box.close();
  box=createAttachmentOutbox(options);await box.ready;assert.deepEqual(box.refs(),['att_zulu','att_alpha','att_middle']);
  await box.remove('att_alpha');await wait(()=>box.snapshot().length===2);
  await box.add([new File(['2'],'second.txt'),new File(['1'],'first.txt')]);await wait(()=>box.canSend());
  assert.deepEqual(box.refs(),['att_zulu','att_middle','att_second','att_first']);
  assert.deepEqual([...disk.data.values()].map(row=>row.order).sort((a,b)=>a-b),[1,3,4,5]);box.close();
  const before=api.calls.length;box=createAttachmentOutbox({...options,allowUpload:()=>false});await box.ready;
  assert.equal(box.canSend(),false);assert.deepEqual(box.refs(),['att_zulu','att_middle','att_second','att_first']);
  await assert.rejects(box.add([new File(['x'],'off')]),e=>e.code==='attachments_disabled');
  await box.retry('att_zulu');await new Promise(r=>setTimeout(r,20));assert.equal(api.calls.length,before);box.close();
});

test('actor A → B → A: a delayed original-scope ACK cannot complete or publish into the new scope',async()=>{
  const disk=memoryDisk(),backend=wire();let current=true,release;
  const api={request:async(op,args)=>{const value=await backend.request(op,args);if(op==='world.attachment.chunk')await new Promise(r=>{release=r;});return value;}};
  const a=createAttachmentOutbox({api,accountId:'actor_a',communityId:'community_a',disk,current:()=>current});await a.ready;await a.add([new File(['one'],'one.txt')]);await wait(()=>!!release);
  current=false;const b=createAttachmentOutbox({api:backend,accountId:'actor_b',communityId:'community_a',disk});await b.ready;assert.deepEqual(b.snapshot(),[]);release();await wait(()=>a.snapshot()[0]?.status==='error');a.close();b.close();
  assert.ok(backend.calls.every(c=>c.args.expectedAccountId==='actor_a'));assert.equal(backend.calls.filter(c=>c.op==='world.attachment.complete').length,0);
  const restored=createAttachmentOutbox({api:backend,accountId:'actor_a',communityId:'community_a',disk});await restored.ready;assert.equal(restored.snapshot().length,1);restored.close();
});

test('cancel during an in-flight chunk needs a server ACK; lost cancel ACK remains retryable without message publication',async()=>{
  const disk=memoryDisk(),backend=wire();let release,fail=true;
  const api={request:async(op,args)=>{const value=await backend.request(op,args);if(op==='world.attachment.chunk')await new Promise(r=>{release=r;});if(op==='world.attachment.cancel'&&fail){fail=false;throw Object.assign(Error('OWN lost cancel ACK'),{code:'own_lost_cancel'});}return value;}};
  const box=createAttachmentOutbox({api,accountId:'actor_a',communityId:'community_a',disk});await box.ready;await box.add([new File(['cancel'],'cancel.txt')]);await wait(()=>!!release);
  const id=box.snapshot()[0].attachmentId;await box.remove(id);release();await wait(()=>box.snapshot()[0]?.status==='cancel-pending'&&!!box.snapshot()[0]?.error);
  assert.equal(box.canSend(),false);assert.equal(disk.data.size,1);await box.retry(id);await wait(()=>box.snapshot().length===0);assert.equal(disk.data.size,0);assert.equal(backend.calls.filter(c=>c.op==='world.chat.send').length,0);box.close();
});

test('attachment intent includes ordered refs; legacy tab/storage events and a late ACK cannot erase a newer selection',()=>{
  const map=new Map(),storage={getItem:k=>map.get(k),setItem:(k,v)=>map.set(k,v),removeItem:k=>map.delete(k)};let id=0;
  const drafts=createChatDraftStore(storage,()=>`intent_${++id}`);drafts.edit('actor_a','community_a','подпись');
  const first=drafts.beginSend('actor_a','community_a','подпись',null,['attachment_1']);
  map.set('soty.chat.draft.v1:actor_a:community_a',JSON.stringify({text:'old tab'}));drafts.storageChanged('soty.chat.draft.v1:actor_a:community_a');
  assert.deepEqual(drafts.read('actor_a','community_a').attachments,['attachment_1']);
  const changed=drafts.beginSend('actor_a','community_a','подпись',null,['attachment_2']);assert.equal(changed.clientId,first.clientId);assert.deepEqual(changed.attachments,['attachment_1']);
  drafts.acknowledge('actor_a','community_a',first.clientId);assert.deepEqual(drafts.read('actor_a','community_a').attachments,['attachment_2']);
  const next=drafts.beginSend('actor_a','community_a','подпись',null,['attachment_2']);assert.notEqual(next.clientId,first.clientId);
  drafts.acknowledge('actor_a','community_a',next.clientId);assert.equal(createChatDraftStore(storage).read('actor_a','community_a').text,'');
});

test('F1 signed HTTP: accepted unknown ACK freezes original caption/reply/refs while preserving the next draft and file',async t=>{
  const f=await attachmentHttpFixture(t),[actor]=f.actors,disk=memoryDisk(),store=new Map();let n=0;
  const drafts=createChatDraftStore({getItem:k=>store.get(k),setItem:(k,v)=>store.set(k,v),removeItem:k=>store.delete(k)},()=>`f1_message_${++n}`);
  const api={request:async(op,args)=>{const r=await f.call(actor,op,args);if(!r.ok)throw Object.assign(Error('own RPC'),{code:r.error.code});return r;}};
  const box=createAttachmentOutbox({api,accountId:actor.accountId,communityId:f.group.communityId,disk});t.after(()=>box.close());await box.ready;
  await box.add([new File(['first'],'first.txt'),new File(['second'],'second.txt')]);await wait(()=>box.canSend());
  const originalIds=box.refs(),proposal=drafts.beginSend(actor.accountId,f.group.communityId,'Подпись A',null,originalIds),first=(await box.prepareSend(proposal)).intent;
  const send=p=>api.request('world.chat.send',{...f.base(actor),clientId:p.clientId,text:p.text,replyTo:p.replyTo,attachments:p.attachments});
  const accepted=await send(first); // Accepted response is deliberately not acknowledged locally.
  await box.add([new File(['next'],'next.txt')]);await wait(()=>box.snapshot().every(row=>row.status==='ready'));
  const legacy=(await api.request('world.chat.list',{communityId:f.group.communityId})).messages[0].messageId;
  drafts.edit(actor.accountId,f.group.communityId,'Новая подпись B',legacy);
  const retry=drafts.beginSend(actor.accountId,f.group.communityId,'Новая подпись B',legacy,box.refs());
  assert.equal(retry.clientId,first.clientId);assert.deepEqual((await box.prepareSend(retry)).intent,first);
  await assert.rejects(box.remove(originalIds[0]),e=>e.code==='attachment_send_unconfirmed');
  assert.equal((await send(first)).message.messageId,accepted.message.messageId);
  await box.acknowledge(first.attachments,first);const next=drafts.acknowledge(actor.accountId,f.group.communityId,first.clientId,first.attachments);
  assert.equal(next.text,'Новая подпись B');assert.equal(next.replyTo,legacy);assert.deepEqual(next.attachments,[box.refs()[0]]);assert.equal(box.snapshot().length,1);
  const second=(await box.prepareSend(drafts.beginSend(actor.accountId,f.group.communityId,next.text,next.replyTo,box.refs()))).intent;
  assert.notEqual(second.clientId,first.clientId);await send(second);await box.acknowledge(second.attachments,second);
  assert.equal((await api.request('world.chat.list',{communityId:f.group.communityId})).messages.length,3);assert.equal(disk.data.size,0);
});

test('F2 signed HTTP: late closed-instance ACK refreshes reopened A without clearing B or newer A files/text',async t=>{
  const f=await attachmentHttpFixture(t),[a,b]=f.actors,disk=memoryDisk(),map=new Map();let sequence=0;
  const drafts=createChatDraftStore({getItem:k=>map.get(k),setItem:(k,v)=>map.set(k,v),removeItem:k=>map.delete(k)},()=>`f2_message_${++sequence}`);
  const api=actor=>({request:async(op,args)=>{const r=await f.call(actor,op,args);if(!r.ok)throw Object.assign(Error('own RPC'),{code:r.error.code});return r;}});
  const options={api:api(a),accountId:a.accountId,communityId:f.group.communityId,disk};
  const old=createAttachmentOutbox(options);await old.ready;await old.add([new File(['old'],'old.txt')]);await wait(()=>old.canSend());
  const first=(await old.prepareSend(drafts.beginSend(a.accountId,f.group.communityId,'A',null,old.refs()))).intent;
  const accepted=await options.api.request('world.chat.send',{...f.base(a),clientId:first.clientId,text:first.text,replyTo:first.replyTo,attachments:first.attachments});old.close();
  const other=createAttachmentOutbox({api:api(b),accountId:b.accountId,communityId:f.group.communityId,disk});t.after(()=>other.close());await other.ready;await other.add([new File(['B'],'b.txt')]);await wait(()=>other.canSend());const otherIds=other.refs();
  let current;current=createAttachmentOutbox({...options,changed:()=>{
    const done=current?.acceptedIntent();if(done)drafts.acknowledge(a.accountId,f.group.communityId,done.clientId,done.attachments);
    const pending=current?.pendingIntent();if(pending)drafts.recoverPending(a.accountId,f.group.communityId,pending);
    if(current)drafts.setAttachments(a.accountId,f.group.communityId,current.snapshot().map(v=>v.attachmentId));
  }});t.after(()=>current.close());await current.ready;
  await current.add([new File(['new'],'new.txt')]);await wait(()=>current.snapshot().every(v=>v.status==='ready'));
  drafts.edit(a.accountId,f.group.communityId,'Следующая A');await old.acknowledge(first.attachments,first);
  assert.deepEqual(current.refs(),current.snapshot().filter(v=>v.name==='new.txt').map(v=>v.attachmentId));assert.equal(current.snapshot().length,1);
  assert.deepEqual(other.refs(),otherIds);assert.equal(drafts.read(a.accountId,f.group.communityId).text,'Следующая A');assert.equal(drafts.read(a.accountId,f.group.communityId).pending,null);
  const next=(await current.prepareSend(drafts.beginSend(a.accountId,f.group.communityId,'Следующая A',null,current.refs()))).intent;
  assert.notEqual(next.clientId,first.clientId);const second=await options.api.request('world.chat.send',{...f.base(a),clientId:next.clientId,text:next.text,replyTo:next.replyTo,attachments:next.attachments});
  assert.notEqual(second.message.messageId,accepted.message.messageId);await current.acknowledge(next.attachments,next);assert.equal(current.snapshot().length,0);assert.deepEqual(other.refs(),otherIds);
  assert.equal((await options.api.request('world.chat.list',{communityId:f.group.communityId})).messages.length,3);
});

function boundaryGate() {
  let enter,release;const entered=new Promise(resolve=>{enter=resolve;}),resume=new Promise(resolve=>{release=resolve;});
  return {entered,release,async pause(){enter();await resume;}};
}
async function separateOutboxModules(label) {
  return Promise.all([import(`./attachment-outbox.mjs?f3=${label}-a`),import(`./attachment-outbox.mjs?f3=${label}-b`)]);
}
function withoutBroadcast(make) {
  const saved=globalThis.BroadcastChannel;globalThis.BroadcastChannel=undefined;
  try{return make();}finally{globalThis.BroadcastChannel=saved;}
}
function assertSqlIntent(db,community,intent) {
  const messages=db.prepare('SELECT id,text FROM messages WHERE community_id=? AND client_id=?').all(community,intent.clientId);
  assert.equal(messages.length,1);assert.equal(messages[0].text,intent.text);
  assert.deepEqual(db.prepare("SELECT id FROM world_chat_attachments WHERE message_id=? AND state='published' ORDER BY position").all(messages[0].id).map(row=>row.id),intent.attachments);
}

test('F3 claim-first signed HTTP: stale remove cannot cancel a claimed ref without broadcast; next draft and other scope survive',async t=>{
  const f=await attachmentHttpFixture(t),[actor,otherActor]=f.actors,disk=memoryDisk(),gate=boundaryGate(),[a,b]=await separateOutboxModules('claim-first');
  const calls=[];const api=who=>({request:async(op,args)=>{calls.push({op,args:structuredClone(args)});const result=await f.call(who,op,args);assert.equal(result.ok,true,result.error?.code);return result;}});
  const base={accountId:actor.accountId,communityId:f.group.communityId},primaryApi=api(actor);
  const remover=withoutBroadcast(()=>a.createAttachmentOutbox({...base,api:primaryApi,disk:{...disk,beginCancel:async(...args)=>{await gate.pause();return disk.beginCancel(...args);}}}));
  await remover.ready;await remover.add([new File(['first'],'first.txt'),new File(['second'],'second.txt')]);await wait(()=>remover.canSend());const refs=remover.refs();
  const sender=withoutBroadcast(()=>b.createAttachmentOutbox({...base,api:primaryApi,disk}));
  const other=withoutBroadcast(()=>a.createAttachmentOutbox({accountId:otherActor.accountId,communityId:f.group.communityId,api:api(otherActor),disk}));
  t.after(()=>{gate.release();remover.close();sender.close();other.close();});await sender.ready;await other.ready;await other.add([new File(['other'],'other.txt')]);await wait(()=>other.canSend());const otherRefs=other.refs();
  const removal=remover.remove(refs[0]).then(()=>null,error=>error.code);await gate.entered;
  const original=(await sender.prepareSend({clientId:'f3_claim_first',text:'Исходная подпись',replyTo:null,attachments:refs})).intent;
  await sender.add([new File(['next'],'next.txt')]);await wait(()=>sender.snapshot().every(row=>row.status==='ready'));
  gate.release();assert.equal(await removal,'attachment_send_unconfirmed');assert.equal(calls.filter(call=>call.op==='world.attachment.cancel').length,0);
  assert.deepEqual(sender.pendingIntent(),original);assert.equal(disk.data.size,4);
  const send=intent=>primaryApi.request('world.chat.send',{...f.base(actor),...intent});
  await send(original);await sender.acknowledge(original.attachments,original);assert.equal(sender.snapshot().length,1);assert.equal(sender.snapshot()[0].name,'next.txt');
  const next=(await sender.prepareSend({clientId:'f3_claim_next',text:'Новый черновик',replyTo:null,attachments:sender.refs()})).intent;
  assert.ok(!next.attachments.some(id=>refs.includes(id)));await send(next);await sender.acknowledge(next.attachments,next);
  assert.deepEqual(other.refs(),otherRefs);assert.equal(sender.snapshot().length,0);assert.equal(sender.pendingIntent(),null);assert.equal(disk.data.size,1);
  const db=new DatabaseSync(f.worldPath,{readOnly:true});try{assertSqlIntent(db,f.group.communityId,original);assertSqlIntent(db,f.group.communityId,next);assert.equal(db.prepare('SELECT COUNT(*) AS n FROM messages WHERE community_id=?').get(f.group.communityId).n,3);}finally{db.close();}
});

test('F3 cancel-first signed HTTP: stale claim fails before send without broadcast, leaves no pending and sends surviving refs',async t=>{
  const f=await attachmentHttpFixture(t),[actor]=f.actors,disk=memoryDisk(),gate=boundaryGate(),[a,b]=await separateOutboxModules('cancel-first'),calls=[];
  const api={request:async(op,args)=>{calls.push({op,args:structuredClone(args)});const result=await f.call(actor,op,args);assert.equal(result.ok,true,result.error?.code);return result;}};
  const base={api,accountId:actor.accountId,communityId:f.group.communityId};
  const remover=withoutBroadcast(()=>a.createAttachmentOutbox({...base,disk}));await remover.ready;await remover.add([new File(['cancel'],'cancel.txt'),new File(['keep'],'keep.txt')]);await wait(()=>remover.canSend());const refs=remover.refs();
  const sender=withoutBroadcast(()=>b.createAttachmentOutbox({...base,disk:{...disk,claimMessage:async(...args)=>{await gate.pause();return disk.claimMessage(...args);}}}));await sender.ready;
  t.after(()=>{gate.release();remover.close();sender.close();});
  const claim=sender.prepareSend({clientId:'f3_cancel_stale',text:'Сохранённый черновик',replyTo:null,attachments:refs}).then(()=>null,error=>error.code);await gate.entered;
  await remover.remove(refs[0]);await wait(()=>remover.snapshot().length===1);gate.release();assert.equal(await claim,'attachment_queue_changed');
  assert.equal(calls.filter(call=>call.op==='world.chat.send').length,0);assert.equal(calls.filter(call=>call.op==='world.attachment.cancel').length,1);assert.equal(disk.messages.size,0);
  await sender.reconcile();assert.deepEqual(sender.refs(),[refs[1]]);assert.equal(sender.pendingIntent(),null);assert.equal(sender.canSend(),true);
  const next=(await sender.prepareSend({clientId:'f3_cancel_next',text:'Сохранённый черновик',replyTo:null,attachments:sender.refs()})).intent;
  await api.request('world.chat.send',{...f.base(actor),...next});await sender.acknowledge(next.attachments,next);assert.equal(disk.data.size,0);assert.equal(sender.pendingIntent(),null);
  const db=new DatabaseSync(f.worldPath,{readOnly:true});try{assertSqlIntent(db,f.group.communityId,next);assert.equal(db.prepare('SELECT state FROM world_chat_attachments WHERE id=?').get(refs[0]).state,'cancelled');assert.equal(db.prepare('SELECT COUNT(*) AS n FROM messages WHERE community_id=?').get(f.group.communityId).n,2);}finally{db.close();}
});

test('download rejects changed chunk bytes and a changed ordered manifest before returning a Blob',async()=>{
  const api=wire(),disk=memoryDisk(),box=createAttachmentOutbox({api,accountId:'actor_a',communityId:'community_a',disk});await box.ready;await box.add([new File(['safe'],'safe.html')]);await wait(()=>box.canSend());
  const scope={expectedAccountId:'actor_a',communityId:'community_a',attachmentId:box.refs()[0]},ref=(await api.request('world.attachment.status',scope)).attachment;
  const tampered={request:async(op,args)=>{const r=await api.request(op,args);return op==='world.attachment.read'?{...r,data:Buffer.from('fake').toString('base64')}:r;}};
  await assert.rejects(downloadAttachment({api:tampered,accountId:'actor_a',communityId:'community_a',attachment:ref}),e=>e.code==='attachment_integrity_failed');
  const changed={request:async(op,args)=>{const r=await api.request(op,args);if(op==='world.attachment.read'){const bytes=Buffer.from('fake');return {...r,data:bytes.toString('base64'),sha256:sha(bytes)};}return r;}};
  await assert.rejects(downloadAttachment({api:changed,accountId:'actor_a',communityId:'community_a',attachment:ref}),e=>e.code==='attachment_integrity_failed');box.close();
});
