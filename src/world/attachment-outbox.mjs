import { ATTACHMENT_LIMITS as L, attachmentManifest } from '../../modules/world/attachment-contract.mjs';
import { attachmentFileName } from '../../modules/world/attachment-file-policy.mjs';
import { readDraftRecord, initializeDraftRecord, writeDraftRecord, selectDraftRecord, acknowledgeDraftRecord, recoverLegacyDraftRecord } from './chat-draft-record.mjs';
const hash = async bytes => Array.from(new Uint8Array(await crypto.subtle.digest('SHA-256', bytes)), b => b.toString(16).padStart(2, '0')).join('');
const base64 = bytes => { let value = ''; for (let i=0;i<bytes.length;i+=8192) value += String.fromCharCode(...bytes.subarray(i,i+8192)); return btoa(value); };
const decode = value => Uint8Array.from(atob(value), b => b.charCodeAt(0));
const need = (ok, code) => { if (!ok) throw Object.assign(Error(code), { code }); };
const intentCopy = value => value ? ({clientId:value.clientId,text:value.text,replyTo:value.replyTo??null,attachments:[...(value.attachments??[])],...(value.draftId?{draftId:value.draftId}:{})}) : null;
const intentKey = prefix => '$message:'+prefix;
const draftKey = prefix => '$draft:'+prefix;
function validIntent(value) {
  return value&&/^[A-Za-z0-9_-]{3,160}$/u.test(value.clientId)&&typeof value.text==='string'&&value.text.length<=6000
    &&(value.replyTo===null||value.replyTo===undefined||/^[A-Za-z0-9_-]{3,160}$/u.test(value.replyTo))
    &&(value.draftId===undefined||/^[A-Za-z0-9_-]{3,160}$/u.test(value.draftId))
    &&Array.isArray(value.attachments)&&value.attachments.length<=L.batch&&(value.attachments.length>0||value.text.trim().length>0)
    &&new Set(value.attachments).size===value.attachments.length&&value.attachments.every(id=>/^[A-Za-z0-9_-]{3,160}$/u.test(id));
}
// Narrow queue invalidation only. IDB remains authoritative, including when a
// tab misses a broadcast; prepareSend rechecks it before every publication.
const queueReaders=new Map(),draftReaders=new Map();let queueChannel;
function subscribeReaders(readers,prefix,read) {
  const set=readers.get(prefix)??new Set();set.add(read);readers.set(prefix,set);
  if(!queueChannel&&typeof BroadcastChannel==='function'){
    queueChannel=new BroadcastChannel('soty.chat.attachment-queue.v1');
    queueChannel.onmessage=event=>{const readers=event.data?.kind==='queue-changed'?queueReaders:event.data?.kind==='draft-changed'?draftReaders:null;
      if(readers&&typeof event.data.scope==='string')for(const load of readers.get(event.data.scope)??[])void load().catch(()=>{});};
    queueChannel.unref?.();
  }
  return ()=>{set.delete(read);if(!set.size)readers.delete(prefix);};
}
const subscribeQueue=(prefix,read)=>subscribeReaders(queueReaders,prefix,read);
export const subscribeChatDraft=(prefix,read)=>subscribeReaders(draftReaders,prefix,read);
async function announceQueue(prefix) {
  await Promise.allSettled([...queueReaders.get(prefix)??[]].map(read=>read()));
  queueChannel?.postMessage({kind:'queue-changed',scope:prefix});
}
export async function announceChatDraft(prefix) {
  await Promise.allSettled([...draftReaders.get(prefix)??[]].map(read=>read()));
  queueChannel?.postMessage({kind:'draft-changed',scope:prefix});
}
export { ATTACHMENT_LIMITS } from '../../modules/world/attachment-contract.mjs';

/** Blob bytes and metadata share one IDB commit. No capability URLs or tokens. */
export function attachmentDisk(indexed = globalThis.indexedDB) {
  let database;
  const opened = () => database ??= new Promise((resolve, reject) => {
    if (!indexed?.open) { reject(Object.assign(Error('attachment_local_storage'), { code: 'attachment_local_storage' })); return; }
    const r=indexed.open('soty.chat.attachments.v1',1);
    let failed = false;
    const fail = () => { failed = true; reject(Object.assign(Error('attachment_local_storage'), { code: 'attachment_local_storage' })); };
    r.onupgradeneeded=event=>{ if (event.oldVersion !== 0) { r.transaction.abort(); fail(); return; } r.result.createObjectStore('outbox',{keyPath:'key'}); };
    r.onsuccess=()=>{ const db=r.result; if(failed||db.version!==1||db.objectStoreNames.length!==1||!db.objectStoreNames.contains('outbox')) {db.close();fail();return;} db.onversionchange=()=>{db.close();database=null;};resolve(db); };
    r.onerror=r.onblocked=fail;
  }).catch(reason=>{database=null;throw reason;});
  async function transaction(mode, callback) {
    const db=await opened();return new Promise((resolve,reject)=>{
      const tx=db.transaction('outbox',mode),store=tx.objectStore('outbox');let result;
      tx.oncomplete=()=>resolve(result);tx.onerror=tx.onabort=()=>reject(Object.assign(Error('attachment_local_storage'),{code:'attachment_local_storage'}));
      try { const request=callback(store);if(request)request.onsuccess=()=>{result=request.result;}; } catch(error){tx.abort();reject(error);}
    });
  }
  async function draftTransaction(prefix, operation) {
    const db=await opened();return new Promise((resolve,reject)=>{
      const tx=db.transaction('outbox','readwrite'),store=tx.objectStore('outbox');let result,error;
      tx.oncomplete=()=>resolve(result);tx.onabort=tx.onerror=()=>reject(error??Object.assign(Error('attachment_local_storage'),{code:'attachment_local_storage'}));
      const request=store.get(draftKey(prefix));request.onsuccess=()=>{try{
        const current=request.result, changed=operation(current);
        if(changed.record)store.put({...changed.record,key:draftKey(prefix)});
        result={...readDraftRecord(changed.record??current),...(changed.head?{head:changed.head}:{}),...(changed.stale?{stale:true}:{})};
      }catch(reason){error=reason;tx.abort();}};
    });
  }
  return {
    list: prefix=>transaction('readonly',store=>store.getAll(IDBKeyRange.bound(prefix,prefix+'\uffff'))),
    put: row=>transaction('readwrite',store=>store.put(row)),
    remove: key=>transaction('readwrite',store=>store.delete(key)),
    removeBatch: keys=>transaction('readwrite',store=>{for(const key of keys)store.delete(key);}),
    async draftRead(prefix) {return readDraftRecord(await transaction('readonly',store=>store.get(draftKey(prefix))));},
    async draftList(accountPrefix) {
      const key=draftKey(accountPrefix),rows=await transaction('readonly',store=>store.getAll(IDBKeyRange.bound(key,key+'\uffff')));
      return rows.map(row=>{try{return {key:row.key,...readDraftRecord(row)};}catch{return {key:row.key,revision:1,heads:[]};}});
    },
    draftInitialize(prefix,legacy,legacyId) {return draftTransaction(prefix,value=>({record:initializeDraftRecord(value,legacy,legacyId)}));},
    draftWrite(prefix,proposal) {return draftTransaction(prefix,value=>writeDraftRecord(value,proposal));},
    draftSelect(prefix,expectedIds,selectedId,newId,writer) {return draftTransaction(prefix,value=>selectDraftRecord(value,expectedIds,selectedId,newId,writer));},
    draftAcknowledge(prefix,id) {return draftTransaction(prefix,value=>({record:acknowledgeDraftRecord(value,id)}));},
    draftRecoverLegacy(prefix,candidate,id,isCurrent) {return draftTransaction(prefix,value=>isCurrent&&!isCurrent()?{stale:true}:{record:recoverLegacyDraftRecord(value,candidate,id)});},
    async scope(prefix) {
      const db=await opened();return new Promise((resolve,reject)=>{
        const tx=db.transaction('outbox','readonly'),store=tx.objectStore('outbox');const result={rows:[],message:null};
        tx.oncomplete=()=>resolve(result);tx.onabort=tx.onerror=()=>reject(Object.assign(Error('attachment_local_storage'),{code:'attachment_local_storage'}));
        store.getAll(IDBKeyRange.bound(prefix,prefix+'\uffff')).onsuccess=event=>{result.rows=event.target.result;};
        store.get(intentKey(prefix)).onsuccess=event=>{result.message=event.target.result??null;};
      });
    },
    async update(row) {
      const db=await opened();return new Promise((resolve,reject)=>{
        const tx=db.transaction('outbox','readwrite'),store=tx.objectStore('outbox');let result=null;
        tx.oncomplete=()=>resolve(result);tx.onabort=tx.onerror=()=>reject(Object.assign(Error('attachment_local_storage'),{code:'attachment_local_storage'}));
        const read=store.get(row.key);read.onsuccess=()=>{if(!read.result)return;
          result={...row,order:Number.isSafeInteger(read.result.order)?read.result.order:row.order};
          if(read.result.status==='cancel-pending')result.status='cancel-pending';
          store.put(result);
        };
      });
    },
    async beginCancel(prefix,id) {
      const db=await opened();return new Promise((resolve,reject)=>{
        const tx=db.transaction('outbox','readwrite'),store=tx.objectStore('outbox');let result=null,error;
        tx.oncomplete=()=>resolve(result);tx.onabort=tx.onerror=()=>reject(error??Object.assign(Error('attachment_local_storage'),{code:'attachment_local_storage'}));
        const read=store.get(intentKey(prefix));read.onsuccess=()=>{try{
          // The intent check and the transition share claimMessage's store
          // transaction. Notifications and earlier scope snapshots are advisory.
          need(!read.result?.pending?.attachments.includes(id),'attachment_send_unconfirmed');
          const file=store.get(prefix+id);file.onsuccess=()=>{if(!file.result)return;
            result={...file.result,status:'cancel-pending',error:null};store.put(result);
          };
        }catch(reason){error=reason;tx.abort();}};
      });
    },
    async claimMessage(prefix, proposal) {
      need(validIntent(proposal),'attachment_queue_changed');
      const db=await opened();return new Promise((resolve,reject)=>{
        const tx=db.transaction('outbox','readwrite'),store=tx.objectStore('outbox');let result,error;
        tx.oncomplete=()=>resolve(result);tx.onabort=tx.onerror=()=>reject(error??Object.assign(Error('attachment_local_storage'),{code:'attachment_local_storage'}));
        const read=store.get(intentKey(prefix));read.onsuccess=()=>{try{
          const message=read.result??{key:intentKey(prefix),kind:'message-intent.v1',pending:null,accepted:null};
          if(message.pending){need(validIntent(message.pending),'attachment_local_storage');result={state:'pending',intent:intentCopy(message.pending)};return;}
          if(message.accepted?.clientId===proposal.clientId){result={state:'accepted',intent:intentCopy(message.accepted)};return;}
          const files=store.getAll(IDBKeyRange.bound(prefix,prefix+'\uffff'));
          files.onsuccess=()=>{try{
            need(proposal.attachments.every(id=>files.result.some(row=>row.attachmentId===id&&row.status==='ready')),'attachment_queue_changed');
            message.pending=intentCopy(proposal);store.put(message);result={state:'pending',intent:intentCopy(message.pending)};
          }catch(reason){error=reason;tx.abort();}};
        }catch(reason){error=reason;tx.abort();}};
      });
    },
    async acceptMessage(prefix,intent) {
      need(validIntent(intent),'attachment_queue_changed');
      const db=await opened();return new Promise((resolve,reject)=>{
        const tx=db.transaction('outbox','readwrite'),store=tx.objectStore('outbox');let error;
        tx.oncomplete=()=>resolve();tx.onabort=tx.onerror=()=>reject(error??Object.assign(Error('attachment_local_storage'),{code:'attachment_local_storage'}));
        const read=store.get(intentKey(prefix));read.onsuccess=()=>{try{
          const message=read.result;
          if(message?.pending?.clientId===intent.clientId){
            need(JSON.stringify(message.pending)===JSON.stringify(intentCopy(intent)),'attachment_queue_changed');
            message.pending=null;message.accepted=intentCopy(intent);store.put(message);
            if(intent.draftId){const draft=store.get(draftKey(prefix));draft.onsuccess=()=>{try{
              const remaining=acknowledgeDraftRecord(draft.result,intent.draftId);if(remaining)store.put({...remaining,key:draftKey(prefix)});
            }catch(reason){error=reason;tx.abort();}};}
          }
          for(const id of intent.attachments)store.delete(prefix+id);
        }catch(reason){error=reason;tx.abort();}};
      });
    },
    async addBatch(prefix, rows) {
      const db=await opened();return new Promise((resolve,reject)=>{
        const tx=db.transaction('outbox','readwrite'),store=tx.objectStore('outbox');let error;
        tx.oncomplete=()=>resolve();tx.onabort=tx.onerror=()=>reject(error||Object.assign(Error('attachment_local_storage'),{code:'attachment_local_storage'}));
        const request=store.getAll(IDBKeyRange.bound(prefix,prefix+'\uffff'));
        request.onsuccess=()=>{try{
          const all=request.result;need(all.length+rows.length<=L.batch&&all.reduce((n,v)=>n+v.size,0)+rows.reduce((n,v)=>n+v.size,0)<=L.batchBytes,'attachment_batch_limit');
          const last=all.reduce((n,row)=>Math.max(n,Number.isSafeInteger(row.order)?row.order:0),0);
          need(Number.isSafeInteger(last+rows.length),'attachment_local_storage');
          rows.forEach((row,index)=>{row.order=last+index+1;});
          for(const row of rows)store.add(row);
        }catch(reason){error=reason;tx.abort();}};
      });
    },
  };
}

/** Exactly one outstanding chunk per file; at most one file upload per scope.
 * Reload replays immutable chunks, never skips an unverified local prefix. */
export function createAttachmentOutbox({ api, accountId, communityId, conversationKey=communityId, conversationScope, disk=attachmentDisk(),
  current=()=>true, allowUpload=()=>true, changed=()=>{}, initialOrder=[], makeId=()=>`att_${crypto.randomUUID()}` }) {
  const prefix=encodeURIComponent(accountId)+':'+encodeURIComponent(conversationKey)+':';
  const scope={...(conversationScope??{communityId}),expectedAccountId:accountId};let rows=[],closed=false,working=false,loading=true,mutating=false,message=null,refreshFlight=null,storageFailed=false;
  const active=()=>!closed&&current();
  const snapshot=()=>rows.map(({blob,key,...row})=>({...row}));
  const notify=()=>changed(snapshot());
  const save=async row=>{
    const saved=await disk.update(row);
    if(!saved){rows=rows.filter(value=>value!==row);throw Object.assign(Error('attachment_queue_changed'),{code:'attachment_queue_changed'});}
    Object.assign(row,saved);
  };
  const request=async(op,args)=>{need(active(),'attachment_context_changed');const result=await api.request(op,{...scope,...args});need(active(),'attachment_context_changed');return result;};
  async function upload(row) {
    row.status='uploading';row.error=null;notify();
    try {
      row.admissionAttempted=true;await save(row);need(row.status!=='cancel-pending','attachment_cancelled');await request('world.attachment.begin',{attachmentId:row.attachmentId,name:row.name,size:row.size,mediaType:row.mediaType});
      const chunks=[];
      for(let offset=0,index=0;offset<row.size;offset+=L.chunkBytes,index++) {
        need(active()&&rows.includes(row)&&row.status!=='cancel-pending','attachment_cancelled');
        const bytes=new Uint8Array(await row.blob.slice(offset,offset+L.chunkBytes).arrayBuffer());
        const digest=await hash(bytes);chunks.push({bytes:bytes.length,sha256:digest});
        need(active()&&rows.includes(row)&&row.status!=='cancel-pending','attachment_cancelled');
        const ack=await request('world.attachment.chunk',{attachmentId:row.attachmentId,index,sha256:digest,data:base64(bytes)});
        need(ack.accepted===true&&ack.index===index&&ack.sha256===digest,'attachment_integrity_failed');
        row.sent=offset+bytes.length;notify();
      }
      const manifestSha256=await hash(new TextEncoder().encode(attachmentManifest(row.size,chunks)));
      const result=await request('world.attachment.complete',{attachmentId:row.attachmentId,manifestSha256});
      need(result.attachment.attachmentId===row.attachmentId&&result.attachment.manifestSha256===manifestSha256
        &&result.attachment.size===row.size&&result.attachment.state==='complete','attachment_integrity_failed');
      need(row.status!=='cancel-pending','attachment_cancelled');
      row.status='ready';row.sent=row.size;row.manifestSha256=manifestSha256;await save(row);
    } catch(error) {
      if(row.status!=='cancel-pending') { row.status='error';row.error=error?.code||'attachment_transfer_failed';try{await save(row);}catch{row.error='attachment_local_storage';} }
    }
    notify();void announceQueue(prefix);
  }
  async function pump() {
    if(working||loading||!active())return;working=true;
    try {
      for(const row of [...rows]) {
        if(!active())break;
        if(row.status==='cancel-pending') {
          try {if(row.admissionAttempted)await request('world.attachment.cancel',{attachmentId:row.attachmentId});await disk.remove(row.key);rows=rows.filter(v=>v!==row);notify();void announceQueue(prefix);}
          catch(error) {row.error=error?.code||'attachment_cancel_pending';notify();}
        } else if(row.status==='pending'&&allowUpload()) await upload(row);
      }
    } finally {working=false;if(active()&&rows.some(v=>v.status==='cancel-pending'&&!v.error))queueMicrotask(()=>{void pump();});}
  }
  async function readQueue() {
    const state=await disk.scope(prefix),stored=state.rows,record=state.message;
    need(Array.isArray(stored)&&stored.length<=L.batch,'attachment_local_storage');
    need(!record||record.kind==='message-intent.v1'&&(!record.pending||validIntent(record.pending))&&(!record.accepted||validIntent(record.accepted)),'attachment_local_storage');
    message=record??null;
    // Earlier, unreleased local pilot rows did not have an ordinal. Recover
    // their draft order once; all new ordinals are allocated inside addBatch.
    const preferred=new Map(initialOrder.map((id,index)=>[id,index]));
    stored.sort((a,b)=>{
      if(Number.isSafeInteger(a.order)&&Number.isSafeInteger(b.order))return a.order-b.order;
      return (preferred.get(a.attachmentId)??Number.MAX_SAFE_INTEGER)-(preferred.get(b.attachmentId)??Number.MAX_SAFE_INTEGER);
    });
    const next=[];
    for(const [index,value] of stored.entries()) {
      need(value.key===prefix+value.attachmentId&&/^[A-Za-z0-9_-]{3,160}$/u.test(value.attachmentId)&&value.blob instanceof Blob
        &&value.blob.size===value.size&&Number.isSafeInteger(value.size)&&value.size>=0&&value.size<=L.fileBytes,'attachment_local_storage');
      need(value.order===undefined||Number.isSafeInteger(value.order)&&value.order>0,'attachment_local_storage');
      const existing=rows.find(row=>row.attachmentId===value.attachmentId);
      const row={...value,order:Number.isSafeInteger(value.order)?value.order:index+1,status:value.status==='uploading'&&existing?.status!=='uploading'?'pending':value.status};
      if(existing?.status==='uploading'&&row.status==='uploading')row.sent=Math.max(row.sent,existing.sent);
      if(value.order===undefined)await disk.update(row);
      if(existing){Object.assign(existing,row);next.push(existing);}else next.push(row);
    }
    need(next.reduce((n,v)=>n+v.size,0)<=L.batchBytes,'attachment_batch_limit');rows=next;storageFailed=false;loading=false;notify();void pump();
  }
  const ready=readQueue();
  function reconcile() {
    if(closed)return Promise.resolve();
    // Each invalidation queues a fresh read after any older snapshot, so an
    // accepted ACK cannot merely await a read that began before its commit.
    return refreshFlight=(refreshFlight??ready).catch(()=>{}).then(async()=>{if(closed)return;try{await readQueue();}catch(error){storageFailed=true;notify();throw error;}});
  }
  const unsubscribe=subscribeQueue(prefix,reconcile);
  return {
    ready,snapshot,reconcile,
    pendingIntent:()=>intentCopy(message?.pending),
    acceptedIntent:()=>intentCopy(message?.accepted),
    isPending:id=>!!message?.pending?.attachments.includes(id),
    async prepareSend(proposal) {
      await ready;need(active(),'attachment_context_changed');
      const result=await disk.claimMessage(prefix,intentCopy(proposal));
      await reconcile();await announceQueue(prefix);return result;
    },
    async add(files) {
      await ready;need(active()&&!mutating,'attachment_queue_busy');need(allowUpload(),'attachments_disabled');mutating=true;
      try {
        need(Array.isArray(files)&&files.length>0&&rows.length+files.length<=L.batch,'attachment_batch_limit');
        need(files.every(file=>file instanceof Blob&&file.size<=L.fileBytes)&&rows.reduce((n,v)=>n+v.size,0)+files.reduce((n,f)=>n+f.size,0)<=L.batchBytes,'attachment_batch_limit');
        const added=[];
        for(const file of files) {
          need(active(),'attachment_context_changed');const attachmentId=makeId();
          const row={key:prefix+attachmentId,attachmentId,order:Math.max(0,...rows.map(row=>row.order))+added.length+1,name:attachmentFileName(file.name||'файл'),mediaType:String(file.type||'').slice(0,128),size:file.size,blob:file,status:'pending',sent:0,error:null,admissionAttempted:false};
          added.push(row);
        }
        await disk.addBatch(prefix,added);rows.push(...added);rows.sort((a,b)=>a.order-b.order);notify();void announceQueue(prefix);
      } finally {mutating=false;void pump();}
    },
    async retry(id) {await ready;const row=rows.find(v=>v.attachmentId===id);if(row&&row.status==='error'){row.status='pending';row.error=null;await save(row);notify();void pump();}else {if(row)row.error=null;void pump();}},
    async remove(id) {
      await ready;await reconcile();let cancelled;
      try{cancelled=await disk.beginCancel(prefix,id);}catch(error){if(error?.code==='attachment_send_unconfirmed')await reconcile();throw error;}
      if(!cancelled){await reconcile();return;}
      const row=rows.find(v=>v.attachmentId===id);if(row)Object.assign(row,cancelled);else rows.push(cancelled);
      notify();void announceQueue(prefix);void pump();
    },
    async acknowledge(ids,intent) {
      if(intent)await disk.acceptMessage(prefix,intentCopy(intent));else await disk.removeBatch(ids.map(id=>prefix+id));
      rows=rows.filter(v=>!ids.includes(v.attachmentId));if(!closed)await reconcile();await announceQueue(prefix);
    },
    refs() {return rows.filter(v=>v.status==='ready').map(v=>v.attachmentId);},
    async localFile(id) { await ready; need(active(), 'attachment_context_changed'); const row=rows.find(value=>value.attachmentId===id); need(row?.blob instanceof Blob, 'attachment_unavailable'); return {name:row.name,blob:row.blob}; },
    canSend() {return !storageFailed&&!loading&&!mutating&&(message?.pending?message.pending.attachments.every(id=>rows.some(v=>v.attachmentId===id&&v.status==='ready')):allowUpload()&&rows.length>0&&rows.every(v=>v.status==='ready'));},
    close() {closed=true;unsubscribe();},
  };
}

/** Every chunk is separately authorized and verified. Bytes remain a download,
 * including HTML/SVG; a local Blob URL is revoked by the UI after the click. */
export async function downloadAttachment({api,accountId,communityId,conversationScope,attachment,current=()=>true}) {
  const scope={...(conversationScope??{communityId}),expectedAccountId:accountId,attachmentId:attachment.attachmentId};
  need(current(),'attachment_context_changed');
  const status=await api.request('world.attachment.status',scope);need(current(),'attachment_context_changed');
  const ref=status.attachment;need(['complete','published'].includes(ref.state)&&ref.attachmentId===attachment.attachmentId
    &&ref.manifestSha256===attachment.manifestSha256&&ref.fileSha256===attachment.fileSha256&&ref.size===attachment.size
    &&Number.isSafeInteger(ref.size)&&ref.size>=0&&ref.size<=L.fileBytes&&ref.chunks===Math.ceil(ref.size/L.chunkBytes),'attachment_integrity_failed');
  const parts=[],chunks=[];
  for(let index=0;index<ref.chunks;index++) {
    need(current(),'attachment_context_changed');const part=await api.request('world.attachment.read',{...scope,index});need(current(),'attachment_context_changed');
    need(typeof part.data==='string'&&part.data.length<=Math.ceil(L.chunkBytes/3)*4&&/^[a-f0-9]{64}$/u.test(part.sha256),'attachment_integrity_failed');
    const bytes=decode(part.data);need(part.index===index&&bytes.length===Math.min(L.chunkBytes,ref.size-index*L.chunkBytes)
      &&part.size===bytes.length&&await hash(bytes)===part.sha256,'attachment_integrity_failed');
    chunks.push({bytes:bytes.length,sha256:part.sha256});parts.push(bytes);
  }
  need(await hash(new TextEncoder().encode(attachmentManifest(ref.size,chunks)))===ref.manifestSha256,'attachment_integrity_failed');
  need(current(),'attachment_context_changed');return {name:attachmentFileName(ref.name),blob:new Blob(parts,{type:'application/octet-stream'})};
}
