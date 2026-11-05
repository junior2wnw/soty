// Current-client privacy fences contain Source identities and operation state,
// never note text, passwords or permission claims. Drafts keep their own rows.
const clone=value=>structuredClone(value);
const fail=code=>new Error(code);
const validId=value=>typeof value==='string'&&/^[A-Za-z0-9][A-Za-z0-9_-]{7,95}$/u.test(value);
const check=(value,code='notes_local_protection_invalid')=>{if(!value)throw fail(code);};
const databases=new WeakMap(),hubs=new WeakMap(),fallbackHubs=new Map();
// A metadata-only shell guard survives disposal/account changes. These private
// ports never return content to another account; draining writes local scope only.
const registeredSessions=new Set();
export const hasRetainedNativeDrafts=()=>[...registeredSessions].some(port=>port.hasUnsavedChanges?.()===true);
export async function flushRetainedNativeDrafts(){for(const port of [...registeredSessions])if(port.hasUnsavedChanges?.()===true)await port.flushLocal();}
/** Other encrypted adapters can join the SAME content-free close guard. This
 * does not open Notes IndexedDB, grant Source access, or expose a snapshot. */
export function registerRetainedNativeDraft(port){
  check(port&&typeof port.flushLocal==='function'&&typeof port.hasUnsavedChanges==='function');let live=true;registeredSessions.add(port);
  return()=>{if(!live)return;live=false;registeredSessions.delete(port);};
}
const sourceKey=(scope,providerId,ref)=>JSON.stringify([scope,providerId,ref]);
const draftKey=(scope,noteId,branchId)=>JSON.stringify([scope,noteId,branchId]);
const provider=value=>['soty.notes','soty.materials'].includes(value);

function database(factory){
  if(!factory)return Promise.reject(fail('notes_local_unavailable'));
  let opening=databases.get(factory);if(opening)return opening;
  opening=new Promise((resolve,reject)=>{
    let retired=false;const request=factory.open('soty-notes-drafts-v1',2);
    request.onupgradeneeded=()=>{
      const db=request.result;
      if(!db.objectStoreNames.contains('drafts')){const store=db.createObjectStore('drafts',{keyPath:'key'});store.createIndex('scope','scope');}
      if(!db.objectStoreNames.contains('nativeProtection'))db.createObjectStore('nativeProtection',{keyPath:'key'});
    };
    request.onerror=request.onblocked=()=>{retired=true;databases.delete(factory);reject(fail('notes_local_unavailable'));};
    request.onsuccess=()=>{const db=request.result;if(retired){db.close();return;}db.onversionchange=()=>{db.close();databases.delete(factory);};resolve(db);};
  }).catch(error=>{if(databases.get(factory)===opening)databases.delete(factory);throw error;});
  databases.set(factory,opening);return opening;
}

async function transaction(factory,mode,operate){
  const db=await database(factory);
  return new Promise((resolve,reject)=>{
    let result,cause;const tx=db.transaction(['drafts','nativeProtection'],mode),drafts=tx.objectStore('drafts'),gates=tx.objectStore('nativeProtection');
    const guard=callback=>()=>{try{callback();}catch(error){cause=error;tx.abort();}};
    try{operate(drafts,gates,value=>{result=value;},guard);}catch(error){cause=error;tx.abort();}
    tx.oncomplete=()=>resolve(result);tx.onabort=tx.onerror=()=>reject(cause??fail('notes_local_unavailable'));
  });
}

function hub(scope,factory=globalThis.indexedDB){
  let group=fallbackHubs;
  if(factory&&(typeof factory==='object'||typeof factory==='function')){if(!hubs.has(factory))hubs.set(factory,new Map());group=hubs.get(factory);}
  if(!group.has(scope))group.set(scope,{sessions:new Map(),listeners:new Map(),blocked:new Map(),channel:null,users:0});
  return group.get(scope);
}
function emit(scope,ref,state,factory){
  const value=hub(scope,factory);
  if(state)value.blocked.set(ref,state);else value.blocked.delete(ref);
  for(const port of [...(value.sessions.get(ref)??[])])try{port.onGate?.(state);}catch{/* Retire every alias even if one view fails. */}
  for(const callback of [...(value.listeners.get(ref)??[])])try{callback(state);}catch{/* One subscriber cannot reopen another alias. */}
}

export function readNativeSourceGate(scope,providerId,ref,factory=globalThis.indexedDB){
  check(typeof scope==='string'&&scope.length<=1800&&provider(providerId)&&validId(ref));
  const local=providerId==='soty.notes'?hub(scope,factory).blocked.get(ref):null;
  if(local)return Promise.resolve({scope,providerId,ref,operationId:'memory',state:local,epoch:'memory'});
  return transaction(factory,'readonly',(_drafts,gates,done,guard)=>{const request=gates.get(sourceKey(scope,providerId,ref));request.onsuccess=guard(()=>done(request.result?clone(request.result):null));});
}

export const nativeDraftDirty=value=>Boolean(value&&(value.generation>value.committedGeneration||value.pending||value.conflict));

/** Version2 preserves every v1 draft row and adds a separate metadata store.
 * A blocked upgrade is an observable failure, never an old-reader fallback. */
export function createNativeDraftStore(accountId,projectId='default',{origin=globalThis.location?.origin,indexedDB:factory=globalThis.indexedDB}={}){
  check([origin,projectId,accountId].every(value=>typeof value==='string'&&value.length>0&&value.length<=512),'notes_local_invalid_scope');
  const scope=JSON.stringify([origin,projectId,accountId]),localHub=hub(scope,factory);
  const gate=(providerId,ref)=>{check(provider(providerId)&&validId(ref));return transaction(factory,'readonly',(_drafts,gates,done,guard)=>{const request=gates.get(sourceKey(scope,providerId,ref));request.onsuccess=guard(()=>done(request.result?clone(request.result):null));});};
  function acquire(){
    localHub.users++;
    if(!localHub.channel&&typeof BroadcastChannel==='function'){
      try{localHub.channel=new BroadcastChannel('soty-notes-native:'+scope);localHub.channel.unref?.();localHub.channel.onmessage=event=>{
        const ref=event.data?.ref;if(!validId(ref)||event.data?.providerId!=='soty.notes')return;
        // A cross-tab message is a hint; the committed IDB fence is the truth.
        void gate('soty.notes',ref).then(value=>{if(value)emit(scope,ref,value.state,factory);}).catch(()=>emit(scope,ref,'pending',factory));
      };}catch{/* Durable reads still enforce the fence without a notification channel. */}
    }
    return()=>{if(--localHub.users<=0){localHub.users=0;localHub.channel?.close();localHub.channel=null;}};
  }
  const changed=(providerId,ref,state)=>{if(providerId==='soty.notes')emit(scope,ref,state,factory);try{localHub.channel?.postMessage({providerId,ref});}catch{}};
  const inspect=noteId=>{check(validId(noteId));return transaction(factory,'readonly',(drafts,_gates,done,guard)=>{const request=drafts.index('scope').getAll(scope,33);request.onsuccess=guard(()=>done(request.result.filter(value=>value.note.noteId===noteId).map(clone)));});};
  const native={scope,gate,
    async mark({providerId,ref,operationId,state='pending',target}){
      check(provider(providerId)&&validId(ref)&&typeof operationId==='string'&&operationId.length<=128&&operationId.length>0&&['pending','sealed'].includes(state));
      check(target===undefined||target&&validId(target.objectId)&&Number.isSafeInteger(target.expectedObjectRevision)&&target.expectedObjectRevision>0);
      const value=await transaction(factory,'readwrite',(_drafts,gates,done,guard)=>{
        const key=sourceKey(scope,providerId,ref),request=gates.get(key);request.onsuccess=guard(()=>{
          const previous=request.result;if(previous&&(previous.state==='sealed'||previous.state===state)){done(clone(previous));return;}
          const value={key,scope,providerId,ref,operationId,state,epoch:crypto.randomUUID(),...(previous?.target?{target:clone(previous.target)}:target?{target:clone(target)}:{})};gates.put(value);done(value);
        });
      });changed(providerId,ref,value.state);return value;
    },
    inspect,
    async finalize({providerId,ref,epoch,expected=[]}){
      check(provider(providerId)&&validId(ref)&&typeof epoch==='string'&&Array.isArray(expected));
      const complete=await transaction(factory,'readwrite',(drafts,gates,done,guard)=>{
        const key=sourceKey(scope,providerId,ref),request=gates.get(key);request.onsuccess=guard(()=>{
          const current=request.result;if(!current||current.epoch!==epoch){done(false);return;}
          const rows=drafts.index('scope').getAll(scope,33);rows.onsuccess=guard(()=>{
            const actual=providerId==='soty.notes'?rows.result.filter(value=>value.note.noteId===ref):[];
            if(actual.length!==expected.length||actual.some(value=>!expected.some(item=>item.branchId===value.branchId&&item.savedAt===value.savedAt&&item.generation===value.generation))){done(false);return;}
            for(const value of actual)drafts.delete(value.key);gates.put({...current,state:'sealed',epoch:crypto.randomUUID()});done(true);
          });
        });
      });if(complete)changed(providerId,ref,'sealed');return complete;
    },
    async cancel({providerId,ref,epoch}){
      check(provider(providerId)&&validId(ref));const removed=await transaction(factory,'readwrite',(_drafts,gates,done,guard)=>{
        const key=sourceKey(scope,providerId,ref),request=gates.get(key);request.onsuccess=guard(()=>{const row=request.result;if(!row||row.state==='sealed'||row.epoch!==epoch){done(false);return;}gates.delete(key);done(true);});
      });if(removed)changed(providerId,ref,null);return removed;
    },
    register(noteId,port){
      check(validId(noteId)&&port&&['suspend','flush','flushLocal','snapshot','releaseRecovered'].every(key=>typeof port[key]==='function'));
      if(!localHub.sessions.has(noteId))localHub.sessions.set(noteId,new Set());localHub.sessions.get(noteId).add(port);registeredSessions.add(port);const release=acquire();
      let live=true;void gate('soty.notes',noteId).then(value=>{if(live&&value)port.onGate?.(value.state);}).catch(()=>{});
      return()=>{if(!live)return;live=false;registeredSessions.delete(port);localHub.sessions.get(noteId)?.delete(port);if(!localHub.sessions.get(noteId)?.size)localHub.sessions.delete(noteId);release();};
    },
    sessions(noteId){check(validId(noteId));return[...(localHub.sessions.get(noteId)??[])];},
    subscribe(noteId,callback){check(validId(noteId)&&typeof callback==='function');if(!localHub.listeners.has(noteId))localHub.listeners.set(noteId,new Set());localHub.listeners.get(noteId).add(callback);const release=acquire();return()=>{localHub.listeners.get(noteId)?.delete(callback);release();};},
    suspend(noteId){check(validId(noteId));localHub.blocked.set(noteId,'pending');for(const port of [...(localHub.sessions.get(noteId)??[])])port.suspend();},
    resume(noteId){check(validId(noteId));emit(scope,noteId,null,factory);},
  };
  return{native,
    async put(value){
      check(value?.note&&validId(value.note.noteId)&&typeof value.branchId==='string','notes_local_invalid_draft');
      const record={...clone(value),scope,key:draftKey(scope,value.note.noteId,value.branchId)},bytes=new TextEncoder().encode(JSON.stringify(record)).length;
      check(bytes<=800*1024,'notes_local_quota');
      return transaction(factory,'readwrite',(drafts,gates,done,guard)=>{
        const fence=gates.get(sourceKey(scope,'soty.notes',record.note.noteId));fence.onsuccess=guard(()=>{
          check(fence.result?.state!=='sealed','notes_note_sealed');
          const request=drafts.index('scope').getAll(scope,33);request.onsuccess=guard(()=>{
            const others=request.result.filter(value=>value.key!==record.key);check(others.length<32&&others.reduce((sum,value)=>sum+(value.bytes??0),bytes)<=4*1024*1024,'notes_local_quota');
            drafts.put({...record,bytes});done();
          });
        });
      });
    },
    async list(){const rows=await transaction(factory,'readonly',(drafts,gates,done,guard)=>{
      const request=drafts.index('scope').getAll(scope,33);request.onsuccess=guard(()=>{
        const rows=request.result;if(!rows.length){done([]);return;}const result=[];let remaining=rows.length;
        for(const value of rows){const fence=gates.get(sourceKey(scope,'soty.notes',value.note.noteId));fence.onsuccess=guard(()=>{if(!fence.result)result.push(clone(value));if(--remaining===0)done(result.sort((a,b)=>b.savedAt-a.savedAt));});}
      });
    });const result=[];for(const row of rows)if(!await readNativeSourceGate(scope,'soty.notes',row.note.noteId,factory))result.push(row);return result;},
    remove(noteId,branchId,savedAt){return transaction(factory,'readwrite',(drafts,_gates,done,guard)=>{
      const key=draftKey(scope,noteId,branchId),request=drafts.get(key);request.onsuccess=guard(()=>{if(request.result&&(savedAt===undefined||request.result.savedAt===savedAt))drafts.delete(key);done();});
    });},
  };
}
