import {prepareMaterialUpload,uploadMaterial,materialSha256} from '../../modules/materials/browser/client.mjs';
import {materialMediaType,MATERIAL_LIMITS} from '../../modules/materials/contract.mjs';
import {validateCanvasMask} from '../../modules/canvas-objects/contract.mjs';
import {readNativeSourceGate} from '../../modules/notes/browser/native-local.mjs';
import {nativeCreationSource,inspectNativeCreationRecords} from './canvas-native-creation-inspection.mjs';

export const CANVAS_CREATE_LIMITS=Object.freeze({pending:8,completed:32,blobBytes:25*1024*1024,metadataBytes:4*1024*1024});
const SCHEMA='soty.canvas-create.v1',CONNECTION_SCHEMA='soty.canvas-create.v2',stages=['connect','source','object','placement','settling','complete'];
const stageOrder=record=>record.schema===CONNECTION_SCHEMA?stages:['source','object','placement','complete'];
const uid=()=>crypto.randomUUID(),clone=value=>structuredClone(value);
export class CanvasCreateError extends Error {
  constructor(code,context={}){super(code);this.name='CanvasCreateError';this.code=code;Object.assign(this,context);}
}
const check=(value,code='canvas_create_invalid')=>{if(!value)throw new CanvasCreateError(code);};
const id=value=>typeof value==='string'&&/^[A-Za-z0-9][A-Za-z0-9_.:-]{0,127}$/u.test(value);
const errorCode=error=>String(error?.code||error?.error?.code||error?.message||'canvas_create_failed');
const creationHubs=new WeakMap(),fallbackCreationHubs=new Map();
function creationHub(factory,scope){
  let group=fallbackCreationHubs;if(factory&&(typeof factory==='object'||typeof factory==='function')){if(!creationHubs.has(factory))creationHubs.set(factory,new Map());group=creationHubs.get(factory);}
  if(!group.has(scope))group.set(scope,{listeners:new Set(),users:0});return{group,hub:group.get(scope)};
}
function metadataSize(record){return new TextEncoder().encode(JSON.stringify(record,(key,value)=>value instanceof Blob?undefined:value)).length;}
function validateRecord(value,accountId,projectId){
  check(value&&[SCHEMA,CONNECTION_SCHEMA].includes(value.schema)&&id(value.operationId)&&value.accountId===accountId&&value.projectId===projectId,'canvas_create_scope_changed');
  check(['note','image','audio','video','widget'].includes(value.kind)&&stages.includes(value.stage)&&Number.isSafeInteger(value.version)&&value.version>0
    &&Number.isSafeInteger(value.createdAt)&&value.createdAt>=0&&id(value.objectRequestId)&&/^[a-f0-9]{64}$/u.test(value.intentHash),'canvas_create_journal_invalid');
  validateCanvasMask(value.presentation?.mask);
  check(Number.isFinite(value.width)&&Number.isFinite(value.height)&&value.width>=16&&value.width<=8192&&value.height>=16&&value.height<=8192,'canvas_create_journal_invalid');
  if(value.schema===CONNECTION_SCHEMA){const link=value.connection;check(value.kind==='widget'&&link&&Object.keys(link).sort().join(',')==='appId,requestId,settled'&&link.requestId===value.operationId&&typeof link.settled==='boolean'&&link.settled===(value.stage==='complete')&&(value.stage==='connect'?link.appId===null:typeof link.appId==='string'&&/^app-[a-f0-9]{32}$/u.test(link.appId)),'canvas_create_journal_invalid');if(['connect','source'].includes(value.stage))check(id(value.sourceIntent?.requestId)&&(value.stage==='connect'?value.sourceIntent.appId===undefined:value.sourceIntent.appId===link.appId),'canvas_create_journal_invalid');}
  else check(!['connect','settling'].includes(value.stage),'canvas_create_journal_invalid');
  if(!['connect','source'].includes(value.stage))check(id(value.sourceRef),'canvas_create_journal_invalid');
  if(['placement','settling','complete'].includes(value.stage))check(id(value.objectId),'canvas_create_journal_invalid');
  if(['settling','complete'].includes(value.stage))check(value.placement&&id(value.placement.shortcutId)&&['saved','pending'].includes(value.placement.status)&&value.placement.localDurable===true,'canvas_create_journal_invalid');
  if(['connect','source'].includes(value.stage))check(value.sourceIntent&&typeof value.sourceIntent==='object','canvas_create_journal_invalid');
  check(metadataSize(value)<=CANVAS_CREATE_LIMITS.metadataBytes,'canvas_create_journal_quota');return clone(value);
}
export {validateRecord as validateCanvasCreateRecord};
function storageError(error,record){return new CanvasCreateError(errorCode(error).startsWith('canvas_create_')?errorCode(error):'canvas_create_local_unavailable',
  {operationId:record?.operationId,localDurable:false,cause:error});}

/** Per-origin/project/account transient creation journal, not another Notes or media source database. */
export function createCanvasCreateStore(accountId,projectId,{origin=globalThis.location?.origin,indexedDB=globalThis.indexedDB}={}){
  check(id(accountId)&&id(projectId)&&typeof origin==='string'&&/^https?:\/\//u.test(origin),'canvas_create_local_unavailable');
  const scope=JSON.stringify([origin,projectId,accountId]);let opening,closed=false;
  const {group:hubGroup,hub:localHub}=creationHub(indexedDB,scope);localHub.users++;const ownListeners=new Set();let channel=null;
  const emitRetired=source=>{for(const callback of [...localHub.listeners])try{callback(clone(source));}catch{/* A live observer cannot undo committed retirement. */}};
  if(indexedDB&&indexedDB===globalThis.indexedDB&&typeof BroadcastChannel==='function'){
    try{channel=new BroadcastChannel('soty-canvas-creation:'+scope);channel.onmessage=event=>{
      const source=event.data;if(!['soty.notes','soty.materials'].includes(source?.providerId)||!id(source.ref)||!id(source.epoch)||closed)return;
      // A hint is never authority to discard RAM: check the actual same-scope
      // transaction's sealed fence before notifying a still-open Flow.
      void sourceFence(source).then(gate=>{if(!closed&&gate?.state==='sealed'&&gate.epoch===source.epoch)emitRetired({providerId:source.providerId,ref:source.ref});}).catch(()=>{});
    };}catch{/* Same-tab durable guard remains; a missing hint never grants a write. */}
  }
  const sourceKey=source=>{check(['soty.notes','soty.materials'].includes(source?.providerId)&&id(source.ref),'canvas_create_scope_changed');return JSON.stringify([scope,source.providerId,source.ref]);};
  const matches=(record,source)=>{const current=nativeCreationSource(record);return current?.providerId===source.providerId&&current.ref===source.ref;};
  const database=()=>{
    check(!closed,'canvas_create_local_unavailable');
    if(!opening)opening=new Promise((resolve,reject)=>{
      if(!indexedDB){reject(new CanvasCreateError('canvas_create_local_unavailable'));return;}
      let retired=false;const request=indexedDB.open('soty-canvas-create-v1',2);
      request.onupgradeneeded=()=>{const db=request.result;if(!db.objectStoreNames.contains('operations')){const store=db.createObjectStore('operations',{keyPath:'key'});store.createIndex('scope','scope');}if(!db.objectStoreNames.contains('nativeProtection'))db.createObjectStore('nativeProtection',{keyPath:'key'});};
      request.onerror=request.onblocked=()=>{retired=true;reject(new CanvasCreateError('canvas_create_local_unavailable'));};
      request.onsuccess=()=>{const db=request.result;if(closed||retired){db.close();reject(new CanvasCreateError('canvas_create_local_unavailable'));return;}db.onversionchange=()=>{db.close();opening=undefined;};resolve(db);};
    }).catch(error=>{opening=undefined;throw error;});return opening;
  };
  const key=operationId=>JSON.stringify([scope,operationId]);
  async function read(operationId){
    const db=await database();return new Promise((resolve,reject)=>{
      const tx=db.transaction('operations','readonly'),request=operationId?tx.objectStore('operations').get(key(operationId)):tx.objectStore('operations').index('scope').getAll(scope,65);let result;
      request.onsuccess=()=>{result=operationId?(request.result?.value??null):request.result.map(row=>row.value);};
      tx.oncomplete=()=>resolve(result);tx.onerror=tx.onabort=()=>reject(new CanvasCreateError('canvas_create_local_unavailable'));
    });
  }
  async function sourceFence(source){const db=await database();return new Promise((resolve,reject)=>{
    const tx=db.transaction('nativeProtection','readonly'),request=tx.objectStore('nativeProtection').get(sourceKey(source));let result;
    request.onsuccess=()=>{result=request.result??null;};tx.oncomplete=()=>resolve(result);tx.onerror=tx.onabort=()=>reject(new CanvasCreateError('canvas_create_local_unavailable'));
  });}
  return {
    get:operationId=>read(operationId),list:()=>read(),
    subscribeSourceRetire(callback){check(typeof callback==='function');localHub.listeners.add(callback);ownListeners.add(callback);return()=>{localHub.listeners.delete(callback);ownListeners.delete(callback);};},
    async inspectSourceIntents(source){return inspectNativeCreationRecords(await read(),source);},
    async snapshotSourceIntents(source){sourceKey(source);return(await read()).filter(value=>matches(value,source)).map(value=>validateRecord(value,accountId,projectId));},
    async nativeGate(source){return await sourceFence(source)??readNativeSourceGate(scope,source.providerId,source.ref,indexedDB);},
    sourceFence,
    async sealSource(source,operationId,target){const key=sourceKey(source);check(id(operationId)&&(!target||id(target.objectId)&&Number.isSafeInteger(target.expectedObjectRevision)&&target.expectedObjectRevision>0));const db=await database();return new Promise((resolve,reject)=>{
      const tx=db.transaction('nativeProtection','readwrite'),gates=tx.objectStore('nativeProtection'),request=gates.get(key);let result;
      request.onsuccess=()=>{result=request.result??{key,scope,providerId:source.providerId,ref:source.ref,operationId,epoch:uid(),state:'pending',...(target?{target:clone(target)}:{})};if(!request.result)gates.put(result);};
      tx.oncomplete=()=>resolve(result);tx.onerror=tx.onabort=()=>reject(new CanvasCreateError('canvas_create_local_unavailable'));
    });},
    async cancelSourceFence(source,epoch){const key=sourceKey(source),db=await database();return new Promise((resolve,reject)=>{
      const tx=db.transaction('nativeProtection','readwrite'),gates=tx.objectStore('nativeProtection'),request=gates.get(key);let done=false;
      request.onsuccess=()=>{const gate=request.result;if(!gate){done=true;return;}if(gate.epoch===epoch&&gate.state==='pending'){gates.delete(key);done=true;}};
      tx.oncomplete=()=>resolve(done);tx.onerror=tx.onabort=()=>reject(new CanvasCreateError('canvas_create_local_unavailable'));
    });},
    /** Caller must authenticate every encrypted archive before exact retirement. */
    async retireSourceIntents(source,epoch,expected){const key=sourceKey(source);check(Array.isArray(expected)&&expected.length<=64,'canvas_create_journal_invalid');const db=await database();return new Promise((resolve,reject)=>{
      const tx=db.transaction(['operations','nativeProtection'],'readwrite'),operations=tx.objectStore('operations'),gates=tx.objectStore('nativeProtection'),request=gates.get(key);let done=false,failure;
      request.onsuccess=()=>{try{const gate=request.result;if(!gate||gate.epoch!==epoch)return;const rows=operations.index('scope').getAll(scope,65);rows.onsuccess=()=>{try{
        check(rows.result.length<=64,'canvas_create_journal_quota');const current=rows.result.map(row=>row.value).filter(value=>matches(value,source));
        if(current.length!==expected.length||new Set(expected.map(value=>value.operationId)).size!==expected.length||current.some(value=>!expected.some(old=>old.operationId===value.operationId&&old.version===value.version&&old.intentHash===value.intentHash)))return;
        for(const value of current)operations.delete(JSON.stringify([scope,value.operationId]));gates.put({...gate,state:'sealed'});done=true;
      }catch(error){failure=error;tx.abort();}};}catch(error){failure=error;tx.abort();}};
      tx.oncomplete=()=>{if(done){emitRetired(source);try{channel?.postMessage({providerId:source.providerId,ref:source.ref,epoch});}catch{}}resolve(done);};tx.onerror=tx.onabort=()=>reject(failure??new CanvasCreateError('canvas_create_local_unavailable'));
    });},
    async put(value,expectedVersion){
      const record=validateRecord(value,accountId,projectId),db=await database();
      return new Promise((resolve,reject)=>{
        const tx=db.transaction(['operations','nativeProtection'],'readwrite'),store=tx.objectStore('operations'),source=nativeCreationSource(record),guard=source?tx.objectStore('nativeProtection').get(sourceKey(source)):null;let wrote=false,failure;
        const run=()=>{const request=store.index('scope').getAll(scope,65);
        request.onsuccess=()=>{try{
          check(request.result.length<=64,'canvas_create_journal_quota');const rows=request.result,current=rows.find(row=>row.value.operationId===record.operationId)?.value;
          if((current?.version??0)!==expectedVersion)return;
          check(Number.isSafeInteger(expectedVersion)&&expectedVersion>=0&&record.version===expectedVersion+1,'canvas_create_journal_conflict');
          check(!current||current.intentHash===record.intentHash,'canvas_create_journal_conflict');
          check(!current||current.kind===record.kind&&current.objectRequestId===record.objectRequestId,'canvas_create_journal_conflict');
          if(current){const before=nativeCreationSource(current),after=nativeCreationSource(record);
            check(current.schema===record.schema&&(!before&&!after||before?.providerId===after?.providerId&&before?.ref===after?.ref)
              &&stageOrder(record).indexOf(record.stage)===stageOrder(current).indexOf(current.stage)+1,'canvas_create_journal_conflict');
            if(current.schema===CONNECTION_SCHEMA)check(record.connection?.requestId===current.connection.requestId&&(current.connection.appId===null||record.connection.appId===current.connection.appId),'canvas_create_journal_conflict');}
          const values=rows.filter(row=>row.value.operationId!==record.operationId).map(row=>row.value).concat(record);
          check(values.filter(value=>value.stage!=='complete').length<=CANVAS_CREATE_LIMITS.pending,'canvas_create_journal_quota');
          check(values.reduce((sum,value)=>sum+(value.sourceIntent?.blob?.size??0),0)<=CANVAS_CREATE_LIMITS.blobBytes
            &&values.reduce((sum,value)=>sum+metadataSize(value),0)<=CANVAS_CREATE_LIMITS.metadataBytes,'canvas_create_journal_quota');
          store.put({scope,key:key(record.operationId),value:record});wrote=true;
          // Until Connect commits its final receipt, linked placements remain
          // `settling`; even an old writer's complete-only GC leaves them intact.
          const completed=values.filter(value=>value.stage==='complete').sort((a,b)=>b.createdAt-a.createdAt||a.operationId.localeCompare(b.operationId));
          for(const old of completed.slice(CANVAS_CREATE_LIMITS.completed))store.delete(key(old.operationId));
        }catch(error){failure=error;tx.abort();}};};
        if(guard)guard.onsuccess=()=>{if(guard.result){failure=new CanvasCreateError('canvas_create_native_recovery_pending');tx.abort();}else run();};else run();
        tx.oncomplete=()=>resolve(wrote);tx.onerror=tx.onabort=()=>reject(failure??new CanvasCreateError('canvas_create_local_unavailable'));
      });
    },
    async remove(operationId){const db=await database();return new Promise((resolve,reject)=>{const tx=db.transaction('operations','readwrite');tx.objectStore('operations').delete(key(operationId));tx.oncomplete=()=>resolve();tx.onabort=tx.onerror=()=>reject(new CanvasCreateError('canvas_create_local_unavailable'));});},
    close(){if(closed)return;closed=true;channel?.close();for(const callback of ownListeners)localHub.listeners.delete(callback);ownListeners.clear();if(--localHub.users===0&&localHub.listeners.size===0)hubGroup.delete(scope);if(opening)void opening.then(db=>db.close(),()=>{});},
  };
}

const dimensions=kind=>kind==='note'?{width:340,height:300}:kind==='audio'?{width:340,height:156}:kind==='widget'?{width:640,height:420}:kind==='video'?{width:420,height:280}:{width:360,height:280};
export function canvasCreateFileKind(blob,kind){
  check(blob instanceof Blob&&blob.size>0&&blob.size<=MATERIAL_LIMITS.fileBytes,'canvas_create_file_size');
  check(typeof blob.type==='string'&&blob.type.trim(),'canvas_create_media_type_unknown');
  const inferred=kind??(blob.type.toLowerCase().startsWith('image/')?'image':blob.type.toLowerCase().startsWith('audio/')?'audio':blob.type.toLowerCase().startsWith('video/')?'video':null);
  try{materialMediaType(inferred,blob.type);}catch{throw new CanvasCreateError('canvas_create_media_type_unsupported');}return inferred;
}
function presentation(mask={shape:'rect'},fontSize=16){const value={mask:validateCanvasMask(mask),fontSize};check(Number.isFinite(fontSize)&&fontSize>=8&&fontSize<=128);return value;}
function noteInput({title='',body,mask,fontSize}={}){
  check(typeof title==='string'&&title.length<=160&&!title.includes('\0')&&typeof body==='string'&&body.length<=100000&&!body.includes('\0'),'canvas_create_note_invalid');
  const source={noteId:uid(),mutationId:uid(),expectedRevision:0,title,body,items:[],color:'plain',pinned:false,state:'active'};
  check(new TextEncoder().encode(JSON.stringify(source)).length<=256*1024,'canvas_create_note_too_large');return{kind:'note',sourceIntent:source,presentation:presentation(mask,fontSize)};
}
function widgetInput({appId,entry,mask}={}){
  check(typeof appId==='string'&&/^app-[a-f0-9]{32}$/u.test(appId),'canvas_create_app_invalid');
  if(entry)check(id(entry.domainId)&&typeof entry.path==='string'&&entry.path.length<=8192&&entry.path.startsWith('/')&&!entry.path.startsWith('//')&&!/[\\\u0000-\u0020\u007f#]/u.test(entry.path),'canvas_create_entry_invalid');
  return{kind:'widget',sourceIntent:{requestId:uid(),appId,...(entry?{entry:{domainId:entry.domainId,path:entry.path}}:{})},presentation:presentation(mask)};
}

export function createCanvasCreateFlow({api,accountId,projectId,isCurrent,place,onObjectReady,onProgress=()=>{},store:providedStore}={}){
  check(api&&typeof api.request==='function'&&id(accountId)&&id(projectId)&&typeof isCurrent==='function'&&typeof place==='function');
  const store=providedStore??createCanvasCreateStore(accountId,projectId);
  let disposed=false;const flights=new Map(),volatile=new Map();
  const unsubscribeRetired=store.subscribeSourceRetire?.(source=>{if(disposed)return;for(const[operationId,value]of volatile){const bound=nativeCreationSource(value);if(bound?.providerId===source.providerId&&bound.ref===source.ref)volatile.delete(operationId);}});
  const current=()=>check(!disposed&&isCurrent(),'canvas_create_account_changed');
  const emit=(record,phase,extra={})=>{if(disposed)return;try{onProgress({operationId:record?.operationId,kind:record?.kind,phase,localDurable:record?record.version>0:false,...extra});}catch{/* A UI observer cannot change durable effects. */}};
  const request=async(op,args)=>{current();const value=await api.request(op,{...args,expectedAccountId:accountId});current();return value;};
  async function assertSourceOpen(record){
    const source=nativeCreationSource(record);if(!source||!store.nativeGate)return;
    const gate=await store.nativeGate(source);current();check(!gate,'canvas_create_native_recovery_pending');
  }
  async function load(operationId){
    current();let saved;try{saved=await store.get(operationId);}catch(error){if(!volatile.has(operationId))throw storageError(error);}
    current();const memory=volatile.get(operationId);
    const value=saved&&(!memory||saved.version>=memory.version)?saved:memory;
    check(value,'canvas_create_journal_missing');const record=validateRecord(value,accountId,projectId);
    if(memory&&(!saved||memory.version>saved.version))return persist(record,record.version-1);
    return record;
  }
  async function persist(record,expectedVersion){
    current();volatile.set(record.operationId,clone(record));let wrote;
    try{wrote=await store.put(record,expectedVersion);}catch(error){emit(record,'local-error',{localDurable:false});throw storageError(error,record);}
    current();
    if(!wrote){const latest=await store.get(record.operationId);check(latest,'canvas_create_journal_removed');const value=validateRecord(latest,accountId,projectId);
      check(value.intentHash===record.intentHash&&value.version>expectedVersion,'canvas_create_journal_conflict');volatile.set(value.operationId,value);return value;}
    volatile.set(record.operationId,clone(record));return record;
  }
  const advance=(record,patch)=>persist({...record,...patch,version:record.version+1},record.version);
  const adapters={
    note:{providerId:'soty.notes',async create(record){const args=record.sourceIntent;check(typeof args.body==='string'&&typeof args.title==='string'&&id(args.noteId)&&id(args.mutationId),'canvas_create_journal_invalid');
      const ack=await request('notes.put',args);check(ack.noteId===args.noteId&&Number.isSafeInteger(ack.revision)&&ack.revision>0,'canvas_create_source_receipt_invalid');return ack.noteId;}},
    media:{providerId:'soty.materials',async create(record){const intent=record.sourceIntent;check(intent.blob instanceof Blob,'canvas_create_file_missing');
      const material=await uploadMaterial({request:async(op,args)=>{await assertSourceOpen(record);return request(op,args);}},{accountId,isCurrent:()=>!disposed&&isCurrent(),onProgress:progress=>emit(record,'source',progress)},intent);
      check(material.materialId===intent.materialId&&material.kind===record.kind,'canvas_create_source_receipt_invalid');return material.materialId;}},
    widget:{providerId:'soty.apps',async create(record){const args=record.sourceIntent,result=await request('canvas.widgets.create',args),widget=result.current;
      check(widget&&id(widget.widgetId)&&widget.ownerId===accountId&&widget.appId===args.appId&&widget.state==='active','canvas_create_source_receipt_invalid');return widget.widgetId;}},
  };
  const adapter=record=>adapters[record.kind==='note'?'note':record.kind==='widget'?'widget':'media'];
  function result(record){return{operationId:record.operationId,objectId:record.objectId,shortcutId:record.placement.shortcutId,status:record.placement.status,localDurable:record.placement.localDurable};}
  function resume(operationId){
    if(flights.has(operationId))return flights.get(operationId);
    const flight=(async()=>{
      let record;
      try {
        record=await load(operationId);
        while(!['settling','complete'].includes(record.stage)){
          current();emit(record,record.stage);
          await assertSourceOpen(record);
          if(record.stage==='connect')throw new CanvasCreateError('canvas_create_connection_pending',{operationId,localDurable:true});
          if(record.stage==='source'){
            const sourceRef=await adapter(record).create(record);
            // Durable source acknowledgement precedes releasing note body/File from the local journal.
            record=await advance(record,{stage:'object',sourceRef,sourceIntent:null});
          }else if(record.stage==='object'){
            const output=await request('canvas.objects.create',{requestId:record.objectRequestId,binding:{providerId:adapter(record).providerId,ref:record.sourceRef},presentation:record.presentation,
              visibility:{audience:'private',accountIds:[],communityIds:[]},hidden:{owner:false,others:false},protection:{kind:'none'}});
            const object=output.current;check(object&&id(object.objectId)&&object.ownerId===accountId&&object.state==='active'
              &&object.binding?.providerId===adapter(record).providerId&&object.binding?.ref===record.sourceRef,'canvas_create_object_receipt_invalid');
            record=await advance(record,{stage:'placement',objectId:object.objectId});
          }else {
            if(onObjectReady){await onObjectReady(record.objectId);current();}
            const ack=await place(record.objectId,{width:record.width,height:record.height});current();
            check(ack&&id(ack.shortcutId)&&['saved','pending'].includes(ack.status)&&ack.localDurable===true,'canvas_create_placement_not_durable');
            record=await advance(record,{stage:record.schema===CONNECTION_SCHEMA?'settling':'complete',placement:{shortcutId:ack.shortcutId,status:ack.status,localDurable:true}});
          }
        }
        emit(record,'complete');return result(record);
      }catch(error){
        if(error instanceof CanvasCreateError){if(!error.operationId)error.operationId=operationId;if(error.localDurable===undefined)error.localDurable=Boolean(record);throw error;}
        throw new CanvasCreateError(errorCode(error),{operationId,localDurable:Boolean(record),cause:error});
      }
    })().finally(()=>flights.delete(operationId));flights.set(operationId,flight);return flight;
  }
  async function draftRecord(input,connection){
    current();const record={schema:connection?CONNECTION_SCHEMA:SCHEMA,operationId:connection?.requestId??uid(),accountId,projectId,kind:input.kind,createdAt:Date.now(),version:1,stage:connection?'connect':'source',objectRequestId:uid(),
      sourceIntent:input.sourceIntent,sourceRef:null,objectId:null,presentation:input.presentation,...dimensions(input.kind),placement:null};
    if(connection)record.connection=connection;
    const hashable={...record,sourceIntent:input.sourceIntent.blob?{...input.sourceIntent,blob:undefined}:input.sourceIntent};
    record.intentHash=await materialSha256(JSON.stringify(hashable));current();return record;
  }
  async function start(input){const record=await draftRecord(input);current();await persist(record,0);return resume(record.operationId);}
  return{
    async prepareConnectedWidget(requestId,{mask}={}){
      current();check(id(requestId),'canvas_create_connection_invalid');let existing;try{existing=await store.get(requestId);}catch(error){throw storageError(error);}current();
      if(existing){const record=validateRecord(existing,accountId,projectId);check(record.schema===CONNECTION_SCHEMA&&record.connection.requestId===requestId,'canvas_create_connection_invalid');return {operationId:requestId};}
      const record=await draftRecord({kind:'widget',sourceIntent:{requestId:uid()},presentation:presentation(mask)},{requestId,appId:null,settled:false});await persist(record,0);return {operationId:requestId};
    },
    async completeConnectedWidget({requestId,appId}){
      current();check(id(requestId)&&typeof appId==='string'&&/^app-[a-f0-9]{32}$/u.test(appId),'canvas_create_connection_invalid');let record=await load(requestId);check(record.schema===CONNECTION_SCHEMA&&record.connection.requestId===requestId,'canvas_create_connection_invalid');
      check(record.connection.appId===null||record.connection.appId===appId,'canvas_create_connection_changed');
      if(record.stage==='connect'){
        const response=await request('apps.directory.resolve',{appIds:[appId]}),value=response.items?.find(item=>item.ref?.kind==='app'&&item.ref.id===appId),entry=value?.app?.entry;
        check(value?.available&&value.app?.id===appId&&entry?.appId===appId,'canvas_create_app_unavailable');widgetInput({appId,entry:{domainId:entry.domainId,path:entry.path}});
        record=await advance(record,{stage:'source',connection:{...record.connection,appId},sourceIntent:{...record.sourceIntent,appId,entry:{domainId:entry.domainId,path:entry.path}}});
      }
      return resume(record.operationId);
    },
    async settleConnectedWidget(requestId){
      current();check(id(requestId),'canvas_create_connection_invalid');let value;try{value=await store.get(requestId);}catch(error){throw storageError(error);}current();if(!value)return;
      const record=validateRecord(value,accountId,projectId);check(record.schema===CONNECTION_SCHEMA&&record.connection.requestId===requestId&&['settling','complete'].includes(record.stage),'canvas_create_connection_pending');
      if(record.stage==='settling')await advance(record,{stage:'complete',connection:{...record.connection,settled:true}});
    },
    async startNote(input){return start(noteInput(input));},
    async startFile(blob,{kind,mask}={}){
      current();canvasCreateFileKind(blob,kind);const normalized=await normalizeCanvasWebmFile(blob,kind);current();blob=normalized.blob;const inferred=canvasCreateFileKind(blob,normalized.kind);
      emit(null,'preparing');const intent=await prepareMaterialUpload(blob,{kind:inferred});current();return start({kind:inferred,sourceIntent:intent,presentation:presentation(mask)});
    },
    async startWidget(input){return start(widgetInput(input));},resume,
    async listPending(){current();let values;try{values=await store.list();}catch(error){throw storageError(error);}current();
      const records=new Map(values.map(value=>[value.operationId,validateRecord(value,accountId,projectId)]));
      for(const value of volatile.values())if(!records.has(value.operationId))records.set(value.operationId,clone(value));
      return Promise.all([...records.values()].filter(value=>value.stage!=='complete').sort((a,b)=>b.createdAt-a.createdAt).map(async value=>{
        const source=nativeCreationSource(value);let quarantined=false;
        if(source&&store.nativeGate)try{quarantined=Boolean(await store.nativeGate(source));}catch{quarantined=true;}current();
        return{operationId:value.operationId,kind:value.kind,stage:value.stage,createdAt:value.createdAt,
          label:quarantined?'Нужен перенос на этом устройстве':value.kind==='note'?(value.sourceIntent?.title||value.sourceIntent?.body?.slice(0,60)||'Записка'):value.kind==='widget'?(value.stage==='connect'?'Подключение приложения':'Окно приложения'):value.sourceIntent?.name||'Материал',objectId:value.objectId,localDurable:values.some(saved=>saved.operationId===value.operationId),...(value.schema===CONNECTION_SCHEMA?{connectionRequestId:value.connection.requestId}:{})};
      }));
    },
    async removeLocal(operationId){current();check(!flights.has(operationId),'canvas_create_in_progress');const value=await store.get(operationId);current();if(value){const record=validateRecord(value,accountId,projectId);check(record.schema!==CONNECTION_SCHEMA||record.stage==='connect'||record.connection.settled,'canvas_create_connection_owned');}await store.remove(operationId);current();volatile.delete(operationId);},
    dispose(){disposed=true;unsubscribeRetired?.();if(!providedStore)store.close?.();},
  };
}
import {normalizeCanvasWebmFile} from './canvas-webm-kind.mjs';
