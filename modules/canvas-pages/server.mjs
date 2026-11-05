import {DatabaseSync} from 'node:sqlite';
import {createHash,randomUUID} from 'node:crypto';
import {mkdirSync} from 'node:fs';
import {dirname} from 'node:path';
import {check,exact,identity,jsonProjection,boundedContent} from '../canvas-objects/contract.mjs';
import {convertFieldDocumentToV2} from '../field/contract.mjs';
import {initializeCanvasPagesStorage} from './storage.mjs';
import {normalizePageSelection,readPageSelection,pageSelectionDto,PAGE_SELECTION_RECORD_SCHEMA} from './selection.mjs';

export const canvasPageOperations=new Set(['canvas.pages.get','canvas.pages.put']);
export const CANVAS_PAGE_BYTES=512*1024;
const canonical=value=>value&&typeof value==='object'?Array.isArray(value)?value.map(canonical):Object.fromEntries(Object.keys(value).sort().map(key=>[key,canonical(value[key])])):value;
const hash=value=>createHash('sha256').update(JSON.stringify(canonical(value))).digest('hex');
const sync=value=>{if(value&&typeof value.then==='function'){Promise.resolve(value).catch(()=>{});check(false,'canvas_page_async_authority',500);}return value;};
const dto=row=>row?{schema:'soty.canvas-page.v1',pageId:row.page_id,ownerId:row.owner_id,revision:row.revision,enabled:Boolean(row.enabled),audience:row.audience,...(pageSelectionDto(row)?{selection:pageSelectionDto(row)}:{})}:null;
const deniedApps=new Set(['apps_access_denied','app_unavailable','app_revoked','canvas_provider_access_denied']);

/** Publication points at the canonical field. Explicit choices delegate only a
 * Root-approved Native object inside a live Page frame, never a Source login. */
export function createCanvasPagesService({databasePath,projectId,actorActive,withActorAuthorityFence,withCanvasSource,objects,withLegacyAppAuthority,allowFormat2Migration=false,onChange=()=>{},clock=Date.now}={}){
 check(typeof databasePath==='string'&&typeof actorActive==='function'&&typeof withActorAuthorityFence==='function'&&typeof withCanvasSource==='function'&&objects&&['project','withObjectRead','withObjectAuthority'].every(key=>typeof objects[key]==='function')&&(withLegacyAppAuthority===undefined||typeof withLegacyAppAuthority==='function')&&typeof onChange==='function'&&typeof clock==='function','canvas_page_configuration_invalid',500);identity(projectId);
 mkdirSync(dirname(databasePath),{recursive:true});const db=new DatabaseSync(databasePath),contexts=new WeakMap(),selectionContexts=new WeakMap();let closed=false,format;
 try{format=initializeCanvasPagesStorage(db,projectId,{allowFormat2Migration});}catch(error){db.close();throw error;}
 const get=(sql,...args)=>db.prepare(sql).get(...args),run=(sql,...args)=>db.prepare(sql).run(...args);
 const page=id=>get('SELECT * FROM canvas_pages WHERE page_id=?',identity(id)),own=actor=>get('SELECT * FROM canvas_pages WHERE owner_id=?',actor.accountId);
 function active(actor){check(!closed,'canvas_page_closed',503);check(actor&&actorActive(actor)===true,'canvas_page_authentication_required',401);return Object.freeze({accountId:identity(actor.accountId),deviceId:identity(actor.deviceId)});}
 function authority(original,callback){active(original);let entered=false,live=true,result;try{sync(withActorAuthorityFence(original,()=>{check(live&&!entered,'canvas_page_actor_authority_invalid',500);entered=true;result=sync(callback(active(original)));active(original);return result;}));check(entered,'canvas_page_actor_authority_invalid',500);return result;}finally{live=false;}}
 function transaction(callback){check(!db.isTransaction,'canvas_page_nested_transaction',500);db.exec('BEGIN IMMEDIATE');try{const result=sync(callback());db.exec('COMMIT');return result;}catch(error){if(db.isTransaction)db.exec('ROLLBACK');throw error;}}
 function source(ownerId,viewer){let entered=false,live=true,result;try{sync(withCanvasSource({ownerId,viewer},snapshot=>{check(live&&!entered&&snapshot?.ownerId===ownerId,'canvas_page_source_invalid',500);entered=true;check(snapshot.field&&Number.isSafeInteger(snapshot.field.revision)&&typeof snapshot.field.contentHash==='string','canvas_page_source_invalid',500);const document=convertFieldDocumentToV2(snapshot.field.document);check(['generic','profile'].includes(snapshot.banner?.kind),'canvas_page_source_invalid',500);
   const banner=snapshot.banner.kind==='profile'?{kind:'profile',profile:{profileId:identity(snapshot.banner.profile.profileId),displayName:snapshot.banner.profile.displayName,avatarColor:snapshot.banner.profile.avatarColor,avatarRevision:snapshot.banner.profile.avatarRevision}}:{kind:'generic',title:'Полотно'};
   result={document,revision:snapshot.field.revision,contentHash:snapshot.field.contentHash,banner:jsonProjection(banner)};return result;}));check(entered,'canvas_page_source_invalid',500);return result;}finally{live=false;}}
 function accessible(row,viewer){check(row&&row.enabled===1&&(row.audience!=='private'||row.owner_id===viewer?.accountId),'canvas_page_unavailable',404);return row;}
 function currentPage(pageId,viewer){return accessible(page(pageId),viewer);}
 const sameEntity=(left,right)=>left?.kind===right?.kind&&left?.id===right?.id;
 const selectionAvailable=()=>format===2&&typeof objects.pageSelectionConfigured==='function'&&objects.pageSelectionConfigured();
 function selectedMembership(row,snapshot,entry){return snapshot.document.shortcuts.find(shortcut=>shortcut.shortcutId===entry.shortcutId&&sameEntity(shortcut.entity,entry.entity));}
 function selectionFrame(row,snapshot,entry,original,viewer,callback){
  const context=Object.freeze({}),proof={active:true,pageId:row.page_id,pageRevision:row.revision,pageOwnerId:row.owner_id,fieldRevision:snapshot.revision,entry,original:original??null,viewer};selectionContexts.set(context,proof);
  try{recheckSelection(proof);return sync(callback(context));}finally{proof.active=false;}
 }
 function recheckSelection(proof){
  check(proof?.active&&!closed,'canvas_page_authority_required',403);if(proof.original)active(proof.original);const current=currentPage(proof.pageId,proof.viewer);check(current.revision===proof.pageRevision,'canvas_page_changed',409);
  const selected=readPageSelection(current),entry=selected?.items.find(item=>item.shortcutId===proof.entry.shortcutId),snapshot=source(current.owner_id,proof.viewer);
  check(entry&&hash(entry)===hash(proof.entry)&&selectedMembership(current,snapshot,entry)&&snapshot.revision===proof.fieldRevision,'canvas_page_object_unavailable',404);return current;
 }
 function captureSelection(actor,requested){
  check(selectionAvailable(),'canvas_page_selection_migration_required',503);const snapshot=source(actor.accountId,actor);check(snapshot.revision===requested.fieldRevision,'canvas_page_selection_stale',409);const shortcuts=new Map(snapshot.document.shortcuts.map(item=>[item.shortcutId,item])),items=[];
  for(const selected of requested.items){const shortcut=shortcuts.get(selected.shortcutId);check(shortcut&&['object','app'].includes(shortcut.entity.kind),'canvas_page_selection_invalid');check(sameEntity(shortcut.entity,selected.entity),'canvas_page_selection_stale',409);
   if(shortcut.entity.kind==='object'){check(selected.expectedAccessRevision!==undefined,'canvas_page_selection_invalid');objects.withPageSelectionTarget({actor,objectId:shortcut.entity.id,expectedAccessRevision:selected.expectedAccessRevision},object=>{items.push({shortcutId:shortcut.shortcutId,entity:{...shortcut.entity},accessRevision:object.accessRevision,binding:{...object.binding}});return true;});}
   else {check(selected.expectedAccessRevision===undefined,'canvas_page_selection_invalid');items.push({shortcutId:shortcut.shortcutId,entity:{...shortcut.entity}});}
  }
  check(source(actor.accountId,actor).contentHash===snapshot.contentHash,'canvas_page_selection_stale',409);return {schema:PAGE_SELECTION_RECORD_SCHEMA,fieldRevision:requested.fieldRevision,items};
 }
 function legacy(actor,entity){if(!withLegacyAppAuthority)return null;let entered=false,live=true,result=null;try{sync(withLegacyAppAuthority({actor,appId:entity.id},scope=>{check(live&&!entered,'canvas_page_app_authority_invalid',500);entered=true;if(scope?.allowed===false)return null;
   check(scope?.app?.appId===entity.id&&scope.entry?.appId===entity.id&&typeof scope.app.name==='string'&&scope.app.name.length<=128&&typeof scope.app.status==='string','canvas_page_app_authority_invalid',500);
   const origin=new URL(scope.entry.origin);check(['http:','https:'].includes(origin.protocol)&&origin.origin===scope.entry.origin&&!origin.username&&!origin.password&&typeof scope.entry.path==='string'&&scope.entry.path.startsWith('/')&&!scope.entry.path.startsWith('//'),'canvas_page_app_authority_invalid',500);
   result={presentation:{mask:{shape:'hex'}},protection:{kind:'none'},view:jsonProjection({schema:'soty.canvas-page-app.v1',kind:'app',app:{appId:entity.id,name:scope.app.name,status:scope.app.status},entry:{appId:entity.id,domainId:identity(scope.entry.domainId),origin:origin.origin,path:scope.entry.path},runtimeAllowed:Boolean(actor)&&scope.runtimeAllowed===true})};return result;}));check(entered,'canvas_page_app_authority_invalid',500);return result;}catch(error){if(deniedApps.has(error?.code))return null;throw error;}finally{live=false;}}
 function composition(row,snapshot,original,viewer){
  const contextMap=new Map(snapshot.document.contexts.map(value=>[value.contextId,value])),contexts=[],shortcuts=[],items=[],usedContexts=new Set(),resolved=new Map();
  const selected=readPageSelection(row),byShortcut=selected?new Map(selected.items.map(entry=>[entry.shortcutId,entry])):null;
  for(const shortcut of snapshot.document.shortcuts){let projected;const key=JSON.stringify(shortcut.entity);
   const entry=byShortcut?.get(shortcut.shortcutId);if(byShortcut&&(!entry||!sameEntity(entry.entity,shortcut.entity)))continue;
   if(!byShortcut&&resolved.has(key))projected=resolved.get(key);
   else if(shortcut.entity.kind==='object'){let item;
    if(entry)item=selectionFrame(row,snapshot,entry,original,viewer,context=>sync(objects.projectPageObject({...(original?{actor:original}:{}),objectId:shortcut.entity.id,pageContext:context})));
    else {const result=sync(objects.project({...(original?{actor:original}:{}),objectIds:[shortcut.entity.id]}));check(result?.schema==='soty.canvas-object-projection.v1'&&result.items?.length===1&&result.items[0].objectId===shortcut.entity.id,'canvas_page_object_projection_invalid',502);item=result.items[0];}
    projected=item.available?{presentation:item.presentation,protection:item.protection,view:item.view,...(typeof item.canUnlock==='boolean'?{canUnlock:item.canUnlock}:{})}:null;resolved.set(key,projected);}
   else if(shortcut.entity.kind==='app'){projected=legacy(original??null,shortcut.entity);resolved.set(key,projected);}
   else continue;
   if(!projected)continue;const rawContext=contextMap.get(shortcut.contextId),contextId='page_context_'+hash([row.page_id,shortcut.contextId]),shortcutId='page_shortcut_'+hash([row.page_id,shortcut.shortcutId]);
   if(!usedContexts.has(contextId)){usedContexts.add(contextId);contexts.push({contextId,title:'Полотно',x:rawContext.x,y:rawContext.y});}
   shortcuts.push({shortcutId,entity:shortcut.entity,contextId,placement:shortcut.placement});items.push({shortcutId,entity:shortcut.entity,...projected});
  }
  return {banner:snapshot.banner,field:{schema:'soty.field.v2',contexts,shortcuts},items};
 }
 function cursor(value,viewRevision){if(value===undefined)return null;check(typeof value==='string'&&value.length<=768&&/^[A-Za-z0-9_-]+$/u.test(value),'canvas_page_cursor_invalid');let parsed;try{parsed=JSON.parse(Buffer.from(value,'base64url').toString('utf8'));}catch{check(false,'canvas_page_cursor_invalid');}exact(parsed,['version','viewRevision','after']);check(parsed.version===1&&typeof parsed.after==='string'&&parsed.after.length<=128,'canvas_page_cursor_invalid');check(parsed.viewRevision===viewRevision,'canvas_page_cursor_stale',409);check(Buffer.from(JSON.stringify(parsed)).toString('base64url')===value,'canvas_page_cursor_invalid');return parsed.after;}
 function chunk(row,full,request){const viewRevision=hash(['soty.canvas-page-view.v1',row.page_id,full]),after=cursor(request.cursor,viewRevision),start=after===null?0:full.items.findIndex(value=>value.shortcutId===after)+1;check(after===null||start>0,'canvas_page_cursor_invalid');const limit=request.limit??24;check(Number.isSafeInteger(limit)&&limit>=1&&limit<=32,'canvas_page_limit_invalid');
  const output={schema:'soty.canvas-page-view.v1',pageId:row.page_id,viewRevision,banner:full.banner,field:{schema:'soty.field.v2',contexts:[],shortcuts:[]},items:[],nextCursor:null},used=new Set();let count=0;
  for(let index=start;index<full.items.length&&count<limit;index++){const item=full.items[index],shortcut=full.field.shortcuts[index],context=full.field.contexts.find(value=>value.contextId===shortcut.contextId),newContext=!used.has(context.contextId),cost=Buffer.byteLength(JSON.stringify(item))+Buffer.byteLength(JSON.stringify(shortcut))+(newContext?Buffer.byteLength(JSON.stringify(context)):0);
   if(Buffer.byteLength(JSON.stringify(output))+cost+1024>CANVAS_PAGE_BYTES)break;output.items.push(item);output.field.shortcuts.push(shortcut);if(newContext){used.add(context.contextId);output.field.contexts.push(context);}count++;
  }
  check(count>0||start===full.items.length,'canvas_page_chunk_too_large',502);if(start+count<full.items.length)output.nextCursor=Buffer.from(JSON.stringify({version:1,viewRevision,after:output.items.at(-1).shortcutId})).toString('base64url');check(Buffer.byteLength(JSON.stringify(output))<=CANVAS_PAGE_BYTES,'canvas_page_chunk_too_large',502);return output;
 }
 function read(request){check(!closed,'canvas_page_closed',503);exact(request,['pageId','actor','cursor','limit'],['pageId']);identity(request.pageId);const original=request.actor,viewer=original?authority(original,value=>value):null;
  for(let attempt=0;attempt<2;attempt++){const row=currentPage(request.pageId,viewer),before=source(row.owner_id,viewer),full=composition(row,before,original,viewer),after=source(row.owner_id,viewer),current=currentPage(row.page_id,viewer);if(original)authority(original,()=>true);
   if(current.revision!==row.revision||before.contentHash!==after.contentHash||hash(before.banner)!==hash(after.banner)){if(attempt===0)continue;check(false,'canvas_page_changed',409);}return chunk(row,full,request);
  }
 }
 function execute({actor:original,op,args={}}){return authority(original,actor=>{check(canvasPageOperations.has(op),'canvas_page_operation_unsupported');check(args.expectedAccountId===actor.accountId,'canvas_page_account_changed',401);const {expectedAccountId,...input}=args;args=input;
  if(op==='canvas.pages.get'){exact(args,[]);return {page:dto(own(actor)),selectionAvailable:selectionAvailable()};}
  exact(args,['requestId','expectedRevision','enabled','audience','selection'],['requestId','expectedRevision','enabled','audience']);identity(args.requestId);check(Number.isSafeInteger(args.expectedRevision)&&args.expectedRevision>=0&&args.expectedRevision<Number.MAX_SAFE_INTEGER&&typeof args.enabled==='boolean'&&['public','unlisted','private'].includes(args.audience),'canvas_page_arguments_invalid');if(args.selection!==undefined)args={...args,selection:normalizePageSelection(args.selection)};const intentHash=hash(['canvas.pages.put',args]);let change;
  const result=transaction(()=>{const previous=get('SELECT * FROM canvas_page_commands WHERE account_id=? AND request_id=?',actor.accountId,args.requestId);if(previous){check(previous.intent_hash===intentHash,'canvas_page_request_conflict',409);return {replayed:true,receipt:JSON.parse(previous.receipt_json),current:dto(own(actor))};}
   check(get('SELECT count(*) n FROM canvas_page_commands WHERE account_id=?',actor.accountId).n<50000,'canvas_page_storage_limit',409);const before=own(actor);check((before?.revision??0)===args.expectedRevision,'canvas_page_revision_conflict',409);
   // Owner revocation cannot depend on the continued availability of old
   // Sources or the canonical field. An omitted choice list keeps its bytes.
   if(args.selection!==undefined||args.enabled&&args.audience!=='private')source(actor.accountId,actor);
   const selected=args.selection===undefined?readPageSelection(before??{}):captureSelection(actor,args.selection);const selectionJson=selected?JSON.stringify(selected):null;
   const time=clock();check(Number.isSafeInteger(time)&&time>=0,'canvas_page_clock_invalid',500);const pageId=before?.page_id??'page_'+randomUUID().replaceAll('-','');
   if(before)run(`UPDATE canvas_pages SET revision=revision+1,enabled=?,audience=?,updated_at=?${format===2?',selection_json=?':''} WHERE page_id=?`,Number(args.enabled),args.audience,time,...(format===2?[selectionJson]:[]),pageId);else run(`INSERT INTO canvas_pages VALUES(?,?,1,?,?,?,?${format===2?',?':''})`,pageId,actor.accountId,Number(args.enabled),args.audience,time,time,...(format===2?[selectionJson]:[]));
   active(original);const current=dto(own(actor)),receipt={pageId:current.pageId,revision:current.revision,committedAt:time};run('INSERT INTO canvas_page_commands VALUES(?,?,?,?)',actor.accountId,args.requestId,intentHash,JSON.stringify(receipt));change=current;return {replayed:false,receipt,current};});if(change)try{onChange(change);}catch{}return result;
 });}
 function membership(pageId,objectId,viewer){const row=currentPage(pageId,viewer),snapshot=source(row.owner_id,viewer),selected=readPageSelection(row);check(selected?selected.items.some(entry=>entry.entity.kind==='object'&&entry.entity.id===objectId&&selectedMembership(row,snapshot,entry)):snapshot.document.shortcuts.some(value=>value.entity.kind==='object'&&value.entity.id===objectId),'canvas_page_object_unavailable',404);return row;}
 function recheck(proof){check(proof?.active&&!closed,'canvas_page_authority_required',403);if(proof.original)active(proof.original);const row=membership(proof.pageId,proof.objectId,proof.viewer);check(row.revision===proof.pageRevision,'canvas_page_changed',409);}
 function withPageObjectRead(request,callback){check(!closed,'canvas_page_closed',503);exact(request,['pageId','objectId','actor'],['pageId','objectId']);identity(request.pageId);identity(request.objectId);check(typeof callback==='function');const original=request.actor,viewer=original?authority(original,value=>value):null,row=membership(request.pageId,request.objectId,viewer);
  const consume=objectContext=>{const context=Object.freeze({schema:'soty.canvas-page-authority.v1',pageId:request.pageId,objectId:request.objectId,viewer:Object.freeze(viewer?{kind:'account',...viewer}:{kind:'public'})}),proof={active:true,original,viewer,pageId:row.page_id,pageRevision:row.revision,objectId:request.objectId,objectContext};contexts.set(context,proof);
   try{recheck(proof);const value=boundedContent(sync(callback(context)));recheck(proof);return value;}finally{proof.active=false;}
  };const selected=readPageSelection(row);let result;
  if(selected){const snapshot=source(row.owner_id,viewer),entry=selected.items.find(entry=>entry.entity.kind==='object'&&entry.entity.id===request.objectId&&selectedMembership(row,snapshot,entry));check(entry,'canvas_page_object_unavailable',404);result=selectionFrame(row,snapshot,entry,original,viewer,pageContext=>objects.withPageObjectRead({...(original?{actor:original}:{}),objectId:request.objectId,pageContext},consume));}
  else result=sync(objects.withObjectRead({...(original?{actor:original}:{}),objectId:request.objectId},consume));if(original)authority(original,()=>true);return result;
 }
 return Object.freeze({operations:canvasPageOperations,execute,read,withPageObjectRead,
  withSelectionAuthority(context,callback){const proof=selectionContexts.get(context);recheckSelection(proof);check(typeof callback==='function');const value=sync(callback(Object.freeze({schema:'soty.canvas-page-selection-scope.v1',pageId:proof.pageId,pageRevision:proof.pageRevision,pageOwnerId:proof.pageOwnerId,objectId:proof.entry.entity.id,accessRevision:proof.entry.accessRevision,binding:Object.freeze({...proof.entry.binding}),originalActor:proof.original})));recheckSelection(proof);return value;},
  withPageObjectAuthority(context,callback){const proof=contexts.get(context);recheck(proof);check(typeof callback==='function');const result=sync(objects.withObjectAuthority(proof.objectContext,objectContext=>sync(callback(objectContext))));recheck(proof);return result;},
  close(){if(!closed){check(!db.isTransaction,'canvas_page_nested_transaction',500);closed=true;db.close();}}
 });
}
