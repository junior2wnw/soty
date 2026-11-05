import {DatabaseSync} from 'node:sqlite';
import {createHash,randomUUID} from 'node:crypto';
import {mkdirSync} from 'node:fs';
import {dirname} from 'node:path';
import {appId,textId} from '../apps/server/protocol.mjs';
import {createLaunchPath} from '../apps/server/launch-path.mjs';
import {check,exact,identity,widgetIdentity,sourcePin,samePin,WIDGET_SCHEMA,WIDGET_LIMITS,CanvasWidgetError} from './contract.mjs';
import {initializeWidgetStorage} from './storage.mjs';

export const canvasWidgetOperations=new Set(['canvas.widgets.create','canvas.widgets.get','canvas.widgets.list','canvas.widgets.resolve','canvas.widgets.refresh','canvas.widgets.remove','canvas.widgets.restore']);
const canonical=value=>value&&typeof value==='object'?Array.isArray(value)?value.map(canonical):Object.fromEntries(Object.keys(value).sort().map(key=>[key,canonical(value[key])])):value;
const hash=value=>createHash('sha256').update(JSON.stringify(canonical(value))).digest('hex');
const sync=value=>{if(value&&typeof value.then==='function'){Promise.resolve(value).catch(()=>{});check(false,'canvas_widget_async_authority',500);}return value;};
const entry=value=>{exact(value,['domainId','path']);textId(value.domainId);return {domainId:value.domainId,path:createLaunchPath(value.path).entryPath};};

/** Instances pin the existing Apps registry; they never store a second app directory or runtime grant. */
export function createCanvasWidgetsService({databasePath,projectId,actorActive,withActorAuthorityFence,withAppSourceAuthority,onChange=()=>{},clock=Date.now}={}){
 check(typeof databasePath==='string'&&typeof actorActive==='function'&&typeof withActorAuthorityFence==='function'&&typeof withAppSourceAuthority==='function'&&typeof onChange==='function'&&typeof clock==='function'&&withActorAuthorityFence.constructor?.name!=='AsyncFunction'&&withAppSourceAuthority.constructor?.name!=='AsyncFunction','canvas_widget_configuration_invalid',500);identity(projectId);
 mkdirSync(dirname(databasePath),{recursive:true});const db=new DatabaseSync(databasePath);let closed=false;try{initializeWidgetStorage(db,projectId);}catch(error){db.close();throw error;}
 const get=(sql,...args)=>db.prepare(sql).get(...args),run=(sql,...args)=>db.prepare(sql).run(...args),all=(sql,...args)=>db.prepare(sql).all(...args);
 function active(actor){check(!closed,'canvas_widget_closed',503);check(actor&&actorActive(actor)===true,'canvas_widget_authentication_required',401);identity(actor.accountId);identity(actor.deviceId);}
 function tuple(actor){active(actor);return Object.freeze({accountId:actor.accountId,deviceId:actor.deviceId,...(typeof actor.label==='string'?{label:actor.label}:{})});}
 function actorFrame(actor,callback){active(actor);let entered=false,live=true,result;try{sync(withActorAuthorityFence(actor,()=>{check(live&&!entered,'canvas_widget_authority_invalid',500);entered=true;result=sync(callback(tuple(actor)));active(actor);return result;}));check(entered,'canvas_widget_authority_invalid',500);return result;}finally{live=false;}}
 function transaction(callback){check(!db.isTransaction,'canvas_widget_nested',500);db.exec('BEGIN IMMEDIATE');try{const result=sync(callback());db.exec('COMMIT');return result;}catch(error){if(db.isTransaction)db.exec('ROLLBACK');throw error;}}
 function row(id){return get('SELECT * FROM canvas_widgets WHERE widget_id=?',widgetIdentity(id));}
 function project(value){if(!value)return null;widgetIdentity(value.widget_id);identity(value.owner_id);appId(value.app_id);check(Number.isSafeInteger(value.revision)&&value.revision>=1&&['active','removed'].includes(value.state),'canvas_widget_storage_invalid',503);return {schema:WIDGET_SCHEMA,widgetId:value.widget_id,ownerId:value.owner_id,revision:value.revision,appId:value.app_id,entry:entry({domainId:value.domain_id,path:value.entry_path}),source:sourcePin({revision:value.source_revision,digest:value.source_digest,profile:value.source_profile}),contractVersion:1,state:value.state};}
 for(const value of all('SELECT * FROM canvas_widgets'))project(value);
 function own(actor,id){const widget=project(row(id));check(widget?.ownerId===actor.accountId,'canvas_widget_owner_required',403);return widget;}
 function sourceFrame(request,callback){let entered=false,live=true,result;try{sync(withAppSourceAuthority(request,value=>{check(live&&!entered,'canvas_widget_source_authority_invalid',500);entered=true;const target=sourcePin(value.target),selected=entry({domainId:value.entry?.domainId,path:value.entry?.path});check(value.app?.appId===request.appId&&value.entry?.appId===request.appId,'canvas_widget_source_authority_invalid',500);
   check(request.domainId===undefined||selected.domainId===request.domainId&&selected.path===request.path,'canvas_widget_source_authority_invalid',500);
   check(typeof value.app.name==='string'&&value.app.name.length<=128&&typeof value.app.status==='string'&&typeof value.runtimeAllowed==='boolean','canvas_widget_source_authority_invalid',500);identity(value.app.ownerAccountId);
   const origin=new URL(value.entry.origin);check(['http:','https:'].includes(origin.protocol)&&origin.origin===value.entry.origin&&!origin.username&&!origin.password,'canvas_widget_source_authority_invalid',500);
   const currentEntry=Object.freeze({appId:request.appId,domainId:selected.domainId,origin:origin.origin,path:selected.path}),app=Object.freeze({appId:request.appId,name:value.app.name,status:value.app.status,ownerAccountId:value.app.ownerAccountId,entry:currentEntry,...(value.app.audience===undefined?{}:{audience:value.app.audience})});
   result=sync(callback(Object.freeze({app,entry:currentEntry,target:Object.freeze(target),runtimeAllowed:value.runtimeAllowed})));return result;}));check(entered,'canvas_widget_source_authority_invalid',500);return result;}finally{live=false;}}
 function command(actor,op,args,callback){
  const requestId=identity(args.requestId),intent=hash([op,args]);
  const prior=()=>{const saved=get('SELECT * FROM widget_commands WHERE account_id=? AND request_id=?',actor.accountId,requestId);if(!saved)return null;check(saved.intent_hash===intent,'canvas_widget_request_conflict',409);const receipt=JSON.parse(saved.receipt_json);return {replayed:true,receipt,current:own(actor,receipt.widgetId)};};
  const replay=prior();if(replay)return replay;let change;
  const apply=source=>transaction(()=>{const replay=prior();if(replay)return replay;active(actor);check(get('SELECT count(*) n FROM widget_commands WHERE account_id=?',actor.accountId).n<WIDGET_LIMITS.commands,'canvas_widget_storage_limit',409);const now=clock();check(Number.isSafeInteger(now)&&now>=0);const widgetId=callback(source,now);active(actor);const current=own(actor,widgetId),receipt={widgetId,revision:current.revision,committedAt:now};run('INSERT INTO widget_commands VALUES(?,?,?,?)',actor.accountId,requestId,intent,JSON.stringify(receipt));change={widgetId,ownerId:actor.accountId,appId:current.appId,revision:current.revision};return {replayed:false,receipt,current};});
  let result;
  if(op==='canvas.widgets.create')result=sourceFrame({actor,appId:args.appId,...(args.entry??{})},apply);
  else if(op==='canvas.widgets.refresh'){const widget=own(actor,args.widgetId);result=sourceFrame({actor,appId:widget.appId,...widget.entry},apply);}
  else result=apply();
  if(change)try{onChange(change);}catch{/* The receipt already committed. */}return result;
 }
 function view(widget,source,actor){const matches=samePin(widget.source,source.target);return {schema:'soty.canvas-widget-view.v1',kind:'widget',widgetId:widget.widgetId,instanceId:widget.widgetId,state:matches?source.app.status:'needs-refresh',app:source.app,entry:source.entry,source:source.target,expectedSource:widget.source,runtimeAllowed:Boolean(actor)&&matches&&source.runtimeAllowed===true,canRefresh:actor?.accountId===widget.ownerId};}
 function withSourceAuthority(request,callback){
  check(!closed,'canvas_widget_closed',503);exact(request,['actor','objectId','objectOwnerId','ref','mode','purpose'],['objectOwnerId','ref','mode']);check(request.purpose===undefined||request.purpose==='protection');identity(request.objectOwnerId);if(request.objectId!==undefined)identity(request.objectId);check(['create','read'].includes(request.mode));if(request.actor)active(request.actor);const before=project(row(request.ref));
  if(!before||before.state!=='active'||before.ownerId!==request.objectOwnerId||request.mode==='create'&&request.actor?.accountId!==before.ownerId)return callback({allowed:false,protection:{kind:'none'}});
  let callbackEntered=false;
  try{return sourceFrame({actor:request.actor??null,appId:before.appId,...before.entry},source=>transaction(()=>{const current=project(row(before.widgetId));callbackEntered=true;if(!current||current.state!=='active'||current.ownerId!==request.objectOwnerId)return callback({allowed:false,protection:{kind:'none'}});check(current.revision===before.revision,'canvas_widget_changed',409);const result=sync(callback({allowed:true,protection:{kind:'none'},view:view(current,source,request.actor)}));if(request.actor)active(request.actor);return result;}));}
  catch(error){if(!['apps_access_denied','app_unavailable','app_revoked'].includes(error?.code))throw error;if(callbackEntered)throw new CanvasWidgetError('canvas_provider_access_denied',403);return callback({allowed:false,protection:{kind:'none'}});}
 }
 function execute({actor:transportActor,op,args={}}){return actorFrame(transportActor,actor=>{
  check(canvasWidgetOperations.has(op));check(args.expectedAccountId===actor.accountId,'canvas_widget_account_changed',401);const {expectedAccountId,...input}=args;args=input;
  if(op==='canvas.widgets.list'){exact(args,['offset'],[]);const offset=args.offset??0;check(Number.isSafeInteger(offset)&&offset>=0&&offset<=10000);const rows=all('SELECT * FROM canvas_widgets WHERE owner_id=? ORDER BY created_at,widget_id LIMIT 101 OFFSET ?',actor.accountId,offset);return {widgets:rows.slice(0,100).map(project),nextOffset:rows.length>100?offset+100:null};}
  if(['canvas.widgets.get','canvas.widgets.resolve'].includes(op)){exact(args,['widgetId']);const widget=own(actor,args.widgetId);if(op==='canvas.widgets.get')return {widget};return withSourceAuthority({actor,objectOwnerId:widget.ownerId,ref:widget.widgetId,mode:'read'},scope=>scope.allowed?{available:true,view:scope.view}:{available:false});}
  if(op==='canvas.widgets.create'){
   exact(args,['requestId','appId','entry'],['requestId','appId']);appId(args.appId);args={...args,...(args.entry===undefined?{}:{entry:entry(args.entry)})};
   return command(actor,op,args,(source,now)=>{check(get('SELECT count(*) n FROM canvas_widgets WHERE owner_id=?',actor.accountId).n<WIDGET_LIMITS.instances,'canvas_widget_storage_limit',409);const widgetId='widget-'+randomUUID(),pin=sourcePin(source.target),target=entry({domainId:source.entry.domainId,path:source.entry.path});
    run('INSERT INTO canvas_widgets VALUES(?,?,1,?,?,?,?,?,?,?,?,?)',widgetId,actor.accountId,args.appId,target.domainId,target.path,pin.revision,pin.digest,pin.profile,'active',now,now);return widgetId;});
  }
  exact(args,['widgetId','requestId','expectedRevision']);widgetIdentity(args.widgetId);check(Number.isSafeInteger(args.expectedRevision)&&args.expectedRevision>=1);
  return command(actor,op,args,(source,now)=>{const before=own(actor,args.widgetId);check(before.revision===args.expectedRevision,'canvas_widget_revision_conflict',409);check(before.revision<Number.MAX_SAFE_INTEGER,'canvas_widget_revision_exhausted',409);
   const pin=source?sourcePin(source.target):before.source,state=op==='canvas.widgets.remove'?'removed':op==='canvas.widgets.restore'?'active':before.state;
   run('UPDATE canvas_widgets SET revision=revision+1,source_revision=?,source_digest=?,source_profile=?,state=?,updated_at=? WHERE widget_id=?',pin.revision,pin.digest,pin.profile,state,now,before.widgetId);return before.widgetId;});
 });}
 return Object.freeze({operations:canvasWidgetOperations,execute,withSourceAuthority,
  // Trusted HTTP host routing only. Detached metadata confers no Object/App/Vault right.
  runtimeRouting(widgetId){check(!closed,'canvas_widget_closed',503);const value=project(row(widgetId));return value?.state==='active'?value:null;},
  ownerRepresents(request){check(!closed,'canvas_widget_closed',503);exact(request,['ownerId','objectId','ref']);identity(request.ownerId);identity(request.objectId);const widget=project(row(request.ref));return widget?.ownerId!==request.ownerId?undefined:widget.state==='active'?[{kind:'app',id:widget.appId}]:[];},
  hasInstance(widgetId){check(!closed,'canvas_widget_closed',503);return typeof widgetId==='string'&&/^widget-[a-f0-9-]{36}$/u.test(widgetId)&&Boolean(get('SELECT 1 FROM canvas_widgets WHERE widget_id=?',widgetId));},
  notifyAppChanged(request){check(!closed,'canvas_widget_closed',503);exact(request,['appId']);appId(request.appId);let notified=0;
   for(const value of db.prepare("SELECT widget_id,owner_id,revision FROM canvas_widgets WHERE app_id=? AND state='active' ORDER BY widget_id").iterate(request.appId)){notified++;try{onChange({widgetId:value.widget_id,ownerId:value.owner_id,appId:request.appId,revision:value.revision});}catch{/* Apps already committed; only current read authority can reveal data. */}}
   return {notified};},
  checkLaunch({actor,widgetId,appId:expectedAppId,entry:expectedEntry,target,phase='launch'}){active(actor);check(['launch','renew'].includes(phase));const widget=project(row(widgetId));check(widget?.state==='active'&&widget.appId===expectedAppId&&widget.entry.domainId===expectedEntry.domainId&&(phase==='renew'||widget.entry.path===expectedEntry.path),'canvas_widget_entry_changed',403);check(samePin(widget.source,sourcePin(target)),'canvas_widget_needs_refresh',409);return true;},
  close(){if(!closed){check(!db.isTransaction,'canvas_widget_nested',500);db.close();closed=true;}}
 });
}

/** A single provider serves every registered app through the existing runtime. */
export function createCanvasWidgetProvider({widgets,withObjectAuthority}){
 const frames=new Map();return Object.freeze({providerId:'soty.apps',ownerRepresents:request=>widgets.ownerRepresents(request),withBindingAuthority(request,callback){identity(request.objectId);return widgets.withSourceAuthority(request,scope=>{if(!scope.allowed)return callback({allowed:false,protection:scope.protection});check(!frames.has(request.objectId),'canvas_widget_nested',500);frames.set(request.objectId,scope.view);try{return callback({allowed:true,protection:scope.protection});}finally{frames.delete(request.objectId);}});},
  project(context){return withObjectAuthority(context,()=>{const frame=frames.get(context.objectId);check(frame&&frame.widgetId===context.ref,'canvas_widget_source_authority_required',403);return frame;});}});
}
