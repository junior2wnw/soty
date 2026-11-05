import {assertApps,appId,textId} from './protocol.mjs';
import {createEngagementTransaction,synchronous} from './engagement-transaction.mjs';
import {createLaunchPath} from './launch-path.mjs';
import {readAppAudience} from './audience.mjs';
import {selectedRuntimeProfile} from './schema.mjs';

/** Metadata/source pin only; the existing launch gateway still mints every runtime. */
export function createWidgetSourceAuthority({db,assertActor,withAuthorityFence,resolveEntry,resolveDefaultEntry,publications,requireScopedTarget,inspectSource,publicCanvasDefault=false}){
 assertApps(typeof publicCanvasDefault==='boolean','apps_canvas_configuration_invalid',500);
 const run=createEngagementTransaction({db,assertActor,withAuthorityFence,responseBytes:128*1024,busyCode:'apps_authority_busy',responseCode:'apps_widget_response_too_large'});
 const states={offline:'offline',unknown:'starting',responding:'ready',unreachable:'stopped'};
 function select(request,actor,callback){
  const id=appId(request.appId),app=db.prepare("SELECT * FROM local_apps WHERE id=? AND state='enabled'").get(id);assertApps(app,'app_unavailable',404);
  const {target}=publications.sourceStateInTransaction(id);let entry;
  if(actor){
   const preferred=request.domainId===undefined?synchronous(resolveDefaultEntry({actor,app,target}),'apps_async_authority'):request;
   entry=resolveEntry({actor,appId:id,...(preferred?.domainId===undefined?{}:{domainId:preferred.domainId}),...(preferred?.path===undefined?{}:{path:preferred.path})});assertApps(entry,'apps_access_denied',403);
  }else{
   assertApps(publicCanvasDefault||request.domainId!==undefined&&request.path!==undefined,'apps_access_denied',403);
   const address=request.domainId===undefined?db.prepare(`SELECT d.* FROM app_publication_domains p
    JOIN app_domains d ON d.id=p.domain_id AND d.app_id=p.app_id AND d.owner_account_id=p.owner_account_id
    JOIN app_domain_zones z ON z.id=d.zone_id AND z.kind='named'
    WHERE p.app_id=? AND p.owner_account_id=? AND d.role='alias' AND d.state='bound' ORDER BY d.created_at,d.id LIMIT 1`).get(id,app.owner_account_id)
    :db.prepare(`SELECT d.* FROM app_domains d JOIN app_domain_zones z ON z.id=d.zone_id AND z.kind='named'
    WHERE d.id=? AND d.app_id=? AND d.owner_account_id=? AND d.role='alias' AND d.state='bound'`).get(request.domainId,id,app.owner_account_id);
   assertApps(address,'apps_access_denied',403);const decision=publications.decideAccess({domainId:address.id,origin:address.origin});assertApps(decision.subject==='public'&&decision.accessBasis==='public','apps_access_denied',403);
   const observed=inspectSource({app,target:{connectorKey:target.connector_key,revision:target.revision,digest:target.digest}}),status=states[observed?.state];assertApps(status,'apps_source_observation_invalid',500);
   entry={appId:id,domainId:address.id,origin:address.origin,path:createLaunchPath(request.path??target.entry_path).entryPath,status};
  }
  if(selectedRuntimeProfile(target.profile))synchronous(requireScopedTarget({appId:id,revision:target.revision,ownerAccountId:target.owner_account_id,connectorKey:target.connector_key,port:target.port,entryPath:target.entry_path,profile:target.profile,digest:target.digest}),'apps_async_authority');
  const checkedEntry=Object.freeze({appId:id,domainId:entry.domainId,origin:entry.origin,path:entry.path});
  const audience=readAppAudience(db,id);
  return synchronous(callback(Object.freeze({app:Object.freeze({appId:id,name:app.name,ownerAccountId:app.owner_account_id,status:entry.status,...(audience===null?{}:{audience}),entry:checkedEntry}),
   entry:checkedEntry,target:Object.freeze({revision:target.revision,digest:target.digest,profile:target.profile}),runtimeAllowed:Boolean(actor)})),'apps_async_authority');
 }
 return function withWidgetSourceAuthority(request,callback){
  assertApps(request&&typeof request==='object'&&!Array.isArray(request)&&Object.keys(request).every(key=>['actor','appId','domainId','path'].includes(key))&&typeof callback==='function','invalid_arguments');appId(request.appId);
  assertApps((request.domainId===undefined)===(request.path===undefined),'invalid_arguments');if(request.domainId!==undefined){textId(request.domainId);createLaunchPath(request.path);}
  if(request.actor!==undefined&&request.actor!==null)return run(request.actor,actor=>select(request,actor,callback));
  return withAuthorityFence(()=>{assertApps(!db.isTransaction,'apps_nested_transaction',500);db.exec('BEGIN IMMEDIATE');try{const result=select(request,null,callback);db.exec('COMMIT');return result;}catch(error){if(db.isTransaction)db.exec('ROLLBACK');throw error;}});
 };
}
