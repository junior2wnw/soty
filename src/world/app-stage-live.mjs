const denied=new Set(['apps_access_denied','app_access_revoked','app_unavailable','app_policy_changed','app_source_changed',
  'scoped_embed_authority_changed','scoped_embed_source_changed','authentication_required','DEVICE_REVOKED','device_revoked']);
const sameEntry=(a,b)=>a===null?b===null:!!b&&['appId','domainId','origin','path'].every(key=>a[key]===b[key]);

/** Refresh display metadata separately from the exact already-open entry. Neither read can launch or rebind it. */
export async function refreshAppStageLive({api,accountId,appId,entry,isCurrent,readEntry,onMetadata,onDenied}){
  const current=()=>isCurrent()&&sameEntry(entry,readEntry());
  if(!current())return;
  const metadata=api.request('apps.directory.resolve',{expectedAccountId:accountId,appIds:[appId]});
  const authority=entry?api.request('apps.entry.get',{expectedAccountId:accountId,appId:entry.appId,domainId:entry.domainId,path:entry.path}):Promise.resolve(null);
  const [display,access]=await Promise.allSettled([metadata,authority]);
  if(!current())return;
  if(access.status==='rejected'){
    if(denied.has(access.reason?.code)){await onDenied(access.reason);return;}
    throw access.reason;
  }
  if(entry&&!sameEntry(entry,access.value?.entry))throw Object.assign(new Error('invalid_live_app_entry'),{code:'invalid_live_app_entry'});
  if(display.status==='rejected')throw display.reason;
  if(display.value?.schema!=='soty.app-directory.v1'||!Array.isArray(display.value.items))throw new Error('invalid_live_app_metadata');
  const item=display.value.items.find(value=>value.ref?.kind==='app'&&value.ref.id===appId);
  // A valid unlisted link can remain usable even when its app is not in discovery.
  if(item?.available===true){
    const app=item.app;
    if(app?.id!==appId||typeof app.name!=='string'||!app.name||typeof app.state!=='string'||typeof app.ownerAccountId!=='string')throw new Error('invalid_live_app_metadata');
    onMetadata({name:app.name,status:app.state,ownerAccountId:app.ownerAccountId});
  }
}
