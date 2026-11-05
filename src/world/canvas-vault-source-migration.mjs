import {CanvasVaultError,vaultCheck,deriveCanvasVaultKey,sealCanvasVaultRecord,openCanvasVaultRecord} from './canvas-vault-local.mjs';
const unavailable=error=>[403,404].includes(error?.status)||['canvas_owner_required','canvas_object_unavailable'].includes(error?.code);

/** Existing Object profiles become ONE Source profile. Metadata lookup is not
 * content authority: caller supplies the fresh exact Object/epoch fence. */
export async function migrateCanvasVaultLegacy({store,accountId,projectId,objectId,scope,session,priorPassword,request,authorize,isCurrent,crypto=globalThis.crypto}){
  const active=()=>vaultCheck(isCurrent()===true,'vault_account_changed');
  let unresolved=[];
  for(let attempt=0;attempt<4;attempt++){
    active();const snapshot=await store.inspectScopes(),pending=snapshot.inventory?.profiles.filter(value=>value.state==='pending'&&value.slot!==session.profile.slot)??[];
    if(!pending.length)return{knownSource:'ready',unresolvedLegacy:[]};
    const status=await authorize();active();
    // Shared viewers keep per-Object read grants. They cannot impersonate an
    // owner to enumerate/migrate another account's Object source identity.
    if(status.ownerId!==accountId)return{knownSource:'ready',unresolvedLegacy:[]};
    const current=(await request('canvas.objects.get',{objectId}))?.object;active();vaultCheck(current?.ownerId===accountId&&current.state==='active','vault_local_scope_changed');
    const from=[],next=[],objectIds=[];unresolved=[];let blocked=false;
    for(const entry of pending){
      active();const profile=snapshot.profiles.find(value=>value.slot===entry.slot&&value.profileId===entry.profileId);vaultCheck(profile,'vault_local_conflict');
      const records=await store.listExact(entry.slot);active();const operation=records.length>0&&records.every(value=>value.purpose==='create');
      let object=null,belongs=false;
      if(!operation){
        try{object=(await request('canvas.objects.get',{objectId:entry.slot}))?.object;}catch(error){if(!unavailable(error))throw error;}
        active();
        if(!object){unresolved.push({slot:entry.slot,kind:'object',reason:'object-unavailable'});continue;}
        if(scope.kind==='placement')belongs=object.objectId===objectId;
        else if(object.ownerId===accountId){const resolved=await request('canvas.vault.bindingScope',{binding:object.binding});active();belongs=resolved?.state==='available'&&resolved.localKeyScope?.kind==='source'&&resolved.localKeyScope.id===scope.id;}
        if(!belongs)continue;
      }
      let oldKey,bodies;
      try{oldKey=await deriveCanvasVaultKey(profile,priorPassword,{crypto});bodies=await Promise.all(records.map(value=>openCanvasVaultRecord(oldKey,value,{crypto})));}
      catch(error){if(error?.code!=='vault_local_password_or_data_invalid')throw error;unresolved.push({slot:entry.slot,kind:operation?'create':'object',reason:'prior-local-password'});if(!operation)blocked=true;continue;}
      active();
      if(operation){
        let other=false;
        for(const body of bodies){vaultCheck(body?.schema==='soty.vault.create.v1'&&body.operationId===entry.slot,'vault_create_journal_invalid');
          if(body.sourceRef){if(scope.kind!=='source'||body.sourceRef!==scope.id)other=true;continue;}
          const receipt=await request('canvas.vault.createReceipt',{sourceRequestId:body.sourceRequestId});active();
          if(receipt?.state==='missing'){other=true;continue;}
          vaultCheck(receipt?.schema==='soty.vault-create-receipt.v1'&&receipt.state==='committed'&&receipt.sourceRequestId===body.sourceRequestId&&receipt.binding?.providerId==='soty.vault'&&receipt.binding.ref===receipt.localKeyScope?.id,'vault_create_source_receipt_invalid');
          if(receipt.localKeyScope.kind!=='source'||scope.kind!=='source'||receipt.localKeyScope.id!==scope.id){other=true;continue;}
          body.stage='object';body.sourceRef=receipt.binding.ref;body.content=null;
        }
        if(other)continue;belongs=true;
      }
      if(!belongs)continue;
      from.push({slot:entry.slot,profileId:entry.profileId,expected:records});if(object)objectIds.push(object.objectId);
      for(let index=0;index<records.length;index++){
        const old=records[index],body=bodies[index],{cipher,...header}=old;
        if(body?.schema==='soty.native-note-recovery.v1'&&body.originObjectId===undefined)body.originObjectId=entry.slot;
        if(body?.schema==='soty.vault.rotate.v1'){vaultCheck(body.objectId===entry.slot&&body.newProfile?.slot===entry.slot,'vault_local_invalid');body.newProfile={...body.newProfile,slot:session.profile.slot};}
        next.push(await sealCanvasVaultRecord(session,{...header,slot:session.profile.slot,version:old.version+1},body,{crypto}));
      }
    }
    active();if(blocked)throw new CanvasVaultError('vault_local_recovery_required',{localDurable:true,localRecovery:{knownSource:'pending',unresolvedLegacy:unresolved}});
    if(!from.length)return{knownSource:'ready',unresolvedLegacy:unresolved};
    const destination=await store.list(session.profile.slot);await authorize();active();
    if(await store.migrateScopes(session.profile.slot,from,{profileId:session.profile.profileId,expected:destination},next,{inventoryGeneration:snapshot.inventory.generation,objectIds})){await authorize();active();return{knownSource:'ready',unresolvedLegacy:unresolved};}
  }
  throw new CanvasVaultError('vault_local_conflict',{localDurable:true});
}
