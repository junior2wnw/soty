import {vaultCheck,vaultId,vaultBase64} from './canvas-vault-local.mjs';
const encoder=new TextEncoder();
export function canvasVaultScope(value){
  vaultCheck(value&&Object.keys(value).length===3&&value.schema==='soty.vault.local-scope.v1'&&['source','placement'].includes(value.kind)&&vaultId(value.id),'vault_local_scope_unavailable');
  return Object.freeze({schema:value.schema,kind:value.kind,id:value.id});
}
export const sameCanvasVaultScope=(a,b)=>a?.schema==='soty.vault.local-scope.v1'&&a.schema===b?.schema&&a.kind===b.kind&&a.id===b.id;
export async function canvasVaultScopeSlot(scope,projectId,crypto=globalThis.crypto){
  const clean=canvasVaultScope(scope);vaultCheck(vaultId(projectId)&&crypto?.subtle,'vault_secure_context_required');
  if(clean.kind==='placement')return clean.id;
  return 'vs_'+vaultBase64(new Uint8Array(await crypto.subtle.digest('SHA-256',encoder.encode(JSON.stringify(['soty.vault.crypto-slot.v1',projectId,clean.id])))));
}
// Internal SDK-to-helper adapter. A JSON cryptoSlot is not a grant. The SDK
// supplies the exact Object session/epoch closure; copied tokens fail closed.
const recoveries=new WeakMap();
export function createCanvasVaultRecoveryScope({objectId,source,profile,isCurrent}){
  canvasVaultScope(source);vaultCheck(vaultId(objectId)&&vaultId(profile?.slot)&&vaultId(profile.profileId)&&typeof isCurrent==='function','vault_local_scope_changed');
  const token=Object.freeze({});recoveries.set(token,{objectId,slot:profile.slot,profileId:profile.profileId,salt:profile.salt,isCurrent});return token;
}
export function canvasVaultRecoveryScope(token,objectId,profile){
  if(token===undefined)return{slot:objectId,active(){}};
  const value=recoveries.get(token);vaultCheck(value&&value.objectId===objectId&&value.slot===profile.slot&&value.profileId===profile.profileId&&value.salt===profile.salt,'vault_local_scope_changed');
  const active=()=>vaultCheck(value.isCurrent()===true,'canvas_object_locked');active();return{slot:value.slot,active};
}
