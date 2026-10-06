import { createCatalog } from '../modules/capabilities/server/catalog.mjs';
import { captureExternalAdapters } from '../modules/capabilities/server/external-adapters.mjs';
import { captureExternalGuidance } from '../modules/capabilities/server/external-guidance.mjs';
import {READONLY_QUERY_PROFILE,captureReadonlyQueryAdapters,createTrustedReadonlyQueryAdapter} from '../modules/capabilities/server/readonly-queries.mjs';
import { canonicalHash, canonicalJson, freezeDeep, AccessError } from '../modules/capabilities/server/validation.mjs';
import { snapshot } from '../modules/app-contract/json.mjs';

const check = value => { if(!value)throw new AccessError('external_application_configuration_invalid'); };
function fields(value,names,optional=[]) {
  check(value&&typeof value==='object'&&!Array.isArray(value)&&[Object.prototype,null].includes(Object.getPrototypeOf(value)));
  const entries=Object.getOwnPropertyDescriptors(value);
  check(Object.getOwnPropertySymbols(value).length===0&&Object.keys(entries).every(name=>names.includes(name)||optional.includes(name))&&names.every(name=>Object.hasOwn(entries,name))
    &&Object.values(entries).every(field=>field.enumerable&&Object.hasOwn(field,'value')));
  return Object.fromEntries(Object.entries(entries).map(([key,value])=>[key,value.value]));
}
/** Private operator-owned composition, not an author manifest or RPC payload.
 * The original Source pin is embedded in a combined immutable Root/Source
 * binding. Changing an app target therefore cannot reroute an old invocation. */
export function captureExternalApplications(input=[]) {
  check(Array.isArray(input)&&input.length<=64);
  const seen=new Set();
  return Object.freeze(input.map(raw=>{
    const value=fields(raw,['appId','target','catalog','adapter'],['guidance']), target=snapshot(value.target), original=snapshot(value.catalog);
    fields(target,['revision','digest']);check(Number.isSafeInteger(target.revision)&&target.revision>=1&&/^[a-f0-9]{64}$/u.test(target.digest));
    check(original.appId===value.appId&&original.executionBinding.kind==='registered'&&!seen.has(original.capabilityId+'@'+original.version));
    seen.add(original.capabilityId+'@'+original.version);
    const binding=original.executionBinding.binding;
    const combined=freezeDeep({...binding,digest:canonicalHash({profile:'soty.registered-app-source.v1',appId:value.appId,target,sourceBinding:binding})});
    const catalog=freezeDeep({...original,executionBinding:{...original.executionBinding,binding:combined}});
    const entry=createCatalog([catalog]).get(catalog.capabilityId,catalog.version);
    const contract=Object.freeze({capabilityId:entry.capabilityId,version:entry.version,digest:entry.digest});
    const profile=Object.getOwnPropertyDescriptor(value.adapter,'profile');check(profile&&'value' in profile);
    const readonly=profile.value===READONLY_QUERY_PROFILE;
    const captured=(readonly?captureReadonlyQueryAdapters:captureExternalAdapters)([{contract,adapter:value.adapter}])[0];
    const guidance = snapshot(value.guidance ?? []); check(Array.isArray(guidance));
    const capturedGuidance = captureExternalGuidance(guidance.map(item=>{
      fields(item,['kind','language','title','summary','content']); return {contract,...item};
    }));
    // The service captures these again before opening storage; do not pass the
    // generated reference as author input or treat it as a capability grant.
    const guideInputs = capturedGuidance.map(({kind,language,title,summary,content})=>({contract,kind,language,title,summary,content}));
    return Object.freeze({appId:value.appId,target,catalog,contract,adapter:captured.adapter,readonly,guidance:freezeDeep(guideInputs)});
  }));
}

export function composeExternalApplications(entries,{withConnectFence,capabilities,apps}) {
  return Object.freeze(entries.map(value=>{
    const withAuthority=(request,callback)=>{
      return withConnectFence(()=>{
        const caps=capabilities();check(caps);
        const ownerFence=request.actor
          ? action=>caps.withOwnerAuthority({actor:request.actor},action)
          : action=>caps.withSnapshotOwnerAuthority({authorization:request.authorization},action);
        return ownerFence(owner=>apps().withAppAuthority({actor:owner,appId:value.appId,mode:'participant'},source=>{
          if(source.target.revision!==value.target.revision||source.target.digest!==value.target.digest)
            throw new AccessError('external_resource_denied');
          // Capture the exact returned callback result. Both the coordinator
          // and Apps guard duplicate/late/async callbacks; source code cannot
          // manufacture a JSON authorization by returning a different value.
          return value.adapter.withAuthority(request,callback);
        }));
      });
    };
    const adapter=value.readonly?createTrustedReadonlyQueryAdapter({withAuthority,query:(request,signal,current)=>value.adapter.query(request,signal,current)}):{
      profile:value.adapter.profile,execute:value.adapter.execute,readProof:value.adapter.readProof,withAuthority};
    return {contract:value.contract,adapter};
  }));
}
