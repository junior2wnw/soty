import { fields,check,syncResult } from './wire.mjs';

const proofs=new WeakMap();
/** Private host capability created after actual Root/RP/Native checks. No JSON
 * or persisted locator can recreate it; Native owns the final SQL checkpoint. */
export function createFeedbackJobAuthorityProof(options){
  const value=fields(options,['sessionHash','bindingDigest','expiresAt','assertCurrent','withCurrent']);
  check(/^[a-f0-9]{64}$/u.test(value.sessionHash)&&/^[a-f0-9]{64}$/u.test(value.bindingDigest)
    &&Number.isSafeInteger(value.expiresAt)&&value.expiresAt>0&&typeof value.assertCurrent==='function'
    &&typeof value.withCurrent==='function','source_feedback_authority_invalid',503);
  const proof=Object.freeze(Object.create(null));let fencing=false;
  const guarded=Object.freeze({...value,withCurrent(action){
    check(!fencing&&typeof action==='function','source_feedback_authority_invalid',503);
    let open=true,entered=false,poisoned=false,result;fencing=true;
    const final=()=>{
      if(!open||entered){poisoned=true;throw new Error('source_feedback_authority_invalid');}
      entered=true;result=syncResult(action());return result;
    };
    try{const returned=syncResult(value.withCurrent(final));check(entered&&!poisoned&&returned===result,'source_feedback_authority_invalid',503);return result;}
    finally{open=false;fencing=false;}
  }});
  proofs.set(proof,guarded);return proof;
}
export function feedbackJobAuthority(proof){const value=proofs.get(proof);check(value,'source_feedback_authority_invalid',503);return value;}
