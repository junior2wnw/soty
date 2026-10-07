import {capture,closed,need} from './profile.mjs';
/** An installed Source assertion from its fixed backend route, never an iframe ready message. */
export function sourceContinuationAck(input,now=Date.now()) {
  const value=capture(input);need(value.schema==='soty.source-session-continuation.v1','scoped_embed_continue_invalid',502);
  if(value.ready===false){closed(value,['schema','ready','reason']);need(value.reason==='login_required','scoped_embed_continue_invalid',502);
    return Object.freeze({ready:false,reason:value.reason});}
  closed(value,['schema','ready','sessionExpiresAt','accessExpiresAt','renewable','receiptDigest']);
  need(value.ready===true&&typeof value.renewable==='boolean'&&Number.isSafeInteger(value.sessionExpiresAt)
    &&value.sessionExpiresAt>now&&value.sessionExpiresAt<=now+86401000&&Number.isSafeInteger(value.accessExpiresAt)
    &&value.accessExpiresAt>now&&value.accessExpiresAt<=Math.min(value.sessionExpiresAt,now+301000)
    &&typeof value.receiptDigest==='string'&&/^[a-f0-9]{64}$/.test(value.receiptDigest),'scoped_embed_continue_invalid',502);
  return Object.freeze({ready:true,sessionExpiresAt:value.sessionExpiresAt,accessExpiresAt:value.accessExpiresAt,
    renewable:value.renewable,receiptDigest:value.receiptDigest});
}
