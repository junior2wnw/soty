import { randomBytes } from 'node:crypto';
import { createScopedEmbedAuthority } from './authority.mjs';
import { capture, closed, hash, need, SCOPED_EMBED_LIMITS } from './profile.mjs';
import {sourceContinuationAck} from './source-continuation.mjs';

const nonce = () => randomBytes(32).toString('base64url');
const opaque = value => typeof value === 'string' && /^[A-Za-z0-9_-]{43}$/.test(value);
const digest = value => typeof value === 'string' && /^[a-f0-9]{64}$/.test(value);

export function createScopedGateway({ admissions, withAppAuthority, withHumanSubjectAuthority, clock = Date.now, onClose = () => {} }) {
  const authority = createScopedEmbedAuthority({ profiles: admissions.profiles(), withAppAuthority, withHumanSubjectAuthority, clock });
  const records = new Map(), handles = new Map(), closedHandles = new Map(), authStates = new Map(), completions = new Map(), attempts=new Map();
  function sweep() {
    for (const record of [...records.values()]) if (record.context.expiresAt <= clock()) stop(record,true);
    for (const [key,item] of closedHandles) if(item.expiresAt<=clock())closedHandles.delete(key);
    for(const[key,item]of attempts)if(item.expiresAt<=clock())attempts.delete(key);
  }
  function current(record, connector) {
    need(record && records.get(record.context.reference.id) === record, 'app_scoped_context_closed', 403);
    let value;try{value=authority.read({ reference: record.context.reference, connector: connector ?? record.profile.connector });}catch(error){stop(record,record.context.expiresAt<=clock());throw error;}
    need(value.target.digest === record.context.target.digest, 'app_scoped_context_changed', 403);
    return value;
  }
  function stop(record,expired=false) {
    if(!expired)attempts.delete(record.handleHash);
    if (records.get(record.context.reference.id) !== record) return;
    records.delete(record.context.reference.id); handles.delete(record.handleHash);
    if(closedHandles.size>=SCOPED_EMBED_LIMITS.continuations)closedHandles.delete(closedHandles.keys().next().value);
    closedHandles.set(record.handleHash, { appId: record.context.appId, accountId:record.context.rootPrincipal.accountId,
      deviceId:record.context.rootPrincipal.deviceId, expiresAt: record.context.expiresAt });
    for(const map of [authStates, completions])for(const [key,item] of map)if(item.record===record)map.delete(key);
    try { authority.invalidate({reference:record.context.reference,connector:record.profile.connector}); } catch {}
    onClose(record);
  }
  function mapping(map, key, record) {
    need(digest(key), 'app_scoped_auth_binding_invalid', 403); sweep();
    need(map.size < SCOPED_EMBED_LIMITS.continuations || map.has(key), 'app_scoped_auth_busy', 429);
    const prior=map.get(key); need(!prior || prior.record===record, 'app_scoped_auth_binding_conflict', 403);
    map.set(key, {record, expiresAt:record.context.expiresAt});
  }
  return Object.freeze({
    open({actor,appId,domainId,target}) {
      sweep(); const profile=admissions.require(target);
      if(profile.schema==='soty.selected-human-embed.v2')need(attempts.size<SCOPED_EMBED_LIMITS.continuations,'app_scoped_attempt_busy',429);
      const context=authority.open({actor,appId,domainId,targetRevision:target.revision}), handle=nonce();
      const record={context,profile,handleHash:hash(handle),session:null,sourceSession:null};records.set(context.reference.id,record);handles.set(record.handleHash,record);
      // A bounded RAM witness permits ONLY a freshly signed attempt. It grants
      // no Source session, carries no cookie and does not make the old ref live.
      // Source must independently confirm its durable current Native consent.
      if(profile.schema==='soty.selected-human-embed.v2')attempts.set(record.handleHash,Object.freeze({context,expiresAt:context.expiresAt+86400000}));
      return { record, closeHandle:handle };
    },
    attach(record,session) { current(record); record.session=session; return record; },
    read(reference,connector) {
      const captured=capture(reference), record=records.get(captured.id);
      need(record && hash(captured)===hash(record.context.reference),'app_scoped_context_closed',403);
      return current(record,connector);
    },
    context(record) { return current(record); },
    sourceContext(record){current(record);const state=record.sourceSession;
      if(!state||state.ready!==true||state.accessExpiresAt<=clock()||state.sessionExpiresAt<=clock())return state?.ready===false?{ready:false,reason:'login_required'}:null;
      return{ready:true,renewable:state.renewable,sessionExpiresAt:state.sessionExpiresAt,accessExpiresAt:state.accessExpiresAt};},
    ownedContext(actor,appId,handle) {
      need(opaque(handle),'app_scoped_context_closed',403);sweep();
      const record=handles.get(hash(handle));
      need(record&&record.context.appId===appId&&actor?.accountId===record.context.rootPrincipal.accountId
        &&actor?.deviceId===record.context.rootPrincipal.deviceId,'app_scoped_context_closed',403);
      return {record,context:current(record)};
    },
    ownedAttempt(actor,appId,handle){
      need(opaque(handle),'app_scoped_context_closed',403);sweep();const key=hash(handle),live=handles.get(key),witness=attempts.get(key);
      if(live?.profile.schema!=='soty.selected-human-embed.v2'&&!witness)return this.ownedContext(actor,appId,handle);
      const context=witness?.context;
      need(context&&context.appId===appId&&actor?.accountId===context.rootPrincipal.accountId&&actor?.deviceId===context.rootPrincipal.deviceId,
        'app_scoped_context_closed',403);
      return{record:live??null,context,sourceOnly:true};
    },
    captureHead(record, auth) {
      current(record); if(auth===undefined)return;
      if(auth?.kind==='continued'){closed(auth,['kind','ack']);const parsed=sourceContinuationAck({schema:'soty.source-session-continuation.v1',...auth.ack},clock());
        current(record);record.sourceSession=parsed;return;}
      const value=capture(auth);closed(value,['kind','digest']);
      need(['start','completion','cancel'].includes(value.kind),'app_scoped_auth_binding_invalid',403);
      if(value.kind==='cancel') {const prior=authStates.get(value.digest);need(!prior||prior.record===record,'app_scoped_auth_binding_conflict',403);authStates.delete(value.digest);return;}
      mapping(value.kind==='start'?authStates:completions,value.digest,record);
    },
    callback(appId, path) {
      sweep(); const url=new URL(path,'https://fixed.invalid');
      let map,key;
      if(url.pathname==='/api/embed/callback') {
        need(url.searchParams.getAll('state').length===1,'app_scoped_callback_invalid',403);
        const state=url.searchParams.get('state');need(opaque(state),'app_scoped_callback_invalid',403);
        map=authStates;key=hash(state);
      } else if(url.pathname==='/api/embed/complete-link') {
        need([...url.searchParams.keys()].length===1 && opaque(url.searchParams.get('intent')),'app_scoped_completion_invalid',403);
        map=completions;key=hash(url.searchParams.get('intent'));
      } else need(false,'app_scoped_callback_invalid',403);
      const item=map.get(key);map.delete(key);
      need(item && item.record.context.appId===appId && item.expiresAt>clock() && item.record.session,'app_scoped_callback_expired',403);
      current(item.record);return item.record;
    },
    abandon(actor, appId, handle) {
      need(opaque(handle),'app_scoped_close_invalid',403);sweep();const key=hash(handle),record=handles.get(key),prior=closedHandles.get(key);
      const attempt=attempts.get(key)?.context;
      need(record?record.context.appId===appId:(!prior||prior.appId===appId)&&(!attempt||attempt.appId===appId),'app_scoped_close_unavailable',403);
      const owner=record?.context.rootPrincipal??attempt?.rootPrincipal??prior;
      need(!owner||actor?.accountId===owner.accountId&&actor?.deviceId===owner.deviceId,'app_scoped_close_unavailable',403);
      if(record)stop(record);attempts.delete(key);return Object.freeze({closed:true});
    },
    invalidateApp(appId) {for(const record of [...records.values()])if(record.context.appId===appId)stop(record);
      for(const[key,item]of attempts)if(item.context.appId===appId)attempts.delete(key);},
    invalidate(record) {stop(record);},
    close() {for(const record of [...records.values()])stop(record);authority.close();authStates.clear();completions.clear();handles.clear();closedHandles.clear();attempts.clear();},
  });
}
