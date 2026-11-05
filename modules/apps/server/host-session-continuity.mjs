import { randomBytes } from 'node:crypto';
import { assertApps,textId,appId } from './protocol.mjs';
import { hasHostSessionContinuitySchema } from './frame-continuity.mjs';
import { canonical, hash } from '../scoped-embed/profile.mjs';

export const HOST_SESSION_SCHEMA='soty.app-host-session.v1';
const opaque=value=>typeof value==='string'&&/^[A-Za-z0-9_-]{43}$/u.test(value);
const secret=()=>randomBytes(32).toString('base64url');
const positive=value=>Number.isSafeInteger(value)&&value>=1;
const fail=(ok,code='app_host_session_closed',status=403)=>assertApps(ok,code,status);
const keys=(value,expected)=>value&&typeof value==='object'&&!Array.isArray(value)&&Object.keys(value).sort().join(',')===expected;
function capture(scope) {
  fail(keys(scope,'accountId,appId,deviceId,entry,instanceId,policyEpoch,runtimeOrigin,runtimeScopeDigest,schema,target')&&scope.schema==='soty.app-host-scope.v1'
    &&positive(scope.policyEpoch)&&keys(scope.entry,'domainId,origin,pathDigest')&&typeof scope.entry.domainId==='string'
    &&/^[a-f0-9]{64}$/.test(scope.entry.pathDigest)&&keys(scope.target,'bindingFloor,digest,profile,revision')
    &&positive(scope.target.revision)&&positive(scope.target.bindingFloor)&&/^[a-f0-9]{64}$/.test(scope.target.digest)
    &&['soty.relay-restricted.v1','soty.relay-self-ip.v1'].includes(scope.target.profile)
    &&(scope.instanceId==='main'||/^widget-[a-f0-9]{8}-[a-f0-9]{4}-[a-f0-9]{4}-[a-f0-9]{4}-[a-f0-9]{12}$/.test(scope.instanceId))&&/^[a-f0-9]{64}$/.test(scope.runtimeScopeDigest),'app_host_session_changed');
  textId(scope.accountId);textId(scope.deviceId);appId(scope.appId);textId(scope.entry.domainId);
  for(const origin of[scope.entry.origin,scope.runtimeOrigin]){let parsed;try{parsed=new URL(origin);}catch{}fail(parsed&&parsed.origin===origin&&!parsed.username&&!parsed.password&&['http:','https:'].includes(parsed.protocol),'app_host_session_changed');}
  const entry=new URL(scope.entry.origin),runtime=new URL(scope.runtimeOrigin),dot=entry.hostname.indexOf('.');
  const prefix=scope.instanceId==='main'?'app-m'+scope.appId.slice(4):'app-w'+scope.instanceId.slice(7).replaceAll('-','');
  fail(dot>=1&&runtime.protocol===entry.protocol&&runtime.port===entry.port
    &&runtime.hostname===prefix+'-'+scope.runtimeScopeDigest.slice(0,24)+'.'+entry.hostname.slice(dot+1),'app_host_session_changed');
  return Object.freeze({...scope,entry:Object.freeze({...scope.entry}),target:Object.freeze({...scope.target})});
}
const metadata=scope=>{const frozen=capture(scope);return Object.freeze({kind:HOST_SESSION_SCHEMA,family:hash(frozen),scope:frozen,lifecycle:'reserved'});};

/** Tagged passive members in the existing bounded continuity table. No Actor,
 * cookie token, password permit, Source ACK or ready state is persisted. */
export function createHostSessionContinuity({db,clock=Date.now,hostId=secret()}) {
  fail(hasHostSessionContinuitySchema(db),'app_host_session_unavailable',503);
  const transaction=fn=>{if(db.isTransaction)return fn();db.exec('BEGIN IMMEDIATE');try{const value=fn();db.exec('COMMIT');return value;}catch(error){if(db.isTransaction)db.exec('ROLLBACK');throw error;}};
  const row=id=>db.prepare('SELECT * FROM app_frame_continuity WHERE frame_id=?').get(id);
  function decode(value) {
    let parsed;try{parsed=JSON.parse(value?.binding_json);}catch{}
    fail(keys(parsed,'family,kind,lifecycle,scope')&&['reserved','admitted'].includes(parsed.lifecycle)&&parsed.kind===HOST_SESSION_SCHEMA&&canonical(parsed)===value.binding_json&&hash(parsed)===value.binding_digest
      &&parsed.family===hash(parsed.scope),'app_host_session_closed');return Object.freeze({...parsed,scope:capture(parsed.scope)});
  }
  function owned(actor,appId,frameId) {
    fail(opaque(frameId));const current=row(frameId),meta=decode(current);
    fail(current.account_id===actor.accountId&&current.device_id===actor.deviceId&&current.app_id===appId);
    return{row:current,meta};
  }
  function active(current) {fail(current.state==='open'&&current.expires_at>clock());}
  function claim(current,meta,lease) {return Object.freeze({frameId:current.frame_id,leaseHash:hash(lease),revision:current.revision,expiresAt:current.expires_at,family:meta.family,scope:meta.scope});}
  function current(input,actor,appId) {
    fail(input&&opaque(input.lease)&&positive(input.revision));const found=owned(actor,appId,input.frameId);active(found.row);
    fail(found.row.revision===input.revision&&found.row.lease_hash===hash(input.lease),'app_host_session_conflict',409);
    return claim(found.row,found.meta,input.lease);
  }
  function assertClaim(value,{candidate=false}={}) {
    const found=row(value.frameId),meta=decode(found);active(found);
    fail(meta.family===value.family&&canonical(meta.scope)===canonical(value.scope),'app_host_session_changed');
    const promoted=found.revision===value.revision&&found.lease_hash===value.leaseHash;
    fail(promoted||candidate&&found.revision===value.priorRevision&&found.lease_hash===value.priorLeaseHash,'app_host_session_conflict',409);
    return{row:found,meta,promoted};
  }
  return Object.freeze({
    create(scope) {
      const frameId=secret(),lease=secret(),meta=metadata(scope),json=canonical(meta),at=clock(),expiresAt=at+30000;
      fail(json.length<=8192,'app_host_session_unavailable',503);
      transaction(()=>{
        db.prepare('DELETE FROM app_frame_continuity WHERE expires_at<=?').run(at);
        const excess=db.prepare('SELECT count(*) n FROM app_frame_continuity').get().n-4095;
        if(excess>0)db.prepare("DELETE FROM app_frame_continuity WHERE frame_id IN (SELECT frame_id FROM app_frame_continuity WHERE state='closed' ORDER BY updated_at,frame_id LIMIT ?)").run(excess);
        fail(db.prepare('SELECT count(*) n FROM app_frame_continuity').get().n<4096
          &&db.prepare("SELECT count(*) n FROM app_frame_continuity WHERE account_id=? AND device_id=? AND state='open'").get(scope.accountId,scope.deviceId).n<256,'apps_sessions_busy',429);
        const family=db.prepare("SELECT binding_json FROM app_frame_continuity WHERE account_id=? AND device_id=? AND app_id=? AND state='open' AND expires_at>?").all(scope.accountId,scope.deviceId,scope.appId,at);
        fail(family.filter(item=>{try{return JSON.parse(item.binding_json).family===meta.family;}catch{return false;}}).length<16,'apps_sessions_busy',429);
        db.prepare('INSERT INTO app_frame_continuity VALUES(?,?,?,?,?,?,?,?,?,?,?,?)').run(frameId,scope.accountId,scope.deviceId,scope.appId,json,hash(meta),hash(lease),hostId,1,'open',expiresAt,at);
      });
      return{descriptor:Object.freeze({schema:HOST_SESSION_SCHEMA,frameId,lease,revision:1,expiresAt}),claim:claim(row(frameId),meta,lease)};
    },
    current,assertClaim,
    admitting(value) {return assertClaim(value).meta.lifecycle==='reserved';},
    activateInitial(value) {return transaction(()=>{
      const found=assertClaim(value);fail(value.revision===1,'app_host_session_conflict',409);
      if(found.meta.lifecycle==='admitted')return Object.freeze({...value,expiresAt:found.row.expires_at});
      const expiresAt=clock()+86400000,meta={...found.meta,lifecycle:'admitted'};
      const result=db.prepare("UPDATE app_frame_continuity SET binding_json=?,binding_digest=?,expires_at=?,updated_at=? WHERE frame_id=? AND state='open' AND revision=? AND lease_hash=? AND binding_digest=?")
        .run(canonical(meta),hash(meta),expiresAt,clock(),value.frameId,value.revision,value.leaseHash,found.row.binding_digest);
      fail(result.changes===1,'app_host_session_conflict',409);return Object.freeze({...value,expiresAt});
    });},
    cookieState(frameId,leaseHash) {
      try{const current=row(frameId),meta=decode(current);return{family:meta.family,revision:current.revision,open:current.state==='open'&&current.expires_at>clock(),leaseHash:current.lease_hash};}catch{return null;}
    },
    invalidateWidget(filter) {return transaction(()=>{
      let count=0;
      for(const current of db.prepare("SELECT * FROM app_frame_continuity WHERE state='open'").all()){
        let meta;try{meta=decode(current);}catch{continue;}
        if(meta.scope.instanceId!==filter.widgetId||filter.accountId&&current.account_id!==filter.accountId||filter.deviceId&&current.device_id!==filter.deviceId)continue;
        count+=db.prepare("UPDATE app_frame_continuity SET state='closed',revision=revision+1,updated_at=? WHERE frame_id=? AND state='open'").run(clock(),current.frame_id).changes;
      }
      return count;
    });},
    prepare(prior) {
      const current=assertClaim(prior);fail(current.meta.lifecycle==='admitted','app_host_session_unconfirmed',409);fail(prior.revision<Number.MAX_SAFE_INTEGER,'app_host_session_conflict',409);const lease=secret(),expiresAt=clock()+86400000;
      return{descriptor:Object.freeze({schema:HOST_SESSION_SCHEMA,frameId:prior.frameId,lease,revision:prior.revision+1,expiresAt}),
        claim:Object.freeze({...prior,leaseHash:hash(lease),revision:prior.revision+1,expiresAt,priorRevision:prior.revision,priorLeaseHash:prior.leaseHash})};
    },
    promote(value) {return transaction(()=>{
      const found=assertClaim(value,{candidate:true});fail(found.meta.lifecycle==='admitted','app_host_session_unconfirmed',409);if(found.promoted)return;
      const result=db.prepare("UPDATE app_frame_continuity SET lease_hash=?,host_id=?,revision=revision+1,expires_at=?,updated_at=? WHERE frame_id=? AND state='open' AND revision=? AND lease_hash=?")
        .run(value.leaseHash,hostId,value.expiresAt,clock(),value.frameId,value.priorRevision,value.priorLeaseHash);
      fail(result.changes===1,'app_host_session_conflict',409);
    });},
    close(actor,appId,input,candidate) {return transaction(()=>{
      fail(input&&opaque(input.lease)&&positive(input.revision));const found=owned(actor,appId,input.frameId);
      if(found.row.state!=='open')return false;
      const presented=hash(input.lease),exact=found.row.lease_hash===presented&&found.row.revision===input.revision;
      const pending=candidate&&candidate.frameId===input.frameId&&candidate.leaseHash===presented&&candidate.revision===input.revision
        &&found.row.lease_hash===candidate.priorLeaseHash&&found.row.revision===candidate.priorRevision;
      if(!exact&&!pending)return false;
      return db.prepare("UPDATE app_frame_continuity SET state='closed',revision=revision+1,updated_at=? WHERE frame_id=? AND state='open' AND revision=? AND lease_hash=?")
        .run(clock(),input.frameId,found.row.revision,found.row.lease_hash).changes===1;
    });},
  });
}
