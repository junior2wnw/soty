import {randomBytes} from 'node:crypto';
import {assertApps} from './protocol.mjs';
import {canonical,hash} from '../scoped-embed/profile.mjs';
import {isRootSignedWorkspaceSource} from '../../source-app/server/standard-profile.mjs';

export const FRAME_CONTINUITY_STORAGE='soty.apps-frame-continuity.v2';
export const LEGACY_FRAME_CONTINUITY_STORAGE='soty.apps-frame-continuity.v1';
export const frameContinuityColumns={app_frame_continuity:['frame_id','account_id','device_id','app_id','binding_json','binding_digest','lease_hash','host_id','revision','state','expires_at','updated_at']};
export const frameContinuityDdl=()=>`CREATE TABLE app_frame_continuity (
 frame_id TEXT PRIMARY KEY NOT NULL CHECK(length(frame_id)=43),account_id TEXT NOT NULL,device_id TEXT NOT NULL,
 app_id TEXT NOT NULL REFERENCES local_apps(id),binding_json TEXT NOT NULL CHECK(length(binding_json) BETWEEN 1 AND 8192),
 binding_digest TEXT NOT NULL CHECK(length(binding_digest)=64),lease_hash TEXT NOT NULL CHECK(length(lease_hash)=64),
 host_id TEXT NOT NULL CHECK(length(host_id)=43),revision INTEGER NOT NULL CHECK(revision>=1),
 state TEXT NOT NULL CHECK(state IN ('open','closed')),expires_at INTEGER NOT NULL,updated_at INTEGER NOT NULL);
 CREATE INDEX app_frame_continuity_owner ON app_frame_continuity(account_id,device_id);
 CREATE INDEX app_frame_continuity_app ON app_frame_continuity(app_id);`;
export function frameContinuityStorageVersion(db){
 if(!db.prepare("SELECT 1 FROM sqlite_schema WHERE type='table' AND name='apps_meta'").get())return 0;
 const row=db.prepare("SELECT value FROM apps_meta WHERE key='frame_continuity'").get();
 assertApps(!row||[FRAME_CONTINUITY_STORAGE,LEGACY_FRAME_CONTINUITY_STORAGE].includes(row.value),'apps_schema_unsupported');
 return !row?0:row.value===FRAME_CONTINUITY_STORAGE?2:1;
}
export const hasFrameContinuitySchema=db=>frameContinuityStorageVersion(db)>0;
export const hasHostSessionContinuitySchema=db=>frameContinuityStorageVersion(db)===2;
const closed=(value,names)=>value&&typeof value==='object'&&!Array.isArray(value)&&Object.keys(value).length===names.length&&names.every(name=>Object.hasOwn(value,name));
const hashString=value=>typeof value==='string'&&/^[a-f0-9]{64}$/u.test(value);
const id=value=>typeof value==='string'&&/^[A-Za-z0-9][A-Za-z0-9_.:@/-]{0,179}$/u.test(value)&&!value.includes('..');
const positive=value=>Number.isSafeInteger(value)&&value>=1;
export function isNativeFrameContinuityBinding(value){
 if(!closed(value,['profileDigest','policyEpoch','entry','target','sourceProfile','resourceDigest','humanDigest','rootInstanceId'])
  ||!hashString(value.profileDigest)||!hashString(value.resourceDigest)||!hashString(value.humanDigest)||!positive(value.policyEpoch)
  ||!closed(value.entry,['domainId','origin','pathDigest'])||!id(value.entry.domainId)||!hashString(value.entry.pathDigest)
  ||!closed(value.target,['revision','digest'])||!positive(value.target.revision)||!hashString(value.target.digest)
  ||!closed(value.sourceProfile,['id','version','digest'])||!id(value.sourceProfile.id)||!positive(value.sourceProfile.version)||!hashString(value.sourceProfile.digest)
  ||value.rootInstanceId!==null&&!id(value.rootInstanceId))return false;
 try{return new URL(value.entry.origin).origin===value.entry.origin;}catch{return false;}
}
export function isFrameContinuityEnvelope(row){
 return row&&typeof row.frame_id==='string'&&/^[A-Za-z0-9_-]{43}$/u.test(row.frame_id)
  &&id(row.account_id)&&id(row.device_id)&&id(row.app_id)&&hashString(row.binding_digest)&&hashString(row.lease_hash)
  &&typeof row.host_id==='string'&&/^[A-Za-z0-9_-]{43}$/u.test(row.host_id)&&positive(row.revision)
  &&['open','closed'].includes(row.state)&&Number.isSafeInteger(row.expires_at)&&Number.isSafeInteger(row.updated_at);
}
const opaque=v=>typeof v==='string'&&/^[A-Za-z0-9_-]{43}$/.test(v);
const denied=ok=>assertApps(ok,'app_scoped_context_closed',403);
const eligible=r=>isRootSignedWorkspaceSource(r.context.sourceProfile);
// Only scope metadata. Actor objects, cookies, Source ACKs and readiness stay RAM-only.
function binding(record,path){const c=record.context;return {
 profileDigest:c.profileDigest,policyEpoch:c.policyEpoch,entry:{...c.entry,pathDigest:hash(path)},target:c.target,
 sourceProfile:c.sourceProfile,resourceDigest:hash(c.resource),humanDigest:hash(c.humanPrincipal),
 rootInstanceId:record.rootInstanceId??null};}

export function createFrameContinuity({db,clock=Date.now,hostId=randomBytes(32).toString('base64url')}){
 assertApps(hasFrameContinuitySchema(db)&&opaque(hostId),'apps_frame_storage_unavailable',503);
 const claims=new WeakSet();
 const transaction=fn=>{if(db.isTransaction)return fn();db.exec('BEGIN IMMEDIATE');try{const result=fn();db.exec('COMMIT');return result;}catch(e){if(db.isTransaction)db.exec('ROLLBACK');throw e;}};
 const read=id=>db.prepare('SELECT * FROM app_frame_continuity WHERE frame_id=?').get(id);
 function active(row,actor,appId){denied(row&&row.state==='open'&&row.expires_at>clock()&&row.account_id===actor.accountId&&row.device_id===actor.deviceId&&row.app_id===appId);}
 function decoded(row){let value;try{value=JSON.parse(row.binding_json);}catch{}assertApps(value&&canonical(value)===row.binding_json&&hash(value)===row.binding_digest
  &&value.entry&&Object.keys(value.entry).sort().join(',')==='domainId,origin,pathDigest'&&typeof value.entry.pathDigest==='string'&&/^[a-f0-9]{64}$/.test(value.entry.pathDigest),'apps_schema_unsupported');return value;}
 function assertRecord(record){
  const claim=record.frameContinuity;if(!claim)return;
  denied(claims.has(claim));const row=read(claim.frameId);active(row,record.context.rootPrincipal,record.context.appId);
  denied(row.binding_digest===hash(binding(record,claim.path))&&(row.lease_hash===record.handleHash
   ||row.revision===claim.revision&&row.lease_hash===claim.leaseHash));return row;
 }
 return Object.freeze({
  frameId(record){return eligible(record)?record.frameContinuity?.frameId??record.context.reference.id:undefined;},
  recover(actor,appId,frameId,leaseHash){
   denied(opaque(frameId));const row=read(frameId);active(row,actor,appId);denied(row.host_id!==hostId&&row.lease_hash===leaseHash);
   const scope=decoded(row),claim=Object.freeze({frameId,revision:row.revision,leaseHash:row.lease_hash,scope});claims.add(claim);return claim;
  },
  prepare(record,frameId,recovery,path){
   denied(eligible(record));const row=read(frameId);active(row,record.context.rootPrincipal,record.context.appId);
   const scope=decoded(row);if(recovery)denied(claims.has(recovery)&&recovery.frameId===frameId&&recovery.revision===row.revision&&row.host_id!==hostId);
   denied(typeof path==='string'&&hash(binding(record,path))===row.binding_digest);
   const claim=Object.freeze({frameId,revision:row.revision,leaseHash:row.lease_hash,path,recovery:Boolean(recovery)});claims.add(claim);record.frameContinuity=claim;
  },
  attach(record,session){
   if(!eligible(record))return;if(record.frameContinuity){assertRecord(record);return;}
   const frameId=session.family.frameId;denied(frameId===record.context.reference.id&&opaque(frameId));
   const scope=binding(record,session.entryPath),json=canonical(scope),now=clock();
   transaction(()=>{db.prepare('DELETE FROM app_frame_continuity WHERE expires_at<=?').run(now);
    const excess=db.prepare('SELECT count(*) n FROM app_frame_continuity').get().n-4095;
    if(excess>0)db.prepare("DELETE FROM app_frame_continuity WHERE frame_id IN (SELECT frame_id FROM app_frame_continuity WHERE state='closed' ORDER BY updated_at,frame_id LIMIT ?)").run(excess);
    assertApps(db.prepare('SELECT count(*) n FROM app_frame_continuity').get().n<4096
     &&db.prepare("SELECT count(*) n FROM app_frame_continuity WHERE account_id=? AND device_id=? AND state='open'").get(record.context.rootPrincipal.accountId,record.context.rootPrincipal.deviceId).n<256,'apps_sessions_busy',429);
    db.prepare('INSERT INTO app_frame_continuity VALUES(?,?,?,?,?,?,?,?,?,?,?,?)').run(frameId,record.context.rootPrincipal.accountId,record.context.rootPrincipal.deviceId,
     record.context.appId,json,hash(scope),record.handleHash,hostId,1,'open',now+86400000,now);});
   const claim=Object.freeze({frameId,revision:1,leaseHash:record.handleHash,path:session.entryPath});claims.add(claim);record.frameContinuity=claim;
  },
  assertRecord,
  isRecovery(record){return claims.has(record.frameContinuity)&&record.frameContinuity.recovery===true;},
  promote(record){return transaction(()=>{const row=assertRecord(record);if(!row||row.lease_hash===record.handleHash)return;
   const changed=db.prepare("UPDATE app_frame_continuity SET lease_hash=?,host_id=?,revision=revision+1,expires_at=?,updated_at=? WHERE frame_id=? AND state='open' AND revision=? AND lease_hash=?")
    .run(record.handleHash,hostId,clock()+86400000,clock(),row.frame_id,row.revision,row.lease_hash);denied(changed.changes===1);});},
  close(record){const claim=record.frameContinuity;if(!claim)return;
   db.prepare("UPDATE app_frame_continuity SET state='closed',revision=revision+1,updated_at=? WHERE frame_id=? AND lease_hash=? AND state='open'").run(clock(),claim.frameId,record.handleHash);},
  closeLost(actor,appId,handleHash){db.prepare("UPDATE app_frame_continuity SET state='closed',revision=revision+1,updated_at=? WHERE account_id=? AND device_id=? AND app_id=? AND lease_hash=? AND state='open'").run(clock(),actor.accountId,actor.deviceId,appId,handleHash);},
  revokeApp(appId){db.prepare("UPDATE app_frame_continuity SET state='closed',revision=revision+1,updated_at=? WHERE app_id=? AND state='open'").run(clock(),appId);},
 });
}
