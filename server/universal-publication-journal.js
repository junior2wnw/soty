import { DatabaseSync } from 'node:sqlite';
import { mkdirSync } from 'node:fs';
import { dirname } from 'node:path';
import { createHash } from 'node:crypto';
import { canonicalContractJson, contractDigest } from '../modules/app-contract/index.mjs';

const hash=value=>createHash('sha256').update(value).digest('hex');
const equal=(a,b)=>canonicalContractJson(a)===canonicalContractJson(b);
const fail=code=>{throw Object.assign(new Error(code),{code,status:409});};
const check=(value,code='registration_publication_source_changed')=>{if(!value)fail(code);};
const encode=canonicalContractJson;
const snapshot=authority=>({appId:authority.appId,ownerId:authority.ownerId,appRevision:authority.appRevision,policyEpoch:authority.policyEpoch,
  target:authority.target,entryPath:authority.entry.path,visibility:authority.visibility,sourceChangeEpoch:authority.sourceChangeEpoch});
const sameSource=(a,b)=>a.appId===b.appId&&a.ownerId===b.ownerId&&a.appRevision===b.appRevision&&a.entryPath===b.entryPath
  &&a.sourceChangeEpoch===b.sourceChangeEpoch&&equal(a.target,b.target);
const ready=value=>value?.state==='ready'&&value.descriptor&&value.feedback?.state==='ready'&&typeof value.feedback.installationId==='string';

/** A receipt journal, not another application registry. Only a current reviewed
 * universal descriptor may become a pre-publication anchor. The Apps source
 * epoch also catches retained Source rollback A→B→A with the original digest. */
export function createUniversalPublicationJournal({databasePath,registration,confirmFeedback}){
  mkdirSync(dirname(databasePath),{recursive:true});const db=new DatabaseSync(databasePath);let closed=false;
  try{
    db.exec('PRAGMA busy_timeout=100;PRAGMA synchronous=FULL;PRAGMA foreign_keys=ON;');
    const version=db.prepare('PRAGMA user_version').get().user_version;
    if(version===0){check(db.prepare("SELECT count(*) AS n FROM sqlite_schema WHERE name NOT GLOB 'sqlite_*'").get().n===0,'registration_publication_storage_unsupported');
      db.exec(`BEGIN IMMEDIATE;
        CREATE TABLE journal_metadata(key TEXT PRIMARY KEY,value TEXT NOT NULL) STRICT;
        INSERT INTO journal_metadata VALUES('schema','soty.universal-publication-journal.v1');
        CREATE TABLE publication_anchors(account_id TEXT NOT NULL,app_id TEXT NOT NULL,value_json TEXT NOT NULL CHECK(json_valid(value_json)),PRIMARY KEY(account_id,app_id)) STRICT;
        CREATE TABLE publication_commands(account_id TEXT NOT NULL,request_key TEXT NOT NULL,intent_hash TEXT NOT NULL,app_id TEXT NOT NULL,anchor_json TEXT NOT NULL CHECK(json_valid(anchor_json)),state TEXT NOT NULL CHECK(state IN ('pending','ready','superseded')),result_json TEXT CHECK(result_json IS NULL OR json_valid(result_json)),PRIMARY KEY(account_id,request_key)) STRICT;
        PRAGMA user_version=1;COMMIT;`);
    }
    check(db.prepare('PRAGMA user_version').get().user_version===1&&db.prepare("SELECT value FROM journal_metadata WHERE key='schema'").get()?.value==='soty.universal-publication-journal.v1','registration_publication_storage_unsupported');
    const pageSize=db.prepare('PRAGMA page_size').get().page_size;db.exec(`PRAGMA max_page_count=${Math.floor(16*1024*1024/pageSize)};PRAGMA journal_mode=WAL;`);
  }catch(error){if(db.isTransaction)db.exec('ROLLBACK');db.close();throw error;}
  const transaction=callback=>{check(!closed,'registration_publication_closed');db.exec('BEGIN IMMEDIATE');try{const result=callback();db.exec('COMMIT');return result;}catch(error){if(db.isTransaction)db.exec('ROLLBACK');throw error;}};
  function anchorFor(actor,appId){const row=db.prepare('SELECT value_json FROM publication_anchors WHERE account_id=? AND app_id=?').get(actor.accountId,appId);return row?JSON.parse(row.value_json):null;}
  function acceptedAnchor(actor,state){
    const current=state.registration;if(!ready(current))return null;
    const authority=snapshot(state.authority),source=current.descriptor.app.source;
    check(source.id===`apps:${authority.appId}/target`&&source.revision===authority.target.revision&&source.digest===authority.target.digest
      &&current.descriptor.app.visibility===authority.visibility,'registration_publication_source_changed');
    const anchor={authority,revision:current.revision,generation:current.generation,authorityDigest:current.authorityDigest,
      descriptorDigest:current.descriptorDigest,descriptor:current.descriptor,installationId:current.feedback.installationId};
    check(Buffer.byteLength(encode(anchor))<=128*1024,'registration_publication_anchor_too_large');
    db.prepare('INSERT INTO publication_anchors VALUES(?,?,?) ON CONFLICT(account_id,app_id) DO UPDATE SET value_json=excluded.value_json').run(actor.accountId,authority.appId,encode(anchor));
    return anchor;
  }
  function captureReady(actor,appId){return registration.withPublicationState({actor,appId},state=>transaction(()=>Boolean(acceptedAnchor(actor,state))));}
  function command(actor,args){const row=db.prepare('SELECT * FROM publication_commands WHERE account_id=? AND request_key=?').get(actor.accountId,hash(args.requestId));
    if(row)check(row.intent_hash===contractDigest(args)&&row.app_id===args.appId,'registration_publication_intent_conflict');return row;}
  function prepare(actor,args){
    return registration.withPublicationState({actor,appId:args.appId},state=>transaction(()=>{
      const prior=command(actor,args);if(prior)return true;
      const anchor=acceptedAnchor(actor,state)??anchorFor(actor,args.appId),current=snapshot(state.authority),record=state.registration;
      // Unknown stale metadata has no automatic path back into admission.
      if(!anchor||!record||anchor.revision!==record.revision||anchor.generation!==record.generation||anchor.authorityDigest!==record.authorityDigest
        ||!sameSource(anchor.authority,current)||anchor.authority.policyEpoch!==current.policyEpoch||current.policyEpoch!==args.expectedPolicyEpoch)return false;
      check(db.prepare('SELECT count(*) AS n FROM publication_commands WHERE account_id=?').get(actor.accountId).n<4096,'registration_publication_receipt_limit');
      db.prepare("INSERT INTO publication_commands VALUES(?,?,?,?,?,'pending',NULL)").run(actor.accountId,hash(args.requestId),contractDigest(args),args.appId,encode(anchor));return true;
    }));
  }
  function reconcile(actor,args,result){
    const row=command(actor,args);if(!row)return {state:'held'};
    const anchor=JSON.parse(row.anchor_json),receipt=result.receipt;
    const matches=authority=>{
      const current=snapshot(authority);return receipt?.appId===args.appId&&receipt.requestKeyHash===hash(args.requestId)
        &&receipt.policyEpoch===current.policyEpoch&&receipt.targetRevision===anchor.authority.target.revision
        &&receipt.targetDigest===anchor.authority.target.digest&&receipt.profile===anchor.authority.target.profile
        &&current.policyEpoch>anchor.authority.policyEpoch&&sameSource(anchor.authority,current)
        &&current.visibility===(receipt.launchPolicy==='anyone'?'public':'private');
    };
    const state=registration.withPublicationState({actor,appId:args.appId},value=>({registration:value.registration,matches:matches(value.authority),visibility:value.authority.visibility}));
    if(!state.matches)return {state:'held'};
    if(row.state==='superseded')return {state:'superseded',registration:state.registration};
    const descriptor=structuredClone(anchor.descriptor);descriptor.app.visibility=state.visibility;
    const requestId=`publication.${hash(actor.accountId+'\0'+args.requestId)}`;
    let admitted;
    try{admitted=registration.reconcilePublication({actor,appId:args.appId,requestId,expectedRevision:anchor.revision,descriptor,verifyCurrent:matches});}
    catch(error){if(error.code!=='registration_revision_conflict')throw error;
      transaction(()=>db.prepare("UPDATE publication_commands SET state='superseded' WHERE account_id=? AND request_key=?").run(actor.accountId,hash(args.requestId)));
      return {state:'superseded',registration:state.registration};}
    if(admitted.receipt.generation!==admitted.registration.generation||admitted.registration.descriptorDigest!==contractDigest(descriptor)){
      transaction(()=>db.prepare("UPDATE publication_commands SET state='superseded' WHERE account_id=? AND request_key=?").run(actor.accountId,hash(args.requestId)));
      return {state:'superseded',registration:admitted.registration};
    }
    const confirmed=confirmFeedback({actor,args:{appId:args.appId}},admitted).registration;
    if(!ready(confirmed))return {state:'pending-feedback',registration:confirmed};
    check(confirmed.feedback.installationId===anchor.installationId,'registration_publication_feedback_changed');
    registration.withPublicationState({actor,appId:args.appId},current=>transaction(()=>{
      check(matches(current.authority)&&current.registration.revision===confirmed.revision&&current.registration.descriptorDigest===confirmed.descriptorDigest);
      acceptedAnchor(actor,current);db.prepare("UPDATE publication_commands SET state='ready',result_json=? WHERE account_id=? AND request_key=?").run(encode({generation:confirmed.generation,revision:confirmed.revision,descriptorDigest:confirmed.descriptorDigest}),actor.accountId,hash(args.requestId));
      return true;
    }));
    return {state:'ready',registration:confirmed};
  }
  return Object.freeze({captureReady,prepare,reconcile,close(){if(!closed){closed=true;db.close();}}});
}
