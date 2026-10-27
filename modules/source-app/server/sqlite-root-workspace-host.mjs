import { DatabaseSync } from 'node:sqlite';
import { existsSync,lstatSync,openSync,closeSync } from 'node:fs';
import {isAbsolute} from 'node:path';
import { selectedResourceProfile } from '../../apps/scoped-embed/resource-profile.mjs';
import { readVerifiedRootContext } from '../../apps/scoped-embed/source-authority-client.mjs';
import { isRootSignedWorkspaceSource } from './standard-profile.mjs';
import { createSourceNativeAuthorityPort, isSourceNativeCommitPort, isSourceNativeReceiptRecoveryPort } from './native-authority.mjs';
import { createSourceSessionCipher } from './session-cipher.mjs';
import { check, fields, digest, nonce, syncResult, jsonCopy } from './wire.mjs';

const SCHEMA='soty.sqlite-root-workspace.v1';
export const SQLITE_ROOT_WORKSPACE_LIMITS=Object.freeze({sessions:4096,nonces:4096,receiptsPerPerson:20000});
const DDL=`CREATE TABLE soty_workspace_meta(id INTEGER PRIMARY KEY CHECK(id=1),schema TEXT NOT NULL,namespace TEXT NOT NULL,personal INTEGER NOT NULL CHECK(personal IN(0,1)));
CREATE TABLE soty_workspace_people(id TEXT PRIMARY KEY,issuer TEXT NOT NULL,subject TEXT NOT NULL,incarnation TEXT NOT NULL,active INTEGER NOT NULL CHECK(active IN(0,1)),generation INTEGER NOT NULL,revision INTEGER NOT NULL,UNIQUE(issuer,subject));
CREATE TABLE soty_workspace_sessions(id_hash TEXT PRIMARY KEY,principal_id TEXT NOT NULL REFERENCES soty_workspace_people(id),resource_id TEXT NOT NULL,native_hash TEXT NOT NULL,active INTEGER NOT NULL CHECK(active IN(0,1)),expires_at INTEGER NOT NULL,cipher TEXT NOT NULL,key_id TEXT NOT NULL);
CREATE TABLE soty_workspace_nonces(nonce TEXT PRIMARY KEY,expires_at INTEGER NOT NULL);
CREATE TABLE soty_workspace_receipts(principal_id TEXT NOT NULL REFERENCES soty_workspace_people(id),request_id TEXT NOT NULL,input_digest TEXT NOT NULL,result_json TEXT NOT NULL,binding_json TEXT NOT NULL,PRIMARY KEY(principal_id,request_id));
CREATE TRIGGER soty_workspace_receipt_immutable BEFORE UPDATE ON soty_workspace_receipts BEGIN SELECT RAISE(ABORT,'soty_workspace_receipt_immutable');END;`;
const DOCUMENT_DDL='CREATE TABLE soty_workspace_documents(principal_id TEXT NOT NULL REFERENCES soty_workspace_people(id),collection TEXT NOT NULL,item_id TEXT NOT NULL,value_json TEXT NOT NULL,PRIMARY KEY(principal_id,collection,item_id))';
const columns={soty_workspace_meta:'id,schema,namespace,personal',soty_workspace_people:'id,issuer,subject,incarnation,active,generation,revision',
  soty_workspace_sessions:'id_hash,principal_id,resource_id,native_hash,active,expires_at,cipher,key_id',soty_workspace_nonces:'nonce,expires_at',
  soty_workspace_receipts:'principal_id,request_id,input_digest,result_json,binding_json'};
const denied=condition=>check(condition,'source_workspace_native_denied',403);
// Reserve room for the domain result and the Source response envelope.
const DOCUMENT_PAGE_BYTES=32768,DOCUMENT_PAGE_NODES=3000,DOCUMENT_PAGE_ITEMS=100;
function jsonNodes(value){
  if(Array.isArray(value))return 1+value.reduce((sum,item)=>sum+jsonNodes(item),0);
  if(value&&typeof value==='object')return 1+Object.values(value).reduce((sum,item)=>sum+1+jsonNodes(item),0);
  return 1;
}

/** New per-person SQLite applications. Existing databases/identity models use
 * the maintained Native/storage ports or an explicit adapter, never a shadow
 * import. Domain callbacks are synchronous code on this SAME transaction/db. */
export function createSqliteRootWorkspaceHost(options) {
  const value=fields(options,['profile','databasePath','cipherKey','keyId','domain'],['initialize','personalWorkspaces','clock','title']);
  const profile=selectedResourceProfile(value.profile),clock=value.clock??Date.now,title=value.title??'Моё';
  check(isRootSignedWorkspaceSource(profile.sourceProfile) && typeof clock==='function' && typeof value.databasePath==='string'&&isAbsolute(value.databasePath)
    && (value.initialize===undefined||typeof value.initialize==='boolean') && (value.personalWorkspaces===undefined||typeof value.personalWorkspaces==='boolean'),
    'source_workspace_host_configuration_invalid',503);
  check(typeof title==='string' && title.isWellFormed() && title.trim() && title.length<=240 && !/[\u0000-\u001f\u007f]/u.test(title),'source_workspace_host_configuration_invalid',503);
  const domain=fields(value.domain,['read'],['initialize','inspect','execute','personalDocuments']);
  const personalDocuments=domain.personalDocuments===true;
  check(domain.personalDocuments===undefined||personalDocuments,'source_workspace_domain_invalid',503);
  check(!personalDocuments||(!domain.initialize&&!domain.inspect),'source_workspace_domain_invalid',503);
  Object.entries(domain).filter(([name])=>name!=='personalDocuments').forEach(([,fn])=>check(typeof fn==='function' && !['AsyncFunction','AsyncGeneratorFunction'].includes(fn.constructor?.name),'source_workspace_domain_invalid',503));
  if(!existsSync(value.databasePath)){
    check(value.initialize===true,'source_workspace_host_not_initialized',503);
    const file=openSync(value.databasePath,'wx',0o600);closeSync(file);
  }
  const databaseFile=lstatSync(value.databasePath);
  check(databaseFile.isFile()&&!databaseFile.isSymbolicLink()&&(process.platform!=='linux'||databaseFile.uid===process.getuid()
    &&(databaseFile.mode&0o777)===0o600),'source_workspace_host_file_denied',503);
  const namespace=digest({schema:SCHEMA,appId:profile.appId,issuer:profile.issuer,resource:profile.resource});
  const cipher=createSourceSessionCipher({key:value.cipherKey,keyId:value.keyId,namespace});
  let db;
  try{db=new DatabaseSync(value.databasePath);}catch(error){cipher.close();throw error;}
  const proofs=new WeakMap();let closed=false,inTransaction=false;
  const current=()=>check(!closed,'source_workspace_host_closed',503);
  function tx(action){current();check(!inTransaction,'source_workspace_host_nested_transaction',503);inTransaction=true;
    try{db.exec('BEGIN IMMEDIATE');const result=syncResult(action());check(db.isTransaction,'source_workspace_domain_transaction_lost',503);db.exec('COMMIT');return result;}
    catch(error){if(db.isTransaction)db.exec('ROLLBACK');throw error;}finally{inTransaction=false;}}
  const read=action=>inTransaction?action():tx(action);
  function verifySchema(){
    const meta=db.prepare('SELECT * FROM soty_workspace_meta').all();
    check(meta.length===1 && meta[0].schema===SCHEMA && meta[0].namespace===namespace && [0,1].includes(meta[0].personal),'source_workspace_host_schema_mismatch',503);
    for(const [table,names]of Object.entries(columns))check(db.prepare(`PRAGMA table_info(${table})`).all().map(row=>row.name).join(',')===names,'source_workspace_host_schema_mismatch',503);
    check(db.prepare('PRAGMA foreign_key_check').all().length===0,'source_workspace_host_corrupt',503);
    check(db.prepare("SELECT sql FROM sqlite_master WHERE type='trigger' AND name='soty_workspace_receipt_immutable'").get()?.sql
      ==="CREATE TRIGGER soty_workspace_receipt_immutable BEFORE UPDATE ON soty_workspace_receipts BEGIN SELECT RAISE(ABORT,'soty_workspace_receipt_immutable');END",'source_workspace_host_schema_mismatch',503);
    if(personalDocuments)check(db.prepare("SELECT sql FROM sqlite_master WHERE type='table' AND name='soty_workspace_documents'").get()?.sql===DOCUMENT_DDL,
      'source_workspace_host_schema_mismatch',503);
    syncResult(domain.inspect?.(db));
  }
  try{db.exec('PRAGMA foreign_keys=ON;PRAGMA busy_timeout=5000');
    const present=db.prepare("SELECT name FROM sqlite_master WHERE type='table' AND name NOT LIKE 'sqlite_%'").all();
    if(!present.length){check(value.initialize===true,'source_workspace_host_not_initialized',503);tx(()=>{db.exec(DDL);if(personalDocuments)db.exec(DOCUMENT_DDL);db.prepare('INSERT INTO soty_workspace_meta VALUES(1,?,?,?)').run(SCHEMA,namespace,value.personalWorkspaces===true?1:0);syncResult(domain.initialize?.(db));verifySchema();});}
    else verifySchema();
    const sample=db.prepare('SELECT * FROM soty_workspace_sessions LIMIT 1').get();if(sample)openSession(sample);
    db.exec('PRAGMA journal_mode=WAL');
  }catch(error){try{db.close();}finally{cipher.close();}throw error;}
  const personId=identity=>'person-'+digest([profile.issuer,identity.subject]);
  const resourceId=id=>'personal:'+id;
  const sourceHash=(binding,id)=>digest([namespace,id,binding.rootPrincipal,binding.rootReferenceDigest,binding.instanceId??'main']);
  function verifyRoot(binding,verified){
    const context=readVerifiedRootContext(verified,profile.digest);
    denied(binding.identity.issuer===profile.issuer && binding.identity.subject===context.humanPrincipal.subject
      && digest(binding.rootPrincipal)===digest(context.rootPrincipal) && digest(binding.resource)===digest(profile.resource)
      && binding.rootReferenceDigest===digest(context.reference) && binding.instanceId===context.instanceId && binding.rootExpiresAt===context.expiresAt
      && binding.rootExpiresAt>clock());return context;
  }
  function person(binding){return db.prepare('SELECT * FROM soty_workspace_people WHERE issuer=? AND subject=?').get(binding.identity.issuer,binding.identity.subject);}
  function inspect(proof,binding,{pending=false}={}){
    current();const held=proofs.get(proof);denied(held && held.bindingDigest===digest(binding));verifyRoot(binding,held.verified);
    const row=person(binding);
    if(!row){denied(pending && held.pending && binding.operation==='root-session' && value.personalWorkspaces===true);return{held,row:null};}
    denied(row.id===held.id && row.active===1 && row.generation===held.generation && (!held.incarnation||row.incarnation===held.incarnation));
    if(binding.operation!=='root-session'){
      const session=db.prepare('SELECT * FROM soty_workspace_sessions WHERE id_hash=?').get(binding.sessionIdHash);
      denied(session && session.active===1 && session.expires_at>clock() && session.principal_id===row.id
        && session.resource_id===resourceId(row.id) && session.native_hash===sourceHash(binding,row.id));
    }
    return{held,row};
  }
  function context(proof,binding){const{row}=inspect(proof,binding);denied(row);
    return{key:digest([namespace,row.id,row.incarnation]),generation:row.generation,title,role:'owner',revision:row.revision,expiresAt:binding.rootExpiresAt};}
  function nativeContext(proof,binding){const{row}=inspect(proof,binding);denied(row);
    return Object.freeze({database:db,principalId:row.id,resourceId:resourceId(row.id)});}
  function callDomain(method,actor,args){
    if(!personalDocuments)return syncResult(domain[method](actor,args));
    let active=true;
    const collection=value=>check(typeof value==='string'&&/^[a-zA-Z0-9_.-]{1,64}$/u.test(value),'source_workspace_collection_invalid');
    const itemId=value=>check(typeof value==='string'&&/^[a-zA-Z0-9_.:-]{1,128}$/u.test(value),'source_workspace_item_invalid');
    const access=()=>check(active&&inTransaction,'source_workspace_personal_context_closed',503);
    const pageDigest=(name,id)=>digest(['soty.personal-page.v1',namespace,actor.principalId,name,id]);
    function list(name,cursor=null){
      access();collection(name);let anchor=null;
      if(cursor!==null){
        const match=typeof cursor==='string'?/^([a-zA-Z0-9_.:-]{1,128})\.([a-f0-9]{64})$/u.exec(cursor):null;
        check(match&&pageDigest(name,match[1])===match[2]&&db.prepare('SELECT 1 FROM soty_workspace_documents WHERE principal_id=? AND collection=? AND item_id=?')
          .get(actor.principalId,name,match[1]),'source_workspace_cursor_invalid');
        anchor=match[1];
      }
      const where=anchor===null?'':' AND rowid<(SELECT rowid FROM soty_workspace_documents WHERE principal_id=? AND collection=? AND item_id=?)';
      const rows=db.prepare('SELECT item_id,value_json FROM soty_workspace_documents WHERE principal_id=? AND collection=?'+where+' ORDER BY rowid DESC LIMIT 101')
        .iterate(...(anchor===null?[actor.principalId,name]:[actor.principalId,name,actor.principalId,name,anchor]));
      const items=[];let bytes=2,nodes=1,last=null,more=false;
      for(const row of rows){
        const item={id:row.item_id,value:JSON.parse(row.value_json)},itemBytes=Buffer.byteLength(JSON.stringify(item))+1,itemNodes=jsonNodes(item);
        if(items.length===DOCUMENT_PAGE_ITEMS||bytes+itemBytes>DOCUMENT_PAGE_BYTES||nodes+itemNodes>DOCUMENT_PAGE_NODES){more=true;break;}
        items.push(item);bytes+=itemBytes;nodes+=itemNodes;last=row.item_id;
      }
      check(!more||last!==null,'source_workspace_page_item_too_large',503);
      const nextCursor=more?`${last}.${pageDigest(name,last)}`:null;
      return{items,nextCursor};
    }
    const personal=Object.freeze({
      list,
      create(name,id,value){access();check(method==='execute','source_workspace_personal_read_only',403);collection(name);itemId(id);
        const document=jsonCopy(value,{bytes:16384,nodes:1200,depth:12});db.prepare('INSERT INTO soty_workspace_documents VALUES(?,?,?,?)').run(actor.principalId,name,id,JSON.stringify(document));},
    });
    try{return syncResult(domain[method](Object.freeze({personal,resourceId:actor.resourceId}),args));}finally{active=false;}
  }
  function openSession(row){
    const record=cipher.open(row.id_hash,row.cipher,row.key_id),sealed=fields(record,['payload','native']);
    check(sealed.payload.idHash===row.id_hash && sealed.payload.expiresAt===row.expires_at && sealed.native.principalId===row.principal_id
      && sealed.native.resourceId===row.resource_id && sealed.native.nativeSessionHash===row.native_hash,'source_session_storage_corrupt',503);
    return{...sealed.payload,active:row.active===1};
  }
  const unsupported=()=>check(false,'source_workspace_root_only',404);
  const storage=Object.freeze({
    consumeNonce(token,expiresAt){return tx(()=>{check(typeof token==='string' && token.length<=128 && Number.isSafeInteger(expiresAt) && expiresAt>clock(),'source_workspace_nonce_invalid',403);
      db.prepare('DELETE FROM soty_workspace_nonces WHERE expires_at<=?').run(clock());
      if(db.prepare('SELECT 1 FROM soty_workspace_nonces WHERE nonce=?').get(token))return false;
      check(db.prepare('SELECT count(*) n FROM soty_workspace_nonces').get().n<SQLITE_ROOT_WORKSPACE_LIMITS.nonces,'source_workspace_nonce_capacity',503);
      db.prepare('INSERT INTO soty_workspace_nonces VALUES(?,?)').run(token,expiresAt);return true;});},
    createRootSession(record,final){return tx(()=>{
      check(record.rootWitness===true && record.expiresAt>clock() && record.expiresAt<=clock()+300000,'source_root_session_invalid',403);
      db.prepare('DELETE FROM soty_workspace_sessions WHERE expires_at<=?').run(clock());const native=syncResult(final());
      const prior=db.prepare('SELECT * FROM soty_workspace_sessions WHERE native_hash=? AND active=1 AND expires_at>? LIMIT 2').all(native.nativeSessionHash,clock());
      check(prior.length<=1,'source_root_session_conflict',409);
      if(prior.length){const old=openSession(prior[0]);check(old.rootReferenceDigest===record.rootReferenceDigest && old.semanticDigest===record.semanticDigest
        && old.instanceId===record.instanceId && digest(old.identity)===digest(record.identity) && digest(old.rootPrincipal)===digest(record.rootPrincipal),'source_root_session_conflict',409);return{idHash:old.idHash};}
      check(db.prepare('SELECT count(*) n FROM soty_workspace_sessions').get().n<SQLITE_ROOT_WORKSPACE_LIMITS.sessions,'source_workspace_session_capacity',503);
      const encrypted=cipher.seal(record.idHash,{payload:record,native});
      db.prepare('INSERT INTO soty_workspace_sessions VALUES(?,?,?,?,1,?,?,?)').run(record.idHash,native.principalId,native.resourceId,native.nativeSessionHash,record.expiresAt,encrypted,value.keyId);
      return{idHash:record.idHash};});},
    readSession(hash){current();const row=db.prepare('SELECT * FROM soty_workspace_sessions WHERE id_hash=?').get(hash);return row?openSession(row):null;},
    revokeSession(hash){tx(()=>db.prepare('UPDATE soty_workspace_sessions SET active=0 WHERE id_hash=?').run(hash));},
    ...Object.fromEntries(['createInteraction','getInteraction','claimInteraction','claimCallback','completeInteraction','readCompletion','consumeCompletion','readTokenProof'].map(name=>[name,unsupported])),
  });
  const native=createSourceNativeAuthorityPort({capture:unsupported,
    captureRoot(binding,verified){verifyRoot(binding,verified);return tx(()=>{
      const row=person(binding),id=personId(binding.identity);
      denied(row?row.active===1 && row.id===id:binding.operation==='root-session' && value.personalWorkspaces===true);
      const proof=Object.freeze({});proofs.set(proof,{verified,id,bindingDigest:digest(binding),generation:row?.generation??1,incarnation:row?.incarnation??null,pending:!row});
      inspect(proof,binding,{pending:true});return proof;});},
    withCurrent(proof,binding,action){return read(()=>{inspect(proof,binding,{pending:true});return syncResult(action());});},
    commitRootSession(proof,binding){check(inTransaction,'source_workspace_native_transaction_required',503);const{held,row}=inspect(proof,binding,{pending:true});
      denied(binding.operation==='root-session');if(!row){const incarnation=nonce();db.prepare('INSERT INTO soty_workspace_people VALUES(?,?,?,?,1,1,1)').run(held.id,binding.identity.issuer,binding.identity.subject,incarnation);held.incarnation=incarnation;}
      else held.incarnation=row.incarnation;
      held.pending=false;
      return{nativeSessionHash:sourceHash(binding,held.id),principalId:held.id,resourceId:resourceId(held.id)};},
    currentContext(proof,binding){return read(()=>context(proof,binding));},
    read(proof,binding,args){return tx(()=>{denied(binding.operation==='read');
      const data=jsonCopy(callDomain('read',nativeContext(proof,binding),args));
      fields(data,[],Object.keys(data??{}));check(!Object.hasOwn(data,'requestId'),'source_workspace_domain_invalid',503);
      return{requestId:args.requestId,...data};});},
    ...(domain.execute?{execute(proof,binding,args,final){check(isSourceNativeCommitPort(final) && typeof final.receiptBinding==='function','source_workspace_commit_unsupported',503);
      return tx(()=>{denied(binding.operation==='execute');const actor=nativeContext(proof,binding),inputDigest=digest(args.input);
        const prior=db.prepare('SELECT * FROM soty_workspace_receipts WHERE principal_id=? AND request_id=?').get(actor.principalId,args.requestId);
        check(!prior||prior.input_digest===inputDigest,'source_workspace_request_conflict',409);
        check(!prior,'source_workspace_request_recorded',409);
        check(db.prepare('SELECT count(*) n FROM soty_workspace_receipts WHERE principal_id=?').get(actor.principalId).n<SQLITE_ROOT_WORKSPACE_LIMITS.receiptsPerPerson,'source_workspace_receipt_capacity',503);
        return final.commit(()=>{const data=fields(callDomain('execute',actor,args),['workspaceReceipt'],['result','data']);
          db.prepare('UPDATE soty_workspace_people SET revision=revision+1 WHERE id=?').run(actor.principalId);
          const contextAfter=final.contextAfter();
          const original={requestId:args.requestId,inputDigest,outcome:'committed',replayed:false,
            receipt:{id:args.requestId,revision:contextAfter.revision},...jsonCopy(data),contextAfter},binding=final.receiptBinding(original);
          db.prepare('INSERT INTO soty_workspace_receipts VALUES(?,?,?,?,?)').run(actor.principalId,args.requestId,inputDigest,JSON.stringify(original),JSON.stringify(binding));return original;});});}}:{}),
    readProof(proof,binding,args,recovery){return tx(()=>{denied(binding.operation==='readProof');const actor=nativeContext(proof,binding),input=fields(args.input,['inputDigest']);
      const row=db.prepare('SELECT * FROM soty_workspace_receipts WHERE principal_id=? AND request_id=?').get(actor.principalId,args.requestId);
      check(!row||row.input_digest===input.inputDigest,'source_workspace_request_conflict',409);
      if(!row)return{requestId:args.requestId,inputDigest:input.inputDigest,outcome:'not_applied'};
      check(isSourceNativeReceiptRecoveryPort(recovery),'source_workspace_recovery_unsupported',503);return recovery.recover(JSON.parse(row.result_json),JSON.parse(row.binding_json));});},
  });
  return Object.freeze({native,storage,profile,
    inspect(){current();verifySchema();const sample=db.prepare('SELECT * FROM soty_workspace_sessions LIMIT 1').get();if(sample)openSession(sample);
      return{schema:SCHEMA,appId:profile.appId,initialized:true,personalWorkspaces:value.personalWorkspaces===true,keyCorrectness:sample?'sample_verified':'unproved_empty',connected:false};},
    setIdentityAccess(identity,active){fields(identity,['issuer','subject']);check(identity.issuer===profile.issuer && typeof identity.subject==='string' && typeof active==='boolean','source_workspace_control_invalid',403);
      return tx(()=>{const row=person({identity});check(row,'source_workspace_person_missing',404);db.prepare('UPDATE soty_workspace_people SET active=?,generation=generation+1 WHERE id=?').run(active?1:0,row.id);});},
    close(){if(!closed){closed=true;try{db.close();}finally{cipher.close();}}},
  });
}
