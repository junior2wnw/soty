import { createHash, randomBytes } from 'node:crypto';
import { coverPrefix, assertCoverSchema } from './schema.mjs';
import { COVER_RECIPES, normalizedCoverCrop } from './codec.mjs';

export class CoverError extends Error { constructor(code,status=400) { super(code); this.code=code; this.status=status; } }
const check = (value,code,status) => { if (!value) throw new CoverError(code,status); };
const hash = bytes => createHash('sha256').update(bytes).digest('hex');
const id = (prefix) => `${prefix}_${randomBytes(16).toString('hex')}`;
const safeId = value => typeof value === 'string' && /^[A-Za-z0-9_-]{3,180}$/u.test(value);
const integer = value => Number.isSafeInteger(value) && value >= 0;
const stable = value => JSON.stringify(value && typeof value==='object' && !Array.isArray(value)
  ? Object.fromEntries(Object.keys(value).sort().map(key=>[key,JSON.parse(stable(value[key]))]))
  : Array.isArray(value) ? value.map(item=>JSON.parse(stable(item))) : value);
const exact = (value,required,optional=[]) => check(value && typeof value==='object' && !Array.isArray(value)
  && required.every(key=>Object.hasOwn(value,key)) && Object.keys(value).every(key=>required.includes(key)||optional.includes(key)), 'cover_arguments_invalid');
const requestShape=args=>{check(typeof args.requestId==='string'&&/^cover1\.[0-9]{13}\.[a-f0-9]{32}$/u.test(args.requestId),'cover_request_invalid');check(safeId(args.targetId),'cover_target_invalid');};
const expectedShape=args=>check(integer(args.expectedTargetRevision)&&integer(args.expectedPolicyEpoch)&&integer(args.expectedCoverRevision),'cover_revision_invalid');
export const COVER_LIMITS = Object.freeze({ ownerBytes:128*1024*1024,globalBytes:2*1024*1024*1024,ownerDrafts:4,targetDrafts:2,globalDrafts:64,draftMs:30*60_000,receiptMs:30*86400_000,requestMs:86400_000,futureMs:120_000,ownerReceipts:4096,globalReceipts:262144,historyVersions:3,historyMs:30*86400_000,undoMs:86400_000,mutationsPerMinute:12,gcRows:32,gcBytes:4*1024*1024 });

/** All ports are host-only. withAuthority is synchronous and establishes fresh
 * Connect -> World -> Apps authority; readTarget/onCommit use this same DB.
 * No schema creation, provider invocation, path, URL or actor is accepted in RPC args. */
export function createCoverStore({ db,kind,instanceId,withAuthority,readTarget,onCommit=()=>{},projectOwner,decoder,clock=Date.now,limits: overrides={} }) {
  const p=coverPrefix(kind); assertCoverSchema(db,kind);
  check(Number(db.prepare('PRAGMA foreign_keys').get().foreign_keys)===1,'cover_foreign_keys_required');
  check(safeId(instanceId) && typeof withAuthority==='function' && typeof readTarget==='function' && typeof onCommit==='function' && decoder && typeof decoder.decode==='function','cover_configuration_invalid');
  check([withAuthority,readTarget,onCommit,...(projectOwner?[projectOwner]:[])].every(fn=>typeof fn==='function'&&!['AsyncFunction','AsyncGeneratorFunction'].includes(fn.constructor?.name)),'cover_configuration_invalid');
  check(kind!=='profile'||typeof projectOwner==='function','cover_profile_adapter_invalid');
  const limits={...COVER_LIMITS,...(kind==='profile'?{ownerBytes:2*1024*1024,globalBytes:256*1024*1024}:{}),...overrides};
  check(Object.keys(overrides).every(key=>Object.hasOwn(COVER_LIMITS,key)) && Object.values(limits).every(value=>Number.isSafeInteger(value)&&value>0),'cover_limits_invalid');
  check(limits.receiptMs>=limits.requestMs+limits.futureMs&&limits.draftMs<=limits.requestMs&&limits.undoMs>=86400_000&&limits.historyMs>=limits.undoMs,'cover_limits_invalid');
  let disposed=false;
  const statements=new Map();const statement=sql=>{let value=statements.get(sql);if(!value){value=db.prepare(sql);statements.set(sql,value);}return value;};
  const q=(sql,...args)=>statement(sql).get(...args), all=(sql,...args)=>statement(sql).all(...args), run=(sql,...args)=>statement(sql).run(...args);
  const table = name=>`${p}_${name}`;
  check(q(`SELECT store_id FROM ${table('usage')} WHERE id=1`).store_id===instanceId,'cover_store_mismatch');
  const capture=actor=>{check(actor&&safeId(actor.accountId)&&safeId(actor.deviceId),'cover_authentication_required',401);return {actor,owner:actor.accountId,device:actor.deviceId};};
  const clockValue=()=>{const now=clock();check(integer(now),'cover_clock_invalid');return now;};
  const nowValue=()=>Math.max(clockValue(),q(`SELECT logical_now FROM ${table('usage')} WHERE id=1`).logical_now);
  function time() { const now=nowValue();run(`UPDATE ${table('usage')} SET logical_now=?,minimum_request_time=max(minimum_request_time,?) WHERE id=1`,now,Math.max(0,now-limits.requestMs));return now; }
  function transaction(write,operation) {
    check(!disposed,'cover_store_closed');check(!db.isTransaction,'cover_nested_transaction_forbidden');
    db.exec(write?'BEGIN IMMEDIATE':'BEGIN');try{const value=operation(write?time():nowValue());check(!value||typeof value.then!=='function','cover_authority_async');db.exec('COMMIT');return value;}catch(error){if(db.isTransaction)db.exec('ROLLBACK');throw error;}
  }
  function fresh(captured,write,operation) {
    check(captured.actor.accountId===captured.owner&&captured.actor.deviceId===captured.device,'cover_actor_changed',409);
    let entered=false;const value=withAuthority(captured.actor,()=>{check(!entered,'cover_authority_reentered');entered=true;return transaction(write,now=>operation(now));});
    check(entered,'cover_authority_not_entered');check(!value||typeof value.then!=='function','cover_authority_async');return value;
  }
  function target(captured,targetId,manage) {
    check(safeId(targetId),'cover_target_invalid');const value=readTarget(captured.actor,targetId);
    check(value&&typeof value.then!=='function'&&safeId(value.ownerAccountId)&&integer(value.targetRevision)&&integer(value.policyEpoch),'cover_target_unavailable',404);
    check(value.state==='active' && (manage?value.ownerAccountId===captured.owner&&value.canManage===true:value.canRead===true||value.ownerAccountId===captured.owner&&value.canManage===true),'cover_target_unavailable',404);
    if(kind==='profile')check(value.ownerAccountId===targetId,'cover_target_scope_invalid');return value;
  }
  const headRow=targetId=>q(`SELECT * FROM ${table('heads')} WHERE target_id=?`,targetId);
  function current(targetId) {
    const h=headRow(targetId);if(!h)return {revision:0,mode:'inherit',version:null,assetRef:null};
    const v=q(`SELECT * FROM ${table('versions')} WHERE id=?`,h.active_version_id);check(v&&v.target_id===targetId&&v.owner_id===h.owner_id&&v.purged_at===null,'cover_storage_corrupt',500);
    return {revision:h.revision,mode:v.mode,version:v.id,artVersion:v.version,assetRef:v.asset_id,cropApplied:true,crop:JSON.parse(v.crop_json)};
  }
  function binding(captured,args,t) {
    check(integer(args.expectedTargetRevision)&&integer(args.expectedPolicyEpoch)&&integer(args.expectedCoverRevision),'cover_revision_invalid');
    check(t.targetRevision===args.expectedTargetRevision && t.policyEpoch===args.expectedPolicyEpoch,'cover_target_changed',409);
    check((headRow(args.targetId)?.revision??0)===args.expectedCoverRevision,'cover_revision_conflict',409);
    return {store:instanceId,kind,targetId:args.targetId,ownerId:captured.owner,deviceId:captured.device,targetRevision:t.targetRevision,policyEpoch:t.policyEpoch,coverRevision:args.expectedCoverRevision};
  }
  function ownerUsage(owner) { return q(`SELECT * FROM ${table('owner_usage')} WHERE owner_id=?`,owner)??{encoded_bytes:0,reserved_bytes:0,draft_count:0,receipt_count:0,rate_at:0,rate_count:0}; }
  function addUsage(owner,{bytes=0,reserved=0,drafts=0,receipts=0}={}) {
    run(`INSERT OR IGNORE INTO ${table('owner_usage')} VALUES(?,0,0,0,0,0,0)`,owner);
    run(`UPDATE ${table('owner_usage')} SET encoded_bytes=encoded_bytes+?,reserved_bytes=reserved_bytes+?,draft_count=draft_count+?,receipt_count=receipt_count+? WHERE owner_id=?`,bytes,reserved,drafts,receipts,owner);
    run(`UPDATE ${table('usage')} SET encoded_bytes=encoded_bytes+?,reserved_bytes=reserved_bytes+?,draft_count=draft_count+?,receipt_count=receipt_count+? WHERE id=1`,bytes,reserved,drafts,receipts);
  }
  function capacity(owner,{bytes=0,draft=false,receipt=true}={}) {
    const own=ownerUsage(owner),global=q(`SELECT * FROM ${table('usage')} WHERE id=1`);
    check(own.encoded_bytes+own.reserved_bytes+bytes<=limits.ownerBytes && global.encoded_bytes+global.reserved_bytes+bytes<=limits.globalBytes,'cover_storage_quota',429);
    if(draft)check(own.draft_count<limits.ownerDrafts&&global.draft_count<limits.globalDrafts,'cover_draft_quota',429);
    if(receipt)check(own.receipt_count<limits.ownerReceipts&&global.receipt_count<limits.globalReceipts,'cover_receipt_quota',429);
  }
  function requestTime(requestId,now) {
    const match=typeof requestId==='string'&&/^cover1\.([0-9]{13})\.[a-f0-9]{32}$/u.exec(requestId);check(match,'cover_request_invalid');
    const issued=Number(match[1]),floor=q(`SELECT minimum_request_time FROM ${table('usage')} WHERE id=1`).minimum_request_time;
    check(issued>=floor&&issued>=now-limits.requestMs&&issued<=now+limits.futureMs,'cover_request_expired',409);
  }
  function rate(owner,now) {
    const own=ownerUsage(owner),same=Math.floor(own.rate_at/60_000)===Math.floor(now/60_000),count=same?own.rate_count:0;
    check(count<limits.mutationsPerMinute,'cover_rate_limited',429);addUsage(owner);run(`UPDATE ${table('owner_usage')} SET rate_at=?,rate_count=? WHERE owner_id=?`,now,count+1,owner);
  }
  const intent=(action,captured,args,extra={})=>hash(stable({store:instanceId,kind,owner:captured.owner,device:captured.device,action,...args,...extra}));
  function replay(captured,requestId,digest,targetId) {
    const receipt=q(`SELECT * FROM ${table('receipts')} WHERE owner_id=? AND request_key=?`,captured.owner,requestId);if(!receipt)return null;
    check(receipt.intent_hash===digest&&receipt.target_id===targetId,'cover_request_conflict',409);
    return {receipt:JSON.parse(receipt.value_json),current:current(targetId),replayed:true};
  }
  function saveReceipt(captured,requestId,digest,action,targetId,value,now) {
    if(value.draftId)run(`UPDATE ${table('drafts')} SET retain_until=max(retain_until,?) WHERE id=? AND owner_id=? AND target_id=?`,now+limits.receiptMs,value.draftId,captured.owner,targetId);
    if(value.versionId)run(`UPDATE ${table('versions')} SET retain_until=max(retain_until,?) WHERE id=? AND owner_id=? AND target_id=?`,now+limits.receiptMs,value.versionId,captured.owner,targetId);
    run(`INSERT INTO ${table('receipts')} VALUES(?,?,?,?,?,?,?,?)`,captured.owner,requestId,digest,action,targetId,JSON.stringify(value),now,now+limits.receiptMs);addUsage(captured.owner,{receipts:1});
    return {receipt:value,current:current(targetId),replayed:false};
  }
  function validateCanonical(canonical,origin) {
    const recipe=COVER_RECIPES[kind];check(canonical&&typeof canonical.recipe==='string'&&canonical.recipe.length<=80&&Array.isArray(canonical.renditions)&&canonical.renditions.length===recipe.length,'cover_canonical_invalid');
    for(const [index,r] of canonical.renditions.entries()) {
      const [role,width,limit]=recipe[index];check(r.role===role&&integer(r.width)&&r.width>0&&r.width<=width&&integer(r.height)&&r.height>0&&r.height<=width&&Buffer.isBuffer(r.bytes)&&r.bytes.length>0&&r.bytes.length<=limit&&hash(r.bytes)===r.sha256,'cover_canonical_invalid');
      check((origin==='legacy'?['image/png','image/jpeg','image/webp']:['image/webp']).includes(r.mime),'cover_canonical_invalid');
    }
    return canonical.renditions.reduce((sum,r)=>sum+r.bytes.length,0);
  }
  function putAsset(captured,targetId,t,canonical,origin,now) {
    const assetId=id('coverasset'),bytes=validateCanonical(canonical,origin);
    run(`INSERT INTO ${table('assets')} VALUES(?,?,?,?,?,?,?,?,?,NULL)`,assetId,targetId,captured.owner,origin,t.targetRevision,t.policyEpoch,canonical.recipe,bytes,now);
    for(const r of canonical.renditions)run(`INSERT INTO ${table('renditions')} VALUES(?,?,?,?,?,?,?,?)`,assetId,r.role,r.width,r.height,r.mime,r.sha256,r.bytes.length,r.bytes);
    return {assetId,bytes};
  }
  function version(captured,targetId,t,mode,assetId,crop,now) {
    const h=headRow(targetId),revision=(h?.revision??0)+1,number=h?.next_version??1;
    check(revision<Number.MAX_SAFE_INTEGER&&number<Number.MAX_SAFE_INTEGER,'cover_revision_exhausted');
    const versionId=id('coverversion');run(`INSERT INTO ${table('versions')} VALUES(?,?,?,?,?,?,?,?,NULL,?)`,versionId,targetId,captured.owner,number,mode,assetId,JSON.stringify(crop),now,now+limits.receiptMs);
    if(h)run(`UPDATE ${table('heads')} SET revision=?,next_version=?,active_version_id=?,previous_version_id=?,previous_replaced_at=?,updated_at=? WHERE target_id=?`,revision,number+1,versionId,h.active_version_id,now,now,targetId);
    else run(`INSERT INTO ${table('heads')} VALUES(?,?,?,?,?,NULL,NULL,?)`,targetId,captured.owner,revision,number+1,versionId,now);
    const projected=onCommit({actor:captured.actor,targetId,mode,assetId,revision,now});check(!projected||typeof projected.then!=='function','cover_authority_async');
    if(kind==='profile'){const after=readTarget(captured.actor,targetId),profile=projectOwner(captured.actor,targetId);check(after?.ownerAccountId===captured.owner&&after.targetRevision===t.targetRevision+1&&after.policyEpoch===t.policyEpoch+1&&profile?.profileId===targetId&&profile.revision===t.targetRevision+1&&profile.avatarRevision===(mode==='custom'?profile.revision:null),'cover_profile_adapter_invalid',500);}
    return {action:'committed',targetId,committedRevision:revision,versionId,assetId,mode};
  }
  const projection=(captured,targetId,result)=>{if(!projectOwner)return result;const value=projectOwner(captured.actor,targetId);check(!value||typeof value.then!=='function','cover_authority_async');return {...result,ownerProjection:value};};
  function draftView(row,now) {if(!row)return null;const state=['processing','ready'].includes(row.state)&&row.expires_at<=now?'expired':row.state;return {id:row.id,state,expiresAt:row.expires_at,assetRef:state==='ready'?row.asset_id:null};}
  function releaseDraft(row,state) {if(!['processing','ready'].includes(row.state))return;run(`UPDATE ${table('drafts')} SET state=?,reserved_bytes=0 WHERE id=?`,state,row.id);addUsage(row.owner_id,{reserved:-row.reserved_bytes,drafts:-1});}
  function failDraft(draftId) {if(disposed)return;transaction(true,()=>{const d=q(`SELECT * FROM ${table('drafts')} WHERE id=?`,draftId);if(d)releaseDraft(d,'failed');});}
  function cleanupBeforeNewRequest(captured,targetId,reserve=0,draft=false) {
    const needed=fresh(captured,false,now=>{target(captured,targetId,true);const own=ownerUsage(captured.owner),global=q(`SELECT * FROM ${table('usage')} WHERE id=1`);const pressure=own.encoded_bytes+own.reserved_bytes+reserve>limits.ownerBytes||global.encoded_bytes+global.reserved_bytes+reserve>limits.globalBytes||own.receipt_count>=limits.ownerReceipts||global.receipt_count>=limits.globalReceipts||draft&&(own.draft_count>=limits.ownerDrafts||global.draft_count>=limits.globalDrafts);return {pressure,expired:Boolean(q(`SELECT 1 FROM ${table('drafts')} WHERE state IN('processing','ready') AND expires_at<=? LIMIT 1`,now)||q(`SELECT 1 FROM ${table('receipts')} WHERE expires_at<=? LIMIT 1`,now))};});
    // A failed new allocation must not roll back successful bounded cleanup.
    if(needed.pressure||needed.expired)collect({pressure:needed.pressure});
  }

  async function prepareUpload(actor,args) {
    exact(args,['requestId','targetId','expectedTargetRevision','expectedPolicyEpoch','expectedCoverRevision','mime','bytes'],['crop']);
    args={...args};requestShape(args);expectedShape(args);
    const captured=capture(actor);check(Buffer.isBuffer(args.bytes)||args.bytes instanceof Uint8Array,'cover_input_limit');
    const bytes=Buffer.from(args.bytes);check(bytes.length>0&&bytes.length<=320*1024,'cover_input_limit');const crop=normalizedCoverCrop(args.crop);
    const digest=intent('prepare',captured,{...args,bytes:hash(bytes),crop});
    return prepare(captured,{...args},crop,digest,()=>({bytes,mime:args.mime}));
  }
  async function prepare(captured,args,crop,digest,source) {
    let input;
    cleanupBeforeNewRequest(captured,args.targetId,COVER_RECIPES[kind].reduce((sum,r)=>sum+r[2],0),true);
    const start=fresh(captured,true,now=>{
      const t=target(captured,args.targetId,true),previous=replay(captured,args.requestId,digest,args.targetId);
      if(previous){const d=q(`SELECT * FROM ${table('drafts')} WHERE id=?`,previous.receipt.draftId);return {...previous,draft:draftView(d,now)};}
      requestTime(args.requestId,now);binding(captured,args,t);input=source();const reserve=COVER_RECIPES[kind].reduce((sum,r)=>sum+r[2],0);
      capacity(captured.owner,{bytes:reserve,draft:true});check(q(`SELECT count(*) AS n FROM ${table('drafts')} WHERE target_id=? AND owner_id=? AND state IN('processing','ready') AND expires_at>?`,args.targetId,captured.owner,now).n<limits.targetDrafts,'cover_draft_quota',429);rate(captured.owner,now);
      const draftId=id('coverdraft');run(`INSERT INTO ${table('drafts')} VALUES(?,?,?,?,?,?,?,?,?,'upload','processing',NULL,?,?,?,?)`,draftId,captured.owner,captured.device,kind,args.targetId,args.requestId,t.targetRevision,t.policyEpoch,args.expectedCoverRevision,reserve,now,now+limits.draftMs,now+limits.receiptMs);
      addUsage(captured.owner,{reserved:reserve,drafts:1});const result=saveReceipt(captured,args.requestId,digest,'prepare',args.targetId,{action:'prepared',draftId},now);return {...result,draft:{id:draftId,state:'processing',expiresAt:now+limits.draftMs,assetRef:null}};
    });
    if(start.replayed)return start;
    try {
      const canonical=await decoder.decode(captured.owner,{kind,...input,crop});
      return fresh(captured,true,now=>{
        const t=target(captured,args.targetId,true),d=q(`SELECT * FROM ${table('drafts')} WHERE id=?`,start.draft.id);
        check(d&&d.owner_id===captured.owner&&d.device_id===captured.device&&d.target_kind===kind&&d.target_id===args.targetId&&d.state==='processing'&&d.expires_at>now,'cover_draft_expired',409);
        binding(captured,{targetId:args.targetId,expectedTargetRevision:d.target_revision,expectedPolicyEpoch:d.policy_epoch,expectedCoverRevision:d.cover_revision},t);
        const added=putAsset(captured,args.targetId,t,canonical,'upload',now);check(added.bytes<=d.reserved_bytes,'cover_output_limit');
        run(`UPDATE ${table('drafts')} SET state='ready',asset_id=?,reserved_bytes=0 WHERE id=?`,added.assetId,d.id);addUsage(captured.owner,{bytes:added.bytes,reserved:-d.reserved_bytes});
        return {...start,current:current(args.targetId),draft:{id:d.id,state:'ready',expiresAt:d.expires_at,assetRef:added.assetId}};
      });
    } catch(error) {try{failDraft(start.draft.id);}catch{/* Startup/maintenance settles an interrupted reservation. */}throw error;}
  }
  function commit(actor,args) {
    exact(args,['requestId','targetId','draftId']);requestShape(args);check(safeId(args.draftId),'cover_draft_unavailable',404);const captured=capture(actor),digest=intent('commit',captured,args);
    cleanupBeforeNewRequest(captured,args.targetId);
    const result=fresh(captured,true,now=>{
      const t=target(captured,args.targetId,true),old=replay(captured,args.requestId,digest,args.targetId);if(old)return projection(captured,args.targetId,old);
      requestTime(args.requestId,now);const d=q(`SELECT * FROM ${table('drafts')} WHERE id=?`,args.draftId);
      check(d&&d.owner_id===captured.owner&&d.device_id===captured.device&&d.target_kind===kind&&d.target_id===args.targetId&&d.state==='ready'&&d.expires_at>now,'cover_draft_unavailable',409);
      binding(captured,{targetId:args.targetId,expectedTargetRevision:d.target_revision,expectedPolicyEpoch:d.policy_epoch,expectedCoverRevision:d.cover_revision},t);
      const asset=q(`SELECT * FROM ${table('assets')} WHERE id=?`,d.asset_id);check(asset&&asset.target_id===args.targetId&&asset.owner_id===captured.owner&&asset.purged_at===null,'cover_asset_unavailable',409);
      capacity(captured.owner);rate(captured.owner,now);const value=version(captured,args.targetId,t,'custom',asset.id,{x:.5,y:.5,zoom:1},now);releaseDraft(d,'committed');
      return projection(captured,args.targetId,saveReceipt(captured,args.requestId,digest,'commit',args.targetId,value,now));
    });return maintainAfterCommit(result);
  }
  function change(actor,args,action) {
    exact(args,['requestId','targetId','expectedTargetRevision','expectedPolicyEpoch','expectedCoverRevision'],action==='restore'?['versionId']:['mode']);
    requestShape(args);expectedShape(args);if(action==='restore')check(safeId(args.versionId),'cover_version_expired',404);
    const captured=capture(actor),digest=intent(action,captured,args);
    cleanupBeforeNewRequest(captured,args.targetId);
    const result=fresh(captured,true,now=>{
      const t=target(captured,args.targetId,true),old=replay(captured,args.requestId,digest,args.targetId);if(old)return projection(captured,args.targetId,old);
      requestTime(args.requestId,now);binding(captured,args,t);capacity(captured.owner);rate(captured.owner,now);
      let mode=args.mode??'none',assetId=null,crop={x:.5,y:.5,zoom:1};
      if(action==='restore') {const v=q(`SELECT * FROM ${table('versions')} WHERE id=? AND target_id=? AND owner_id=?`,args.versionId,args.targetId,captured.owner);check(v&&v.purged_at===null,'cover_version_expired',409);mode=v.mode;assetId=v.asset_id;crop=JSON.parse(v.crop_json);if(assetId)check(q(`SELECT 1 FROM ${table('assets')} WHERE id=? AND purged_at IS NULL`,assetId),'cover_version_expired',409);run(`UPDATE ${table('versions')} SET retain_until=max(retain_until,?) WHERE id=?`,now+limits.receiptMs,v.id);}
      else check(['inherit','none'].includes(mode),'cover_mode_invalid');
      return projection(captured,args.targetId,saveReceipt(captured,args.requestId,digest,action,args.targetId,version(captured,args.targetId,t,mode,assetId,crop,now),now));
    });return maintainAfterCommit(result);
  }
  function read(actor,{targetId,versionId,width=160}) {
    const captured=capture(actor);return fresh(captured,false,()=>{
      const t=target(captured,targetId,false),head=current(targetId),requested=versionId??head.version;
      check(!versionId||t.canManage===true&&t.ownerAccountId===captured.owner||versionId===head.version,'cover_target_unavailable',404);
      if(!requested)return {current:head,image:null};const v=q(`SELECT * FROM ${table('versions')} WHERE id=? AND target_id=? AND owner_id=?`,requested,targetId,t.ownerAccountId);
      check(v&&v.purged_at===null,'cover_version_expired',404);if(!v.asset_id)return {current:head,image:null};
      check(Number.isSafeInteger(width)&&width>0&&width<=960,'cover_width_invalid');const rows=all(`SELECT role,width,height,mime,sha256,bytes FROM ${table('renditions')} WHERE asset_id=? ORDER BY width`,v.asset_id);
      check(rows.length,'cover_version_expired',404);const selected=kind==='profile'?rows.find(r=>r.role==='image'):(rows.find(r=>r.width>=width)??rows.at(-1));
      check(selected&&hash(selected.bytes)===selected.sha256,'cover_storage_corrupt',500);
      return {current:head,versionId:v.id,image:{...selected,bytes:Buffer.from(selected.bytes)}};
    });
  }
  function status(actor,{targetId,requestId,draftId}) {const captured=capture(actor);return fresh(captured,false,now=>{
    const t=target(captured,targetId,true);const receipt=requestId?q(`SELECT * FROM ${table('receipts')} WHERE owner_id=? AND request_key=? AND target_id=?`,captured.owner,requestId,targetId):null;
    const wanted=draftId??(receipt?JSON.parse(receipt.value_json).draftId:null),d=wanted?q(`SELECT * FROM ${table('drafts')} WHERE id=? AND owner_id=? AND target_id=?`,wanted,captured.owner,targetId):null;
    const head=current(targetId);return {current:head,scope:{storeInstanceId:instanceId,kind,targetId,ownerAccountId:t.ownerAccountId,targetRevision:t.targetRevision,policyEpoch:t.policyEpoch,coverRevision:head.revision},receipt:receipt?JSON.parse(receipt.value_json):null,draft:draftView(d,now),serverTime:now};
  });}
  function versions(actor,{targetId,limit=3}) {
    check(Number.isSafeInteger(limit)&&limit>=1&&limit<=20,'cover_page_invalid');const captured=capture(actor);
    return fresh(captured,false,()=>{target(captured,targetId,true);return {current:current(targetId),versions:all(`SELECT v.id,v.version,v.mode,v.asset_id,v.created_at,v.purged_at,a.purged_at AS asset_purged,a.origin FROM ${table('versions')} v LEFT JOIN ${table('assets')} a ON a.id=v.asset_id WHERE v.target_id=? AND v.owner_id=? ORDER BY v.version DESC LIMIT ?`,targetId,captured.owner,limit).map(v=>({id:v.id,version:v.version,mode:v.mode,createdAt:v.created_at,sourceOrigin:v.origin??null,restorable:v.purged_at===null&&(v.asset_id===null||v.asset_purged===null)}))};});
  }
  function cancel(actor,args) {
    exact(args,['requestId','targetId','draftId']);requestShape(args);check(safeId(args.draftId),'cover_draft_unavailable',404);const captured=capture(actor),digest=intent('cancel',captured,args);
    cleanupBeforeNewRequest(captured,args.targetId);
    return fresh(captured,true,now=>{target(captured,args.targetId,true);const old=replay(captured,args.requestId,digest,args.targetId);if(old)return old;requestTime(args.requestId,now);capacity(captured.owner);rate(captured.owner,now);const d=q(`SELECT * FROM ${table('drafts')} WHERE id=? AND owner_id=? AND target_id=?`,args.draftId,captured.owner,args.targetId);check(d,'cover_draft_unavailable',404);const action=['processing','ready'].includes(d.state)?'cancelled':d.state;releaseDraft(d,'cancelled');return saveReceipt(captured,args.requestId,digest,'cancel',args.targetId,{action,draftId:d.id},now);});
  }
  /** Called only by the host compatibility adapter, never directly registered as
   * an agent/RPC operation. Canonical legacy bytes have already been validated. */
  function legacyCommit(actor,{targetId,expectedRevision,canonical}) {
    check(kind==='profile','cover_legacy_kind_invalid');const captured=capture(actor);
    cleanupBeforeNewRequest(captured,targetId,canonical?validateCanonical(canonical,'legacy'):0);
    const result=fresh(captured,true,now=>{
      const t=target(captured,targetId,true);check(t.targetRevision===expectedRevision,'revision_conflict',409);
      const bytes=canonical?validateCanonical(canonical,'legacy'):0;capacity(captured.owner,{bytes});rate(captured.owner,now);
      const added=canonical?putAsset(captured,targetId,t,canonical,'legacy',now):null;if(added)addUsage(captured.owner,{bytes:added.bytes});
      const value=version(captured,targetId,t,canonical?'custom':'none',added?.assetId??null,{x:.5,y:.5,zoom:1},now);
      const requestId=`cover1.${now}.${randomBytes(16).toString('hex')}`;
      const result=projection(captured,targetId,saveReceipt(captured,requestId,intent('legacy-set',captured,{targetId,expectedRevision}),'legacy-set',targetId,value,now));
      check(result.ownerProjection?.profileId===targetId&&result.ownerProjection.revision===expectedRevision+1,'cover_profile_adapter_invalid',500);return result;
    });return maintainAfterCommit(result);
  }
  function maintainAfterCommit(result) {
    if(result.replayed)return result;
    try{collect();return result;}catch{return {...result,maintenancePending:true};}
  }
  function collect({pressure=false}={}) {return transaction(true,now=>{
    let changed=0,freed=0;
    for(const d of all(`SELECT * FROM ${table('drafts')} WHERE state IN('processing','ready') AND expires_at<=? LIMIT ?`,now,limits.gcRows)){releaseDraft(d,'expired');changed++;}
    const candidates=all(`SELECT v.* FROM ${table('versions')} v JOIN ${table('heads')} h ON h.target_id=v.target_id WHERE v.purged_at IS NULL AND v.id<>h.active_version_id AND NOT(v.id IS h.previous_version_id AND h.previous_replaced_at>?) AND (?=1 OR v.created_at<=? OR (SELECT count(*) FROM ${table('versions')} n WHERE n.target_id=v.target_id AND n.purged_at IS NULL)>?) ORDER BY v.created_at,v.version LIMIT ?`,now-limits.undoMs,pressure?1:0,now-limits.historyMs,limits.historyVersions,limits.gcRows);
    for(const v of candidates) {
      if(changed>=limits.gcRows)break;
      const h=headRow(v.target_id);
      if(h.previous_version_id===v.id&&h.previous_replaced_at>now-limits.undoMs)continue;
      const count=q(`SELECT count(*) AS n FROM ${table('versions')} WHERE target_id=? AND purged_at IS NULL`,v.target_id).n;
      if(!pressure&&count<=limits.historyVersions&&v.created_at>now-limits.historyMs)continue;
      run(`UPDATE ${table('versions')} SET purged_at=? WHERE id=?`,now,v.id);changed++;
    }
    for(const a of all(`SELECT a.* FROM ${table('assets')} a WHERE a.purged_at IS NULL AND NOT EXISTS(SELECT 1 FROM ${table('versions')} v WHERE v.asset_id=a.id AND v.purged_at IS NULL) AND NOT EXISTS(SELECT 1 FROM ${table('drafts')} d WHERE d.asset_id=a.id AND d.state IN('processing','ready') AND d.expires_at>?) ORDER BY a.created_at LIMIT ?`,now,limits.gcRows)) {
      if(changed>=limits.gcRows||freed+a.encoded_bytes>limits.gcBytes)break;
      run(`DELETE FROM ${table('renditions')} WHERE asset_id=?`,a.id);run(`UPDATE ${table('assets')} SET purged_at=? WHERE id=?`,now,a.id);addUsage(a.owner_id,{bytes:-a.encoded_bytes});freed+=a.encoded_bytes;changed++;
    }
    for(const r of all(`SELECT owner_id,request_key FROM ${table('receipts')} WHERE expires_at<=? LIMIT ?`,now,Math.max(0,limits.gcRows-changed))){run(`DELETE FROM ${table('receipts')} WHERE owner_id=? AND request_key=?`,r.owner_id,r.request_key);addUsage(r.owner_id,{receipts:-1});changed++;}
    for(const d of all(`SELECT id FROM ${table('drafts')} WHERE state NOT IN('processing','ready') AND retain_until<=? LIMIT ?`,now,Math.max(0,limits.gcRows-changed))){run(`DELETE FROM ${table('drafts')} WHERE id=?`,d.id);changed++;}
    for(const v of all(`SELECT id FROM ${table('versions')} WHERE purged_at IS NOT NULL AND retain_until<=? LIMIT ?`,now,Math.max(0,limits.gcRows-changed))){run(`DELETE FROM ${table('versions')} WHERE id=?`,v.id);changed++;}
    for(const a of all(`SELECT id FROM ${table('assets')} a WHERE purged_at IS NOT NULL AND NOT EXISTS(SELECT 1 FROM ${table('versions')} WHERE asset_id=a.id) AND NOT EXISTS(SELECT 1 FROM ${table('drafts')} WHERE asset_id=a.id) LIMIT ?`,Math.max(0,limits.gcRows-changed))){run(`DELETE FROM ${table('assets')} WHERE id=?`,a.id);changed++;}
    return {changed,freedBytes:freed};
  });}
  function audit() {return transaction(false,()=>{
    check(!q(`SELECT 1 FROM ${table('heads')} h LEFT JOIN ${table('versions')} v ON v.id=h.active_version_id WHERE v.id IS NULL OR v.target_id<>h.target_id OR v.owner_id<>h.owner_id OR v.purged_at IS NOT NULL OR h.revision<1 OR v.version<>h.next_version-1 OR h.revision<>v.version LIMIT 1`),'cover_storage_corrupt',500);
    check(!q(`SELECT 1 FROM ${table('heads')} h LEFT JOIN ${table('versions')} v ON v.id=h.previous_version_id WHERE h.previous_version_id IS NOT NULL AND ((v.id IS NOT NULL AND (v.target_id<>h.target_id OR v.owner_id<>h.owner_id OR v.version<>h.next_version-2)) OR (h.previous_replaced_at>? AND (v.id IS NULL OR v.purged_at IS NOT NULL))) LIMIT 1`,nowValue()-limits.undoMs),'cover_storage_corrupt',500);
    check(!q(`SELECT 1 FROM ${table('assets')} a LEFT JOIN(SELECT asset_id,sum(byte_count) AS n FROM ${table('renditions')} GROUP BY asset_id) r ON r.asset_id=a.id WHERE a.purged_at IS NULL AND a.encoded_bytes<>coalesce(r.n,0) LIMIT 1`),'cover_storage_corrupt',500);
    const rows=all(`SELECT * FROM ${table('owner_usage')}`);for(const own of rows){
      const values={bytes:q(`SELECT coalesce(sum(encoded_bytes),0) AS n FROM ${table('assets')} WHERE owner_id=? AND purged_at IS NULL`,own.owner_id).n,reserved:q(`SELECT coalesce(sum(reserved_bytes),0) AS n FROM ${table('drafts')} WHERE owner_id=? AND state IN('processing','ready')`,own.owner_id).n,drafts:q(`SELECT count(*) AS n FROM ${table('drafts')} WHERE owner_id=? AND state IN('processing','ready')`,own.owner_id).n,receipts:q(`SELECT count(*) AS n FROM ${table('receipts')} WHERE owner_id=?`,own.owner_id).n};
      check(values.bytes===own.encoded_bytes&&values.reserved===own.reserved_bytes&&values.drafts===own.draft_count&&values.receipts===own.receipt_count,'cover_usage_mismatch',500);
    }
    const global=q(`SELECT * FROM ${table('usage')} WHERE id=1`);for(const key of ['encoded_bytes','reserved_bytes','draft_count','receipt_count'])check(rows.reduce((sum,row)=>sum+row[key],0)===global[key],'cover_usage_mismatch',500);
    check(q(`SELECT coalesce(sum(encoded_bytes),0) AS n FROM ${table('assets')} WHERE purged_at IS NULL`).n===global.encoded_bytes
      &&q(`SELECT coalesce(sum(reserved_bytes),0) AS n FROM ${table('drafts')} WHERE state IN('processing','ready')`).n===global.reserved_bytes
      &&q(`SELECT count(*) AS n FROM ${table('drafts')} WHERE state IN('processing','ready')`).n===global.draft_count
      &&q(`SELECT count(*) AS n FROM ${table('receipts')}`).n===global.receipt_count,'cover_usage_mismatch',500);
    return {ok:true,usage:{encodedBytes:global.encoded_bytes,reservedBytes:global.reserved_bytes,drafts:global.draft_count,receipts:global.receipt_count}};
  });}
  async function prepareFromVersion(actor,args) {
    exact(args,['requestId','targetId','expectedTargetRevision','expectedPolicyEpoch','expectedCoverRevision','versionId'],['crop']);requestShape(args);expectedShape(args);check(safeId(args.versionId),'cover_version_expired',404);
    args={...args};const captured=capture(actor),crop=normalizedCoverCrop(args.crop),digest=intent('prepare-from-version',captured,{...args,crop});
    return prepare(captured,args,crop,digest,()=>{const v=q(`SELECT * FROM ${table('versions')} WHERE id=? AND target_id=? AND owner_id=? AND purged_at IS NULL`,args.versionId,args.targetId,captured.owner);check(v?.asset_id,'cover_version_expired',404);const r=kind==='profile'?q(`SELECT mime,bytes FROM ${table('renditions')} WHERE asset_id=? AND role='image'`,v.asset_id):q(`SELECT mime,bytes FROM ${table('renditions')} WHERE asset_id=? ORDER BY width DESC LIMIT 1`,v.asset_id);check(r,'cover_version_expired',404);return {mime:r.mime,bytes:Buffer.from(r.bytes)};});
  }
  audit();
  return Object.freeze({prepareUpload,prepareFromVersion,commit,restore:(actor,args)=>change(actor,args,'restore'),remove:(actor,args)=>change(actor,args,'remove'),read,status,versions,cancel,legacyCommit,collect,audit,
    generationAvailability:()=>({available:false,reason:'cover_generation_provider_unavailable'}),dispose(){disposed=true;statements.clear();}});
}
