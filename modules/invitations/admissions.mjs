import {createHash} from 'node:crypto';
import {check,identity} from './model.mjs';

/** Claims commit in Invitations before another authority can admit a person. */
export function createInvitationAdmissions({db,clock,admitTarget,readTargetAdmission,onAdmission=()=>{}}){
 const get=(sql,...args)=>db.prepare(sql).get(...args),run=(sql,...args)=>db.prepare(sql).run(...args);
 const project=row=>{
  if(!row)return null;
  const request=get('SELECT state,created_at FROM requests WHERE id=?',row.request_id),link=get('SELECT revoked FROM links WHERE id=(SELECT link_id FROM requests WHERE id=?)',row.request_id);
  const policy=get('SELECT body FROM policies WHERE app_id=? AND owner_id=?',row.target_id,row.owner_id);
  return {claimId:row.claim_id,target:{kind:row.target_kind,id:row.target_id},ownerAccountId:row.owner_id,issuerAccountId:row.issuer_id,
   recipientAccountId:row.recipient_id,recipientDeviceId:row.recipient_device_id,recipientAccountLabel:row.recipient_account_label,
   recipientLabel:row.recipient_label,requestId:row.request_id,decision:request?.state,linkRevoked:!link||Boolean(link.revoked)||!policy||JSON.parse(policy.body).enabled!==true,createdAt:request?.created_at,state:row.state,errorCode:row.error_code};
 };
 const readClaim=claimId=>{identity(claimId);return project(get('SELECT * FROM admissions WHERE claim_id=?',claimId));};
 function stage(target,requestId,actor){
  check(db.isTransaction,'invitation_claim_transaction_required',500);
  const request=get('SELECT * FROM requests WHERE id=?',requestId),link=get('SELECT * FROM links WHERE id=?',request.link_id),assignment=get('SELECT * FROM link_assignments WHERE link_id=?',link.id);
  check(request.state==='approved'&&request.app_id===target.appId&&request.owner_id===target.ownerId,'invitation_claim_invalid',409);
  const claimId='clm_'+createHash('sha256').update(requestId).digest('hex');
  const subject=get('SELECT * FROM request_subjects WHERE request_id=?',requestId);
  run(`INSERT OR IGNORE INTO admissions VALUES(?,?,?,?,?,?,?,?,?,?,'pending',NULL,?)`,claimId,requestId,target.appId,target.kind??'app',target.ownerId,
   link.mode==='direct'?link.issued_by:request.decided_by,request.account_id,subject?.device_id??actor.deviceId,subject?.account_label??actor.label??'',assignment?.recipient_label??'',clock());
  return claimId;
 }
 function readAdmission(claimId){
  const claim=readClaim(claimId);if(!claim)return null;
  let actual;
  if(claim.target.kind==='app')actual=get('SELECT active FROM members WHERE app_id=? AND owner_id=? AND account_id=?',claim.target.id,claim.ownerAccountId,claim.recipientAccountId)?.active===1?'admitted':'removed';
  else actual=readTargetAdmission?.({claim})?.state??'pending';
  return {...claim,state:actual==='pending'&&claim.state==='blocked'?'blocked':actual};
 }
 function complete(claimId,actor){
  const claim=readClaim(claimId);check(claim,'invitation_claim_unavailable',404);
  let result;
  if(claim.target.kind==='app')result=readAdmission(claimId);
  else{
   check(typeof admitTarget==='function','invitation_admission_unavailable',503);
   try{result=admitTarget({actor,claimId,readClaim});}
   catch(error){
    if(!['invitation_unavailable','community_banned','invitation_issuer_required','invitation_owner_changed','invitation_claim_blocked'].includes(error?.code))throw error;
    run("UPDATE admissions SET state='blocked',error_code=?,updated_at=? WHERE claim_id=?",error.code,clock(),claimId);return readAdmission(claimId);
   }
  }
  check(result&&['admitted','removed'].includes(result.state),'invitation_admission_unconfirmed',503);
  run('UPDATE admissions SET state=?,error_code=NULL,updated_at=? WHERE claim_id=?',result.state==='admitted'?'admitted':'blocked',clock(),claimId);
  const current=readAdmission(claimId);
  if(current.state==='admitted')try{onAdmission({claimId,target:claim.target,issuerAccountId:claim.issuerAccountId,recipientAccountId:claim.recipientAccountId});}catch{/* A callback cannot roll back admission; reconcile repeats it. */}
  return current;
 }
 return {stage,readClaim,readAdmission,complete,forRequest:requestId=>get('SELECT claim_id FROM admissions WHERE request_id=?',requestId)?.claim_id??null,
  list({after='',limit=100}={}){check(typeof after==='string'&&Number.isSafeInteger(limit)&&limit>=1&&limit<=100,'invitation_invalid');return db.prepare('SELECT claim_id FROM admissions WHERE claim_id>? ORDER BY claim_id LIMIT ?').all(after,limit).map(row=>row.claim_id);}};
}
