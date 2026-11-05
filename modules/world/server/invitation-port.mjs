import {createHash} from 'node:crypto';
import {assert,identifier} from './validation.mjs';

/** Host-only adapter. World membership and its admission receipt share one World commit. */
export function createWorldInvitationPort({db,m,clock,withCurrentContacts,notifyMembership,onChange}){
 let targetReads=0;
 const op='world.invitation.admit',digest=claim=>createHash('sha256').update(JSON.stringify([claim.claimId,claim.target,claim.ownerAccountId,claim.issuerAccountId,claim.recipientAccountId,claim.recipientLabel])).digest('hex');
 const manager=(id,accountId)=>{const row=m.membership(id,accountId);return row?.state==='active'&&['owner','moderator'].includes(row.role);};
 function transaction(actor,write,fn){
  const run=()=>{assert(!db.isTransaction,'world_authority_mutation_forbidden');db.exec(write?'BEGIN IMMEDIATE':'BEGIN');try{if(actor)m.validateActor(actor);const result=fn();assert(!result||typeof result.then!=='function','world_invitation_async');db.exec('COMMIT');return result;}catch(error){if(db.isTransaction)db.exec('ROLLBACK');throw error;}};
  return actor&&withCurrentContacts?withCurrentContacts(actor,run):run();
 }
 const group=id=>{identifier(id);const row=m.get("SELECT * FROM communities WHERE id=? AND state='active'",id);assert(row,'invitation_unavailable');return row;};
 function checked(claim){
  assert(claim?.target?.kind==='community'&&typeof claim.claimId==='string','invitation_claim_invalid');
  identifier(claim.target.id);identifier(claim.recipientAccountId);identifier(claim.issuerAccountId);
  const row=group(claim.target.id);assert(row.owner_id===claim.ownerAccountId,'invitation_owner_changed');return row;
 }
 function read(claim){
  const row=checked(claim),receipt=m.get('SELECT * FROM receipts WHERE account_id=? AND operation=? AND request_id=?',claim.recipientAccountId,op,claim.claimId);
  if(!receipt)return {state:'pending'};
  assert(receipt.digest===digest(claim),'invitation_claim_invalid');
  return {state:m.membership(row.id,claim.recipientAccountId)?.state==='active'?'admitted':'removed',receipt:JSON.parse(receipt.result)};
 }
 return Object.freeze({
  withTarget({appId:id,actor},callback){return transaction(actor,false,()=>{const row=group(id),canManage=Boolean(actor&&manager(id,actor.accountId));
   targetReads++;try{return callback(Object.freeze({appId:id,kind:'community',ownerId:row.owner_id,title:row.name,canManage,
    members(offset){assert(canManage,'community_permission_denied');const rows=m.all('SELECT member.*,p.display_name FROM memberships member JOIN profiles p ON p.account_id=member.account_id WHERE member.community_id=? ORDER BY member.account_id LIMIT 51 OFFSET ?',id,offset);return {members:rows.slice(0,50).map(value=>({accountId:value.account_id,label:value.display_name,role:value.role,active:value.state==='active',revision:value.revision})),nextOffset:rows.length>50?offset+50:null};}}));}finally{targetReads--;}});},
  ownedTargets(actor){return transaction(actor,false,()=>m.all("SELECT c.id FROM communities c JOIN memberships member ON member.community_id=c.id WHERE c.state='active' AND member.account_id=? AND member.state='active' AND member.role IN('owner','moderator')",actor.accountId).map(row=>({kind:'community',id:row.id})));},
  readAdmission({claim}){try{return targetReads&&db.isTransaction?read(claim):transaction(null,false,()=>read(claim));}catch(error){if(['invitation_unavailable','invitation_owner_changed'].includes(error.code))return {state:'removed'};throw error;}},
  admit({actor,claimId,readClaim}){
   assert(typeof readClaim==='function','invitation_claim_reader_required');let changed;
   const result=transaction(actor,true,()=>{
    const claim=readClaim(claimId),row=checked(claim);assert(claim.claimId===claimId,'invitation_claim_invalid');
    assert(actor.accountId===claim.recipientAccountId||manager(row.id,actor.accountId),'community_permission_denied');
    const existing=read(claim);if(existing.state!=='pending')return existing;
    assert(claim.decision==='approved'&&!claim.linkRevoked&&claim.state!=='blocked','invitation_claim_blocked');
    assert(manager(row.id,claim.issuerAccountId),'invitation_issuer_required');
    const prior=m.membership(row.id,claim.recipientAccountId);assert(prior?.state!=='banned','community_banned');
    const now=clock();m.ensurePerson({accountId:claim.recipientAccountId,deviceId:claim.recipientDeviceId,label:claim.recipientAccountLabel||'Участник'},now);
    if(prior?.state!=='active'){
     m.run(`INSERT INTO memberships(community_id,account_id,role,state,joined_at,updated_at) VALUES(?,?,'member','active',?,?)
      ON CONFLICT(community_id,account_id) DO UPDATE SET state='active',role='member',joined_at=excluded.joined_at,updated_at=excluded.updated_at,revision=memberships.revision+1`,row.id,claim.recipientAccountId,now,now);
     const member=m.membership(row.id,claim.recipientAccountId);changed={communityId:row.id,profileId:claim.recipientAccountId,state:member.state,revision:member.revision};
    }
    const receipt={schema:'soty.world-invitation-admission.v1',claimId,target:{kind:'community',id:row.id},recipientAccountId:claim.recipientAccountId};
    m.run('INSERT INTO receipts(account_id,operation,request_id,digest,result,created_at) VALUES(?,?,?,?,?,?)',claim.recipientAccountId,op,claimId,digest(claim),JSON.stringify(receipt),now);
    m.audit(actor.accountId,op,row.id,now);return {state:'admitted',receipt};
   });
   if(changed){notifyMembership(changed);onChange(actor,{kind:'community',id:changed.communityId},false,[changed.profileId]);}
   return result;
  },
 });
}
