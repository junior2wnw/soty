import {check} from './model.mjs';

/** One invitation ledger; each target keeps its existing authoritative permissions. */
export function createInvitationTargetAdapter({apps,world}){
 return Object.freeze({
  withTarget(request,callback){return request.kind==='community'?world.invitations.withTarget(request,callback):apps.withInvitationTarget(request,callback);},
  ownedTargets(actor){return [...apps.invitationOwnedTargets(actor),...world.invitations.ownedTargets(actor)];},
  admitTarget(request){return world.invitations.admit(request);},
  readTargetAdmission(request){check(request.claim.target.kind==='community','invitation_target_mismatch');return world.invitations.readAdmission(request);},
 });
}
