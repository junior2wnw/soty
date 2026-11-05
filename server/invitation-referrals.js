/** Admission is an authoritative source event, never a caller-provided money operation. */
export function createInvitationReferralSource({ invitations, upstream } = {}) {
  if (typeof invitations !== 'function' || upstream !== undefined && typeof upstream !== 'function') throw new TypeError('referrals_source_configuration_invalid');
  return context => {
    if (context?.kind !== 'invitation-admission') return upstream?.(context);
    if (typeof context.claimId !== 'string' || Object.keys(context).some(key => !['kind', 'claimId'].includes(key))) return null;
    const admission = invitations()?.readAdmission(context.claimId);
    if (admission?.state !== 'admitted' || admission.target.kind !== 'app' || admission.issuerAccountId === admission.recipientAccountId) return null;
    return { kind: 'attribution', appId: admission.target.id, eventId: admission.claimId,
      accountId: admission.recipientAccountId, parentAccountId: admission.issuerAccountId };
  };
}

export function applyInvitationReferral({ referrals, claimId }) {
  return referrals.internal.applyEvent({ context: { kind: 'invitation-admission', claimId } });
}
