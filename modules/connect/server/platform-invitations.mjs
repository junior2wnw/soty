import { createHash, randomBytes } from 'node:crypto';

export const PLATFORM_INVITATION_OPERATIONS = ['platform.admission.get', 'platform.admission.update', 'platform.invites.issue', 'platform.invites.list', 'platform.invites.revoke'];
const lifetime = 7 * 24 * 60 * 60_000;
const hash = token => createHash('sha256').update(token).digest('base64url');

/** Called only inside Connect's signed action transaction and active account fence. */
export function createPlatformInvitations({ get, all, run, assert, exactArgs, textId, adminAccountIds }) {
  const admins = new Set(adminAccountIds);
  const policy = () => get('SELECT * FROM platform_admission WHERE id=1');
  const tokenHash = token => {
    assert(typeof token === 'string' && /^[A-Za-z0-9_-]{43}$/u.test(token)
      && Buffer.from(token, 'base64url').toString('base64url') === token, 'platform_invite_invalid');
    return hash(token);
  };
  const project = row => ({ id: row.id, createdAt: row.created_at, expiresAt: row.expires_at,
    revokedAt: row.revoked_at, redeemedAt: row.redeemed_at });
  function dispatch(op, args, accountId, digest, timestamp) {
    const manageAllowed = admins.has(accountId), current = policy();
    if (op === 'platform.admission.get') {
      exactArgs(args);
      return { inviteOnly: Boolean(current.invite_only), defaultQuota: current.quota, revision: current.revision, manageAllowed };
    }
    if (op === 'platform.invites.list') {
      exactArgs(args);
      const invitations = all('SELECT * FROM platform_invitations WHERE issuer_id=? ORDER BY created_at DESC LIMIT 200', accountId);
      const used = get('SELECT count(*) AS n FROM platform_invitations WHERE issuer_id=? AND (redeemed_at IS NOT NULL OR (revoked_at IS NULL AND expires_at>?))', accountId, timestamp).n;
      return { invitations: invitations.map(project), quota: current.quota, used, remaining: Math.max(0, current.quota - used) };
    }
    if (op === 'platform.admission.update') {
      exactArgs(args, ['requestId', 'expectedRevision', 'inviteOnly', 'defaultQuota']);
      assert(manageAllowed, 'platform_admin_required');
      assert(Number.isSafeInteger(args.expectedRevision) && args.expectedRevision >= 0 && typeof args.inviteOnly === 'boolean'
        && Number.isSafeInteger(args.defaultQuota) && args.defaultQuota >= 0 && args.defaultQuota <= 100, 'invalid_arguments');
    } else if (op === 'platform.invites.issue') {
      exactArgs(args, ['requestId', 'token']); tokenHash(args.token);
    } else { exactArgs(args, ['requestId', 'invitationId']); textId(args.invitationId); }
    textId(args.requestId);
    const receipt = get('SELECT * FROM platform_invitation_receipts WHERE account_id=? AND request_id=?', accountId, args.requestId);
    if (receipt) {
      assert(receipt.operation === op && receipt.digest === digest, 'platform_request_conflict');
      return JSON.parse(receipt.result_json);
    }
    let result;
    if (op === 'platform.admission.update') {
      assert(current.revision === args.expectedRevision, 'platform_policy_conflict');
      run('UPDATE platform_admission SET invite_only=?,quota=?,revision=revision+1 WHERE id=1', Number(args.inviteOnly), args.defaultQuota);
      result = { inviteOnly: args.inviteOnly, defaultQuota: args.defaultQuota, revision: current.revision + 1, manageAllowed: true };
    } else if (op === 'platform.invites.issue') {
      const used = get('SELECT count(*) AS n FROM platform_invitations WHERE issuer_id=? AND (redeemed_at IS NOT NULL OR (revoked_at IS NULL AND expires_at>?))', accountId, timestamp).n;
      assert(used < current.quota, 'platform_invite_quota_exhausted');
      assert(!get('SELECT 1 FROM platform_invitations WHERE token_hash=?', tokenHash(args.token)), 'platform_invite_token_reused');
      const id = `pinv_${randomBytes(24).toString('base64url')}`;
      run('INSERT INTO platform_invitations(id,issuer_id,token_hash,created_at,expires_at) VALUES(?,?,?,?,?)', id, accountId, tokenHash(args.token), timestamp, timestamp + lifetime);
      result = { invitation: { id, createdAt: timestamp, expiresAt: timestamp + lifetime, revokedAt: null, redeemedAt: null } };
    } else {
      const row = get('SELECT * FROM platform_invitations WHERE id=? AND issuer_id=?', args.invitationId, accountId);
      assert(row, 'platform_invite_unavailable');
      assert(row.redeemed_at === null, 'platform_invite_already_redeemed');
      if (row.revoked_at === null) run('UPDATE platform_invitations SET revoked_at=? WHERE id=?', timestamp, row.id);
      result = { invitation: project({ ...row, revoked_at: row.revoked_at ?? timestamp }) };
    }
    run('INSERT INTO platform_invitation_receipts VALUES(?,?,?,?,?)', accountId, args.requestId, op, digest, JSON.stringify(result));
    return result;
  }
  function admit(token, accountId, timestamp) {
    if (token === undefined) { assert(!policy().invite_only, 'platform_invitation_required'); return; }
    const row = get('SELECT * FROM platform_invitations WHERE token_hash=?', tokenHash(token));
    assert(row && row.revoked_at === null && row.redeemed_at === null && row.expires_at > timestamp, 'platform_invite_unavailable');
    // Account insert, token consumption and device installation share one transaction.
    run('UPDATE platform_invitations SET redeemed_at=?,redeemed_account_id=? WHERE id=?', timestamp, accountId, row.id);
  }
  return { dispatch, admit };
}
