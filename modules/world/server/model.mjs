import { assert, identifier, text, folded } from './validation.mjs';

export function createModel(db, emit) {
  const statements = new Map();
  const statement = sql => {
    let value = statements.get(sql);
    if (!value) { value = db.prepare(sql); statements.set(sql, value); }
    return value;
  };
  const get = (sql, ...params) => statement(sql).get(...params);
  const all = (sql, ...params) => statement(sql).all(...params);
  const run = (sql, ...params) => statement(sql).run(...params);
  const person = profileId => get('SELECT * FROM profiles WHERE account_id=?', identifier(profileId));
  function ensurePerson(actor, now) {
    assert(actor && typeof actor === 'object', 'authentication_required');
    identifier(actor.accountId); identifier(actor.deviceId);
    const name = text(actor.label, 80, { empty: false });
    const existing = person(actor.accountId); if (existing) return existing;
    run(`INSERT OR IGNORE INTO profiles(account_id,display_name,search_text,created_at,updated_at) VALUES (?,?,?,?,?)`, actor.accountId, name, folded(name), now, now);
    return person(actor.accountId);
  }
  function profile(row, own = false) {
    const result = { profileId: row.account_id, displayName: row.display_name, bio: row.bio,
      interests: JSON.parse(row.interests), avatarColor: row.avatar_color, avatarRevision: row.avatar_revision ?? null, revision: row.revision };
    if (own) Object.assign(result, { discoverable: Boolean(row.discoverable), showPresence: Boolean(row.show_presence),
      showMemberships: Boolean(row.show_memberships), contactPolicy: row.contact_policy });
    return result;
  }
  const membership = (communityId, profileId) => get('SELECT * FROM memberships WHERE community_id=? AND account_id=?', communityId, profileId);
  function member(row) {
    return row ? { state: row.state, role: row.role, revision: row.revision, pinned: Boolean(row.pinned),
      muted: Boolean(row.muted), showInProfile: Boolean(row.show_in_profile), joinedAt: row.joined_at } : null;
  }
  function group(communityId) {
    const row = get("SELECT * FROM communities WHERE id=? AND state='active'", identifier(communityId));
    assert(row, 'community_not_found'); return row;
  }
  function visibleGroup(communityId, actorId) {
    const row = group(communityId); const participation = membership(row.id, actorId);
    assert(row.join_policy !== 'invite' || ['active', 'invited'].includes(participation?.state), 'community_not_found');
    return row;
  }
  function requireMember(communityId, actorId) {
    const row = group(communityId); const participation = membership(row.id, actorId);
    assert(participation?.state === 'active', 'community_membership_required');
    return { row, participation };
  }
  function requireManager(communityId, actorId, owner = false) {
    const data = requireMember(communityId, actorId);
    assert(owner ? data.participation.role === 'owner' : ['owner', 'moderator'].includes(data.participation.role), 'community_permission_denied');
    return data;
  }
  function canAccess(communityId, accountId) {
    return Boolean(get(`SELECT 1 FROM memberships m JOIN communities c ON c.id=m.community_id
      WHERE m.community_id=? AND m.account_id=? AND m.state='active' AND c.state='active'`, communityId, accountId));
  }
  const unread = (communityId, actorId) => get(`SELECT COUNT(*) AS n FROM messages msg JOIN memberships m ON m.community_id=msg.community_id
    AND m.account_id=? AND m.state='active' WHERE msg.community_id=? AND msg.seq>m.last_read_seq AND msg.author_id<>? AND msg.deleted_at IS NULL`, actorId, communityId, actorId).n;
  function canRequestContact(accountId, targetId) {
    if (accountId === targetId) return false;
    const target = person(targetId); if (!target || target.contact_policy === 'nobody') return false;
    const shared = Boolean(get(`SELECT 1 FROM memberships a JOIN memberships b ON b.community_id=a.community_id JOIN communities c ON c.id=a.community_id
      WHERE a.account_id=? AND b.account_id=? AND a.state='active' AND b.state='active' AND c.state='active' LIMIT 1`, accountId, targetId));
    return Boolean((target.discoverable || shared) && (target.contact_policy === 'everyone' || shared));
  }
  function community(row, actorId) {
    const participation = membership(row.id, actorId);
    const active = participation?.state === 'active';
    const manager = active && ['owner', 'moderator'].includes(participation.role);
    const previews = row.show_members ? all(`SELECT p.* FROM memberships m JOIN profiles p ON p.account_id=m.account_id
      WHERE m.community_id=? AND m.state='active' AND m.show_in_profile=1 AND p.discoverable=1 AND p.show_memberships=1
      ORDER BY m.joined_at,p.account_id LIMIT 6`, row.id).map(item => profile(item)) : [];
    return { communityId: row.id, name: row.name, description: row.description, topics: JSON.parse(row.topics),
      joinPolicy: row.join_policy, showMembers: Boolean(row.show_members), showcase: row.showcase, symbol: row.symbol, color: row.color,
      revision: row.revision, memberCount: get("SELECT COUNT(*) AS n FROM memberships WHERE community_id=? AND state='active'", row.id).n,
      previewMembers: previews, membership: member(participation), permissions: { canManage: manager, canModerate: manager, canWrite: active },
      ...(manager ? { pendingCount: get("SELECT COUNT(*) AS n FROM memberships WHERE community_id=? AND state='requested'", row.id).n } : {}),
      unreadCount: active ? unread(row.id, actorId) : 0, createdAt: row.created_at };
  }
  function audit(actorId, operation, targetId, now) {
    run('INSERT INTO world_audit(account_id,operation,target_id,created_at) VALUES (?,?,?,?)', actorId, operation, targetId, now);
  }
  function rate(key, maximum, interval, now) {
    const bucket = Math.floor(now / interval);
    const current = get('SELECT * FROM world_rate_limits WHERE key=?', key);
    assert(current?.bucket !== bucket || current.count < maximum, 'rate_limited');
    run(`INSERT INTO world_rate_limits(key,bucket,count) VALUES (?,?,1) ON CONFLICT(key) DO UPDATE SET
      bucket=excluded.bucket,count=CASE WHEN world_rate_limits.bucket=excluded.bucket THEN world_rate_limits.count+1 ELSE 1 END`, key, bucket);
  }
  function changed(communityId, profileId) {
    const current = membership(communityId, profileId);
    emit({ communityId, profileId, state: current.state, revision: current.revision });
  }
  return { db, get, all, run, person, ensurePerson, profile, membership, member, group, visibleGroup, requireMember, requireManager,
    canAccess, canRequestContact, unread, community, audit, rate, changed, emit };
}
