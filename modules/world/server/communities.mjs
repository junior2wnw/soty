import { createHash } from 'node:crypto';
import { assert, exact, identifier, id, text, tags, boolean, choice, revision, folded, limit, cursor, nextCursor } from './validation.mjs';

const EDIT_FIELDS = ['name', 'description', 'topics', 'joinPolicy', 'showMembers', 'showcase', 'symbol', 'color'];
export const COMMUNITY_OPERATIONS = [
  'world.community.create', 'world.community.get', 'world.community.list', 'world.community.update', 'world.community.archive',
  'world.membership.join', 'world.membership.leave', 'world.membership.invite', 'world.membership.decide',
  'world.membership.remove', 'world.membership.ban', 'world.membership.unban', 'world.membership.role',
  'world.membership.transfer', 'world.membership.list', 'world.membership.preferences',
];
function settings(args, existing = {}) {
  const value = {
    name: args.name === undefined ? existing.name : text(args.name, 80, { empty: false }),
    description: args.description === undefined ? existing.description ?? '' : text(args.description, 1500, { multiline: true }),
    topics: args.topics === undefined ? existing.topics ?? '[]' : JSON.stringify(tags(args.topics)),
    join_policy: args.joinPolicy === undefined ? existing.join_policy ?? 'open' : choice(args.joinPolicy, ['open', 'request', 'invite']),
    show_members: args.showMembers === undefined ? existing.show_members ?? 1 : boolean(args.showMembers),
    showcase: args.showcase === undefined ? existing.showcase ?? '' : text(args.showcase, 2500, { multiline: true }),
    symbol: args.symbol === undefined ? existing.symbol ?? '✦' : text(args.symbol, 12, { empty: false }),
    color: args.color === undefined ? existing.color ?? '#C89448' : args.color,
  };
  assert(/^#[0-9a-fA-F]{6}$/u.test(value.color), 'invalid_color');
  return value;
}
const canonical = value => value === null || typeof value !== 'object' ? JSON.stringify(value)
  : Array.isArray(value) ? `[${value.map(canonical).join(',')}]`
    : `{${Object.keys(value).sort().map(key => `${JSON.stringify(key)}:${canonical(value[key])}`).join(',')}}`;
function replay(m, actor, op, args, now, action) {
  identifier(args.requestId);
  const digest = createHash('sha256').update(canonical(args)).digest('base64url');
  const existing = m.get('SELECT * FROM receipts WHERE account_id=? AND operation=? AND request_id=?', actor.accountId, op, args.requestId);
  if (existing) { assert(existing.digest === digest, 'request_conflict'); return JSON.parse(existing.result); }
  const result = action();
  m.run('INSERT INTO receipts(account_id,operation,request_id,digest,result,created_at) VALUES (?,?,?,?,?,?)', actor.accountId, op, args.requestId, digest, JSON.stringify(result), now);
  return result;
}
export function communityOperation(m, op, args, actor, now) {
  const actorId = actor.accountId;
  const reply = communityId => ({ community: m.community(m.group(communityId), actorId) });
  function changeState(communityId, profileId, state) {
    const previous = m.membership(communityId, profileId);
    if (previous?.state === state) return previous;
    m.run(`INSERT INTO memberships(community_id,account_id,state,joined_at,updated_at) VALUES (?,?,?,?,?)
      ON CONFLICT(community_id,account_id) DO UPDATE SET state=excluded.state,
      role=CASE WHEN excluded.state IN ('left','removed','banned','declined') THEN 'member' ELSE memberships.role END,
      joined_at=CASE WHEN excluded.state='active' THEN excluded.joined_at ELSE memberships.joined_at END,
      updated_at=excluded.updated_at,revision=memberships.revision+1`, communityId, profileId, state, state === 'active' ? now : null, now);
    m.changed(communityId, profileId); m.audit(actorId, op, communityId, now);
    return m.membership(communityId, profileId);
  }
  if (op === 'world.community.create') {
    exact(args, ['requestId', 'name'], EDIT_FIELDS.filter(key => key !== 'name'));
    return replay(m, actor, op, args, now, () => {
      const value = settings(args);
      assert(m.get("SELECT COUNT(*) AS n FROM communities WHERE owner_id=? AND state='active'", actorId).n < 100, 'community_limit');
      m.rate('create:' + actorId, 10, 60_000, now);
      const communityId = id('group');
      m.run(`INSERT INTO communities(id,owner_id,name,description,topics,search_text,join_policy,show_members,showcase,symbol,color,created_at,updated_at)
        VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?)`, communityId, actorId, value.name, value.description, value.topics,
        folded([value.name, value.description, ...JSON.parse(value.topics)].join(' ')), value.join_policy, value.show_members,
        value.showcase, value.symbol, value.color, now, now);
      m.run("INSERT INTO memberships(community_id,account_id,role,state,joined_at,updated_at) VALUES (?,?,'owner','active',?,?)", communityId, actorId, now, now);
      m.audit(actorId, op, communityId, now); m.changed(communityId, actorId);
      // A receipt stores only the created identity. Current privacy/roles are projected on every reply.
      return { communityId };
    });
  }
  if (op === 'world.community.get') { exact(args, ['communityId']); return { community: m.community(m.visibleGroup(args.communityId, actorId), actorId) }; }
  if (op === 'world.community.list') {
    exact(args);
    const rows = m.all(`SELECT c.* FROM communities c JOIN memberships member ON member.community_id=c.id
      WHERE member.account_id=? AND member.state IN('active','requested','invited') AND c.state='active'
      ORDER BY member.pinned DESC,member.updated_at DESC,c.id LIMIT 100`, actorId);
    return { communities: rows.map(row => m.community(row, actorId)) };
  }
  if (op === 'world.community.update') {
    exact(args, ['communityId', 'expectedRevision'], EDIT_FIELDS);
    const { row, participation } = m.requireManager(args.communityId, actorId); revision(args, row);
    assert(EDIT_FIELDS.some(key => Object.hasOwn(args, key)), 'empty_update');
    if (args.joinPolicy !== undefined) assert(participation.role === 'owner', 'community_permission_denied');
    const value = settings(args, row);
    m.run(`UPDATE communities SET name=?,description=?,topics=?,search_text=?,join_policy=?,show_members=?,showcase=?,symbol=?,color=?,revision=revision+1,updated_at=? WHERE id=?`,
      value.name, value.description, value.topics, folded([value.name, value.description, ...JSON.parse(value.topics)].join(' ')),
      value.join_policy, value.show_members, value.showcase, value.symbol, value.color, now, row.id);
    m.audit(actorId, op, row.id, now); return reply(row.id);
  }
  if (op === 'world.community.archive') {
    exact(args, ['communityId', 'expectedRevision']);
    const { row } = m.requireManager(args.communityId, actorId, true); revision(args, row);
    m.run("UPDATE communities SET state='archived',revision=revision+1,updated_at=? WHERE id=?", now, row.id);
    m.audit(actorId, op, row.id, now); m.emit({ communityId: row.id, profileId: null, state: 'archived', revision: row.revision + 1 });
    return { archived: true };
  }
  if (op === 'world.membership.join') {
    exact(args, ['communityId']);
    const row = m.visibleGroup(args.communityId, actorId); const previous = m.membership(row.id, actorId);
    assert(previous?.state !== 'banned', 'community_banned');
    if (previous?.state === 'active' || previous?.state === 'requested') return reply(row.id);
    assert(row.join_policy !== 'invite' || previous?.state === 'invited', 'community_invitation_required');
    m.rate('join:' + actorId, 20, 60_000, now);
    changeState(row.id, actorId, row.join_policy === 'request' && previous?.state !== 'invited' ? 'requested' : 'active');
    return reply(row.id);
  }
  if (op === 'world.membership.leave') {
    exact(args, ['communityId']);
    const row = m.group(args.communityId); const previous = m.membership(row.id, actorId);
    assert(previous && previous.state !== 'banned', 'community_membership_required');
    assert(previous.role !== 'owner', 'owner_must_transfer_or_archive');
    changeState(row.id, actorId, 'left');
    // Invite-only groups cease to be readable after leaving. Return no stale private preview.
    return row.join_policy === 'invite' ? { left: true, community: null } : reply(row.id);
  }
  if (op === 'world.membership.list') {
    exact(args, ['communityId'], ['state', 'limit', 'cursor']);
    const { row, participation } = m.requireMember(args.communityId, actorId);
    const state = choice(args.state ?? 'active', ['active', 'requested', 'invited', 'banned']);
    if (state !== 'active') assert(['owner', 'moderator'].includes(participation.role), 'community_permission_denied');
    const scope = JSON.stringify([row.id, state]); const offset = cursor(args.cursor, scope); const size = limit(args.limit);
    const rows = m.all(`SELECT p.*,member.role,member.state,member.revision AS membership_revision,member.joined_at,
      member.show_in_profile,member.pinned,member.muted FROM memberships member JOIN profiles p ON p.account_id=member.account_id
      WHERE member.community_id=? AND member.state=? ORDER BY member.joined_at,p.account_id LIMIT ? OFFSET ?`, row.id, state, size + 1, offset);
    return { members: rows.slice(0, size).map(item => ({ profile: m.profile(item), role: item.role, state: item.state,
      revision: item.membership_revision, joinedAt: item.joined_at })),
      nextCursor: rows.length > size ? nextCursor(offset + size, scope) : null };
  }
  if (op === 'world.membership.preferences') {
    exact(args, ['communityId'], ['pinned', 'muted', 'showInProfile']);
    const { row, participation } = m.requireMember(args.communityId, actorId);
    assert(['pinned', 'muted', 'showInProfile'].some(key => Object.hasOwn(args, key)), 'empty_update');
    m.run('UPDATE memberships SET pinned=?,muted=?,show_in_profile=?,revision=revision+1 WHERE community_id=? AND account_id=?',
      args.pinned === undefined ? participation.pinned : boolean(args.pinned), args.muted === undefined ? participation.muted : boolean(args.muted),
      args.showInProfile === undefined ? participation.show_in_profile : boolean(args.showInProfile), row.id, actorId);
    return reply(row.id);
  }
  if (op === 'world.membership.transfer') {
    exact(args, ['communityId', 'profileId', 'requestId']);
    return replay(m, actor, op, args, now, () => {
      const { row } = m.requireManager(args.communityId, actorId, true); const profileId = identifier(args.profileId);
      assert(profileId !== actorId, 'invalid_target');
      const target = m.membership(row.id, profileId); assert(target?.state === 'active', 'community_membership_required');
      m.run("UPDATE memberships SET role='moderator',revision=revision+1,updated_at=? WHERE community_id=? AND account_id=?", now, row.id, actorId);
      m.run("UPDATE memberships SET role='owner',revision=revision+1,updated_at=? WHERE community_id=? AND account_id=?", now, row.id, profileId);
      m.run('UPDATE communities SET owner_id=?,revision=revision+1,updated_at=? WHERE id=?', profileId, now, row.id);
      m.changed(row.id, actorId); m.changed(row.id, profileId); m.audit(actorId, op, row.id, now);
      return { communityId: row.id };
    });
  }
  if (op === 'world.membership.invite') {
    exact(args, ['communityId', 'profileId']);
    const { row } = m.requireManager(args.communityId, actorId);
    const profileId = identifier(args.profileId); assert(m.person(profileId), 'profile_not_found');
    const previous = m.membership(row.id, profileId);
    assert(previous?.state !== 'banned', 'community_banned');
    if (['active', 'invited'].includes(previous?.state)) return reply(row.id);
    m.rate('invite:' + actorId, 30, 60_000, now);
    changeState(row.id, profileId, 'invited');
    m.run('UPDATE memberships SET invited_by=? WHERE community_id=? AND account_id=?', actorId, row.id, profileId);
    return reply(row.id);
  }
  if (op === 'world.membership.decide') {
    exact(args, ['communityId', 'profileId', 'accept']); boolean(args.accept);
    const { row } = m.requireManager(args.communityId, actorId); const profileId = identifier(args.profileId);
    const previous = m.membership(row.id, profileId); const target = args.accept ? 'active' : 'declined';
    if (previous?.state === target) return reply(row.id);
    assert(previous?.state === 'requested', 'membership_request_unavailable');
    changeState(row.id, profileId, target); return reply(row.id);
  }
  if (['world.membership.remove', 'world.membership.ban', 'world.membership.unban', 'world.membership.role'].includes(op)) {
    exact(args, ['communityId', 'profileId'], op === 'world.membership.role' ? ['role'] : []);
    const { row, participation } = m.requireManager(args.communityId, actorId, op === 'world.membership.role');
    const profileId = identifier(args.profileId); assert(profileId !== actorId, 'invalid_target');
    const target = m.membership(row.id, profileId); assert(target, 'community_membership_required');
    assert(target.role !== 'owner' && (participation.role === 'owner' || target.role === 'member'), 'community_permission_denied');
    if (op === 'world.membership.role') {
      const role = choice(args.role, ['moderator', 'member']); assert(target.state === 'active', 'community_membership_required');
      if (target.role !== role) {
        m.run('UPDATE memberships SET role=?,revision=revision+1,updated_at=? WHERE community_id=? AND account_id=?', role, now, row.id, profileId);
        m.changed(row.id, profileId); m.audit(actorId, op, row.id, now);
      }
    } else if (op === 'world.membership.unban') {
      assert(['banned', 'removed'].includes(target.state), 'membership_not_banned');
      changeState(row.id, profileId, 'removed');
    } else {
      assert(op !== 'world.membership.remove' || target.state !== 'banned', 'membership_is_banned');
      changeState(row.id, profileId, op === 'world.membership.ban' ? 'banned' : 'removed');
    }
    return reply(row.id);
  }
}
