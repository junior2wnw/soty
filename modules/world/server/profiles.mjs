import { createHash } from 'node:crypto';
import { assert, exact, identifier, text, tags, boolean, choice, integer, revision, folded } from './validation.mjs';
import { discoveryOperation } from './discovery.mjs';

const FIELDS = ['displayName', 'bio', 'interests', 'audience', 'discoverable', 'showPresence', 'showMemberships', 'contactPolicy', 'avatarColor'];
export const PROFILE_OPERATIONS = ['world.profile.get', 'world.profile.update', 'world.profile.view', 'world.discovery.search'];
export function profileOperation(m, op, args, actor, now) {
  if (op === 'world.profile.get') { exact(args); return { profile: m.profile(m.person(actor.accountId), true) }; }
  if (op === 'world.profile.update') {
    exact(args, ['expectedRevision'], [...FIELDS, 'expectedAccountId', 'requestId']);
    const durable = Object.hasOwn(args, 'requestId') || Object.hasOwn(args, 'expectedAccountId');
    if (durable) {
      exact(args, ['expectedRevision', 'audience', 'expectedAccountId', 'requestId']);
      identifier(args.requestId); identifier(args.expectedAccountId);
      assert(args.expectedAccountId === actor.accountId, 'profile_account_changed');
      integer(args.expectedRevision, 1, Number.MAX_SAFE_INTEGER - 1);
      choice(args.audience, ['private', 'friends', 'link', 'public']);
    }
    const digest = durable ? createHash('sha256').update(JSON.stringify([
      args.expectedAccountId, args.requestId, args.expectedRevision, args.audience,
    ])).digest('base64url') : null;
    if (durable) {
      const accepted = m.get('SELECT * FROM receipts WHERE account_id=? AND operation=? AND request_id=?', actor.accountId, op, args.requestId);
      if (accepted) {
        assert(accepted.digest === digest, 'profile_request_conflict');
        return { profile: m.profile(m.person(actor.accountId), true), replayed: true, receipt: JSON.parse(accepted.result) };
      }
    }
    const row = m.person(actor.accountId); revision(args, row);
    assert(FIELDS.some(key => Object.hasOwn(args, key)), 'empty_update');
    const audience = args.audience === undefined
      ? args.discoverable === undefined ? row.audience : boolean(args.discoverable) ? 'public' : 'private'
      : choice(args.audience, ['private', 'friends', 'link', 'public']);
    if (args.audience !== undefined && args.discoverable !== undefined) {
      assert(Boolean(boolean(args.discoverable)) === (audience === 'public'), 'profile_audience_conflict');
    }
    const values = { display_name: args.displayName === undefined ? row.display_name : text(args.displayName, 80, { empty: false }),
      bio: args.bio === undefined ? row.bio : text(args.bio, 500, { multiline: true }),
      interests: args.interests === undefined ? row.interests : JSON.stringify(tags(args.interests)),
      discoverable: Number(audience === 'public'),
      show_presence: args.showPresence === undefined ? row.show_presence : boolean(args.showPresence),
      show_memberships: args.showMemberships === undefined ? row.show_memberships : boolean(args.showMemberships),
      contact_policy: args.contactPolicy === undefined ? row.contact_policy : choice(args.contactPolicy, ['everyone', 'members', 'nobody']),
      avatar_color: args.avatarColor === undefined ? row.avatar_color : args.avatarColor };
    assert(/^#[0-9a-fA-F]{6}$/u.test(values.avatar_color), 'invalid_color');
    m.run(`UPDATE profiles SET display_name=?,bio=?,interests=?,audience=?,discoverable=?,show_presence=?,show_memberships=?,contact_policy=?,avatar_color=?,
      search_text=?,revision=revision+1,updated_at=? WHERE account_id=?`, values.display_name, values.bio, values.interests,
      audience, values.discoverable, values.show_presence, values.show_memberships, values.contact_policy, values.avatar_color,
      folded([values.display_name, values.bio, ...JSON.parse(values.interests)].join(' ')), now, actor.accountId);
    m.audit(actor.accountId, op, actor.accountId, now);
    const profile = m.profile(m.person(actor.accountId), true);
    if (!durable) return { profile };
    const receipt = { schema: 'soty.world-profile-audience.receipt.v1', requestId: args.requestId,
      accountId: actor.accountId, audience, expectedRevision: args.expectedRevision, acceptedRevision: profile.revision };
    m.run('INSERT INTO receipts(account_id,operation,request_id,digest,result,created_at) VALUES (?,?,?,?,?,?)',
      actor.accountId, op, args.requestId, digest, JSON.stringify(receipt), now);
    return { profile, replayed: false, receipt };
  }
  if (op === 'world.profile.view') {
    exact(args, ['profileId']);
    const row = m.person(identifier(args.profileId));
    assert(m.canViewFullProfile(row, actor.accountId), 'profile_not_found');
    const groups = row.show_memberships ? m.all(`SELECT c.* FROM communities c JOIN memberships member ON member.community_id=c.id
      WHERE member.account_id=? AND member.state='active' AND member.show_in_profile=1 AND c.state='active' AND c.join_policy<>'invite'
      ORDER BY c.name,c.id LIMIT 60`, row.account_id).map(item => m.community(item, actor.accountId)) : [];
    return { profile: m.profile(row, row.account_id === actor.accountId), communities: groups,
      canRequestContact: m.canRequestContact(actor.accountId, row.account_id) };
  }
  if (op === 'world.discovery.search') return discoveryOperation(m, args, actor);
}
