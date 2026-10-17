import { assert, exact, identifier, text, tags, boolean, choice, revision, folded } from './validation.mjs';
import { discoveryOperation } from './discovery.mjs';

const FIELDS = ['displayName', 'bio', 'interests', 'discoverable', 'showPresence', 'showMemberships', 'contactPolicy', 'avatarColor'];
export const PROFILE_OPERATIONS = ['world.profile.get', 'world.profile.update', 'world.profile.view', 'world.discovery.search'];
export function profileOperation(m, op, args, actor, now) {
  if (op === 'world.profile.get') { exact(args); return { profile: m.profile(m.person(actor.accountId), true) }; }
  if (op === 'world.profile.update') {
    exact(args, ['expectedRevision'], FIELDS);
    const row = m.person(actor.accountId); revision(args, row);
    assert(FIELDS.some(key => Object.hasOwn(args, key)), 'empty_update');
    const values = { display_name: args.displayName === undefined ? row.display_name : text(args.displayName, 80, { empty: false }),
      bio: args.bio === undefined ? row.bio : text(args.bio, 500, { multiline: true }),
      interests: args.interests === undefined ? row.interests : JSON.stringify(tags(args.interests)),
      discoverable: args.discoverable === undefined ? row.discoverable : boolean(args.discoverable),
      show_presence: args.showPresence === undefined ? row.show_presence : boolean(args.showPresence),
      show_memberships: args.showMemberships === undefined ? row.show_memberships : boolean(args.showMemberships),
      contact_policy: args.contactPolicy === undefined ? row.contact_policy : choice(args.contactPolicy, ['everyone', 'members', 'nobody']),
      avatar_color: args.avatarColor === undefined ? row.avatar_color : args.avatarColor };
    assert(/^#[0-9a-fA-F]{6}$/u.test(values.avatar_color), 'invalid_color');
    m.run(`UPDATE profiles SET display_name=?,bio=?,interests=?,discoverable=?,show_presence=?,show_memberships=?,contact_policy=?,avatar_color=?,
      search_text=?,revision=revision+1,updated_at=? WHERE account_id=?`, values.display_name, values.bio, values.interests,
      values.discoverable, values.show_presence, values.show_memberships, values.contact_policy, values.avatar_color,
      folded([values.display_name, values.bio, ...JSON.parse(values.interests)].join(' ')), now, actor.accountId);
    m.audit(actor.accountId, op, actor.accountId, now);
    return { profile: m.profile(m.person(actor.accountId), true) };
  }
  if (op === 'world.profile.view') {
    exact(args, ['profileId']);
    const row = m.person(identifier(args.profileId));
    assert(row && (row.account_id === actor.accountId || row.discoverable), 'profile_not_found');
    const groups = row.show_memberships ? m.all(`SELECT c.* FROM communities c JOIN memberships member ON member.community_id=c.id
      WHERE member.account_id=? AND member.state='active' AND member.show_in_profile=1 AND c.state='active' AND c.join_policy<>'invite'
      ORDER BY c.name,c.id LIMIT 60`, row.account_id).map(item => m.community(item, actor.accountId)) : [];
    return { profile: m.profile(row, row.account_id === actor.accountId), communities: groups,
      canRequestContact: m.canRequestContact(actor.accountId, row.account_id) };
  }
  if (op === 'world.discovery.search') return discoveryOperation(m, args, actor);
}
