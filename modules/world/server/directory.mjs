import { createHash } from 'node:crypto';
import { assert, choice, exact, identifier, limit, text, WorldError } from './validation.mjs';

export const DIRECTORY_OPERATIONS = Object.freeze(['world.directory.search', 'world.directory.resolve']);
const schema = 'soty.world-directory.v1';
const digest = value => createHash('sha256').update(JSON.stringify(value)).digest('hex');
const fold = value => value.normalize('NFKD').replace(/\p{M}/gu, '').toLocaleLowerCase('ru');
const tokens = value => fold(value).match(/[\p{L}\p{N}]+/gu) ?? [];

/** Lexical prefix matching for the caller's private groups, never a global private index. */
export function directoryMatches(value, query) {
  const required = tokens(query), words = tokens(value);
  return Number(required.every(term => words.some(word => word.startsWith(term))));
}

function assertAccount(args, actor) {
  identifier(args.expectedAccountId);
  assert(args.expectedAccountId === actor.accountId, 'directory_account_changed');
}
function cursorFor(scope, after) {
  return Buffer.from(JSON.stringify({ version: 1, scope, kind: after.kind, id: after.entity_id })).toString('base64url');
}
function readCursor(value, scope) {
  if (value === undefined || value === null) return { kind: '', id: '' };
  assert(typeof value === 'string' && value.length <= 768 && /^[A-Za-z0-9_-]+$/u.test(value), 'invalid_directory_cursor');
  try {
    const parsed = JSON.parse(Buffer.from(value, 'base64url').toString('utf8'));
    exact(parsed, ['version', 'scope', 'kind', 'id']);
    assert(parsed.version === 1 && parsed.scope === scope && ['person', 'community'].includes(parsed.kind), 'invalid_directory_cursor');
    identifier(parsed.id);
    assert(cursorFor(scope, { kind: parsed.kind, entity_id: parsed.id }) === value, 'invalid_directory_cursor');
    return { kind: parsed.kind, id: parsed.id };
  } catch { throw new WorldError('invalid_directory_cursor'); }
}

function visibleCommunity(m, id, actorId) {
  return m.get(`SELECT c.* FROM communities c LEFT JOIN memberships member
    ON member.community_id=c.id AND member.account_id=?
    WHERE c.id=? AND c.state='active' AND (c.join_policy<>'invite' OR member.state IN('active','invited'))`, actorId, id);
}

export function directoryOperation(m, op, args, actor) {
  if (op === 'world.directory.resolve') {
    exact(args, ['expectedAccountId', 'entities']); assertAccount(args, actor);
    assert(Array.isArray(args.entities) && args.entities.length <= 60, 'invalid_directory_entities');
    const refs = args.entities.map(ref => {
      exact(ref, ['kind', 'id']); choice(ref.kind, ['person', 'community']); identifier(ref.id);
      return { kind: ref.kind, id: ref.id };
    });
    return { schema, items: refs.map(ref => {
      if (ref.kind === 'person') {
        const row = m.person(ref.id);
        if (!row) return { ref, available: false };
        if (row.account_id === actor.accountId || row.discoverable) {
          return { ref, available: true, person: m.profile(row, row.account_id === actor.accountId),
            access: row.account_id === actor.accountId ? 'owner' : 'public' };
        }
        const roster = m.get(`SELECT 1 FROM memberships mine JOIN memberships target ON target.community_id=mine.community_id
          JOIN communities c ON c.id=mine.community_id WHERE mine.account_id=? AND target.account_id=?
          AND mine.state='active' AND target.state='active' AND c.state='active' LIMIT 1`, actor.accountId, ref.id);
        // Same authority as membership.list(active), but do not disclose the
        // shared group's identity or hidden bio/interests to this resolver.
        return roster ? { ref, available: true, access: 'member', person: { profileId: row.account_id,
          displayName: row.display_name, avatarColor: row.avatar_color, avatarRevision: row.avatar_revision ?? null, revision: row.revision } }
          : { ref, available: false };
      }
      const row = visibleCommunity(m, ref.id, actor.accountId);
      return row ? { ref, available: true, community: m.community(row, actor.accountId) } : { ref, available: false };
    }) };
  }
  exact(args, ['expectedAccountId'], ['query', 'kind', 'scope', 'limit', 'cursor']); assertAccount(args, actor);
  const query = text(args.query ?? '', 100);
  assert(query.isWellFormed(), 'invalid_text');
  const kind = choice(args.kind ?? 'all', ['all', 'people', 'communities']);
  const visibility = choice(args.scope ?? 'all', ['all', 'mine', 'public']);
  const entityKind = kind === 'people' ? 'person' : kind === 'communities' ? 'community' : null;
  const size = limit(args.limit), terms = tokens(query);
  const scope = digest([schema, actor.accountId, actor.deviceId, query, kind, visibility]);
  const after = readCursor(args.cursor, scope);
  // Only the existing discoverable index and this caller's memberships enter the
  // candidate set. The seek predicate uses immutable identities, not page offsets.
  const match = terms.length ? `search_text : (${terms.map(term => `"${term}"*`).join(' AND ')})` : '';
  const publicQuery = !query ? '1' : match
    ? '(d.entity_id=? OR d.seq IN (SELECT rowid FROM world_directory_fts WHERE world_directory_fts MATCH ?))'
    : 'd.entity_id=?';
  const privateQuery = !query ? '1' : terms.length ? '(c.id=? OR world_directory_match(c.search_text,?)=1)' : 'c.id=?';
  const publicArgs = !query ? [] : match ? [query, match] : [query];
  const privateArgs = !query ? [] : terms.length ? [query, query] : [query];
  const rows = m.all(`SELECT * FROM (
      SELECT d.kind,d.entity_id FROM world_directory d
        WHERE (? IS NULL OR d.kind=?) AND ${publicQuery}
          AND (?<>'mine' OR (d.kind='community' AND EXISTS(SELECT 1 FROM memberships mine
            WHERE mine.community_id=d.entity_id AND mine.account_id=? AND mine.state IN('active','requested','invited'))))
      UNION
      SELECT 'community' AS kind,c.id AS entity_id FROM memberships member JOIN communities c ON c.id=member.community_id
        WHERE member.account_id=? AND member.state IN('active','invited') AND c.state='active' AND c.join_policy='invite'
          AND (? IS NULL OR ?='community') AND ${privateQuery}
          AND ?<>'public'
    ) WHERE (kind>? OR (kind=? AND entity_id>?)) ORDER BY kind,entity_id LIMIT ?`,
  entityKind, entityKind, ...publicArgs, visibility, actor.accountId,
  actor.accountId, entityKind, entityKind, ...privateArgs, visibility, after.kind, after.kind, after.id, size + 1);
  const page = rows.slice(0, size);
  return { schema,
    people: page.filter(row => row.kind === 'person').map(row => m.profile(m.person(row.entity_id))),
    communities: page.filter(row => row.kind === 'community').map(row => m.community(m.group(row.entity_id), actor.accountId)),
    nextCursor: rows.length > size ? cursorFor(scope, page.at(-1)) : null };
}
