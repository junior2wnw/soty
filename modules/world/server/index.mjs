import { DatabaseSync } from 'node:sqlite';
import { mkdirSync, realpathSync } from 'node:fs';
import { basename, dirname, resolve, sep } from 'node:path';
import { fileURLToPath } from 'node:url';
import { assert, identifier, folded, WorldError } from './validation.mjs';
import { migrateWorld, SCHEMA_VERSION } from './schema.mjs';
import { createModel } from './model.mjs';
import { PROFILE_OPERATIONS, profileOperation } from './profiles.mjs';
import { COMMUNITY_OPERATIONS, communityOperation } from './communities.mjs';
import { CHAT_OPERATIONS, chatOperation } from './chat.mjs';
import { AVATAR_OPERATIONS, avatarOperation } from './avatars.mjs';

export { WorldError, SCHEMA_VERSION };
export const WORLD_OPERATIONS = Object.freeze([...PROFILE_OPERATIONS, ...COMMUNITY_OPERATIONS, ...CHAT_OPERATIONS, ...AVATAR_OPERATIONS]);
const moduleRoot = dirname(dirname(fileURLToPath(import.meta.url)));
const READ_OPERATIONS = new Set(['world.profile.get', 'world.profile.view', 'world.discovery.search',
  'world.community.get', 'world.community.list', 'world.membership.list', 'world.chat.list', 'world.profile.avatar.read', 'world.profile.avatars']);
function physicalPath(file) {
  try { return realpathSync(file); }
  catch (error) { if (error.code !== 'ENOENT') throw error; return resolve(physicalPath(dirname(file)), basename(file)); }
}
function outsideModule(file) {
  const fold = value => process.platform === 'win32' ? value.toLowerCase() : value;
  const root = fold(physicalPath(moduleRoot));
  return ![file, physicalPath(file)].some(value => fold(value) === root || fold(value).startsWith(root + sep));
}

/** Trusted service boundary only. Mount as a Connect extension after proof/device/account verification.
 * Do not expose execute directly as an HTTP handler and do not construct actor from request arguments. */
export function createWorldService({ databasePath, projectId, clock = Date.now } = {}) {
  assert(typeof databasePath === 'string' && databasePath.length > 0, 'database_path_required');
  assert(typeof projectId === 'string' && /^[A-Za-z0-9][A-Za-z0-9._:-]{0,127}$/u.test(projectId), 'project_id_required');
  assert(typeof clock === 'function', 'invalid_clock');
  const file = databasePath === ':memory:' ? databasePath : resolve(databasePath);
  if (file !== ':memory:') {
    assert(outsideModule(file), 'database_must_be_outside_module');
    mkdirSync(dirname(file), { recursive: true, mode: 0o700 });
  }
  const db = new DatabaseSync(file); let closed = false, authorityFenceActive = false;
  try { migrateWorld(db, projectId); } catch (error) { db.close(); throw error; }
  db.function('world_fold', { deterministic: true }, folded);
  const subscribers = new Set(); let events = [];
  const m = createModel(db, event => { events.push(Object.freeze(event)); });
  const operations = new Set(WORLD_OPERATIONS);
  return {
    projectId, schemaVersion: SCHEMA_VERSION, operations,
    /** Host-only, synchronous authority boundary. Lock order is Connect -> World -> Apps.
     * Read predicates may use this connection; World operations and async work may not.
     * Only the downstream Apps transaction writes. This is not a cross-file commit. */
    withCommunityAuthorityFence(callback) {
      assert(!closed, 'service_closed');
      assert(typeof callback === 'function' && callback.constructor?.name !== 'AsyncFunction'
        && callback.constructor?.name !== 'AsyncGeneratorFunction', 'world_authority_callback_invalid');
      assert(!authorityFenceActive && !db.isTransaction, 'world_authority_fence_nested');
      const priorTimeout = Number(db.prepare('PRAGMA busy_timeout').get().timeout);
      let began = false;
      try {
        // A short SQLite wait policy, not a hard wall-clock deadline. No network
        // or await is allowed while blocking a membership writer.
        db.exec('PRAGMA busy_timeout=100; BEGIN IMMEDIATE'); began = true; authorityFenceActive = true;
        const result = callback();
        if (result && typeof result.then === 'function') {
          if (typeof result.catch === 'function') result.catch(() => {});
          throw new WorldError('world_authority_callback_async');
        }
        db.exec('COMMIT'); began = false;
        return result;
      } catch (error) {
        if (began && db.isTransaction) db.exec('ROLLBACK');
        if ([5, 6].includes(Number(error?.errcode) & 255)) throw new WorldError('world_authority_busy');
        throw error;
      } finally {
        authorityFenceActive = false;
        db.exec(`PRAGMA busy_timeout=${priorTimeout}`);
      }
    },
    execute({ op, args = {}, actor } = {}) {
      assert(!closed, 'service_closed'); assert(operations.has(op), 'unsupported_operation');
      assert(!authorityFenceActive, 'world_authority_mutation_forbidden');
      const now = clock(); assert(Number.isSafeInteger(now) && now >= 0, 'invalid_clock');
      events = [];
      // Existing profiles use a read snapshot for polling/discovery. They do not contend for
      // SQLite's writer lock; first-use provisioning and mutations acquire it up front.
      const known = actor && typeof actor.accountId === 'string' && m.person(actor.accountId);
      db.exec(READ_OPERATIONS.has(op) && known ? 'BEGIN' : 'BEGIN IMMEDIATE');
      let result;
      try {
        m.ensurePerson(actor, now);
        if (PROFILE_OPERATIONS.includes(op)) result = profileOperation(m, op, args, actor, now);
        else if (COMMUNITY_OPERATIONS.includes(op)) result = communityOperation(m, op, args, actor, now);
        else if (CHAT_OPERATIONS.includes(op)) result = chatOperation(m, op, args, actor, now);
        else result = avatarOperation(m, op, args, actor, now);
        if (result?.communityId && ['world.community.create', 'world.membership.transfer'].includes(op)) {
          const row = m.get("SELECT * FROM communities WHERE id=? AND state='active'", result.communityId);
          result = { ...result, community: row ? m.community(m.visibleGroup(row.id, actor.accountId), actor.accountId) : null };
        }
        assert(result !== undefined, 'unsupported_operation');
        db.exec('COMMIT');
      } catch (error) { db.exec('ROLLBACK'); events = []; throw error; }
      const committed = events; events = [];
      for (const event of committed) for (const listener of subscribers) {
        try { const pending = listener(event); if (pending && typeof pending.catch === 'function') pending.catch(() => {}); }
        catch { /* State is already durable. A failing consumer cannot undo revocation. */ }
      }
      return result;
    },
    canAccessCommunity(accountId, communityId) {
      assert(!closed, 'service_closed'); identifier(accountId); identifier(communityId);
      return m.canAccess(communityId, accountId);
    },
    activeCommunityIds(accountId) {
      assert(!closed, 'service_closed'); identifier(accountId);
      return m.all(`SELECT member.community_id FROM memberships member JOIN communities c ON c.id=member.community_id
        WHERE member.account_id=? AND member.state='active' AND c.state='active' ORDER BY member.community_id`, accountId).map(row => row.community_id);
    },
    appCommunityAuthority(accountId, ownerAccountId, relevantCommunityIds) {
      assert(!closed, 'service_closed');
      assert(authorityFenceActive, 'world_authority_fence_required');
      identifier(accountId); identifier(ownerAccountId);
      // At most1000 materialized audiences of64 groups plus the current head.
      // Restrict the JOIN to this bounded candidate set, not every World group.
      assert(Array.isArray(relevantCommunityIds) && relevantCommunityIds.length <= 65536, 'world_authority_candidates_invalid');
      const candidates = [...new Set(relevantCommunityIds.map(identifier))];
      if (candidates.length === 0) return Object.freeze([]);
      return Object.freeze(m.all(`SELECT member.community_id FROM memberships member
        JOIN memberships publisher ON publisher.community_id=member.community_id
        JOIN communities c ON c.id=member.community_id
        WHERE member.account_id=? AND member.state='active'
          AND publisher.account_id=? AND publisher.state='active' AND publisher.role IN('owner','moderator')
          AND c.state='active' AND member.community_id IN(SELECT value FROM json_each(?))
        ORDER BY member.community_id`, accountId, ownerAccountId, JSON.stringify(candidates)).map(row => row.community_id));
    },
    isGroupAdmin(accountId, communityId) {
      assert(!closed, 'service_closed'); identifier(accountId); identifier(communityId);
      return Boolean(m.get(`SELECT 1 FROM memberships member JOIN communities c ON c.id=member.community_id
        WHERE member.community_id=? AND member.account_id=? AND member.state='active' AND member.role IN('owner','moderator') AND c.state='active'`, communityId, accountId));
    },
    canRequestContact(accountId, targetId) {
      assert(!closed, 'service_closed'); identifier(accountId); identifier(targetId);
      return m.canRequestContact(accountId, targetId);
    },
    subscribeMembership(listener) {
      assert(!closed && typeof listener === 'function', 'invalid_listener'); subscribers.add(listener);
      return () => { subscribers.delete(listener); };
    },
    close() { assert(!authorityFenceActive, 'world_authority_fence_active'); if (!closed) { closed = true; subscribers.clear(); db.close(); } },
  };
}
