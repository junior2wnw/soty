import { DatabaseSync } from 'node:sqlite';
import { mkdirSync, realpathSync } from 'node:fs';
import { basename, dirname, resolve, sep } from 'node:path';
import { fileURLToPath } from 'node:url';
import { assert, integer, identifier, folded, revision as expectedRevision, WorldError } from './validation.mjs';
import { migrateWorld, SCHEMA_VERSION } from './schema.mjs';
import { createModel } from './model.mjs';
import { PROFILE_OPERATIONS, profileOperation } from './profiles.mjs';
import { COMMUNITY_OPERATIONS, communityOperation } from './communities.mjs';
import { CHAT_OPERATIONS, chatOperation } from './chat.mjs';
import { AVATAR_OPERATIONS, avatarOperation } from './avatars.mjs';
import { DIRECTORY_OPERATIONS, directoryMatches, directoryOperation } from './directory.mjs';
import { FIELD_OPERATIONS, fieldOperation, migrateField } from './field.mjs';
import { DIRECT_OPERATIONS, DIRECT_READ_OPERATIONS, directOperation } from './direct.mjs';
import { ATTACHMENT_OPERATIONS, ATTACHMENT_READ_OPERATIONS, attachmentOperation } from './attachments.mjs';
import { createCoverStore } from '../../cover-art/server/index.mjs';
import { createLegacyAvatarAdapter } from '../../cover-art/server/legacy-avatar.mjs';
import { profileAvatarStoreId } from './schema.mjs';

export { WorldError, SCHEMA_VERSION };
export const WORLD_OPERATIONS = Object.freeze([...PROFILE_OPERATIONS, ...COMMUNITY_OPERATIONS, ...CHAT_OPERATIONS, ...AVATAR_OPERATIONS, ...DIRECTORY_OPERATIONS, ...FIELD_OPERATIONS, ...DIRECT_OPERATIONS, ...ATTACHMENT_OPERATIONS]);
const moduleRoot = dirname(dirname(fileURLToPath(import.meta.url)));
const READ_OPERATIONS = new Set(['world.profile.get', 'world.profile.view', 'world.discovery.search',
  ...DIRECTORY_OPERATIONS,
  'world.field.get', ...DIRECT_READ_OPERATIONS,
  ...ATTACHMENT_READ_OPERATIONS,
  'world.community.get', 'world.community.list', 'world.membership.list', 'world.chat.list', 'world.profile.avatar.read', 'world.profile.avatars']);
export const WORLD_PROFILE_COVER_OPERATIONS = Object.freeze(['world.profile.cover.status', 'world.profile.cover.prepare',
  'world.profile.cover.prepareFromVersion', 'world.profile.cover.commit', 'world.profile.cover.restore', 'world.profile.cover.remove',
  'world.profile.cover.read', 'world.profile.cover.versions', 'world.profile.cover.cancel']);
function coverArguments(value, required, optional = []) {
  assert(value && typeof value === 'object' && [Object.prototype, null].includes(Object.getPrototypeOf(value)), 'invalid_arguments');
  const descriptors = Object.getOwnPropertyDescriptors(value), allowed = new Set([...required, ...optional]);
  assert(Reflect.ownKeys(descriptors).every(key => typeof key === 'string' && allowed.has(key)
    && Object.hasOwn(descriptors[key], 'value') && descriptors[key].enumerable === true)
    && required.every(key => Object.hasOwn(descriptors, key)), 'invalid_arguments');
}
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
export function createWorldService({ databasePath, projectId, clock = Date.now, withCurrentContacts, allowAudienceMigration = false, allowWorld6Migration = false, allowWorld7Migration = false, profileCoverDecoder, profileCoverRegistered = () => false } = {}) {
  assert(typeof databasePath === 'string' && databasePath.length > 0, 'database_path_required');
  assert(typeof projectId === 'string' && /^[A-Za-z0-9][A-Za-z0-9._:-]{0,127}$/u.test(projectId), 'project_id_required');
  assert(typeof clock === 'function', 'invalid_clock');
  assert(withCurrentContacts === undefined || typeof withCurrentContacts === 'function', 'invalid_contacts_authority');
  assert(typeof allowAudienceMigration === 'boolean', 'invalid_audience_migration_mode');
  assert(typeof allowWorld6Migration === 'boolean', 'invalid_world6_migration_mode');
  assert(typeof allowWorld7Migration === 'boolean', 'invalid_world7_migration_mode');
  assert(profileCoverDecoder === undefined || profileCoverDecoder && typeof profileCoverDecoder.decode === 'function', 'invalid_profile_cover_decoder');
  assert(typeof profileCoverRegistered === 'function' && !['AsyncFunction', 'AsyncGeneratorFunction'].includes(profileCoverRegistered.constructor?.name), 'invalid_profile_cover_registry');
  const file = databasePath === ':memory:' ? databasePath : resolve(databasePath);
  if (file !== ':memory:') {
    assert(outsideModule(file), 'database_must_be_outside_module');
    mkdirSync(dirname(file), { recursive: true, mode: 0o700 });
  }
  const db = new DatabaseSync(file); let closed = false, authorityFenceActive = false, storageVersion;
  try {
    const migrationNow = clock(); assert(Number.isSafeInteger(migrationNow) && migrationNow >= 0, 'invalid_clock');
    storageVersion = migrateWorld(db, projectId, { allowAudienceMigration, allowWorld6Migration, allowWorld7Migration, now: Math.max(1, migrationNow) }); migrateField(db);
  } catch (error) { db.close(); throw error; }
  db.function('world_fold', { deterministic: true }, folded);
  db.function('world_directory_match', { deterministic: true }, directoryMatches);
  const subscribers = new Set(), messageSubscribers = new Set(); let events = [], currentContactView = null;
  let profileCoverStore, legacyAvatarAdapter, profileCoverEnabled = false, coverOperation = null;
  const m = createModel(db, event => { events.push(Object.freeze(event)); }, () => currentContactView,
    () => profileCoverReady());
  m.schemaVersion = storageVersion;
  function profileCoverReady() {
    if (closed || !profileCoverEnabled || !profileCoverStore) return false;
    try { return WORLD_PROFILE_COVER_OPERATIONS.every(operation => profileCoverRegistered(operation) === true); } catch { return false; }
  }
  function contactsFor(actor, contacts) {
    if (withCurrentContacts) assert(contacts && contacts.accountId === actor?.accountId && contacts.deviceId === actor?.deviceId
      && typeof contacts.has === 'function' && typeof contacts.accountIds === 'function', 'world_contacts_actor_mismatch');
  }
  function notificationMessage(event) {
    if (!event || typeof event !== 'object' || Object.keys(event).sort().join(',') !== 'conversationId,kind,messageId'
      || !['room', 'direct'].includes(event.kind)) return null;
    try { identifier(event.conversationId); identifier(event.messageId); } catch { return null; }
    return event.kind === 'room' ? m.get(`SELECT message.* FROM messages message JOIN communities room ON room.id=message.community_id
      WHERE message.id=? AND message.community_id=? AND message.deleted_at IS NULL AND room.state='active'`, event.messageId, event.conversationId)
      : m.get(`SELECT message.*,thread.account_low,thread.account_high,thread.relationship_id AS current_relationship_id
        FROM world_direct_messages message JOIN world_direct_threads thread ON thread.id=message.thread_id
        WHERE message.id=? AND message.thread_id=? AND message.deleted_at IS NULL AND message.relationship_id=thread.relationship_id`, event.messageId, event.conversationId);
  }
  function withProfileAuthority(actor, callback) {
    assert(!closed, 'service_closed'); assert(!authorityFenceActive && !db.isTransaction, 'world_authority_fence_nested');
    const enter = contacts => {
      contactsFor(actor, contacts); const prior = currentContactView; currentContactView = contacts;
      try { return callback(); } finally { currentContactView = prior; }
    };
    return withCurrentContacts ? withCurrentContacts(actor, enter) : enter(null);
  }
  function messageNotificationTarget(actor, event, delivery) {
    assert(!closed, 'service_closed');
    try { identifier(actor?.accountId); identifier(actor?.deviceId); } catch { return null; }
    return withProfileAuthority(actor, () => {
      db.exec('BEGIN');
      try {
        const item = notificationMessage(event); if (!item || item.author_id === actor.accountId) return null;
        if (event.kind === 'room') {
          const member = m.membership(item.community_id, actor.accountId);
          return member?.state === 'active' && (!delivery || !member.muted && member.last_read_seq < item.seq)
            ? { kind: 'room', communityId: item.community_id } : null;
        }
        const peer = item.account_low === actor.accountId ? item.account_high : item.account_high === actor.accountId ? item.account_low : null;
        if (!peer || typeof currentContactView?.relationshipId !== 'function' || currentContactView.relationshipId(peer) !== item.current_relationship_id) return null;
        const prefs = delivery ? m.get('SELECT * FROM world_direct_read_state WHERE thread_id=? AND account_id=?', item.thread_id, actor.accountId) : null;
        return !delivery || !prefs?.muted && (prefs?.last_read_seq ?? 0) < item.seq
          ? { kind: 'direct', threadId: item.thread_id, peerAccountId: peer, relationshipId: item.current_relationship_id } : null;
      } finally { db.exec('COMMIT'); }
    });
  }
  async function profileCoverExecute({ op, args = {}, actor } = {}) {
    assert(profileCoverReady() && WORLD_PROFILE_COVER_OPERATIONS.includes(op), 'profile_cover_unavailable');
    const common = ['expectedAccountId', 'targetId'], expected = ['requestId', 'expectedTargetRevision', 'expectedPolicyEpoch', 'expectedCoverRevision'];
    if (op === 'world.profile.cover.prepare') coverArguments(args, [...common, ...expected, 'mime', 'dataBase64'], ['crop']);
    else if (op === 'world.profile.cover.prepareFromVersion') coverArguments(args, [...common, ...expected, 'versionId'], ['crop']);
    else if (op === 'world.profile.cover.commit' || op === 'world.profile.cover.cancel') coverArguments(args, [...common, 'requestId', 'draftId']);
    else if (op === 'world.profile.cover.restore') coverArguments(args, [...common, ...expected, 'versionId']);
    else if (op === 'world.profile.cover.remove') coverArguments(args, [...common, ...expected], ['mode']);
    else if (op === 'world.profile.cover.read') coverArguments(args, common, ['versionId', 'width']);
    else if (op === 'world.profile.cover.versions') coverArguments(args, common, ['limit']);
    else coverArguments(args, common, ['requestId', 'draftId']);
    identifier(args.expectedAccountId); identifier(args.targetId);
    assert(args.expectedAccountId === actor?.accountId, 'profile_account_changed');
    const { expectedAccountId: _account, ...input } = args;
    if (op === 'world.profile.cover.prepare') {
      assert(['image/png', 'image/jpeg', 'image/webp'].includes(input.mime), 'invalid_avatar_mime');
      assert(typeof input.dataBase64 === 'string' && input.dataBase64.length > 0 && input.dataBase64.length <= Math.ceil(320 * 1024 / 3) * 4
        && /^[A-Za-z0-9+/]+={0,2}$/u.test(input.dataBase64), 'cover_input_limit');
      const bytes = Buffer.from(input.dataBase64, 'base64');
      assert(bytes.length > 0 && bytes.length <= 320 * 1024 && bytes.toString('base64') === input.dataBase64, 'cover_input_limit');
      const { dataBase64: _bytes, ...value } = input;
      return profileCoverStore.prepareUpload(actor, { ...value, bytes });
    }
    if (op === 'world.profile.cover.prepareFromVersion') return profileCoverStore.prepareFromVersion(actor, input);
    if (op === 'world.profile.cover.read') {
      if (input.versionId !== undefined) identifier(input.versionId);
      const result = profileCoverStore.read(actor, input), image = result.image;
      return { ...result, image: image ? { role: image.role, width: image.width, height: image.height, mime: image.mime,
        sha256: image.sha256, byteCount: image.bytes.length, dataBase64: Buffer.from(image.bytes).toString('base64') } : null };
    }
    if (op === 'world.profile.cover.status') {
      if (input.requestId !== undefined) assert(typeof input.requestId === 'string' && /^cover1\.[0-9]{13}\.[a-f0-9]{32}$/u.test(input.requestId), 'cover_request_invalid');
      if (input.draftId !== undefined) identifier(input.draftId);
      return profileCoverStore.status(actor, input);
    }
    if (op === 'world.profile.cover.versions') return profileCoverStore.versions(actor, input);
    if (op === 'world.profile.cover.cancel') return profileCoverStore.cancel(actor, input);
    const prior = coverOperation; coverOperation = op;
    try {
      if (op === 'world.profile.cover.commit') return profileCoverStore.commit(actor, input);
      if (op === 'world.profile.cover.restore') return profileCoverStore.restore(actor, input);
      return profileCoverStore.remove(actor, input);
    } finally { coverOperation = prior; }
  }
  const operations = new Set(WORLD_OPERATIONS);
  const service = {
    projectId, schemaVersion: storageVersion, operations, profileCoverExtension: undefined,
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
      if (op === 'world.profile.avatar.set') {
        m.validateActor(actor);
        coverArguments(args, ['expectedRevision', 'avatarUrl', 'thumbnailUrl']); integer(args.expectedRevision, 1);
        // Common writer owns its own World transaction and fresh contact fence.
        // Never enter the old World BEGIN before this synchronous legacy call.
        const prior = coverOperation; coverOperation = op;
        try { return legacyAvatarAdapter.set(actor, args); } finally { coverOperation = prior; }
      }
      const perform = contacts => {
        if (withCurrentContacts) assert(contacts && contacts.accountId === actor?.accountId && contacts.deviceId === actor?.deviceId
          && typeof contacts.has === 'function' && typeof contacts.accountIds === 'function', 'world_contacts_actor_mismatch');
        const priorContactView = currentContactView; currentContactView = contacts;
        try {
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
            else if (DIRECTORY_OPERATIONS.includes(op)) result = directoryOperation(m, op, args, actor);
            else if (FIELD_OPERATIONS.includes(op)) result = fieldOperation(m, op, args, actor, now);
            else if (DIRECT_OPERATIONS.includes(op)) {
              assert(withCurrentContacts && contacts && typeof contacts.relationshipId === 'function', 'direct_authority_unavailable');
              result = directOperation(m, op, args, actor, now, { projectId, relationshipId: peer => contacts.relationshipId(peer) });
            }
            else if (ATTACHMENT_OPERATIONS.includes(op)) result = attachmentOperation(m, op, args, actor, now,
              contacts && typeof contacts.relationshipId === 'function' ? { relationshipId: peer => contacts.relationshipId(peer) } : null);
            else result = avatarOperation(m, op, args, actor, now);
            if (op === 'world.profile.get' && storageVersion === 7) result.profile.chatAttachmentsVersion = 1;
            if (result?.communityId && ['world.community.create', 'world.membership.transfer'].includes(op)) {
              const row = m.get("SELECT * FROM communities WHERE id=? AND state='active'", result.communityId);
              result = { ...result, community: row ? m.community(m.visibleGroup(row.id, actor.accountId), actor.accountId) : null };
            }
            assert(result !== undefined, 'unsupported_operation');
            db.exec('COMMIT');
          } catch (error) { db.exec('ROLLBACK'); events = []; throw error; }
          const committed = events; events = [];
          for (const event of committed) for (const listener of ['room', 'direct'].includes(event.kind) ? messageSubscribers : subscribers) {
            try { const pending = listener(event); if (pending && typeof pending.catch === 'function') pending.catch(() => {}); }
            catch { /* State is already durable. A failing consumer cannot undo revocation. */ }
          }
          return result;
        } finally { currentContactView = priorContactView; }
      };
      // The live Connect fence is entered before taking any World snapshot or writer lock.
      return withCurrentContacts ? withCurrentContacts(actor, perform) : perform(null);
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
    notificationRecipients(event) {
      assert(!closed, 'service_closed'); const item = notificationMessage(event); if (!item) return [];
      if (event.kind === 'room') return m.all(`SELECT account_id FROM memberships WHERE community_id=? AND state='active'
        AND account_id<>? AND muted=0 AND last_read_seq<? ORDER BY account_id`, item.community_id, item.author_id, item.seq).map(row => row.account_id);
      const peer = item.author_id === item.account_low ? item.account_high : item.account_low;
      const prefs = m.get('SELECT * FROM world_direct_read_state WHERE thread_id=? AND account_id=?', item.thread_id, peer);
      return prefs?.muted || (prefs?.last_read_seq ?? 0) >= item.seq ? [] : [peer];
    },
    canNotify(actor, event) { return messageNotificationTarget(actor, event, true); },
    notificationTarget(actor, event) { return messageNotificationTarget(actor, event, false); },
    subscribeMessages(listener) {
      assert(!closed && typeof listener === 'function', 'invalid_listener'); messageSubscribers.add(listener);
      return () => { messageSubscribers.delete(listener); };
    },
    subscribeMembership(listener) {
      assert(!closed && typeof listener === 'function', 'invalid_listener'); subscribers.add(listener);
      return () => { subscribers.delete(listener); };
    },
    close() { assert(!authorityFenceActive, 'world_authority_fence_active'); if (!closed) { closed = true; profileCoverStore?.dispose(); subscribers.clear(); messageSubscribers.clear(); db.close(); } },
  };
  try {
    // Legacy set needs no native decode. New upload is advertised only when the
    // host provides a real decoder AND the fresh Connect relationship bridge.
    const unavailableDecoder = { decode() { throw new WorldError('profile_cover_unavailable'); } };
    profileCoverStore = createCoverStore({ db, kind: 'profile', instanceId: profileAvatarStoreId(db, projectId),
      decoder: profileCoverDecoder ?? unavailableDecoder, clock, withAuthority: withProfileAuthority,
      readTarget(actor, targetId) {
        const row = m.person(targetId); if (!row) return null;
        return { ownerAccountId: row.account_id, targetRevision: row.revision, policyEpoch: row.revision,
          state: 'active', canManage: row.account_id === actor.accountId, canRead: m.canViewFullProfile(row, actor.accountId) };
      },
      onCommit({ actor, targetId, mode, now }) {
        assert(db.isTransaction && targetId === actor.accountId, 'profile_cover_owner_mismatch');
        const changed = m.run(`UPDATE profiles SET avatar_revision=CASE WHEN ?='custom' THEN revision+1 ELSE NULL END,
          revision=revision+1,updated_at=? WHERE account_id=? AND revision<?`, mode, now, targetId, Number.MAX_SAFE_INTEGER);
        assert(changed.changes === 1, 'profile_revision_exhausted');
        m.audit(actor.accountId, coverOperation ?? 'world.profile.cover.commit', targetId, now);
      },
      projectOwner(actor, targetId) { assert(actor.accountId === targetId, 'profile_cover_owner_mismatch'); return m.profile(m.person(targetId), true); },
    });
    legacyAvatarAdapter = createLegacyAvatarAdapter({ store: { legacyCommit(actor, input) {
      // First-use provisioning is the same private own profile as profile.get.
      // Validation/CAS for a nonexistent profile happens before its provisioning.
      if (!m.person(actor.accountId)) { expectedRevision({ expectedRevision: input.expectedRevision }, { revision: 1 });
        service.execute({ op: 'world.profile.get', args: {}, actor }); }
      return profileCoverStore.legacyCommit(actor, input);
    } } });
    profileCoverEnabled = Boolean(profileCoverDecoder && withCurrentContacts);
    if (profileCoverEnabled) service.profileCoverExtension = Object.freeze({ operations: new Set(WORLD_PROFILE_COVER_OPERATIONS), executeAsync: profileCoverExecute });
  } catch (error) { profileCoverStore?.dispose(); db.close(); throw error; }
  return service;
}
