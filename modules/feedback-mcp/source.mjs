import { DatabaseSync } from 'node:sqlite';
import { createCipheriv, createDecipheriv, randomBytes } from 'node:crypto';
import { createSourceAppBff, createSourceNativeAuthorityPort, readVerifiedRootContext,
  STANDARD_ROOT_WORKSPACE_SOURCE, createSqliteSourceStorage } from '../source-app/server/index.mjs';
import { selectedResourceProfile } from '../apps/scoped-embed/resource-profile.mjs';
import { check, fields, digest, jsonCopy, syncResult } from '../source-app/server/wire.mjs';
import { callFeedbackTool } from './tools.mjs';

const reads = new Set(['projects_list', 'integration_get', 'tickets_list', 'ticket_get', 'attachment_chunk']);
const writes = new Set(['project_create', 'ticket_submit', 'ticket_reply', 'ticket_status', 'ticket_accept']);

// Only short Root-witness sessions live here. Native changes and their proofs
// remain in the service's own transaction; this store is never an effect ledger.
function openStorage(config, profile, clock) {
  const key = Buffer.from(config.storageKey), db = new DatabaseSync(config.databasePath);
  let closed = false;
  const current = () => check(!closed, 'feedback_source_closed', 503);
  function transaction(action) {
    current(); check(!db.isTransaction, 'feedback_source_nested_transaction', 503);
    db.exec('BEGIN IMMEDIATE');
    try { const result = syncResult(action()); db.exec('COMMIT'); return result; }
    catch (error) { if (db.isTransaction) db.exec('ROLLBACK'); throw error; }
  }
  try {
    db.exec('PRAGMA busy_timeout=2000; PRAGMA journal_mode=WAL; PRAGMA max_page_count=8192;');
    const version = db.prepare('PRAGMA user_version').get().user_version;
    check(version === 0 || version === 1, 'feedback_source_storage_version', 503);
    if (version === 0) transaction(() => { db.exec(`
      CREATE TABLE source_meta(profile_digest TEXT NOT NULL);
      CREATE TABLE source_nonces(nonce TEXT PRIMARY KEY,expires_at INTEGER NOT NULL) STRICT;
      CREATE TABLE source_sessions(id_hash TEXT PRIMARY KEY,native_session_hash TEXT NOT NULL,
        principal_id TEXT NOT NULL,resource_id TEXT NOT NULL,active INTEGER NOT NULL CHECK(active IN(0,1)),
        expires_at INTEGER NOT NULL,cipher TEXT NOT NULL,key_id TEXT NOT NULL) STRICT;
      PRAGMA user_version=1;`);
      db.prepare('INSERT INTO source_meta VALUES(?)').run(profile.digest);
    });
    check(db.prepare('SELECT profile_digest FROM source_meta').all().length === 1
      && db.prepare('SELECT profile_digest FROM source_meta').get().profile_digest === profile.digest,
    'feedback_source_storage_profile_changed', 503);
    const aad = (model, id, revision) => Buffer.from(JSON.stringify(['feedback-source.v1', profile.digest, config.storageKeyId, model, id, revision]));
    const storage = createSqliteSourceStorage({ database: db, current, clock, keyId: config.storageKeyId,
      transaction(action) { return transaction(() => {
        db.prepare('DELETE FROM source_sessions WHERE expires_at<=?').run(clock());
        check(db.prepare('SELECT count(*) n FROM source_sessions').get().n < 4096, 'feedback_source_capacity', 503);
        return action();
      }); },
      encrypt(model, id, revision, value) {
        current(); const iv = randomBytes(12), cipher = createCipheriv('aes-256-gcm', key, iv);
        cipher.setAAD(aad(model, id, revision));
        const bytes = Buffer.concat([cipher.update(JSON.stringify(jsonCopy(value))), cipher.final()]);
        return Buffer.concat([iv, cipher.getAuthTag(), bytes]).toString('base64');
      },
      decrypt(model, id, revision, encoded, keyId) {
        current(); check(keyId === config.storageKeyId, 'feedback_source_key_unavailable', 503);
        try {
          const bytes = Buffer.from(encoded, 'base64'); check(bytes.length > 28);
          const cipher = createDecipheriv('aes-256-gcm', key, bytes.subarray(0, 12));
          cipher.setAAD(aad(model, id, revision)); cipher.setAuthTag(bytes.subarray(12, 28));
          return jsonCopy(JSON.parse(Buffer.concat([cipher.update(bytes.subarray(28)), cipher.final()]).toString()));
        } catch { check(false, 'feedback_source_storage_corrupt', 503); }
      }
    });
    return { storage, sessionActive: id => {
      current(); const row = db.prepare('SELECT active,expires_at FROM source_sessions WHERE id_hash=?').get(id);
      return row?.active === 1 && row.expires_at > clock();
    }, close() { if (!closed) { closed = true; db.close(); key.fill(0); } } };
  } catch (error) { db.close(); key.fill(0); throw error; }
}

/** Optional trusted host assembly. No HTTP data can enable/configure Source. */
export function createFeedbackSource({ service, config, now = Date.now } = {}) {
  if (config === undefined) return Object.freeze({ enabled: false, async handleRequest() { return false; }, close() {} });
  const value = fields(config, ['profile', 'transportKey', 'connectorPort', 'databasePath', 'storageKey', 'storageKeyId']);
  check(typeof now === 'function' && typeof value.databasePath === 'string' && value.databasePath.length > 0
    && Buffer.isBuffer(value.storageKey) && value.storageKey.length === 32
    && Buffer.isBuffer(value.transportKey) && value.transportKey.length === 32
    && /^[A-Za-z0-9_.-]{1,64}$/u.test(value.storageKeyId)
    && Number.isInteger(value.connectorPort) && value.connectorPort > 0 && value.connectorPort <= 65535,
  'feedback_source_configuration_invalid', 503);
  const profile = selectedResourceProfile(value.profile);
  check(digest(profile.sourceProfile) === digest(STANDARD_ROOT_WORKSPACE_SOURCE)
    && profile.nativeOrigin === service.origin && profile.resource.selection.kind === 'soty.workspace.v1',
  'feedback_source_profile_invalid', 503);
  for (const name of ['identity', 'actor', 'execute', 'sourceContext', 'sourceExecute', 'sourceReceipt'])
    check(typeof service[name] === 'function', 'feedback_source_service_unavailable', 503);
  const store = openStorage(value, profile, now), proofs = new WeakMap(); let closed = false;
  function inspect(proof, binding) {
    const held = proofs.get(proof); check(!closed && held, 'feedback_source_authority_denied', 403);
    const context = readVerifiedRootContext(held.context, profile.digest);
    check(binding.identity.issuer === context.humanPrincipal.issuer && binding.identity.subject === context.humanPrincipal.subject
      && digest(binding.rootPrincipal) === digest(context.rootPrincipal) && digest(binding.resource) === digest(context.resource)
      && binding.rootReferenceDigest === digest(context.reference) && binding.instanceId === context.instanceId
      && binding.rootExpiresAt === context.expiresAt && context.expiresAt > now(), 'feedback_source_authority_denied', 403);
    if (binding.operation !== 'root-session') check(store.sessionActive(binding.sessionIdHash), 'authentication_required', 401);
    const projection = syncResult(service.sourceContext(held.actor));
    fields(projection, ['key', 'title', 'role', 'revision']);
    check(projection.key === held.actor.accountId && projection.role === 'owner', 'feedback_source_workspace_denied', 403);
    return { ...held, projection };
  }
  const native = createSourceNativeAuthorityPort({
    capture() { check(false, 'feedback_source_root_required', 403); },
    captureRoot(binding, context) {
      readVerifiedRootContext(context, profile.digest);
      check(binding.identity.issuer === context.humanPrincipal.issuer && binding.identity.subject === context.humanPrincipal.subject,
        'feedback_source_authority_denied', 403);
      const accountId = service.identity(context.humanPrincipal.issuer, context.humanPrincipal.subject);
      const actor = service.actor(accountId, { expiresAt: context.expiresAt,
        current: () => { try { readVerifiedRootContext(context, profile.digest); return !closed && context.expiresAt > now(); } catch { return false; } } });
      const proof = Object.freeze({}); proofs.set(proof, { actor, context }); inspect(proof, binding); return proof;
    },
    withCurrent(proof, binding, action) { inspect(proof, binding); const result = syncResult(action()); inspect(proof, binding); return result; },
    commitRootSession(proof, binding) {
      const { actor } = inspect(proof, binding);
      return { principalId: actor.accountId, resourceId: profile.resource.resourceId,
        nativeSessionHash: digest([actor.accountId, binding.rootReferenceDigest, binding.instanceId ?? null]) };
    },
    currentContext(proof, binding) {
      const { projection } = inspect(proof, binding);
      return { ...projection, generation: 1, expiresAt: binding.rootExpiresAt };
    },
    read(proof, binding, args) {
      const { actor } = inspect(proof, binding), { operation, ...input } = args.input;
      check(reads.has(operation), 'feedback_source_operation_denied', 403);
      if (operation === 'attachment_chunk') return syncResult(service.attachmentChunk(actor, input));
      const result = syncResult(callFeedbackTool(service, actor, 'feedback_' + operation, input)).structuredContent;
      // Root's bounded schema dialect has no nullable unions. Keep this wire
      // projection Source-only; direct MCP and domain records retain null.
      if (result.nextCursor === null) result.nextCursor = '';
      if (result.messagesPage?.nextOffset === null) result.messagesPage.nextOffset = -1;
      return result;
    },
    execute(proof, binding, args, final) {
      const { actor } = inspect(proof, binding);
      check(writes.has(args.input.operation) && !Object.hasOwn(args.input, 'requestId'), 'feedback_source_operation_denied', 403);
      return syncResult(service.sourceExecute(actor, { requestId: args.requestId, input: args.input }, final));
    },
    readProof(proof, binding, args) {
      const { actor } = inspect(proof, binding); fields(args.input, ['inputDigest']);
      return syncResult(service.sourceReceipt(actor, { requestId: args.requestId, inputDigest: args.input.inputDigest }));
    }
  });
  try {
    const bff = createSourceAppBff({ profile, transportKey: value.transportKey, connectorPort: value.connectorPort,
      storage: store.storage, native, clock: now });
    return Object.freeze({ enabled: true, handleRequest: (req, res) => bff.handleRequest(req, res),
      close() { if (!closed) { closed = true; bff.close(); store.close(); } } });
  } catch (error) { closed = true; store.close(); throw error; }
}
