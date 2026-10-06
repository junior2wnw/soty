import { DatabaseSync } from 'node:sqlite';
import { createHash, randomBytes } from 'node:crypto';
import { mkdirSync } from 'node:fs';
import { dirname, resolve } from 'node:path';
import { canonicalContractJson, contractDigest } from '../../app-contract/json.mjs';
import { initializeFeedbackSchema } from './schema.mjs';
import { DEFAULT_FEEDBACK_PROFILE, FEEDBACK_LIMITS } from './profile.mjs';
import { validateFeedbackAttachments } from './media.mjs';

export class FeedbackError extends Error {
  constructor(code, status = 400) { super(code); this.name = 'FeedbackError'; this.code = code; this.status = status; }
}
const requireThat = (ok, code = 'feedback_invalid_arguments', status) => { if (!ok) throw new FeedbackError(code, status); };
const object = value => value && typeof value === 'object' && !Array.isArray(value);
const exact = (value, keys, required = keys) => requireThat(object(value) && Object.keys(value).every(key => keys.includes(key))
  && required.every(key => Object.hasOwn(value, key)));
const id = value => { requireThat(typeof value === 'string' && /^[A-Za-z0-9][A-Za-z0-9_.:-]{0,127}$/.test(value)); return value; };
const body = value => { requireThat(typeof value === 'string' && value.isWellFormed() && value.trim().length > 0
  && value.length <= FEEDBACK_LIMITS.bodyChars && !/[\u0000-\u0008\u000b\u000c\u000e-\u001f\u007f]/u.test(value), 'feedback_invalid_body'); return value; };
const hash = value => createHash('sha256').update(value).digest('hex');
const newId = prefix => prefix + randomBytes(16).toString('hex');
const positive = value => requireThat(Number.isSafeInteger(value) && value >= 1, 'feedback_invalid_revision');
const sync = value => {
  if (value && typeof value.then === 'function') { Promise.resolve(value).catch(() => {}); throw new FeedbackError('feedback_async_authority', 500); }
  return value;
};
const same = (left, right) => canonicalContractJson(left) === canonicalContractJson(right);
export const feedbackOperations = Object.freeze(['apps.feedback.context', 'apps.feedback.submit', 'apps.feedback.list',
  'apps.feedback.get', 'apps.feedback.reply', 'apps.feedback.status', 'apps.feedback.accept']);

/** Verified Connect actor + Apps participant/source fence are supplied by the
 * host. Neither installation IDs nor actor fields from a body grant access. */
export function createFeedbackService({ databasePath, registryId = 'soty', environmentId = 'production',
  actorActive, withAppAuthority, now = Date.now, limits = {} } = {}) {
  requireThat(typeof databasePath === 'string' && typeof actorActive === 'function' && typeof withAppAuthority === 'function', 'feedback_host_required', 500);
  id(registryId); id(environmentId);
  exact(limits, ['maxDatabaseBytes'], []);
  const maxDatabaseBytes = limits.maxDatabaseBytes ?? 64 * 1024 * 1024;
  requireThat(Number.isSafeInteger(maxDatabaseBytes) && maxDatabaseBytes >= 256 * 1024
    && maxDatabaseBytes <= 64 * 1024 * 1024, 'feedback_host_limits_invalid', 500);
  const filename = databasePath === ':memory:' ? databasePath : resolve(databasePath);
  if (filename !== ':memory:') mkdirSync(dirname(filename), { recursive: true, mode: 0o700 });
  const db = new DatabaseSync(filename); let closed = false;
  let maxPages;
  try {
    db.exec('PRAGMA foreign_keys=ON;PRAGMA busy_timeout=100;'); initializeFeedbackSchema(db, registryId, environmentId);
    db.exec('PRAGMA journal_mode=WAL;');
    maxPages = Math.floor(maxDatabaseBytes / Number(db.prepare('PRAGMA page_size').get().page_size));
    // An existing larger store remains readable. It cannot accept new effects
    // under a smaller operational budget, and nothing is deleted to make room.
    db.exec(`PRAGMA max_page_count=${maxPages}`);
  }
  catch (error) { db.close(); throw error; }
  const operations = new Set(feedbackOperations);
  function writeCapacity() { requireThat(Number(db.prepare('PRAGMA page_count').get().page_count) < maxPages, 'feedback_storage_capacity', 503); }
  function authenticate(actor) { requireThat(!closed, 'feedback_closed', 503); requireThat(actor && sync(actorActive(actor)) === true, 'feedback_authentication_required', 401); id(actor.accountId); id(actor.deviceId); }
  function transaction(callback) {
    requireThat(!db.isTransaction, 'feedback_nested_transaction', 500);
    db.exec('BEGIN IMMEDIATE');
    try { const result = sync(callback()); db.exec('COMMIT'); return result; }
    catch (error) { if (db.isTransaction) db.exec('ROLLBACK');
      if ([5, 6].includes(Number(error.errcode) & 255)) throw new FeedbackError('feedback_busy', 503);
      if ((Number(error.errcode) & 255) === 13) throw new FeedbackError('feedback_storage_capacity', 503);
      throw error; }
  }
  const installation = key => db.prepare('SELECT * FROM feedback_installations WHERE provisioning_key=?').get(key);
  function install(scope, provisioningKey) {
    requireThat(scope.registryId === registryId && scope.environmentId === environmentId, 'feedback_scope_mismatch', 403);
    const existing = installation(provisioningKey);
    if (existing) {
      requireThat(existing.registry_id === scope.registryId && existing.tenant_id === scope.tenantId
        && existing.app_id === scope.appId && existing.environment_id === scope.environmentId, 'feedback_scope_mismatch', 403);
      return existing;
    }
    writeCapacity();
    const result = { id: newId('fbi_'), provisioning_key: provisioningKey, registry_id: scope.registryId,
      tenant_id: scope.tenantId, app_id: scope.appId, environment_id: scope.environmentId, created_at: now() };
    db.prepare('INSERT INTO feedback_installations VALUES (?,?,?,?,?,?,?)').run(result.id, provisioningKey,
      result.registry_id, result.tenant_id, result.app_id, result.environment_id, result.created_at);
    return result;
  }
  function validateProviderRequest(request) {
    exact(request, ['scope', 'source', 'profile', 'provisioningKey', 'intentDigest', 'authorityDigest', 'generation']);
    exact(request.scope, ['registryId', 'tenantId', 'appId', 'environmentId']); Object.values(request.scope).forEach(id);
    requireThat(same(request.profile, DEFAULT_FEEDBACK_PROFILE), 'feedback_profile_not_approved', 403);
    requireThat(request.scope.registryId === registryId && request.scope.environmentId === environmentId, 'feedback_scope_mismatch', 403);
    requireThat(request.provisioningKey === contractDigest({ scope: request.scope, providerId: request.profile.feedback.provider.id }), 'feedback_provisioning_mismatch', 403);
    requireThat(/^[a-f0-9]{64}$/.test(request.authorityDigest) && /^[a-f0-9]{64}$/.test(request.intentDigest)
      && Number.isSafeInteger(request.generation) && request.generation >= 1, 'feedback_invalid_generation');
    // This host-only proof is materialized by Registration under its current
    // source fence. It is not accepted by any network operation.
    return contractDigest(request);
  }
  function providerReceipt(request) {
    const key = validateProviderRequest(request), row = db.prepare('SELECT proof_json FROM feedback_provider_receipts WHERE receipt_key=?').get(key);
    return row ? JSON.parse(row.proof_json) : null;
  }
  const provider = Object.freeze({
    ensureInstallation(request) {
      requireThat(!closed, 'feedback_closed', 503);
      const key = validateProviderRequest(request);
      return transaction(() => {
        const prior = providerReceipt(request); if (prior) return prior;
        writeCapacity();
        const row = install(request.scope, request.provisioningKey);
        const payload = { installationId: row.id, provisioningKey: request.provisioningKey, scope: request.scope,
          source: request.source, profile: { id: request.profile.id, version: request.profile.version, digest: request.profile.digest },
          authorityDigest: request.authorityDigest, generation: request.generation };
        const proof = { ...payload, receiptDigest: contractDigest(payload) };
        db.prepare('INSERT INTO feedback_provider_receipts VALUES (?,?,?,?)').run(key, row.id, JSON.stringify(proof), now());
        return proof;
      });
    },
    inspectInstallation(request) { requireThat(!closed, 'feedback_closed', 503); return providerReceipt(request); }
  });
  function scoped(actor, args, callback) {
    authenticate(actor);
    const captured = Object.freeze({ accountId: actor.accountId, deviceId: actor.deviceId });
    let active = true, entered = false, outcome;
    try {
      const returned = withAppAuthority({ actor: captured, appId: args.appId, mode: 'participant',
        ...(args.domainId === undefined ? {} : { domainId: args.domainId }), ...(args.path === undefined ? {} : { path: args.path }) }, context => {
        requireThat(active && !entered, 'feedback_authority_fence_invalid', 500); entered = true;
        requireThat(object(context) && context.appId === args.appId && context.accountId === captured.accountId
          && typeof context.canManage === 'boolean' && context.canManage === (context.ownerId === captured.accountId),
        'feedback_authority_context_invalid', 500);
        id(context.ownerId); id(context.appId); authenticate(captured);
        const scope = { registryId, tenantId: context.ownerId, appId: context.appId, environmentId };
        const key = contractDigest({ scope, providerId: DEFAULT_FEEDBACK_PROFILE.feedback.provider.id });
        outcome = transaction(() => {
          const row = install(scope, key);
          if (args.installationId !== undefined) requireThat(args.installationId === row.id, 'feedback_installation_mismatch', 403);
          const result = callback(context, row);
          authenticate(captured); return result;
        });
        return outcome;
      });
      sync(returned); requireThat(entered, 'feedback_authority_fence_invalid', 500);
      return outcome;
    } finally { active = false; }
  }
  function ticket(actor, context, inbox, ticketId) {
    id(ticketId);
    const row = db.prepare('SELECT * FROM feedback_tickets WHERE id=? AND installation_id=? AND app_id=?').get(ticketId, inbox.id, context.appId);
    requireThat(row && (row.reporter_id === actor.accountId || (context.canManage && row.owner_id === context.ownerId)), 'feedback_ticket_unavailable', 404);
    return row;
  }
  function project(row, actor, context, includeBytes = false, summary = false) {
    const attachments = db.prepare('SELECT * FROM feedback_attachments WHERE ticket_id=? ORDER BY ordinal').all(row.id).map(item => ({
      id: item.id, kind: item.kind, name: item.name, mimeType: item.mime_type, byteLength: item.byte_length,
      ...(includeBytes ? { dataBase64: Buffer.from(item.bytes).toString('base64') } : {}) }));
    const messages = summary ? [] : db.prepare('SELECT id,kind,body,created_at FROM feedback_messages WHERE ticket_id=? ORDER BY ordinal').all(row.id)
      .map(item => ({ id: item.id, kind: item.kind, body: item.body, createdAt: item.created_at }));
    return { id: row.id, appId: row.app_id, installationId: row.installation_id, body: summary ? row.body.slice(0, 240) : row.body, status: row.status,
      revision: row.revision, createdAt: row.created_at, updatedAt: row.updated_at, attachments, messages,
      canReply: true, canManage: context.canManage && row.owner_id === context.ownerId, canAccept: row.reporter_id === actor.accountId };
  }
  function priorReceipt(actor, inbox, requestId, intentDigest) {
    id(requestId); const requestKey = hash(requestId);
    const prior = db.prepare('SELECT * FROM feedback_receipts WHERE installation_id=? AND account_id=? AND request_key=?').get(inbox.id, actor.accountId, requestKey);
    if (prior) { requireThat(prior.intent_digest === intentDigest, 'feedback_request_conflict', 409); return { value: JSON.parse(prior.result_json), requestKey }; }
    requireThat(db.prepare('SELECT count(*) AS n FROM feedback_receipts WHERE installation_id=?').get(inbox.id).n < FEEDBACK_LIMITS.receiptsPerInstallation, 'feedback_receipt_capacity', 503);
    return { value: null, requestKey };
  }
  function saveReceipt(actor, inbox, requestKey, intentDigest, value) {
    db.prepare('INSERT INTO feedback_receipts VALUES (?,?,?,?,?)').run(inbox.id, actor.accountId, requestKey, intentDigest, JSON.stringify(value));
  }
  return {
    operations, provider, schemaVersion: 1,
    execute({ op, actor, args = {} }) {
      requireThat(operations.has(op), 'unsupported_operation');
      authenticate(actor);
      // Every inner receipt, reporter projection and authority check uses the
      // same verified identity, even if a host retains its input object.
      actor = Object.freeze({ accountId: actor.accountId, deviceId: actor.deviceId });
      if (op === 'apps.feedback.context') {
        exact(args, ['appId', 'domainId', 'path'], ['appId']);
        return scoped(actor, args, (context, inbox) => ({ readiness: 'ready', context: { installationId: inbox.id, appId: context.appId,
          entry: context.entry, title: context.title, recipientLabel: 'Владелец приложения', ticketVisibility: 'reporter-and-support',
          canSubmit: true, canManage: context.canManage, capabilities: { text: true, voice: true, screenshot: true, asr: false },
          limits: { bodyChars: FEEDBACK_LIMITS.bodyChars, totalAttachmentBytes: FEEDBACK_LIMITS.totalAttachmentBytes,
            maxAttachments: FEEDBACK_LIMITS.maxAttachments, maxAudioSeconds: FEEDBACK_LIMITS.maxAudioSeconds } } }));
      }
      if (op === 'apps.feedback.submit') {
        exact(args, ['appId', 'installationId', 'requestId', 'body', 'attachments']); body(args.body); id(args.installationId);
        const media = validateFeedbackAttachments(args.attachments, { limits: {
          maxAttachments: FEEDBACK_LIMITS.maxAttachments, totalAttachmentBytes: FEEDBACK_LIMITS.totalAttachmentBytes,
          maxAudioSeconds: FEEDBACK_LIMITS.maxAudioSeconds } });
        const intent = hash(JSON.stringify(['feedback.submit.v1', args.appId, args.installationId, args.body,
          media.map(item => [item.kind, item.name, item.mimeType, hash(item.bytes)])]));
        return scoped(actor, args, (context, inbox) => {
          const prior = priorReceipt(actor, inbox, args.requestId, intent);
          if (prior.value) { const current = ticket(actor, context, inbox, prior.value.ticketId); return { requestId: args.requestId, replayed: true, receipt: prior.value, ticket: project(current, actor, context) }; }
          writeCapacity();
          requireThat(db.prepare('SELECT count(*) AS n FROM feedback_tickets WHERE installation_id=?').get(inbox.id).n < FEEDBACK_LIMITS.ticketsPerInstallation, 'feedback_ticket_capacity', 503);
          const timestamp = now(), ticketId = newId('fbt_');
          db.prepare('INSERT INTO feedback_tickets VALUES (?,?,?,?,?,?,?,?,?,?)').run(ticketId, inbox.id, context.appId,
            actor.accountId, context.ownerId, args.body, 'received', 1, timestamp, timestamp);
          const insert = db.prepare('INSERT INTO feedback_attachments VALUES (?,?,?,?,?,?,?,?)');
          media.forEach((item, ordinal) => insert.run(newId('fba_'), ticketId, ordinal, item.kind, item.name, item.mimeType, item.byteLength, item.bytes));
          const receipt = { ticketId, revision: 1, createdAt: timestamp };
          saveReceipt(actor, inbox, prior.requestKey, intent, receipt);
          return { requestId: args.requestId, replayed: false, receipt, ticket: project(ticket(actor, context, inbox, ticketId), actor, context) };
        });
      }
      if (op === 'apps.feedback.list') {
        exact(args, ['appId', 'installationId', 'limit', 'cursor'], ['appId', 'installationId']);
        const limit = args.limit ?? FEEDBACK_LIMITS.pageSize;
        requireThat(Number.isSafeInteger(limit) && limit >= 1 && limit <= FEEDBACK_LIMITS.maxPageSize, 'feedback_invalid_limit');
        return scoped(actor, args, (context, inbox) => {
          const scope = hash(JSON.stringify([inbox.id, actor.accountId, context.canManage])); let after = null;
          if (args.cursor !== undefined && args.cursor !== null) {
            try {
              requireThat(typeof args.cursor === 'string' && args.cursor.length <= 768);
              after = JSON.parse(Buffer.from(args.cursor, 'base64url').toString('utf8'));
              exact(after, ['scope', 'createdAt', 'id']); id(after.id);
              requireThat(after.scope === scope && Number.isSafeInteger(after.createdAt) && after.createdAt >= 0
                && Buffer.from(JSON.stringify(after)).toString('base64url') === args.cursor);
            } catch { throw new FeedbackError('feedback_invalid_cursor'); }
          }
          const rows = db.prepare(`SELECT * FROM feedback_tickets WHERE installation_id=?
            AND (?=1 OR reporter_id=?) AND (? IS NULL OR created_at<? OR (created_at=? AND id<?))
            ORDER BY created_at DESC,id DESC LIMIT ?`).all(inbox.id, Number(context.canManage), actor.accountId,
              after?.createdAt ?? null, after?.createdAt ?? null, after?.createdAt ?? null, after?.id ?? '', limit + 1);
          const page = rows.slice(0, limit), last = page.at(-1);
          return { tickets: page.map(row => project(row, actor, context, false, true)), nextCursor: rows.length > limit && last
            ? Buffer.from(JSON.stringify({ scope, createdAt: last.created_at, id: last.id })).toString('base64url') : null };
        });
      }
      if (op === 'apps.feedback.get') {
        exact(args, ['appId', 'installationId', 'ticketId']);
        return scoped(actor, args, (context, inbox) => ({ ticket: project(ticket(actor, context, inbox, args.ticketId), actor, context, true) }));
      }
      const kind = op === 'apps.feedback.reply' ? 'reply' : op === 'apps.feedback.status' ? 'status' : 'accept';
      exact(args, ['appId', 'installationId', 'ticketId', 'requestId', 'expectedRevision', ...(kind === 'reply' ? ['body'] : kind === 'status' ? ['status'] : [])]);
      positive(args.expectedRevision);
      if (kind === 'reply') body(args.body);
      if (kind === 'status') requireThat(['in_progress', 'needs_action', 'ready_to_check'].includes(args.status), 'feedback_invalid_status');
      const intent = hash(JSON.stringify([op, args.appId, args.installationId, args.ticketId, args.expectedRevision, args.body ?? null, args.status ?? null]));
      return scoped(actor, args, (context, inbox) => {
        const row = ticket(actor, context, inbox, args.ticketId), prior = priorReceipt(actor, inbox, args.requestId, intent);
        if (prior.value) return { requestId: args.requestId, replayed: true, receipt: prior.value, ticket: project(row, actor, context) };
        writeCapacity();
        requireThat(row.revision === args.expectedRevision, 'feedback_revision_conflict', 409);
        if (kind === 'status') requireThat(context.canManage && row.owner_id === context.ownerId, 'feedback_support_required', 403);
        if (kind === 'accept') requireThat(row.reporter_id === actor.accountId && row.status === 'ready_to_check', 'feedback_acceptance_required', 403);
        const timestamp = now(), revision = row.revision + 1;
        let status = kind === 'status' ? args.status : kind === 'accept' ? 'resolved' : row.status;
        if (kind === 'reply') {
          const count = db.prepare('SELECT count(*) AS n FROM feedback_messages WHERE ticket_id=?').get(row.id).n;
          requireThat(count < FEEDBACK_LIMITS.maxMessagesPerTicket, 'feedback_message_capacity', 503);
          const bytes = db.prepare('SELECT COALESCE(sum(length(CAST(body AS BLOB))),0) AS n FROM feedback_messages WHERE ticket_id=?').get(row.id).n;
          requireThat(bytes + Buffer.byteLength(args.body, 'utf8') <= FEEDBACK_LIMITS.conversationBytes, 'feedback_conversation_capacity', 503);
          const role = row.reporter_id === actor.accountId ? 'reporter' : 'support';
          db.prepare('INSERT INTO feedback_messages VALUES (?,?,?,?,?,?,?)').run(newId('fbm_'), row.id, count, actor.accountId, role, args.body, timestamp);
          if (role === 'support' && row.status === 'received') status = 'in_progress';
        }
        db.prepare('UPDATE feedback_tickets SET status=?,revision=?,updated_at=? WHERE id=? AND revision=?').run(status, revision, timestamp, row.id, row.revision);
        const receipt = { ticketId: row.id, revision, createdAt: timestamp }; saveReceipt(actor, inbox, prior.requestKey, intent, receipt);
        return { requestId: args.requestId, replayed: false, receipt, ticket: project(ticket(actor, context, inbox, row.id), actor, context) };
      });
    },
    close() { if (!closed) { db.close(); closed = true; } }
  };
}
