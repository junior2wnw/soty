import { DatabaseSync } from 'node:sqlite';
import { mkdirSync } from 'node:fs';
import { dirname } from 'node:path';
import { validateDescriptor, contractDigest, canonicalContractJson } from '../../app-contract/index.mjs';
import { snapshot } from '../../app-contract/json.mjs';

export const APP_AGENT_DIRECTORY_OPERATIONS = Object.freeze(['apps.agentDescriptor.get', 'apps.agentDescriptor.put']);
const schema = 'soty.app-agent-directory.v1';
export class AppAgentDirectoryError extends Error {
  constructor(code, status = 400) { super(code); this.code = code; this.status = status; }
}
const require = (condition, code = 'directory_invalid_input', status = 400) => { if (!condition) throw new AppAgentDirectoryError(code, status); };
const closed = (value, required, optional = []) => require(value && typeof value === 'object' && !Array.isArray(value)
  && required.every(key => Object.hasOwn(value, key)) && Object.keys(value).every(key => [...required, ...optional].includes(key)));
const id = value => { require(typeof value === 'string' && /^[A-Za-z0-9][A-Za-z0-9._:-]{0,127}$/u.test(value)); return value; };
const integer = value => require(Number.isSafeInteger(value) && value >= 0 && value <= 1000000);
const sync = value => { require(!value || typeof value.then !== 'function', 'directory_async_authority', 503); return value; };
const same = (left, right) => canonicalContractJson(left) === canonicalContractJson(right);
export const appAgentSource = authority => ({ id: `apps:${authority.appId}/target`, revision: authority.target.revision, digest: authority.target.digest });
const denied = error => ['app_unavailable', 'apps_access_denied', 'apps_owner_required', 'app_public_unavailable', 'apps_authentication_required',
  'authentication_required', 'access_denied', 'device_revoked', 'app_scoped_admission_required', 'directory_not_found'].includes(error?.code);
const normalized = value => value.normalize('NFKC').toLocaleLowerCase('ru');

/** An opt-in metadata extension of AppsService, never an application or execution registry. */
export function createAppAgentDirectory({ databasePath, projectId, actorActive, withAppAuthority, withPublicAppAuthority,
  readPublicApps, transport = null, now = Date.now }) {
  id(projectId);
  require(typeof databasePath === 'string' && typeof actorActive === 'function' && typeof withAppAuthority === 'function'
    && typeof withPublicAppAuthority === 'function' && typeof readPublicApps === 'function', 'directory_configuration_invalid', 500);
  if (databasePath !== ':memory:') mkdirSync(dirname(databasePath), { recursive: true });
  const db = new DatabaseSync(databasePath); let disposed = false;
  db.exec(`PRAGMA journal_mode=WAL; PRAGMA busy_timeout=1000; PRAGMA foreign_keys=ON;
    CREATE TABLE IF NOT EXISTS directory_identity (id INTEGER PRIMARY KEY CHECK(id=1),project_id TEXT NOT NULL);
    CREATE TABLE IF NOT EXISTS app_agent_descriptors (
      app_id TEXT PRIMARY KEY,owner_id TEXT NOT NULL,revision INTEGER NOT NULL CHECK(revision>0),enabled INTEGER NOT NULL,
      descriptor_json TEXT,descriptor_digest TEXT,updated_at INTEGER NOT NULL);
    CREATE TABLE IF NOT EXISTS app_agent_descriptor_receipts (
      account_id TEXT NOT NULL,request_id TEXT NOT NULL,input_digest TEXT NOT NULL,result_json TEXT NOT NULL,
      PRIMARY KEY(account_id,request_id));`);
  const identity = db.prepare('SELECT project_id FROM directory_identity WHERE id=1').get();
  if (identity && identity.project_id !== projectId) { db.close(); throw new AppAgentDirectoryError('directory_project_mismatch', 503); }
  if (!identity) db.prepare('INSERT INTO directory_identity(id,project_id) VALUES(1,?)').run(projectId);
  const ready = () => require(!disposed, 'directory_closed', 503);
  const active = actor => { ready(); require(actor && actorActive(actor) === true, 'directory_authentication_required', 401); };
  const rowFor = appId => db.prepare('SELECT * FROM app_agent_descriptors WHERE app_id=?').get(appId);
  function ownerRecord(row, authority) {
    if (!row) return { appId: authority.appId, revision: 0, enabled: false, source: appAgentSource(authority), descriptor: null, current: false };
    const descriptor = row.descriptor_json ? validateDescriptor(JSON.parse(row.descriptor_json)) : null;
    return { appId: authority.appId, revision: row.revision, enabled: row.enabled === 1, descriptor,
      descriptorDigest: row.descriptor_digest, source: appAgentSource(authority),
      current: row.owner_id === authority.ownerId && descriptor !== null && same(descriptor.app.source, appAgentSource(authority))
        && descriptor.app.visibility === authority.visibility, updatedAt: row.updated_at };
  }
  function withAuthority(appId, actor, callback, mode = 'participant') {
    ready(); id(appId);
    if (actor) { active(actor); return sync(withAppAuthority({ actor, appId, mode }, authority => {
      active(actor); const value = sync(callback(authority)); active(actor); return value;
    })); }
    require(mode !== 'owner', 'directory_authentication_required', 401);
    return sync(withPublicAppAuthority({ appId }, authority => {
      require(authority.visibility === 'public', 'directory_not_found', 404); return sync(callback(authority));
    }));
  }
  function project(row, authority, actor) {
    const record = ownerRecord(row, authority);
    require(record.enabled && record.current, 'directory_not_found', 404);
    const descriptor = { ...record.descriptor, app: { ...record.descriptor.app, title: authority.title } };
    const capabilities = descriptor.capabilities.map(({ id, version, digest, title, description, effects }) => ({ id, version, digest,
      ...(title === undefined ? {} : { title }), ...(description === undefined ? {} : { description }), effects, executionReady: false }));
    return { schema: 'soty.discovered-app.v1', appId: authority.appId, title: authority.title,
      visibility: authority.visibility, audience: authority.audience ?? (authority.visibility === 'public' ? 'public' : 'private'),
      revision: record.revision, descriptorDigest: contractDigest(descriptor), source: appAgentSource(authority),
      entry: { origin: authority.entry.origin, path: authority.entry.path }, discoveryReady: true,
      executionReady: capabilities.some(capability => capability.executionReady),
      descriptor, capabilities,
      canManage: actor?.type !== 'service' && authority.canManage === true,
      authority: 'metadata-only; execution requires a separate current grant and trusted binding' };
  }
  function get({ actor, appId }) {
    const captured = withAuthority(appId, actor, authority => ({ authority, value: project(rowFor(appId), authority, actor) }));
    if (!actor || !transport) return captured.value;
    // An admitted transport may acquire its own Apps/Root fence. Never nest it
    // inside the Apps transaction or hold that transaction over a Source call.
    const capabilities = captured.value.descriptor.capabilities.map((capability, index) => {
      const execution = sync(transport.inspect({ actor, authority: captured.authority, descriptor: captured.value.descriptor, capability }));
      return { ...captured.value.capabilities[index], executionReady: execution?.ready === true, ...(execution?.ready === true ? { execution } : {}) };
    });
    const current = withAuthority(appId, actor, authority => project(rowFor(appId), authority, actor));
    const fence = value => ({ revision: value.revision, descriptorDigest: value.descriptorDigest, source: value.source, entry: value.entry,
      visibility: value.visibility, audience: value.audience, title: value.title, canManage: value.canManage });
    require(same(fence(captured.value), fence(current)), 'directory_source_changed', 409); active(actor);
    return { ...current, capabilities, executionReady: capabilities.some(capability => capability.executionReady) };
  }
  function list({ actor, query = '', limit = 20, cursor = null } = {}) {
    ready(); if (actor) active(actor);
    require(typeof query === 'string' && query.length <= 100 && query.isWellFormed() && !/[\u0000-\u001f\u007f]/u.test(query));
    require(Number.isSafeInteger(limit) && limit >= 1 && limit <= 60);
    const scope = contractDigest({ schema, projectId, accountId: actor?.accountId ?? null, deviceId: actor?.deviceId ?? null, query });
    let after = '';
    if (cursor !== null) {
      require(typeof cursor === 'string' && /^[A-Za-z0-9_-]{1,512}$/u.test(cursor), 'directory_cursor_invalid');
      let value; try { value = JSON.parse(Buffer.from(cursor, 'base64url').toString()); } catch { throw new AppAgentDirectoryError('directory_cursor_invalid'); }
      closed(value, ['scope', 'after']); id(value.after);
      require(value.scope === scope && Buffer.from(JSON.stringify(value)).toString('base64url') === cursor, 'directory_cursor_invalid'); after = value.after;
    }
    const publicApps = actor ? null : new Set(sync(readPublicApps()).apps.map(app => app.id));
    const rows = db.prepare('SELECT * FROM app_agent_descriptors WHERE enabled=1 AND app_id>? ORDER BY app_id LIMIT 257').all(after);
    const items = []; let scanned = 0, responseBytes = 1024, hasMore = false;
    for (const row of rows.slice(0, 256)) {
      const previousAfter = after; scanned++; after = row.app_id;
      if (publicApps && !publicApps.has(row.app_id)) continue;
      let value;
      try { value = get({ appId: row.app_id, actor }); }
      catch (error) { if (denied(error)) continue; throw error; }
      if (query && !normalized([value.title, ...value.capabilities.map(capability => `${capability.id} ${capability.title ?? ''} ${capability.description ?? ''}`)].join(' ')).includes(normalized(query))) continue;
      responseBytes += Buffer.byteLength(JSON.stringify(value));
      // MCP also returns a JSON-escaped text copy of structuredContent.
      if (responseBytes > 512 * 1024) { after = previousAfter; hasMore = true; break; }
      items.push(value); if (items.length === limit) break;
    }
    if (actor) active(actor);
    return { schema, items, nextCursor: hasMore || scanned < rows.length
      ? Buffer.from(JSON.stringify({ scope, after })).toString('base64url') : null };
  }
  function execute({ op, actor, args: input = {} }) {
    require(APP_AGENT_DIRECTORY_OPERATIONS.includes(op)); active(actor);
    const args = snapshot(input);
    closed(args, ['expectedAccountId', 'appId'], op === 'apps.agentDescriptor.put' ? ['requestId', 'expectedRevision', 'enabled', 'descriptor'] : []);
    require(args.expectedAccountId === actor.accountId, 'directory_account_mismatch', 403); id(args.appId);
    if (op === 'apps.agentDescriptor.put') {
      require(['requestId', 'expectedRevision', 'enabled'].every(key => Object.hasOwn(args, key))); id(args.requestId); integer(args.expectedRevision);
      require(typeof args.enabled === 'boolean'); require(!args.enabled || args.descriptor !== undefined);
    }
    return withAuthority(args.appId, actor, authority => {
      require(authority.ownerId === actor.accountId, 'directory_owner_required', 403);
      const row = rowFor(args.appId);
      if (op === 'apps.agentDescriptor.get') return ownerRecord(row, authority);
      const inputDigest = contractDigest(args);
      db.exec('BEGIN IMMEDIATE');
      try {
        const receipt = db.prepare('SELECT * FROM app_agent_descriptor_receipts WHERE account_id=? AND request_id=?').get(actor.accountId, args.requestId);
        if (receipt) {
          require(receipt.input_digest === inputDigest, 'directory_request_conflict', 409);
          const committed = JSON.parse(receipt.result_json);
          const result = { ...ownerRecord(rowFor(args.appId), authority), requestId: args.requestId, receipt: committed.receipt, replayed: true };
          active(actor); db.exec('COMMIT'); return result;
        }
        const previous = rowFor(args.appId);
        require((previous?.revision ?? 0) === args.expectedRevision, 'directory_revision_conflict', 409);
        require(!previous || previous.owner_id === actor.accountId, 'directory_owner_required', 403);
        const descriptor = args.descriptor === undefined ? previous?.descriptor_json ? validateDescriptor(JSON.parse(previous.descriptor_json)) : null
          : validateDescriptor(args.descriptor);
        if (args.enabled || args.descriptor !== undefined) {
          require(descriptor?.app.id === authority.appId, 'directory_app_mismatch', 409);
          require(same(descriptor.app.source, appAgentSource(authority)), 'directory_source_changed', 409);
          require(descriptor.app.visibility === authority.visibility, 'directory_visibility_changed', 409);
        }
        const revision = args.expectedRevision + 1; integer(revision);
        const time = now(); require(Number.isSafeInteger(time) && time >= 0, 'directory_clock_invalid', 503);
        db.prepare(`INSERT INTO app_agent_descriptors(app_id,owner_id,revision,enabled,descriptor_json,descriptor_digest,updated_at)
          VALUES(?,?,?,?,?,?,?) ON CONFLICT(app_id) DO UPDATE SET revision=excluded.revision,enabled=excluded.enabled,
          descriptor_json=excluded.descriptor_json,descriptor_digest=excluded.descriptor_digest,updated_at=excluded.updated_at`)
          .run(args.appId, actor.accountId, revision, Number(args.enabled), descriptor ? canonicalContractJson(descriptor) : null,
            descriptor ? contractDigest(descriptor) : null, time);
        const record = ownerRecord(rowFor(args.appId), authority);
        const result = { ...record, requestId: args.requestId, replayed: false, receipt: { appId: args.appId, requestId: args.requestId,
          committedRevision: revision, descriptorDigest: record.descriptorDigest, enabled: args.enabled, committedAt: time } };
        db.prepare('INSERT INTO app_agent_descriptor_receipts(account_id,request_id,input_digest,result_json) VALUES(?,?,?,?)')
          .run(actor.accountId, args.requestId, inputDigest, JSON.stringify(result));
        active(actor); db.exec('COMMIT'); return result;
      } catch (error) { if (db.isTransaction) db.exec('ROLLBACK'); throw error; }
    }, 'owner');
  }
  async function query({ actor, appId, capabilityId, input, idempotencyKey }) {
    active(actor); id(appId); id(idempotencyKey);
    require(typeof capabilityId === 'string' && /^[A-Za-z][A-Za-z0-9._-]{0,63}:[A-Za-z][A-Za-z0-9._/-]{0,95}$/u.test(capabilityId));
    require(transport && typeof transport.query === 'function', 'directory_execution_unavailable', 503);
    const before = get({ actor, appId }), capability = before.capabilities.find(value => value.id === capabilityId);
    require(capability?.executionReady === true && capability.effects.every(effect => effect === 'read'), 'directory_execution_denied', 403);
    const value = await transport.query({ actor, appId, capabilityId, input: snapshot(input), idempotencyKey, execution: capability.execution });
    active(actor); const after = get({ actor, appId }), current = after.capabilities.find(item => item.id === capabilityId);
    require(before.revision === after.revision && before.descriptorDigest === after.descriptorDigest && same(before.source, after.source)
      && current?.executionReady === true && same(current.execution, capability.execution), 'directory_source_changed', 409);
    return value;
  }
  function recheckQuery({ actor, appId, capabilityId, input, idempotencyKey, value }) {
    active(actor); const current = get({ actor, appId }), capability = current.capabilities.find(item => item.id === capabilityId);
    require(capability?.executionReady === true && typeof transport?.recheck === 'function', 'directory_execution_denied', 403);
    transport.recheck({ actor, execution: capability.execution, input, idempotencyKey, value }); active(actor);
  }
  return Object.freeze({ operations: new Set(APP_AGENT_DIRECTORY_OPERATIONS), execute, list, get, query, recheckQuery,
    close() { if (!disposed) { disposed = true; db.close(); } } });
}
