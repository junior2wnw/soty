import { createHash, randomBytes } from 'node:crypto';
import { AppsError, assertApps, appId, appPort, runtimePath, textId, connectorKey } from './protocol.mjs';
import { RUNTIME_PROFILE, runtimeTargetDigest } from './schema.mjs';

export const sourceOperations = new Set(['apps.source.prepare', 'apps.source.promote', 'apps.source.history']);
export const SOURCE_RECEIPTS_PER_APP = 64;
export const SOURCE_LIMITS = Object.freeze({ global: 256, perAccount: 4, perApp: 2, ttlMs: 30_000, probeMs: 5_000 });
const hash = value => createHash('sha256').update(value).digest('hex');
const exact = (value, keys) => assertApps(value && typeof value === 'object' && !Array.isArray(value)
  && Object.keys(value).every(key => keys.includes(key)), 'unexpected_argument');
const positive = value => { assertApps(Number.isSafeInteger(value) && value >= 1, 'invalid_source_revision'); return value; };
const opaqueId = value => { assertApps(typeof value === 'string' && /^[A-Za-z0-9_-]{43}$/u.test(value), 'invalid_source_preparation'); return value; };
const isAsync = value => value && typeof value.then === 'function';
const safeClock = value => { assertApps(Number.isSafeInteger(value) && value >= 0 && value <= Number.MAX_SAFE_INTEGER - 30_000, 'apps_source_clock_invalid', 500); return value; };

/** One registry/database owns durable mutations. Preparation is bounded process
 * memory and is never authority for an active route before the commit. */
export function createSourceRegistry({ db, now = Date.now, assertActor, publications, prepareTarget, verifyPreparedTarget,
  onChanged = () => {}, blockedPorts = [], limits: overrides = {} }) {
  assertApps(db && typeof assertActor === 'function' && typeof publications?.sourceStateInTransaction === 'function'
    && typeof publications?.promoteSourceInTransaction === 'function' && typeof prepareTarget === 'function'
    && typeof verifyPreparedTarget === 'function' && typeof onChanged === 'function', 'apps_source_dependencies_required', 500);
  exact(overrides, Object.keys(SOURCE_LIMITS));
  const limits = { ...SOURCE_LIMITS, ...overrides };
  for (const key of Object.keys(limits)) assertApps(Number.isSafeInteger(limits[key]) && limits[key] > 0
    && limits[key] <= SOURCE_LIMITS[key], 'apps_source_limits_invalid', 500);
  const preparations = new Map(); let closed = false;
  const appRow = id => db.prepare('SELECT * FROM local_apps WHERE id=?').get(id);
  function authenticate(actor) {
    const result = assertActor(actor);
    if (isAsync(result)) Promise.resolve(result).catch(() => {});
    assertApps(!isAsync(result) && result !== false, 'apps_authentication_required', 401);
    textId(actor?.accountId); textId(actor?.deviceId);
  }
  function owned(actor, id) {
    authenticate(actor); const app = appRow(id);
    assertApps(app && app.owner_account_id === actor.accountId, 'apps_owner_required', 403); return app;
  }
  function transaction(write, callback) {
    assertApps(!closed, 'apps_closed', 503); assertApps(!db.isTransaction, 'apps_nested_transaction', 500);
    db.exec(write ? 'BEGIN IMMEDIATE' : 'BEGIN');
    try { const result = callback(); assertApps(!isAsync(result), 'apps_async_transaction', 500); db.exec('COMMIT'); return result; }
    catch (error) { if (db.isTransaction) db.exec('ROLLBACK'); throw error; }
  }
  function state(actor, id, epoch, revision, enabled = true) {
    const app = owned(actor, id), current = publications.sourceStateInTransaction(id);
    if (epoch !== undefined) assertApps(current.policy.policy_epoch === epoch, 'app_publication_revision_conflict', 409);
    if (revision !== undefined) assertApps(current.target.revision === revision, 'app_publication_target_conflict', 409);
    if (enabled) assertApps(app.state === 'enabled', 'app_revoked', 409);
    return current;
  }
  function targetTuple(row) {
    return Object.freeze({ appId: row.app_id, revision: row.revision, ownerAccountId: row.owner_account_id,
      connectorKey: row.connector_key, port: row.port, entryPath: row.entry_path, profile: row.profile, digest: row.digest });
  }
  function deviceFor(key, owner) {
    const row = db.prepare('SELECT * FROM app_devices WHERE connector_key=?').get(key);
    assertApps(row?.owner_account_id === owner, 'apps_device_not_owned', 403);
    let identity;
    try { identity = JSON.parse(row.identity_json); } catch { throw new AppsError('apps_registry_corrupt', 500); }
    assertApps(connectorKey(identity) === key && typeof row.name === 'string', 'apps_registry_corrupt', 500);
    return { hostDeviceId: identity.hostDeviceId, connectorId: identity.connectorId, deviceName: row.name };
  }
  function targetView(target) {
    return { revision: target.revision, digest: target.digest, profile: target.profile, port: target.port, entryPath: target.entryPath,
      ...deviceFor(target.connectorKey, target.ownerAccountId) };
  }
  function occupied(id, key, port) {
    assertApps(!db.prepare(`SELECT 1 FROM local_apps a JOIN app_publications p ON p.app_id=a.id
      JOIN app_runtime_targets t ON t.app_id=p.app_id AND t.revision=p.active_target_revision
      WHERE a.id<>? AND a.state='enabled' AND t.connector_key=? AND t.port=? LIMIT 1`).get(id, key, port), 'app_port_already_registered', 409);
  }
  function normalizePrepare(args) {
    exact(args, ['appId', 'expectedPolicyEpoch', 'expectedTargetRevision', 'source', 'targetRevision']);
    const id = appId(args.appId), epoch = positive(args.expectedPolicyEpoch), revision = positive(args.expectedTargetRevision);
    assertApps((args.source !== undefined) !== (args.targetRevision !== undefined), 'invalid_source_selection');
    if (args.targetRevision !== undefined) return { id, epoch, revision, targetRevision: positive(args.targetRevision) };
    exact(args.source, ['hostDeviceId', 'connectorId', 'port', 'entryPath']);
    return { id, epoch, revision, source: { hostDeviceId: textId(args.source.hostDeviceId), connectorId: textId(args.source.connectorId),
      port: appPort(args.source.port, blockedPorts), entryPath: runtimePath(args.source.entryPath) } };
  }
  function selectTarget(actor, intent) {
    state(actor, intent.id, intent.epoch, intent.revision);
    let target;
    if (intent.targetRevision !== undefined) {
      const row = db.prepare('SELECT * FROM app_runtime_targets WHERE app_id=? AND revision=?').get(intent.id, intent.targetRevision);
      assertApps(row?.owner_account_id === actor.accountId, 'apps_source_target_unavailable', 409);
      target = targetTuple(row); appPort(target.port, blockedPorts); runtimePath(target.entryPath);
    } else {
      const matches = db.prepare('SELECT * FROM app_devices WHERE owner_account_id=?').all(actor.accountId).filter(row => {
        let identity; try { identity = JSON.parse(row.identity_json); } catch { throw new AppsError('apps_registry_corrupt', 500); }
        return identity.hostDeviceId === intent.source.hostDeviceId && identity.connectorId === intent.source.connectorId;
      });
      assertApps(matches.length > 0, 'apps_device_not_owned', 403);
      assertApps(matches.length === 1, 'apps_source_device_ambiguous', 409);
      const max = db.prepare('SELECT max(revision) AS revision FROM app_runtime_targets WHERE app_id=?').get(intent.id).revision;
      assertApps(Number.isSafeInteger(max) && max < Number.MAX_SAFE_INTEGER, 'apps_source_revision_exhausted', 409);
      const tuple = { appId: intent.id, revision: max + 1, ownerAccountId: actor.accountId, connectorKey: matches[0].connector_key,
        port: intent.source.port, entryPath: intent.source.entryPath, profile: RUNTIME_PROFILE };
      target = Object.freeze({ ...tuple, digest: runtimeTargetDigest(tuple) });
    }
    assertApps(target.profile === RUNTIME_PROFILE && target.digest === runtimeTargetDigest(target), 'apps_registry_corrupt', 500);
    deviceFor(target.connectorKey, actor.accountId); occupied(intent.id, target.connectorKey, target.port);
    return target;
  }
  function forget(entry) {
    clearTimeout(entry.expiryTimer); clearTimeout(entry.probeTimer);
    if (preparations.get(entry.id) === entry) preparations.delete(entry.id);
  }
  function expire(entry, code = 'apps_source_preparation_expired') {
    entry.expired = true; entry.controller.abort(new AppsError(code, 409));
    // An uncooperative asynchronous provider retains its slot until it settles.
    // Freeing it on timeout would permit unbounded outstanding probe closures.
    if (entry.settled) forget(entry);
  }
  function sweep() {
    const timestamp = safeClock(now());
    for (const entry of preparations.values()) if (timestamp < entry.createdAt || entry.expiresAt <= timestamp) expire(entry);
    return timestamp;
  }
  function verification(entry, actor) {
    const result = verifyPreparedTarget({ evidence: entry.evidence, preparationId: entry.id, actor, target: entry.target, requiredBindingVersion: 2 });
    if (isAsync(result)) Promise.resolve(result).catch(() => {});
    assertApps(result === true, 'apps_source_preparation_stale', 409);
  }
  function freshAt(entry) {
    const timestamp = safeClock(now());
    assertApps(!closed && !entry.expired && !entry.controller.signal.aborted
      && timestamp >= entry.createdAt && entry.expiresAt > timestamp, 'apps_source_preparation_expired', 409);
    return timestamp;
  }
  async function prepare(actor, args) {
    const intent = normalizePrepare(args), capturedActor = Object.freeze({ accountId: actor?.accountId, deviceId: actor?.deviceId });
    const target = transaction(false, () => selectTarget(capturedActor, intent));
    const timestamp = sweep();
    assertApps(preparations.size < limits.global, 'apps_source_preparation_capacity', 429);
    let accountCount = 0, appCount = 0;
    for (const entry of preparations.values()) { if (entry.actor.accountId === capturedActor.accountId) accountCount++; if (entry.target.appId === intent.id) appCount++; }
    assertApps(accountCount < limits.perAccount && appCount < limits.perApp, 'apps_source_preparation_capacity', 429);
    const entry = { id: randomBytes(32).toString('base64url'), actor: capturedActor, target, intent, createdAt: timestamp,
      expiresAt: timestamp + limits.ttlMs, controller: new AbortController(), settled: false, ready: false, expired: false };
    preparations.set(entry.id, entry);
    const aborted = new Promise((_, reject) => entry.controller.signal.addEventListener('abort', () => reject(entry.controller.signal.reason), { once: true }));
    entry.probeTimer = setTimeout(() => expire(entry, 'apps_source_probe_timeout'), Math.min(limits.probeMs, limits.ttlMs));
    entry.probeTimer.unref?.();
    const probing = Promise.resolve().then(() => {
      assertApps(!closed && !entry.controller.signal.aborted, 'apps_closed', 503);
      return prepareTarget({ preparationId: entry.id, actor: capturedActor, appId: intent.id,
        expectedPolicyEpoch: intent.epoch, expectedTargetRevision: intent.revision, target, requiredBindingVersion: 2, signal: entry.controller.signal });
    })
      .finally(() => { entry.settled = true; if (entry.expired || closed) forget(entry); });
    try {
      entry.evidence = await Promise.race([probing, aborted]); clearTimeout(entry.probeTimer);
      freshAt(entry);
      const view = transaction(false, () => {
        state(capturedActor, intent.id, intent.epoch, intent.revision); deviceFor(target.connectorKey, capturedActor.accountId);
        verification(entry, capturedActor); freshAt(entry); return targetView(target);
      });
      const checkedAt = freshAt(entry);
      entry.ready = true;
      entry.expiryTimer = setTimeout(() => expire(entry), entry.expiresAt - checkedAt); entry.expiryTimer.unref?.();
      return { schema: 'soty.app-source-preparation.v1', appId: intent.id, preparationId: entry.id,
        expectedPolicyEpoch: intent.epoch, expectedTargetRevision: intent.revision, requiredBindingVersion: 2,
        target: view, checkedAt, expiresAt: entry.expiresAt };
    } catch (error) { expire(entry); if (entry.settled) forget(entry); throw error; }
  }
  function normalizePromote(args) {
    exact(args, ['appId', 'requestId', 'preparationId', 'expectedPolicyEpoch', 'expectedTargetRevision', 'launchPolicy', 'listed', 'exposureAck']);
    const id = appId(args.appId), requestId = textId(args.requestId), preparationId = opaqueId(args.preparationId);
    const epoch = positive(args.expectedPolicyEpoch), revision = positive(args.expectedTargetRevision);
    assertApps(['restricted', 'anyone'].includes(args.launchPolicy) && typeof args.listed === 'boolean', 'invalid_publication_policy');
    assertApps(!args.listed || args.launchPolicy === 'anyone', 'invalid_publication_listing');
    let exposureAck = null;
    if (args.launchPolicy === 'anyone') {
      exact(args.exposureAck, ['scope', 'targetRevision', 'targetDigest', 'profile']);
      assertApps(args.exposureAck.scope === 'whole-port' && Number.isSafeInteger(args.exposureAck.targetRevision) && args.exposureAck.targetRevision >= 1
        && typeof args.exposureAck.targetDigest === 'string' && /^[a-f0-9]{64}$/u.test(args.exposureAck.targetDigest)
        && args.exposureAck.profile === RUNTIME_PROFILE, 'app_exposure_ack_required');
      exposureAck = { scope: 'whole-port', targetRevision: args.exposureAck.targetRevision, targetDigest: args.exposureAck.targetDigest, profile: args.exposureAck.profile };
    } else assertApps(args.exposureAck === undefined || args.exposureAck === null, 'unexpected_exposure_ack');
    const normalized = { id, requestId, preparationId, epoch, revision, launchPolicy: args.launchPolicy, listed: args.listed, exposureAck };
    return { ...normalized, requestKey: hash(requestId), intentHash: hash(JSON.stringify(['apps.source.promote.v1', id, preparationId,
      epoch, revision, args.launchPolicy, args.listed, exposureAck])) };
  }
  function promote(actor, args) {
    const intent = normalizePromote(args);
    const result = transaction(true, () => {
      const app = owned(actor, intent.id);
      const previous = db.prepare('SELECT intent_hash,value_json FROM app_source_receipts WHERE account_id=? AND request_key=?').get(actor.accountId, intent.requestKey);
      if (previous) {
        assertApps(previous.intent_hash === intent.intentHash, 'app_source_request_conflict', 409);
        return { receipt: JSON.parse(previous.value_json), replayed: true,
          current: publications.execute({ op: 'apps.publication.get', actor, args: { appId: app.id } }) };
      }
      const current = state(actor, intent.id, intent.epoch, intent.revision), entry = preparations.get(intent.preparationId);
      const checkedAt = safeClock(now());
      assertApps(entry && entry.ready && !entry.expired && checkedAt >= entry.createdAt && entry.expiresAt > checkedAt, 'apps_source_preparation_expired', 409);
      assertApps(entry.actor.accountId === actor.accountId && entry.actor.deviceId === actor.deviceId && entry.target.appId === intent.id
        && entry.intent.epoch === intent.epoch && entry.intent.revision === intent.revision, 'apps_source_preparation_mismatch', 409);
      const target = entry.target;
      deviceFor(target.connectorKey, actor.accountId); occupied(intent.id, target.connectorKey, target.port);
      if (intent.exposureAck) assertApps(intent.exposureAck.targetRevision === target.revision && intent.exposureAck.targetDigest === target.digest
        && intent.exposureAck.profile === target.profile, 'app_exposure_ack_required');
      // No async work can occur between this exact-channel check and COMMIT.
      verification(entry, actor); authenticate(actor);
      db.prepare('UPDATE app_source_heads SET required_binding_version=2 WHERE app_id=?').run(intent.id);
      if (entry.intent.targetRevision === undefined) {
        const maximum = db.prepare('SELECT max(revision) AS revision FROM app_runtime_targets WHERE app_id=?').get(intent.id).revision;
        assertApps(maximum + 1 === target.revision, 'app_source_target_conflict', 409);
        db.prepare('INSERT INTO app_runtime_targets VALUES (?,?,?,?,?,?,?,?,?)').run(intent.id, target.revision, actor.accountId,
          target.connectorKey, target.port, target.entryPath, target.profile, target.digest, safeClock(now()));
      } else {
        const retained = db.prepare('SELECT * FROM app_runtime_targets WHERE app_id=? AND revision=?').get(intent.id, target.revision);
        assertApps(retained && retained.digest === target.digest, 'apps_source_target_unavailable', 409);
      }
      const changed = publications.promoteSourceInTransaction({ appId: intent.id, targetRevision: target.revision,
        launchPolicy: intent.launchPolicy, listed: intent.listed, exposureAck: intent.exposureAck });
      const timestamp = safeClock(now()), receipt = { schema: 'soty.app-source-receipt.v1', namespace: 'apps.source.promote.v1',
        requestKeyHash: intent.requestKey, preparationKeyHash: hash(intent.preparationId), appId: intent.id,
        previousTargetRevision: current.target.revision, targetRevision: target.revision, targetDigest: target.digest, profile: target.profile,
        requiredBindingVersion: 2, policyEpoch: changed.policyEpoch, launchPolicy: intent.launchPolicy, listed: intent.listed,
        exposureAck: intent.exposureAck, committedAt: timestamp };
      db.prepare('INSERT INTO app_source_receipts VALUES (?,?,?,?,?,?,?)').run(actor.accountId, intent.requestKey,
        intent.intentHash, intent.id, changed.policyEpoch, JSON.stringify(receipt), timestamp);
      db.prepare(`DELETE FROM app_source_receipts WHERE app_id=? AND committed_epoch NOT IN
        (SELECT committed_epoch FROM app_source_receipts WHERE app_id=? ORDER BY committed_epoch DESC LIMIT ?)`)
        .run(intent.id, intent.id, SOURCE_RECEIPTS_PER_APP);
      // Recheck after all SQL too: slow synchronous work must not extend a
      // preparation lease, and a failure rolls back every write above.
      authenticate(actor); verification(entry, actor); freshAt(entry);
      return { receipt, replayed: false, current: changed.current };
    });
    const entry = preparations.get(intent.preparationId); if (entry) { entry.controller.abort(); forget(entry); }
    // Replayed receipts may describe an older target. Reconcile the CURRENT
    // route again; a post-commit notification failure never undoes the receipt.
    const old = db.prepare('SELECT connector_key FROM app_runtime_targets WHERE app_id=? AND revision=?').get(intent.id, result.receipt.previousTargetRevision);
    const active = db.prepare('SELECT connector_key FROM app_runtime_targets WHERE app_id=? AND revision=?').get(intent.id, result.current.activeTargetRevision);
    const notified = onChanged({ appId: intent.id, policyEpoch: result.current.policyEpoch, oldConnectorKey: old?.connector_key,
      newConnectorKey: active?.connector_key, replayed: result.replayed });
    if (isAsync(notified)) Promise.resolve(notified).catch(() => {});
    assertApps(!isAsync(notified), 'apps_source_notification_invalid', 500);
    return { requestId: intent.requestId, ...result };
  }
  function history(actor, args) {
    exact(args, ['appId', 'limit', 'cursor']); const id = appId(args.appId), limit = args.limit ?? 20;
    assertApps(Number.isSafeInteger(limit) && limit >= 1 && limit <= 50, 'invalid_source_history_limit');
    return transaction(false, () => {
      const current = state(actor, id, undefined, undefined, false);
      let through = db.prepare('SELECT max(revision) AS revision FROM app_runtime_targets WHERE app_id=?').get(id).revision, before = null;
      if (args.cursor !== undefined) {
        assertApps(typeof args.cursor === 'string' && args.cursor.length <= 1024 && /^[A-Za-z0-9_-]+$/u.test(args.cursor), 'invalid_source_history_cursor');
        let cursor; try { cursor = JSON.parse(Buffer.from(args.cursor, 'base64url').toString('utf8')); } catch { throw new AppsError('invalid_source_history_cursor'); }
        exact(cursor, ['v', 'accountId', 'appId', 'through', 'before']);
        assertApps(cursor.v === 1 && cursor.accountId === actor.accountId && cursor.appId === id
          && Number.isSafeInteger(cursor.through) && cursor.through >= 1 && cursor.through <= through
          && Number.isSafeInteger(cursor.before) && cursor.before >= 1 && cursor.before <= cursor.through, 'invalid_source_history_cursor');
        through = cursor.through; before = cursor.before;
      }
      const rows = db.prepare('SELECT * FROM app_runtime_targets WHERE app_id=? AND revision<=? AND (? IS NULL OR revision<?) ORDER BY revision DESC LIMIT ?').all(id, through, before, before, limit + 1);
      const page = rows.slice(0, limit), targets = page.map(row => ({ ...targetView(targetTuple(row)), createdAt: row.created_at }));
      const nextCursor = rows.length > limit ? Buffer.from(JSON.stringify({ v: 1, accountId: actor.accountId, appId: id, through, before: page.at(-1).revision })).toString('base64url') : null;
      return { schema: 'soty.app-source-history.v1', appId: id, policyEpoch: current.policy.policy_epoch,
        activeTargetRevision: current.target.revision, requiredBindingVersion: current.requiredBindingVersion, targets, nextCursor };
    });
  }
  return {
    execute({ op, actor, args = {} }) {
      assertApps(!closed, 'apps_closed', 503); assertApps(sourceOperations.has(op), 'unsupported_operation');
      if (op === 'apps.source.prepare') return prepare(actor, args);
      if (op === 'apps.source.promote') return promote(actor, args);
      return history(actor, args);
    },
    close() { if (closed) return; closed = true; for (const entry of preparations.values()) expire(entry, 'apps_closed'); },
  };
}
