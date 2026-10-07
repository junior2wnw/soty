import { createHash } from 'node:crypto';
import { assertApps, appId, textId } from './protocol.mjs';
import { ensureInitialPublication, runtimeTargetDigest, requiredBindingVersion, RUNTIME_PROFILE, supportedRuntimeProfile } from './schema.mjs';
import { syncDiscussionAudienceInTransaction } from './discussions.mjs';

export const publicationOperations = new Set(['apps.publication.get', 'apps.publication.update']);
export const PUBLICATION_RECEIPTS_PER_APP = 64;
const maxActiveDomains = 100, accountTtl = 3_600_000, publicTtl = 30_000;
const digest = value => createHash('sha256').update(value).digest('hex');
const exact = (value, keys) => assertApps(value && typeof value === 'object' && !Array.isArray(value)
  && Object.keys(value).every(key => keys.includes(key)), 'unexpected_argument');
const positive = value => { assertApps(Number.isSafeInteger(value) && value >= 1, 'invalid_publication_revision'); return value; };
const domainId = value => { assertApps(typeof value === 'string' && /^dom_[a-f0-9]{32}$/u.test(value), 'invalid_app_domain_id'); return value; };

export function createPublicationRegistry({ db, now = Date.now, assertActor, canUse, onChanged = () => {} }) {
  assertApps(typeof assertActor === 'function' && typeof canUse === 'function' && typeof onChanged === 'function', 'apps_policy_validator_required', 500);
  const decisions = new WeakSet();
  const appRow = id => db.prepare('SELECT * FROM local_apps WHERE id=?').get(id);
  function transaction(callback) {
    assertApps(!db.isTransaction, 'apps_nested_transaction', 500);
    db.exec('BEGIN IMMEDIATE');
    try { const value = callback(); db.exec('COMMIT'); return value; }
    catch (error) { if (db.isTransaction) db.exec('ROLLBACK'); throw error; }
  }
  function snapshot(callback) {
    if (db.isTransaction) return callback();
    db.exec('BEGIN');
    try { const value = callback(); db.exec('COMMIT'); return value; }
    catch (error) { if (db.isTransaction) db.exec('ROLLBACK'); throw error; }
  }
  function own(actor, id) {
    assertActor(actor);
    const app = appRow(id);
    assertApps(app && app.owner_account_id === actor.accountId, 'apps_owner_required', 403);
    return app;
  }
  function state(app) {
    const policy = db.prepare('SELECT * FROM app_publications WHERE app_id=?').get(app.id);
    assertApps(policy && policy.owner_account_id === app.owner_account_id, 'apps_registry_corrupt', 500);
    const target = db.prepare('SELECT * FROM app_runtime_targets WHERE app_id=? AND revision=?').get(app.id, policy.active_target_revision);
    assertApps(target && target.owner_account_id === app.owner_account_id && supportedRuntimeProfile(target.profile)
      && target.digest === runtimeTargetDigest({ appId: target.app_id, revision: target.revision, ownerAccountId: target.owner_account_id,
        connectorKey: target.connector_key, port: target.port, entryPath: target.entry_path, profile: target.profile }), 'apps_registry_corrupt', 500);
    const device = db.prepare('SELECT owner_account_id FROM app_devices WHERE connector_key=?').get(target.connector_key);
    assertApps(device?.owner_account_id === app.owner_account_id, 'apps_registry_corrupt', 500);
    return { app, policy, target, requiredBindingVersion: requiredBindingVersion(db, app.id) };
  }
  function activeIds(id) {
    return db.prepare('SELECT domain_id FROM app_publication_domains WHERE app_id=? ORDER BY domain_id').all(id).map(item => item.domain_id);
  }
  function view(app) {
    const { policy, target, requiredBindingVersion } = state(app);
    return { schema: 'soty.app-publication.v1', appId: app.id, appState: app.state,
      launchPolicy: policy.launch_policy, listed: Boolean(policy.listed), policyEpoch: policy.policy_epoch,
      activeTargetRevision: target.revision, requiredBindingVersion, activeDomainIds: activeIds(app.id),
      target: { revision: target.revision, digest: target.digest, profile: target.profile, port: target.port, entryPath: target.entry_path },
      updatedAt: policy.updated_at, runtimeReady: false, receiptRetention: { perApp: PUBLICATION_RECEIPTS_PER_APP } };
  }
  function read(actor, id) { return snapshot(() => view(own(actor, id))); }
  function bumpEpoch(id, timestamp = now()) {
    assertApps(db.isTransaction, 'apps_transaction_required', 500);
    const current = db.prepare('SELECT policy_epoch FROM app_publications WHERE app_id=?').get(id);
    assertApps(current && Number.isSafeInteger(current.policy_epoch) && current.policy_epoch >= 1, 'apps_registry_corrupt', 500);
    assertApps(current.policy_epoch < Number.MAX_SAFE_INTEGER, 'apps_policy_epoch_exhausted', 500);
    db.prepare('UPDATE app_publications SET policy_epoch=policy_epoch+1,updated_at=? WHERE app_id=?').run(timestamp, id);
    return { appId: id, policyEpoch: current.policy_epoch + 1 };
  }
  function normalize(args) {
    exact(args, ['appId', 'requestId', 'expectedPolicyEpoch', 'expectedTargetRevision', 'launchPolicy', 'listed', 'activeDomainIds', 'exposureAck']);
    const id = appId(args.appId), requestId = textId(args.requestId), epoch = positive(args.expectedPolicyEpoch), targetRevision = positive(args.expectedTargetRevision);
    assertApps(['restricted', 'anyone'].includes(args.launchPolicy) && typeof args.listed === 'boolean', 'invalid_publication_policy');
    assertApps(!args.listed || args.launchPolicy === 'anyone', 'invalid_publication_listing');
    assertApps(Array.isArray(args.activeDomainIds) && args.activeDomainIds.length <= maxActiveDomains, 'invalid_publication_domains');
    const activeDomainIds = args.activeDomainIds.map(domainId).sort();
    assertApps(new Set(activeDomainIds).size === activeDomainIds.length, 'invalid_publication_domains');
    assertApps(!args.listed || activeDomainIds.length > 0, 'app_publication_domain_required');
    let exposureAck = null;
    if (args.launchPolicy === 'anyone') {
      assertApps(args.exposureAck && typeof args.exposureAck === 'object' && !Array.isArray(args.exposureAck), 'app_exposure_ack_required');
      exact(args.exposureAck, ['scope', 'targetRevision', 'targetDigest', 'profile']);
      assertApps(args.exposureAck.scope === 'whole-port' && args.exposureAck.targetRevision === targetRevision
        && typeof args.exposureAck.targetDigest === 'string' && /^[a-f0-9]{64}$/u.test(args.exposureAck.targetDigest)
        && args.exposureAck.profile === RUNTIME_PROFILE, 'app_exposure_ack_required');
      exposureAck = { scope: 'whole-port', targetRevision, targetDigest: args.exposureAck.targetDigest, profile: args.exposureAck.profile };
    } else assertApps(args.exposureAck === undefined || args.exposureAck === null, 'unexpected_exposure_ack');
    const normalized = { id, requestId, epoch, targetRevision, launchPolicy: args.launchPolicy, listed: args.listed, activeDomainIds, exposureAck };
    return { ...normalized, requestKey: digest(requestId), intentHash: digest(JSON.stringify(['apps.publication.update.v1', id, epoch,
      targetRevision, args.launchPolicy, args.listed, activeDomainIds, exposureAck])) };
  }
  function update(actor, args) {
    const intent = normalize(args);
    const result = transaction(() => {
      // Owner validity is current, even when returning historical successful intent.
      const app = own(actor, intent.id);
      const previous = db.prepare('SELECT intent_hash,value_json FROM app_publication_receipts WHERE account_id=? AND request_key=?')
        .get(actor.accountId, intent.requestKey);
      if (previous) {
        assertApps(previous.intent_hash === intent.intentHash, 'app_publication_request_conflict', 409);
        return { receipt: JSON.parse(previous.value_json), replayed: true, current: view(app) };
      }
      const { policy, target } = state(app);
      assertApps(policy.policy_epoch === intent.epoch, 'app_publication_revision_conflict', 409);
      assertApps(target.revision === intent.targetRevision, 'app_publication_target_conflict', 409);
      assertApps(app.state === 'enabled', 'app_revoked', 409);
      if (intent.exposureAck) assertApps(intent.exposureAck.targetDigest === target.digest && intent.exposureAck.profile === target.profile, 'app_exposure_ack_required');
      for (const id of intent.activeDomainIds) {
        const domain = db.prepare('SELECT app_id,owner_account_id,role,state FROM app_domains WHERE id=?').get(id);
        assertApps(domain && domain.app_id === app.id && domain.owner_account_id === actor.accountId
          && domain.role === 'alias' && domain.state === 'bound', 'app_publication_domain_unavailable', 409);
      }
      const timestamp = now(), changed = bumpEpoch(app.id, timestamp);
      db.prepare(`UPDATE app_publications SET launch_policy=?,listed=?,exposure_ack_revision=?,exposure_ack_json=? WHERE app_id=?`)
        .run(intent.launchPolicy, Number(intent.listed), intent.exposureAck ? target.revision : null,
          intent.exposureAck ? JSON.stringify(intent.exposureAck) : null, app.id);
      db.prepare('DELETE FROM app_publication_domains WHERE app_id=?').run(app.id);
      const insert = db.prepare('INSERT INTO app_publication_domains VALUES (?,?,?)');
      for (const id of intent.activeDomainIds) insert.run(app.id, id, actor.accountId);
      syncDiscussionAudienceInTransaction(db, app.id, timestamp);
      const current = view(app);
      const receipt = { schema: 'soty.app-publication-receipt.v1', namespace: 'apps.publication.update.v1', requestKeyHash: intent.requestKey,
        appId: app.id, policyEpoch: changed.policyEpoch, launchPolicy: current.launchPolicy, listed: current.listed,
        activeDomainIds: [...intent.activeDomainIds], targetRevision: target.revision, targetDigest: target.digest,
        profile: target.profile, exposureAck: intent.exposureAck, committedAt: timestamp };
      db.prepare('INSERT INTO app_publication_receipts VALUES (?,?,?,?,?,?,?)')
        .run(actor.accountId, intent.requestKey, intent.intentHash, app.id, changed.policyEpoch, JSON.stringify(receipt), timestamp);
      // Every accepted intent, including a no-op, advances epoch. A pruned
      // identical retry therefore fails CAS; it is never silently rebased.
      db.prepare(`DELETE FROM app_publication_receipts WHERE app_id=? AND committed_epoch NOT IN
        (SELECT committed_epoch FROM app_publication_receipts WHERE app_id=? ORDER BY committed_epoch DESC LIMIT ?)`)
        .run(app.id, app.id, PUBLICATION_RECEIPTS_PER_APP);
      return { receipt, replayed: false, current };
    });
    if (!result.replayed) onChanged({ appId: intent.id, policyEpoch: result.receipt.policyEpoch });
    return { requestId: intent.requestId, ...result };
  }
  function ttl(value, subject) {
    assertApps(Number.isSafeInteger(value) && value >= 1 && value <= (subject === 'public' ? publicTtl : accountTtl), 'invalid_app_access_ttl');
    return value;
  }
  function loadAccess({ domainId: id, origin, actor }, forcedBasis) {
    domainId(id);
    const domain = db.prepare('SELECT * FROM app_domains WHERE id=?').get(id), app = domain && appRow(domain.app_id);
    assertApps(domain && domain.origin === origin && domain.state === 'bound' && app?.state === 'enabled'
      && domain.owner_account_id === app.owner_account_id, 'apps_access_denied', 403);
    const { policy, target, requiredBindingVersion } = state(app);
    if (domain.role === 'alias') assertApps(db.prepare('SELECT 1 FROM app_publication_domains WHERE app_id=? AND domain_id=? AND owner_account_id=?')
      .get(app.id, domain.id, app.owner_account_id), 'apps_access_denied', 403);
    const subject = actor === undefined ? 'public' : 'account';
    if (subject === 'account') assertActor(actor);
    // Only recheck supplies forcedBasis, after verifying the original brand.
    // A public stream keeps its original capacity class when membership grows;
    // a grant stream may never silently fall back to public after a revocation.
    const granted = subject === 'account' && forcedBasis !== 'public' && canUse(actor, app) === true;
    const accessBasis = forcedBasis ?? (granted ? 'grant' : 'public');
    if (accessBasis === 'grant') assertApps(granted, 'apps_access_denied', 403);
    else assertApps(accessBasis === 'public' && domain.role === 'alias' && policy.launch_policy === 'anyone', 'apps_access_denied', 403);
    return { subject, accessBasis, ...(subject === 'account' ? { actor: { accountId: actor.accountId, deviceId: actor.deviceId } } : {}),
      appId: app.id, domainId: domain.id, origin: domain.origin, policyEpoch: policy.policy_epoch,
      targetRevision: target.revision, targetDigest: target.digest, profile: target.profile, requiredBindingVersion,
      route: { connectorKey: target.connector_key, port: target.port, entryPath: target.entry_path } };
  }
  function brand(value, expiresAt) {
    assertApps(Number.isSafeInteger(expiresAt), 'invalid_app_access_ttl');
    const result = Object.freeze({ ...value, ...(value.actor ? { actor: Object.freeze(value.actor) } : {}), route: Object.freeze(value.route), expiresAt });
    decisions.add(result); return result;
  }
  function decideAccess({ domainId, origin, actor, ttlMs = publicTtl }) {
    return snapshot(() => {
      const value = loadAccess({ domainId, origin, actor });
      return brand(value, now() + ttl(ttlMs, value.subject));
    });
  }
  function recheckAccess(decision, options = {}) {
    assertApps(decisions.has(decision), 'app_access_decision_required', 403);
    exact(options, ['ttlMs']);
    return snapshot(() => {
      const timestamp = now(); assertApps(decision.expiresAt > timestamp, 'app_access_expired', 403);
      const fresh = loadAccess({ domainId: decision.domainId, origin: decision.origin, actor: decision.actor }, decision.accessBasis);
      assertApps(['subject', 'accessBasis', 'appId', 'domainId', 'origin', 'policyEpoch', 'targetRevision', 'targetDigest', 'profile', 'requiredBindingVersion']
        .every(key => fresh[key] === decision[key]), 'app_access_changed', 403);
      return brand(fresh, options.ttlMs === undefined ? decision.expiresAt : timestamp + ttl(options.ttlMs, fresh.subject));
    });
  }
  return {
    execute({ op, actor, args = {} }) {
      assertApps(publicationOperations.has(op), 'unsupported_operation');
      if (op === 'apps.publication.get') { exact(args, ['appId']); return read(actor, appId(args.appId)); }
      return update(actor, args);
    },
    initForApp(app) { ensureInitialPublication(db, app); syncDiscussionAudienceInTransaction(db, app.id, now()); },
    sourceStateInTransaction(id) {
      assertApps(db.isTransaction, 'apps_transaction_required', 500);
      const app = appRow(appId(id)); assertApps(app, 'apps_registry_corrupt', 500);
      return state(app);
    },
    promoteSourceInTransaction({ appId: id, targetRevision, launchPolicy, listed, exposureAck }) {
      assertApps(db.isTransaction, 'apps_transaction_required', 500);
      appId(id); positive(targetRevision);
      assertApps(['restricted', 'anyone'].includes(launchPolicy) && typeof listed === 'boolean'
        && (!listed || launchPolicy === 'anyone'), 'invalid_publication_policy');
      assertApps(!listed || activeIds(id).length > 0, 'app_publication_domain_required');
      const target = db.prepare('SELECT * FROM app_runtime_targets WHERE app_id=? AND revision=?').get(id, targetRevision);
      assertApps(target, 'apps_registry_corrupt', 500);
      assertApps(target.profile === RUNTIME_PROFILE || launchPolicy === 'restricted', 'app_scoped_private_required', 403);
      if (launchPolicy === 'anyone') {
        exact(exposureAck, ['scope', 'targetRevision', 'targetDigest', 'profile']);
        assertApps(exposureAck.scope === 'whole-port' && exposureAck.targetRevision === targetRevision
          && exposureAck.targetDigest === target.digest && exposureAck.profile === target.profile, 'app_exposure_ack_required');
      } else assertApps(exposureAck === null || exposureAck === undefined, 'unexpected_exposure_ack');
      const changed = bumpEpoch(id);
      db.prepare(`UPDATE app_publications SET active_target_revision=?,launch_policy=?,listed=?,exposure_ack_revision=?,exposure_ack_json=? WHERE app_id=?`)
        .run(targetRevision, launchPolicy, Number(listed), exposureAck ? targetRevision : null, exposureAck ? JSON.stringify(exposureAck) : null, id);
      syncDiscussionAudienceInTransaction(db, id, now());
      return { ...changed, current: view(appRow(id)) };
    },
    grantsChangedInTransaction(id) {
      const changed = bumpEpoch(appId(id));
      syncDiscussionAudienceInTransaction(db, id, now());
      return changed;
    },
    revokeInTransaction(id) {
      assertApps(db.isTransaction, 'apps_transaction_required', 500); appId(id);
      const changed = bumpEpoch(id);
      db.prepare("UPDATE app_publications SET launch_policy='restricted',listed=0,exposure_ack_revision=NULL,exposure_ack_json=NULL WHERE app_id=?").run(id);
      db.prepare('DELETE FROM app_publication_domains WHERE app_id=?').run(id);
      syncDiscussionAudienceInTransaction(db, id, now());
      return changed;
    },
    retireInTransaction({ appId: id, domainId: retiringId }) {
      assertApps(db.isTransaction, 'apps_transaction_required', 500); appId(id); domainId(retiringId);
      const result = db.prepare('DELETE FROM app_publication_domains WHERE app_id=? AND domain_id=?').run(id, retiringId);
      if (!result.changes) return null;
      const changed = bumpEpoch(id);
      if (activeIds(id).length === 0) db.prepare('UPDATE app_publications SET listed=0 WHERE app_id=?').run(id);
      return changed;
    },
    notifyChanged: onChanged,
    decideAccess, recheckAccess,
  };
}
