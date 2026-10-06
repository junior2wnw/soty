import { DatabaseSync } from 'node:sqlite';
import { mkdirSync } from 'node:fs';
import { dirname } from 'node:path';
import { createAdmissionHost, closeAdmissionHost, materializeAuthorDraft, validateDescriptor, validateAuthorDraft,
  parseContractJson, planAdmission, contractDigest, canonicalContractJson } from '../index.mjs';
import { snapshot } from '../json.mjs';
import { freezeDeep } from '../../capabilities/server/validation.mjs';
import { RegistrationError, requireRegistration as require, closed, literalId, integer, synchronous,
  refPart, captureReviewedConfiguration, buildTrustedAppConfiguration, configurationPins, REGISTRIES } from './authority.mjs';
import { initializeRegistrationSchema } from './schema.mjs';

export const UNIVERSAL_REGISTRATION_OPERATIONS = Object.freeze([
  'apps.universal.plan', 'apps.universal.admit', 'apps.universal.get', 'apps.universal.history',
]);
export const REGISTRATION_LIMITS = Object.freeze({ appsPerOwner: 128, receiptsPerOwner: 4096,
  versionsPerApp: 256, referenceHistory: 4096, historyPage: 50, responseBytes: 128 * 1024 });
export const REGISTRATION_STORAGE_LIMITS = Object.freeze({ maxDatabaseBytes: 16 * 1024 * 1024, minDatabaseBytes: 64 * 1024 });
const same = (a, b) => canonicalContractJson(a) === canonicalContractJson(b);
const encode = value => canonicalContractJson(value);
const readJson = value => parseContractJson(value);
const isBusy = error => Number.isInteger(error?.errcode) && [5, 6].includes(error.errcode & 255);
const validFunction = (fn, code) => require(typeof fn === 'function' && fn.constructor?.name !== 'AsyncFunction', code, 503);
function captureActor(actor, active) {
  const value = snapshot({ accountId: actor?.accountId, deviceId: actor?.deviceId });
  literalId(value.accountId); literalId(value.deviceId);
  require(synchronous(active(value)) === true, 'registration_authentication_required', 401);
  return freezeDeep(value);
}
function captureFeedback(port) {
  if (port === undefined) return null;
  closed(port, ['ensureInstallation', 'inspectInstallation'], [], 'registration_feedback_port_invalid');
  validFunction(port.ensureInstallation, 'registration_feedback_port_invalid');
  validFunction(port.inspectInstallation, 'registration_feedback_port_invalid');
  return Object.freeze({ ensureInstallation: port.ensureInstallation.bind(port), inspectInstallation: port.inspectInstallation.bind(port) });
}
function limitsFor(input = {}) {
  const values = snapshot(input); closed(values, [], Object.keys(REGISTRATION_LIMITS), 'registration_limits_invalid');
  const limits = { ...REGISTRATION_LIMITS, ...values };
  for (const [key, value] of Object.entries(limits)) require(Number.isSafeInteger(value) && value >= 1
    && value <= REGISTRATION_LIMITS[key], 'registration_limits_invalid');
  return Object.freeze(limits);
}
function proposal(input) {
  closed(input, ['kind'], ['draft', 'json']);
  if (input.kind === 'author-draft') {
    closed(input, ['kind', 'draft']); return freezeDeep({ kind: input.kind, draft: validateAuthorDraft(input.draft) });
  }
  require(input.kind === 'descriptor-json', 'registration_proposal_unsupported');
  closed(input, ['kind', 'json']);
  return freezeDeep({ kind: input.kind, descriptor: validateDescriptor(parseContractJson(input.json)) });
}
function feedbackProof(input, expected) {
  const proof = snapshot(input);
  closed(proof, ['installationId', 'receiptDigest', 'provisioningKey', 'scope', 'source', 'profile', 'authorityDigest', 'generation'],
    [], 'feedback_receipt_mismatch');
  literalId(proof.installationId);
  require(typeof proof.receiptDigest === 'string' && /^[a-f0-9]{64}$/u.test(proof.receiptDigest), 'feedback_receipt_mismatch', 409);
  const received = { provisioningKey: proof.provisioningKey, scope: proof.scope, source: proof.source,
    profile: proof.profile, authorityDigest: proof.authorityDigest, generation: proof.generation };
  const pinned = { provisioningKey: expected.provisioningKey, scope: expected.scope, source: expected.source,
    profile: refPart(expected.profile), authorityDigest: expected.authorityDigest, generation: expected.generation };
  require(same(received, pinned), 'feedback_receipt_mismatch', 409);
  return freezeDeep(proof);
}

/**
 * Durable metadata/admission service. Hosts must hold current Connect authority before calling this service.
 * withReviewedAppAuthority additionally holds the owner/source Apps fence through this store's COMMIT.
 * No provider/transport discovery, executable descriptor or capability grant is installed here.
 */
export function createUniversalRegistrationService({ databasePath, registryId, environmentId, actorActive,
  withReviewedAppAuthority, reviewedProfile, approvedReferences = {}, selectApprovedReferences, feedback, now = Date.now, limits = {},
  maxDatabaseBytes = REGISTRATION_STORAGE_LIMITS.maxDatabaseBytes } = {}) {
  literalId(registryId); literalId(environmentId);
  require(typeof databasePath === 'string' && databasePath.length > 0, 'registration_database_required', 503);
  validFunction(actorActive, 'registration_authority_required'); validFunction(withReviewedAppAuthority, 'registration_authority_required');
  validFunction(now, 'registration_clock_invalid');
  if (selectApprovedReferences !== undefined) validFunction(selectApprovedReferences, 'registration_reference_selector_invalid');
  require(Number.isSafeInteger(maxDatabaseBytes) && maxDatabaseBytes >= REGISTRATION_STORAGE_LIMITS.minDatabaseBytes
    && maxDatabaseBytes <= REGISTRATION_STORAGE_LIMITS.maxDatabaseBytes, 'registration_database_quota_invalid', 503);
  const policy = captureReviewedConfiguration(reviewedProfile, approvedReferences), bound = limitsFor(limits), provider = captureFeedback(feedback);
  mkdirSync(dirname(databasePath), { recursive: true });
  const db = new DatabaseSync(databasePath);
  let identity, maxPages, disposed = false;
  try {
    db.exec('PRAGMA foreign_keys=ON; PRAGMA busy_timeout=100; PRAGMA synchronous=FULL;');
    identity = initializeRegistrationSchema(db, { registryId, environmentId });
    db.exec('PRAGMA journal_mode=WAL;');
    const pageSize = Number(db.prepare('PRAGMA page_size').get().page_size);
    require(Number.isSafeInteger(pageSize) && pageSize >= 512, 'registration_storage_corrupt', 503);
    maxPages = Math.floor(maxDatabaseBytes / pageSize);
    // A lower operational quota retains old readers and exact replay. It does
    // not evict durable pin/history rows to obtain room for a new mutation.
    db.exec(`PRAGMA max_page_count=${maxPages}`);
  } catch (error) { db.close(); throw error; }
  const head = scopeKey => db.prepare('SELECT * FROM registration_heads WHERE scope_key=?').get(scopeKey);
  const version = row => db.prepare('SELECT * FROM registration_versions WHERE scope_key=? AND generation=?').get(row.scope_key, row.generation);
  const timestamp = () => integer(now(), 0, Number.MAX_SAFE_INTEGER);
  const writeCapacity = () => require(Number(db.prepare('PRAGMA page_count').get().page_count) < maxPages, 'registration_storage_full', 503);
  function pins(configuration) {
    for (const { kind, value } of configurationPins(configuration)) {
      const prior = db.prepare('SELECT content_digest FROM registration_reference_history WHERE kind=? AND ref_id=? AND ref_version=?')
        .get(kind, value.id, value.version);
      const digest = contractDigest(value);
      require(!prior || prior.content_digest === digest, kind === 'capabilityContracts' ? 'immutable_capability_conflict' : 'immutable_pin_conflict', 409);
      if (!prior) {
        const count = Number(db.prepare('SELECT count(*) AS count FROM registration_reference_history').get().count);
        require(count < bound.referenceHistory, 'registration_reference_history_limit', 429);
        writeCapacity();
        db.prepare('INSERT INTO registration_reference_history(kind,ref_id,ref_version,content_digest,content_json) VALUES(?,?,?,?,?)')
          .run(kind, value.id, value.version, digest, encode(value));
      }
    }
  }
  function reviewed(sourceSnapshot, actor, appId) {
    const source = freezeDeep(snapshot(sourceSnapshot));
    require(source.appId === appId, 'registration_app_not_owned', 403);
    let initial = buildTrustedAppConfiguration({ sourceSnapshot: source, actor, registryId, environmentId, ...policy });
    if (selectApprovedReferences) {
      // Scope/source come from the captured current Apps owner transaction.
      // The port is synchronous host code and can select only the admitted
      // startup universe; RPC args never choose a ref, URL or handler here.
      const selection = snapshot(synchronous(selectApprovedReferences(freezeDeep({
        scope: initial.configuration.context.scope, source: initial.configuration.context.source, actor,
      }))));
      closed(selection, ['references', 'bindingDigest'], [], 'registration_reference_selection_invalid');
      require(typeof selection.bindingDigest === 'string' && /^[a-f0-9]{64}$/u.test(selection.bindingDigest), 'registration_reference_selection_invalid');
      const selected = captureReviewedConfiguration(policy.reviewedProfile, selection.references);
      for (const kind of REGISTRIES) {
        for (const value of selected.approvedReferences[kind]) require(policy.approvedReferences[kind].some(approved => same(approved, value)),
          'registration_reference_not_approved', 403);
      }
      const order = (a, b) => a.id < b.id ? -1 : a.id > b.id ? 1 : a.version - b.version;
      const normalized = { ...selected, approvedReferences: Object.fromEntries(REGISTRIES.map(kind => [kind, [...selected.approvedReferences[kind]].sort(order)])) };
      initial = buildTrustedAppConfiguration({ sourceSnapshot: source, actor, registryId, environmentId, ...normalized });
      initial = { ...initial, fingerprint: contractDigest({ fingerprint: initial.fingerprint, bindingDigest: selection.bindingDigest }) };
    }
    const prior = db.prepare('SELECT * FROM registration_authorities WHERE scope_key=?').get(initial.scopeKey);
    const generation = prior ? prior.generation + Number(prior.fingerprint !== initial.fingerprint) : 1;
    integer(generation, 1);
    const configuration = { ...initial.configuration, context: { ...initial.configuration.context, authorityRevision: generation } };
    const authorityDigest = contractDigest(configuration);
    pins(configuration);
    if (!prior) {
      const count = Number(db.prepare('SELECT count(*) AS count FROM registration_authorities WHERE owner_id=?').get(actor.accountId).count);
      require(count < bound.appsPerOwner, 'registration_app_limit', 429);
      writeCapacity();
      db.prepare('INSERT INTO registration_authorities(scope_key,scope_json,owner_id,generation,fingerprint,authority_digest,updated_at) VALUES(?,?,?,?,?,?,?)')
        .run(initial.scopeKey, encode(configuration.context.scope), actor.accountId, generation, initial.fingerprint, authorityDigest, timestamp());
    } else if (generation !== prior.generation) {
      require(prior.owner_id === actor.accountId, 'registration_app_not_owned', 403);
      writeCapacity();
      db.prepare('UPDATE registration_authorities SET generation=?,fingerprint=?,authority_digest=?,updated_at=? WHERE scope_key=? AND generation=?')
        .run(generation, initial.fingerprint, authorityDigest, timestamp(), initial.scopeKey, prior.generation);
    } else require(prior.authority_digest === authorityDigest, 'registration_storage_corrupt', 503);
    return { scopeKey: initial.scopeKey, configuration: freezeDeep(configuration), authorityDigest };
  }
  function fenced(actorInput, appId, callback) {
    require(!disposed, 'registration_closed', 503);
    const actor = captureActor(actorInput, actorActive);
    literalId(appId);
    let active = true, entered = false, outcome;
    try {
      const value = withReviewedAppAuthority({ actor, appId }, sourceSnapshot => {
        require(active && !entered, 'registration_authority_fence_invalid', 500); entered = true;
        require(!db.isTransaction, 'registration_nested_transaction', 500);
        try {
          db.exec('BEGIN IMMEDIATE');
          require(synchronous(actorActive(actor)) === true, 'registration_authentication_required', 401);
          const context = reviewed(sourceSnapshot, actor, appId);
          outcome = synchronous(callback(actor, context));
          require(Buffer.byteLength(JSON.stringify(outcome)) <= bound.responseBytes, 'registration_response_limit', 500);
          require(synchronous(actorActive(actor)) === true, 'registration_authentication_required', 401);
          db.exec('COMMIT'); return outcome;
        } catch (error) {
          if (db.isTransaction) db.exec('ROLLBACK');
          if (isBusy(error)) throw new RegistrationError('registration_busy', 503);
          if (Number.isInteger(error?.errcode) && (error.errcode & 255) === 13) throw new RegistrationError('registration_storage_full', 503);
          throw error;
        }
      });
      synchronous(value, 'registration_authority_fence_invalid');
      require(entered, 'registration_authority_fence_invalid', 500);
      return freezeDeep(outcome);
    } finally { active = false; }
  }
  function project(row, context) {
    if (!row) return null;
    const saved = version(row); require(saved, 'registration_storage_corrupt', 503);
    const descriptor = validateDescriptor(readJson(saved.descriptor_json));
    require(contractDigest(descriptor) === row.descriptor_digest && saved.authority_digest === row.authority_digest,
      'registration_storage_corrupt', 503);
    const current = row.authority_digest === context.authorityDigest;
    const plan = readJson(saved.plan_json), receipt = row.feedback_json ? readJson(row.feedback_json) : null;
    let feedbackCurrent = false;
    if (current && row.status === 'ready' && provider) {
      const outbox = db.prepare('SELECT request_json FROM registration_feedback_outbox WHERE scope_key=? AND generation=?').get(row.scope_key, row.generation);
      require(outbox, 'registration_storage_corrupt', 503);
      const request = freezeDeep(readJson(outbox.request_json));
      const inspected = synchronous(provider.inspectInstallation(request));
      if (inspected !== null && inspected !== undefined) {
        const fresh = feedbackProof(inspected, request);
        require(same(fresh, receipt), 'feedback_receipt_conflict', 409); feedbackCurrent = true;
      }
    }
    const state = !current ? 'authority-stale' : row.status === 'ready' && !feedbackCurrent ? 'feedback-held' : row.status;
    return { schema: 'soty.universal-registration.v1', appId: row.app_id, revision: row.revision, generation: row.generation,
      state, descriptorDigest: row.descriptor_digest, authorityDigest: row.authority_digest,
      // An old authority may inspect metadata but does not expose old descriptor/receipt bodies as current grants.
      ...(current ? { descriptor } : {}),
      feedback: { state: feedbackCurrent ? 'ready' : 'pending-feedback', provisioningKey: plan.feedbackIntent.provisioningKey,
        ...(feedbackCurrent ? { installationId: receipt.installationId } : {}) },
      gates: { ui: !current || state === 'feedback-held' ? 'held' : feedbackCurrent ? 'ready' : 'pending-feedback',
        agent: 'not-admitted', local: 'not-admitted' } };
  }
  function prepare(context, requested, requestId) {
    const host = createAdmissionHost(context.configuration);
    try {
      const descriptor = requested.kind === 'author-draft' ? materializeAuthorDraft(host, requested.draft) : requested.descriptor;
      require(same(descriptor.feedback, policy.reviewedProfile.feedback), 'feedback_authority_mismatch', 403);
      const plan = planAdmission(host, descriptor, requestId);
      return { descriptor, plan };
    } finally { closeAdmissionHost(host); }
  }
  function expected(row, revision) {
    integer(revision); require((row?.revision || 0) === revision, 'registration_revision_conflict', 409);
  }
  function write(actor, context, args, requested) {
    const inputDigest = contractDigest({ appId: args.appId, expectedRevision: args.expectedRevision, proposal: requested });
    const occupied = db.prepare('SELECT * FROM registration_receipts WHERE account_id=? AND request_id=?').get(actor.accountId, args.requestId);
    const existing = head(context.scopeKey);
    if (occupied) {
      require(occupied.intent_hash === inputDigest && occupied.scope_key === context.scopeKey, 'registration_intent_conflict', 409);
      const receipt = readJson(occupied.receipt_json);
      require(receipt.authorityDigest === context.authorityDigest, 'registration_authority_changed', 409);
      return { registration: project(existing, context), receipt, replayed: true };
    }
    expected(existing, args.expectedRevision);
    writeCapacity();
    const { descriptor, plan } = prepare(context, requested, args.requestId);
    const count = Number(db.prepare('SELECT count(*) AS count FROM registration_receipts WHERE account_id=?').get(actor.accountId).count);
    require(count < bound.receiptsPerOwner, 'registration_receipt_limit', 429);
    const versionCount = Number(db.prepare('SELECT count(*) AS count FROM registration_versions WHERE scope_key=?').get(context.scopeKey).count);
    require(versionCount < bound.versionsPerApp, 'registration_version_limit', 429);
    const revision = (existing?.revision || 0) + 1, generation = (existing?.generation || 0) + 1, time = timestamp();
    integer(revision, 1); integer(generation, 1);
    if (existing) {
      const changed = db.prepare(`UPDATE registration_heads SET revision=?,generation=?,descriptor_digest=?,authority_digest=?,authority_revision=?,
        intent_digest=?,request_id=?,status='pending-feedback',feedback_json=NULL,updated_at=? WHERE scope_key=? AND revision=?`)
        .run(revision, generation, plan.descriptorDigest, plan.authorityDigest, plan.authorityRevision, plan.intentDigest, args.requestId, time,
          context.scopeKey, args.expectedRevision);
      require(changed.changes === 1, 'registration_revision_conflict', 409);
    } else db.prepare(`INSERT INTO registration_heads(scope_key,app_id,owner_id,revision,generation,descriptor_digest,authority_digest,authority_revision,
      intent_digest,request_id,status,feedback_json,updated_at) VALUES(?,?,?,?,?,?,?,?,?,?,'pending-feedback',NULL,?)`)
      .run(context.scopeKey, args.appId, actor.accountId, revision, generation, plan.descriptorDigest, plan.authorityDigest,
        plan.authorityRevision, plan.intentDigest, args.requestId, time);
    db.prepare(`INSERT INTO registration_versions(scope_key,generation,committed_revision,descriptor_digest,descriptor_json,authority_digest,
      authority_revision,plan_json,created_at) VALUES(?,?,?,?,?,?,?,?,?)`)
      .run(context.scopeKey, generation, revision, plan.descriptorDigest, encode(descriptor), plan.authorityDigest, plan.authorityRevision,
        encode({ scope: plan.scope, feedbackIntent: plan.feedbackIntent }), time);
    const feedbackRequest = { scope: plan.scope, source: descriptor.app.source, profile: policy.reviewedProfile,
      provisioningKey: plan.feedbackIntent.provisioningKey, intentDigest: plan.intentDigest, authorityDigest: plan.authorityDigest, generation };
    db.prepare('INSERT INTO registration_feedback_outbox(scope_key,generation,provisioning_key,request_json,created_at) VALUES(?,?,?,?,?)')
      .run(context.scopeKey, generation, feedbackRequest.provisioningKey, encode(feedbackRequest), time);
    const receipt = { schema: 'soty.app-registration-receipt.v1', requestId: args.requestId, appId: args.appId, generation,
      committedRevision: revision, descriptorDigest: plan.descriptorDigest, authorityDigest: plan.authorityDigest, intentDigest: plan.intentDigest };
    db.prepare('INSERT INTO registration_receipts(account_id,request_id,intent_hash,scope_key,generation,receipt_json,created_at) VALUES(?,?,?,?,?,?,?)')
      .run(actor.accountId, args.requestId, inputDigest, context.scopeKey, generation, encode(receipt), time);
    return { registration: project(head(context.scopeKey), context), receipt, replayed: false };
  }
  function argsFor(op, actor, input) {
    const args = snapshot(input);
    const writeOp = op === 'apps.universal.plan' || op === 'apps.universal.admit';
    closed(args, writeOp ? ['expectedAccountId', 'appId', 'requestId', 'expectedRevision', 'proposal'] : ['expectedAccountId', 'appId'],
      op === 'apps.universal.history' ? ['limit', 'cursor'] : []);
    require(args.expectedAccountId === actor?.accountId, 'registration_account_mismatch', 403);
    literalId(args.appId);
    if (writeOp) { literalId(args.requestId); integer(args.expectedRevision); }
    return args;
  }
  function history(context, args) {
    const limit = args.limit === undefined ? Math.min(20, bound.historyPage) : integer(args.limit, 1, bound.historyPage);
    let before = 1000001;
    if (args.cursor !== undefined && args.cursor !== null) {
      require(typeof args.cursor === 'string' && /^[A-Za-z0-9_-]{1,256}$/u.test(args.cursor), 'registration_cursor_invalid');
      let cursor; try { cursor = readJson(Buffer.from(args.cursor, 'base64url')); } catch { throw new RegistrationError('registration_cursor_invalid'); }
      closed(cursor, ['scopeKey', 'before'], [], 'registration_cursor_invalid');
      require(cursor.scopeKey === context.scopeKey && Number.isSafeInteger(cursor.before) && cursor.before >= 1 && cursor.before <= 1000000
        && Buffer.from(encode(cursor)).toString('base64url') === args.cursor, 'registration_cursor_invalid');
      before = cursor.before;
    }
    const rows = db.prepare(`SELECT generation,committed_revision,descriptor_digest,authority_digest,created_at FROM registration_versions
      WHERE scope_key=? AND generation<? ORDER BY generation DESC LIMIT ?`).all(context.scopeKey, before, limit + 1);
    const more = rows.length > limit, items = rows.slice(0, limit).map(row => ({ generation: row.generation, committedRevision: row.committed_revision,
      descriptorDigest: row.descriptor_digest, authorityDigest: row.authority_digest, authorityCurrent: row.authority_digest === context.authorityDigest,
      createdAt: row.created_at }));
    return { schema: 'soty.app-registration-history.v1', appId: args.appId, items,
      nextCursor: more ? Buffer.from(encode({ scopeKey: context.scopeKey, before: items.at(-1).generation })).toString('base64url') : null };
  }
  return Object.freeze({
    operations: new Set(UNIVERSAL_REGISTRATION_OPERATIONS), schemaVersion: identity.schemaVersion,
    execute({ op, actor, args: input = {} }) {
      require(UNIVERSAL_REGISTRATION_OPERATIONS.includes(op), 'unsupported_operation');
      const args = argsFor(op, actor, input);
      const requested = args.proposal === undefined ? null : proposal(args.proposal);
      return fenced(actor, args.appId, (captured, context) => {
        if (op === 'apps.universal.get') return { registration: project(head(context.scopeKey), context) };
        if (op === 'apps.universal.history') return history(context, args);
        if (op === 'apps.universal.admit') return write(captured, context, args, requested);
        const existing = head(context.scopeKey); expected(existing, args.expectedRevision);
        const { descriptor, plan } = prepare(context, requested, args.requestId);
        return { schema: 'soty.universal-registration-plan.v1', appId: args.appId, expectedRevision: args.expectedRevision,
          nextGeneration: (existing?.generation || 0) + 1, descriptorDigest: plan.descriptorDigest, authorityDigest: plan.authorityDigest,
          descriptor, feedback: { state: 'pending-feedback', provisioningKey: plan.feedbackIntent.provisioningKey },
          gates: { ui: 'pending-feedback', agent: 'not-admitted', local: 'not-admitted' } };
      });
    },
    /** Host orchestration only, under an outer Connect authority fence. Never register this method as an incoming ready mutation. */
    reconcileFeedback({ actor, appId, expectedRevision }) {
      integer(expectedRevision, 1);
      return fenced(actor, appId, (_captured, context) => {
        const current = head(context.scopeKey); require(current, 'registration_not_found', 404);
        expected(current, expectedRevision);
        require(current.authority_digest === context.authorityDigest, 'registration_authority_changed', 409);
        if (current.status === 'ready' || !provider) return { registration: project(current, context) };
        const outbox = db.prepare('SELECT request_json FROM registration_feedback_outbox WHERE scope_key=? AND generation=?').get(context.scopeKey, current.generation);
        require(outbox, 'registration_storage_corrupt', 503);
        const request = freezeDeep(readJson(outbox.request_json));
        writeCapacity();
        let proof = synchronous(provider.inspectInstallation(request));
        if (proof === null || proof === undefined) {
          const created = synchronous(provider.ensureInstallation(request));
          if (created !== null && created !== undefined) feedbackProof(created, request);
          proof = synchronous(provider.inspectInstallation(request));
        }
        if (proof === null || proof === undefined) return { registration: project(current, context) };
        const receipt = feedbackProof(proof, request);
        const changed = db.prepare(`UPDATE registration_heads SET revision=revision+1,status='ready',feedback_json=?,updated_at=? WHERE scope_key=? AND revision=?`)
          .run(encode(receipt), timestamp(), context.scopeKey, expectedRevision);
        require(changed.changes === 1, 'registration_revision_conflict', 409);
        return { registration: project(head(context.scopeKey), context) };
      });
    },
    close() { if (disposed) return false; disposed = true; db.close(); return true; },
  });
}
