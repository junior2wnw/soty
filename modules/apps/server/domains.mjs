import { createHash, randomBytes } from 'node:crypto';
import { assertApps, appId, textId } from './protocol.mjs';
import { normalizeDomainLimits, normalizeAppSlug, isReservedAppSlug, normalizeNamedAppZone, namedZone, validateNamedOrigins } from './domain-policy.mjs';
import { ensureCanonicalDomain, insertDomainZone } from './schema.mjs';

export const domainOperations = new Set(['apps.domains.get', 'apps.names.check', 'apps.domains.claim', 'apps.domains.retire']);
const digest = value => createHash('sha256').update(value).digest('hex');
const exact = (args, keys) => assertApps(Object.keys(args).every(key => keys.includes(key)), 'unexpected_argument');
const revision = value => { assertApps(Number.isSafeInteger(value) && value >= 0, 'invalid_app_domains_revision'); return value; };
const domainId = value => { assertApps(typeof value === 'string' && /^dom_[a-f0-9]{32}$/u.test(value), 'invalid_app_domain_id'); return value; };

export function readNamedOrigins(db) {
  return db.prepare("SELECT * FROM app_domain_zones WHERE kind='named'").all().map(row => {
    const origin = normalizeNamedAppZone(`${row.scheme}://${row.suffix}${row.port ? `:${row.port}` : ''}`);
    const zone = namedZone(origin);
    assertApps(zone.template === row.origin_template && zone.suffix === row.suffix && zone.port === row.port, 'apps_registry_corrupt', 500);
    return origin;
  });
}

export function createDomainRegistry({ db, now = Date.now, assertActor, legacyTemplate = '', namedAppZone = '', domainLimits = {}, shellOrigins = [], validateNamedZone,
  onRetireInTransaction, onPolicyChanged }) {
  assertApps(typeof assertActor === 'function', 'apps_actor_validator_required', 500);
  assertApps(typeof onRetireInTransaction === 'function' && typeof onPolicyChanged === 'function', 'apps_policy_validator_required', 500);
  const limits = normalizeDomainLimits(domainLimits);
  const zoneOrigin = normalizeNamedAppZone(namedAppZone);
  function transaction(callback) {
    assertApps(!db.isTransaction, 'apps_nested_transaction', 500);
    db.exec('BEGIN IMMEDIATE');
    try { const value = callback(); db.exec('COMMIT'); return value; }
    catch (error) { if (db.isTransaction) db.exec('ROLLBACK'); throw error; }
  }
  let zoneId = null;
  transaction(() => {
    validateNamedOrigins([...readNamedOrigins(db), zoneOrigin], { shellOrigins, validateNamedZone });
    if (!zoneOrigin) return;
    const zone = namedZone(zoneOrigin);
    const existing = db.prepare("SELECT * FROM app_domain_zones WHERE kind='named'").all();
    assertApps(existing.length <= 1 && (!existing.length || existing[0].origin_template === zone.template), 'apps_named_zone_changed', 409);
    zoneId = insertDomainZone(db, zone, now());
  });
  const app = id => db.prepare('SELECT * FROM local_apps WHERE id=?').get(id);
  const head = id => db.prepare('SELECT revision FROM app_domain_heads WHERE app_id=?').get(id)?.revision;
  function own(actor, id) {
    assertActor(actor);
    const result = app(id);
    assertApps(result && result.owner_account_id === actor.accountId, 'apps_owner_required', 403);
    return result;
  }
  function read(actor, id) {
    own(actor, id);
    const rows = db.prepare("SELECT * FROM app_domains WHERE app_id=? ORDER BY CASE role WHEN 'canonical' THEN 0 ELSE 1 END,created_at,id").all(id);
    return { schema: 'soty.app-domains.v1', revision: head(id),
      canonicalOrigin: rows.find(item => item.role === 'canonical')?.origin ?? null,
      domains: rows.map(item => ({ id: item.id, slug: item.slug, origin: item.origin, role: item.role, state: item.state,
        runtimeMode: item.role === 'canonical' ? 'legacy-private' : 'status-only',
        ...(item.role === 'alias' ? { runtimeReady: false } : {}), createdAt: item.created_at, retiredAt: item.retired_at })),
      limits: { perApp: limits.perApp, perAccount: limits.perAccount,
        usedByApp: rows.filter(item => item.role === 'alias').length,
        usedByAccount: db.prepare("SELECT count(*) AS n FROM app_domains WHERE owner_account_id=? AND role='alias'").get(actor.accountId).n } };
  }
  function receiptView(item) {
    const domain = db.prepare('SELECT app_id,origin,slug FROM app_domains WHERE id=?').get(item.domain_id);
    assertApps(domain, 'apps_registry_corrupt', 500);
    return { schema: 'soty.app-domain-receipt.v1', requestKeyHash: item.request_key, action: item.action,
      appId: domain.app_id, domainId: item.domain_id, origin: domain.origin, slug: domain.slug,
      // Historical result of this mutation, not a claim that a later retire never happened.
      state: item.action === 'claim' ? 'bound' : 'tombstone', revision: item.committed_revision, committedAt: item.created_at };
  }
  function replay(actor, requestKey, intentHash) {
    const receipt = db.prepare('SELECT * FROM app_domain_receipts WHERE account_id=? AND request_key=?').get(actor.accountId, requestKey);
    if (!receipt) return null;
    assertApps(receipt.intent_hash === intentHash, 'app_domain_request_conflict', 409);
    return { receipt: receiptView(receipt), replayed: true };
  }
  function commitReceipt(actor, requestKey, intentHash, action, id, appIdentifier, timestamp) {
    db.prepare('UPDATE app_domain_heads SET revision=revision+1 WHERE app_id=?').run(appIdentifier);
    const currentRevision = head(appIdentifier);
    db.prepare('INSERT INTO app_domain_receipts VALUES (?,?,?,?,?,?,?)')
      .run(actor.accountId, requestKey, intentHash, action, id, currentRevision, timestamp);
    return { receipt: receiptView({ request_key: requestKey, action, domain_id: id, committed_revision: currentRevision, created_at: timestamp }), replayed: false };
  }
  function claim(actor, args) {
    exact(args, ['appId', 'slug', 'requestId', 'expectedDomainsRevision']);
    const id = appId(args.appId), slug = normalizeAppSlug(args.slug), expected = revision(args.expectedDomainsRevision);
    const requestKey = digest(textId(args.requestId)), intentHash = digest(JSON.stringify(['claim', id, slug, expected]));
    const result = transaction(() => {
      const currentApp = own(actor, id);
      const previous = replay(actor, requestKey, intentHash);
      if (previous) return previous;
      assertApps(zoneId && zoneOrigin, 'apps_named_zone_disabled', 503);
      assertApps(currentApp.state === 'enabled', 'app_revoked', 409);
      assertApps(head(id) === expected, 'app_domains_revision_conflict', 409);
      assertApps(!isReservedAppSlug(slug), 'app_name_reserved', 409);
      const origin = new URL(`${new URL(zoneOrigin).protocol}//${slug}.${new URL(zoneOrigin).host}`).origin;
      const hostname = new URL(origin).hostname;
      assertApps(!db.prepare('SELECT 1 FROM app_domains WHERE hostname=?').get(hostname), 'app_name_unavailable', 409);
      const usedByApp = db.prepare("SELECT count(*) AS n FROM app_domains WHERE app_id=? AND role='alias'").get(id).n;
      const usedByAccount = db.prepare("SELECT count(*) AS n FROM app_domains WHERE owner_account_id=? AND role='alias'").get(actor.accountId).n;
      assertApps(usedByApp < limits.perApp && usedByAccount < limits.perAccount, 'apps_domain_limit_reached', 429);
      const timestamp = now(), newId = `dom_${randomBytes(16).toString('hex')}`;
      db.prepare("INSERT INTO app_domains VALUES (?,?,?,?,?,?,?,'alias','bound',?,NULL)")
        .run(newId, zoneId, hostname, origin, slug, id, actor.accountId, timestamp);
      return commitReceipt(actor, requestKey, intentHash, 'claim', newId, id, timestamp);
    });
    return { requestId: args.requestId, ...result };
  }
  function retire(actor, args) {
    exact(args, ['appId', 'domainId', 'requestId', 'expectedDomainsRevision']);
    const id = appId(args.appId), retiringId = domainId(args.domainId), expected = revision(args.expectedDomainsRevision);
    const requestKey = digest(textId(args.requestId)), intentHash = digest(JSON.stringify(['retire', id, retiringId, expected]));
    let policyChanged = null;
    const result = transaction(() => {
      own(actor, id);
      const previous = replay(actor, requestKey, intentHash);
      if (previous) return previous;
      const domain = db.prepare('SELECT * FROM app_domains WHERE id=? AND app_id=? AND owner_account_id=?').get(retiringId, id, actor.accountId);
      assertApps(domain, 'app_domain_not_found', 404);
      assertApps(domain.role === 'alias', 'app_canonical_domain_immutable', 409);
      assertApps(head(id) === expected, 'app_domains_revision_conflict', 409);
      assertApps(domain.state === 'bound', 'app_domain_retired', 409);
      const timestamp = now();
      // The active set and policy epoch change in this same transaction. A
      // receipt fault rolls back both the tombstone and its loss of authority.
      policyChanged = onRetireInTransaction({ appId: id, domainId: retiringId });
      db.prepare("UPDATE app_domains SET state='tombstone',retired_at=? WHERE id=?").run(timestamp, retiringId);
      return commitReceipt(actor, requestKey, intentHash, 'retire', retiringId, id, timestamp);
    });
    if (policyChanged) onPolicyChanged(policyChanged);
    return { requestId: args.requestId, ...result };
  }
  return {
    execute({ actor, op, args = {} }) {
      assertActor(actor);
      assertApps(args && typeof args === 'object' && !Array.isArray(args), 'invalid_arguments');
      assertApps(domainOperations.has(op), 'unsupported_operation');
      if (op === 'apps.domains.get') { exact(args, ['appId']); return read(actor, appId(args.appId)); }
      if (op === 'apps.domains.claim') return claim(actor, args);
      if (op === 'apps.domains.retire') return retire(actor, args);
      exact(args, ['slug']);
      const slug = normalizeAppSlug(args.slug);
      if (!zoneOrigin) return { slug, available: false, reason: 'disabled' };
      if (isReservedAppSlug(slug)) return { slug, available: false, reason: 'reserved' };
      const hostname = `${slug}.${new URL(zoneOrigin).hostname}`;
      return db.prepare('SELECT 1 FROM app_domains WHERE hostname=?').get(hostname)
        ? { slug, available: false, reason: 'unavailable' } : { slug, available: true };
    },
    // Internal registration hook: cannot commit a second, partially independent transaction.
    ensureCanonicalForApp(value) { ensureCanonicalDomain(db, value, legacyTemplate); },
    originFor(id) { return db.prepare("SELECT origin FROM app_domains WHERE app_id=? AND role='canonical'").get(id)?.origin ?? null; },
  };
}
