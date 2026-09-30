import { createHash, randomBytes } from 'node:crypto';
import { assert, AccessError, canonicalHash, canonicalJson, exact, freezeDeep, identifier, integer, newId, now, record, stringSet, text } from './validation.mjs';
import { createNativeBaselineGuard } from './native-baseline.mjs';

export const ACCESS_OPERATIONS = Object.freeze([
  'access.principals.create', 'access.principals.list', 'access.principals.revoke',
  'access.grants.issue', 'access.grants.derive', 'access.grants.list', 'access.grants.revoke',
  'access.credentials.issue', 'access.credentials.revoke', 'access.events.list'
]);
const GRANT_KEYS = ['expectedAccountId', 'principalId', 'capabilities', 'resources', 'effects', 'recipients', 'expiresAt', 'allowDelegation', 'maxDepth'];
const TOKEN_PREFIX = 'soty_cap_';
const tokenDigest = value => createHash('sha256').update(value).digest('hex');
const subset = (requested, allowed) => requested.every(item => allowed.includes(item));

function capabilities(value) {
  assert(Array.isArray(value) && value.length > 0 && value.length <= 16);
  const result = value.map(item => {
    exact(item, ['capabilityId', 'version']);
    return { capabilityId: identifier(item.capabilityId), version: integer(item.version, 1, 1000000) };
  }).sort((a, b) => a.capabilityId.localeCompare(b.capabilityId) || a.version - b.version);
  assert(new Set(result.map(item => `${item.capabilityId}@${item.version}`)).size === result.length);
  return result;
}
function capSubset(requested, allowed) {
  return requested.every(item => allowed.some(parent => item.capabilityId === parent.capabilityId && item.version === parent.version));
}
function grantDto(row) {
  return {
    id: row.id, accountId: row.account_id, principalId: row.principal_id, clientId: row.client_id,
    parentGrantId: row.parent_id, rootGrantId: row.root_id, capabilities: JSON.parse(row.capabilities_json),
    resources: JSON.parse(row.resources_json), effects: JSON.parse(row.effects_json), recipients: JSON.parse(row.recipients_json),
    allowDelegation: row.allow_delegation === 1, maxDepth: row.max_depth, depth: row.depth,
    expiresAt: row.expires_at, createdAt: row.created_at, revokedAt: row.revoked_at, policyEpoch: row.policy_epoch
  };
}
function principalDto(row) {
  return { id: row.id, accountId: row.account_id, clientId: row.client_id, label: row.label, kind: row.kind, state: row.state, createdAt: row.created_at, revokedAt: row.revoked_at };
}
function audience(value) {
  text(value, { max: 2048 });
  assert(!/[\s?#\\]/u.test(value), 'audience_invalid');
  let parsed;
  try { parsed = new URL(value); } catch { assert(false, 'audience_invalid'); }
  if (['https:', 'http:'].includes(parsed.protocol)) {
    assert(parsed.hostname && !parsed.username && !parsed.password && !parsed.search && !parsed.hash, 'audience_invalid');
    assert(parsed.protocol === 'https:' || ['127.0.0.1', 'localhost', '[::1]'].includes(parsed.hostname), 'audience_invalid');
    // Preserve the documented bare-origin form; otherwise require the canonical URL.
    const canonical = parsed.href === `${parsed.origin}/` ? parsed.origin : parsed.href;
    assert(value === canonical || (parsed.href === `${parsed.origin}/` && value === parsed.href), 'audience_invalid');
  } else {
    assert(['urn:', 'soty:'].includes(parsed.protocol) && /^[a-z]+:[A-Za-z0-9][A-Za-z0-9_.:@/-]*$/u.test(value), 'audience_invalid');
  }
  return value;
}

export function createAccessStore({ db, clock = Date.now, transaction, actorActive, catalog, limits = {}, captureNativeSettlement }) {
  assert(typeof actorActive === 'function' && typeof transaction === 'function', 'host_auth_required');
  const maxTtl = limits.maxGrantTtlMs ?? 30 * 24 * 60 * 60 * 1000;
  integer(maxTtl, 1, 365 * 24 * 60 * 60 * 1000);
  const actorRefs = new WeakMap();
  const authorizationRefs = new WeakSet();
  const native = createNativeBaselineGuard({ db, error: code => new AccessError(code) });

  function publicGrant(row) {
    const value = grantDto(row);
    const budget = db.prepare('SELECT * FROM cap_budgets WHERE root_grant_id=?').get(row.root_id);
    if (budget) {
      const uncertain = db.prepare("SELECT COALESCE(sum(amount),0) AS amount FROM cap_budget_reservations WHERE root_grant_id=? AND disposition='uncertain'").get(row.root_id).amount;
      value.budget = { unit: budget.unit, limit: budget.limit_amount, reserved: budget.reserved_amount, spent: budget.spent_amount,
        remaining: budget.limit_amount - budget.reserved_amount - budget.spent_amount, uncertain };
    }
    return value;
  }
  function pagePosition(args, fingerprint) {
    const limit = integer(args.limit ?? 40, 1, 100);
    let position = null;
    if (args.cursor !== undefined && args.cursor !== null) {
      text(args.cursor, { max: 600 });
      try { position = JSON.parse(Buffer.from(args.cursor, 'base64url').toString('utf8')); }
      catch { assert(false, 'cursor_invalid'); }
      exact(position, ['fingerprint', 'createdAt', 'id'], 'cursor_invalid');
      assert(position.fingerprint === fingerprint, 'cursor_invalid');
      integer(position.createdAt, 0, Number.MAX_SAFE_INTEGER, 'cursor_invalid'); identifier(position.id, 'cursor_invalid');
    }
    return { limit, position };
  }
  function pageCursor(rows, limit, fingerprint) {
    const last = rows.slice(0, limit).at(-1);
    return rows.length > limit ? Buffer.from(JSON.stringify({ fingerprint, createdAt: last.created_at, id: last.id })).toString('base64url') : null;
  }

  function hostOwner(actor, args) {
    record(actor, 'authorization_required'); record(args);
    assert(typeof actor.accountId === 'string' && typeof actor.deviceId === 'string', 'authorization_required');
    assert(actorActive(actor) === true, 'authorization_required');
    assert(args.expectedAccountId === actor.accountId, 'account_mismatch');
    return { accountId: actor.accountId, deviceId: actor.deviceId };
  }
  function ownedPrincipal(accountId, id) {
    identifier(id);
    const row = db.prepare('SELECT * FROM cap_principals WHERE id=? AND account_id=?').get(id, accountId);
    assert(row, 'not_found');
    return row;
  }
  function activePrincipal(accountId, principalId, clientId) {
    const principal = db.prepare('SELECT * FROM cap_principals WHERE id=? AND account_id=? AND client_id=?').get(principalId, accountId, clientId);
    const client = db.prepare('SELECT * FROM cap_clients WHERE id=? AND account_id=?').get(clientId, accountId);
    assert(principal?.state === 'active' && client?.state === 'active', 'access_denied');
    assert(actorActive({ accountId, deviceId: principal.creator_device_id }) === true, 'access_denied');
    return principal;
  }
  function chain(grantId, accountId, time = now(clock)) {
    const result = [];
    const visited = new Set();
    let id = grantId;
    while (id) {
      assert(!visited.has(id) && result.length <= 8, 'access_denied');
      visited.add(id);
      const row = db.prepare('SELECT * FROM cap_grants WHERE id=? AND account_id=?').get(id, accountId);
      assert(row && row.revoked_at === null && row.not_before <= time && row.expires_at > time, 'access_denied');
      activePrincipal(accountId, row.principal_id, row.client_id);
      assert(actorActive({ accountId, deviceId: row.creator_device_id }) === true, 'access_denied');
      result.push(row);
      id = row.parent_id;
    }
    assert(result.length > 0, 'access_denied');
    const root = result.at(-1);
    assert(root.id === root.root_id && root.depth === 0 && root.parent_id === null, 'access_denied');
    for (let i = 0; i < result.length; i++) {
      const child = result[i];
      assert(child.root_id === root.id && child.depth === result.length - i - 1, 'access_denied');
      const parent = result[i + 1];
      if (!parent) continue;
      assert(parent.allow_delegation === 1 && parent.max_depth > child.max_depth && child.expires_at <= parent.expires_at, 'access_denied');
      assert(capSubset(JSON.parse(child.capabilities_json), JSON.parse(parent.capabilities_json)), 'access_denied');
      for (const column of ['resources_json', 'effects_json', 'recipients_json']) assert(subset(JSON.parse(child[column]), JSON.parse(parent[column])), 'access_denied');
    }
    return result;
  }
  function currentCredential(reference) {
    const row = db.prepare('SELECT * FROM cap_credentials WHERE id=?').get(reference.credentialId);
    const time = now(clock);
    assert(row && row.revoked_at === null && row.expires_at > time, 'authorization_required');
    assert(row.account_id === reference.accountId && row.client_id === reference.clientId && row.principal_id === reference.principalId && row.grant_id === reference.grantId && row.audience === reference.audience, 'authorization_required');
    activePrincipal(row.account_id, row.principal_id, row.client_id);
    const ancestry = chain(row.grant_id, row.account_id, time);
    assert(ancestry[0].client_id === row.client_id && ancestry[0].principal_id === row.principal_id, 'access_denied');
    return { credential: row, ancestry };
  }
  function resolveActor(actor) {
    assert(actor && typeof actor === 'object' && actorRefs.has(actor), 'authorization_required');
    return currentCredential(actorRefs.get(actor));
  }
  function credentialReference(row) {
    return { credentialId: row.id, accountId: row.account_id, clientId: row.client_id, principalId: row.principal_id, grantId: row.grant_id, audience: row.audience };
  }
  function authenticateCredential({ token, audience: target }) {
    audience(target);
    assert(typeof token === 'string' && /^soty_cap_[A-Za-z0-9_-]{43}$/u.test(token), 'authorization_required');
    const row = db.prepare('SELECT * FROM cap_credentials WHERE digest=?').get(tokenDigest(token));
    assert(row && row.audience === target, 'authorization_required');
    const reference = credentialReference(row);
    currentCredential(reference);
    const actor = freezeDeep({ type: 'service', accountId: row.account_id, clientId: row.client_id, principalId: row.principal_id, grantId: row.grant_id });
    actorRefs.set(actor, reference);
    return actor;
  }
  function descriptor(current, capabilityId, version) {
    const { credential, ancestry } = current;
    const leaf = ancestry[0];
    const root = ancestry.at(-1);
    const result = {
      ...credentialReference(credential), rootGrantId: root.id, policyEpoch: root.policy_epoch,
      expiresAt: Math.min(credential.expires_at, ...ancestry.map(item => item.expires_at)),
      resources: JSON.parse(leaf.resources_json), effects: JSON.parse(leaf.effects_json), recipients: JSON.parse(leaf.recipients_json)
    };
    if (capabilityId !== undefined) {
      identifier(capabilityId); integer(version, 1, 1000000);
      assert(JSON.parse(leaf.capabilities_json).some(item => item.capabilityId === capabilityId && item.version === version), 'access_denied');
      const entry = catalog.get(capabilityId, version);
      assert(entry, 'not_found');
      assert(subset(entry.resources, result.resources) && subset(entry.effects, result.effects) && subset(entry.recipients, result.recipients), 'access_denied');
      Object.assign(result, {
        capabilityId, version, capabilityDigest: entry.digest, resources: entry.resources, effects: entry.effects,
        recipients: entry.recipients, executionBinding: entry.executionBinding, charges: entry.charges
      });
    }
    freezeDeep(result);
    authorizationRefs.add(result);
    return result;
  }
  function authorize({ actor, capabilityId, version, resources, effects, recipients, input, action = 'invoke' }) {
    assert(['invoke', 'read', 'cancel', 'history'].includes(action), 'access_denied');
    const current = resolveActor(actor);
    const approved = descriptor(current, capabilityId, version);
    if (action === 'invoke') {
      assert(capabilityId !== undefined, 'invalid_input');
      const entry = catalog.get(capabilityId, version);
      assert(entry.executionEnabled, 'capability_disabled');
      if (input !== undefined) catalog.validateInput(entry, input);
    }
    for (const [value, allowed] of [[resources, approved.resources], [effects, approved.effects], [recipients, approved.recipients]]) {
      if (value !== undefined) assert(subset(stringSet(value), allowed), 'access_denied');
    }
    return approved;
  }
  function authorizeInvocation(request) {
    const action = request.action || 'invoke';
    if (action === 'dispatch') {
      const snapshot = record(request.authorizationSnapshot, 'authorization_required');
      const row = record(request.invocation, 'authorization_required');
      const current = currentCredential(snapshot);
      const approved = descriptor(current, row.capabilityId ?? row.capability_id, row.version ?? row.capabilityVersion ?? row.capability_version);
      assert(approved.accountId === (row.accountId ?? row.account_id) && approved.clientId === (row.clientId ?? row.client_id) && approved.principalId === (row.principalId ?? row.principal_id) && approved.grantId === (row.grantId ?? row.grant_id), 'access_denied');
      assert(approved.capabilityDigest === (row.capabilityDigest ?? row.capability_digest), 'capability_changed');
      assert(catalog.get(approved.capabilityId, approved.version).executionEnabled, 'capability_disabled');
      return approved;
    }
    const approved = authorize(request);
    if (request.invocation) {
      const row = request.invocation;
      assert(approved.accountId === (row.accountId ?? row.account_id) && approved.clientId === (row.clientId ?? row.client_id) && approved.principalId === (row.principalId ?? row.principal_id), 'not_found');
      assert(approved.grantId === (row.grantId ?? row.grant_id), 'not_found');
    }
    return approved;
  }
  function normalizeGrant(args, time, parent) {
    const caps = capabilities(args.capabilities);
    const resources = stringSet(args.resources); const effects = stringSet(args.effects); const recipients = stringSet(args.recipients);
    for (const item of caps) {
      const entry = catalog.get(item.capabilityId, item.version);
      assert(entry, 'not_found');
      assert(subset(entry.resources, resources) && subset(entry.effects, effects) && subset(entry.recipients, recipients), 'invalid_scope');
    }
    integer(args.expiresAt, time + 1, time + maxTtl, 'expiry_invalid');
    const allowDelegation = args.allowDelegation ?? false;
    assert(typeof allowDelegation === 'boolean');
    const maxDepth = args.maxDepth ?? 0;
    integer(maxDepth, 0, 8);
    assert(allowDelegation ? maxDepth > 0 : maxDepth === 0, 'delegation_invalid');
    if (parent) {
      assert(parent.allow_delegation === 1 && maxDepth < parent.max_depth && args.expiresAt <= parent.expires_at, 'delegation_denied');
      assert(capSubset(caps, JSON.parse(parent.capabilities_json)), 'delegation_denied');
      assert(subset(resources, JSON.parse(parent.resources_json)) && subset(effects, JSON.parse(parent.effects_json)) && subset(recipients, JSON.parse(parent.recipients_json)), 'delegation_denied');
    }
    return { caps, resources, effects, recipients, allowDelegation, maxDepth };
  }
  function insertGrant(owner, principal, args, time, parent) {
    activePrincipal(owner.accountId, principal.id, principal.client_id);
    const normalized = normalizeGrant(args, time, parent);
    assert(db.prepare('SELECT count(*) AS n FROM cap_grants WHERE account_id=?').get(owner.accountId).n < 10000, 'quota_exceeded');
    const id = newId('grant');
    db.prepare(`INSERT INTO cap_grants(id,account_id,client_id,principal_id,parent_id,root_id,creator_device_id,
      capabilities_json,resources_json,effects_json,recipients_json,allow_delegation,max_depth,depth,not_before,expires_at,created_at)
      VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)`).run(id, owner.accountId, principal.client_id, principal.id, parent?.id ?? null, parent?.root_id ?? id, owner.deviceId,
      canonicalJson(normalized.caps), canonicalJson(normalized.resources), canonicalJson(normalized.effects), canonicalJson(normalized.recipients),
      normalized.allowDelegation ? 1 : 0, normalized.maxDepth, parent ? parent.depth + 1 : 0, time, args.expiresAt, time);
    if (!parent) {
      exact(args.budget, ['unit', 'limit']);
      assert(args.budget.unit === 'invocations', 'budget_unit_unsupported');
      integer(args.budget.limit, 0, 1000000);
      db.prepare('INSERT INTO cap_budgets(root_grant_id,unit,limit_amount) VALUES(?,?,?)').run(id, args.budget.unit, args.budget.limit);
    }
    return publicGrant(db.prepare('SELECT * FROM cap_grants WHERE id=?').get(id));
  }
  function revokeGrant(id, accountId, time) {
    const grant = db.prepare('SELECT * FROM cap_grants WHERE id=? AND account_id=?').get(id, accountId);
    assert(grant, 'not_found');
    if (grant.revoked_at === null) {
      db.prepare('UPDATE cap_grants SET revoked_at=? WHERE id=?').run(time, grant.id);
      db.prepare('UPDATE cap_grants SET policy_epoch=policy_epoch+1 WHERE id=?').run(grant.root_id);
    }
    return { grant: publicGrant(db.prepare('SELECT * FROM cap_grants WHERE id=?').get(id)) };
  }

  function execute({ op, args, actor }) {
    assert(ACCESS_OPERATIONS.includes(op), 'operation_unknown');
    const owner = hostOwner(actor, args);
    return transaction(() => {
      hostOwner(actor, args);
      const time = now(clock);
      const result = (() => { switch (op) {
        case 'access.principals.create': {
          exact(args, ['expectedAccountId', 'label', 'clientLabel']);
          const label = text(args.label, { max: 100 });
          const clientLabel = args.clientLabel === undefined ? label : text(args.clientLabel, { max: 100 });
          assert(db.prepare('SELECT count(*) AS n FROM cap_principals WHERE account_id=?').get(owner.accountId).n < 1000, 'quota_exceeded');
          const clientId = newId('client'); const principalId = newId('principal');
          db.prepare("INSERT INTO cap_clients(id,account_id,label,state,created_at) VALUES(?,?,?,'active',?)").run(clientId, owner.accountId, clientLabel, time);
          db.prepare("INSERT INTO cap_principals(id,account_id,client_id,kind,label,state,creator_device_id,created_at) VALUES(?,?,?,'service',?,'active',?,?)").run(principalId, owner.accountId, clientId, label, owner.deviceId, time);
          return { principal: principalDto(ownedPrincipal(owner.accountId, principalId)), client: { id: clientId, label: clientLabel, state: 'active' } };
        }
        case 'access.principals.list': {
          exact(args, ['expectedAccountId', 'limit', 'cursor']);
          const fingerprint = canonicalHash({ op, accountId: owner.accountId });
          const { limit, position } = pagePosition(args, fingerprint);
          const rows = position
            ? db.prepare('SELECT * FROM cap_principals WHERE account_id=? AND (created_at<? OR (created_at=? AND id<?)) ORDER BY created_at DESC,id DESC LIMIT ?').all(owner.accountId, position.createdAt, position.createdAt, position.id, limit + 1)
            : db.prepare('SELECT * FROM cap_principals WHERE account_id=? ORDER BY created_at DESC,id DESC LIMIT ?').all(owner.accountId, limit + 1);
          return { principals: rows.slice(0, limit).map(principalDto), cursor: pageCursor(rows, limit, fingerprint) };
        }
        case 'access.principals.revoke': {
          exact(args, ['expectedAccountId', 'principalId']);
          const principal = ownedPrincipal(owner.accountId, args.principalId);
          if (principal.state !== 'revoked') {
            db.prepare("UPDATE cap_principals SET state='revoked',revoked_at=? WHERE id=?").run(time, principal.id);
            db.prepare("UPDATE cap_clients SET state='revoked',revoked_at=?,policy_epoch=policy_epoch+1 WHERE id=?").run(time, principal.client_id);
            db.prepare('UPDATE cap_grants SET policy_epoch=policy_epoch+1 WHERE id IN (SELECT DISTINCT root_id FROM cap_grants WHERE principal_id=?)').run(principal.id);
          }
          return { principal: principalDto(ownedPrincipal(owner.accountId, principal.id)) };
        }
        case 'access.grants.issue': {
          exact(args, [...GRANT_KEYS, 'budget']);
          const principal = ownedPrincipal(owner.accountId, args.principalId);
          return { grant: insertGrant(owner, principal, args, time, null) };
        }
        case 'access.grants.derive': {
          exact(args, [...GRANT_KEYS, 'parentGrantId']);
          identifier(args.parentGrantId);
          const parent = chain(args.parentGrantId, owner.accountId, time)[0];
          const principal = ownedPrincipal(owner.accountId, args.principalId ?? parent.principal_id);
          return { grant: insertGrant(owner, principal, args, time, parent) };
        }
        case 'access.grants.list': {
          exact(args, ['expectedAccountId', 'principalId', 'limit', 'cursor']);
          if (args.principalId !== undefined) ownedPrincipal(owner.accountId, args.principalId);
          const fingerprint = canonicalHash({ op, accountId: owner.accountId, principalId: args.principalId ?? null });
          const { limit, position } = pagePosition(args, fingerprint);
          const rows = db.prepare(`SELECT * FROM cap_grants WHERE account_id=?
            ${args.principalId === undefined ? '' : 'AND principal_id=?'}
            ${position ? 'AND (created_at<? OR (created_at=? AND id<?))' : ''}
            ORDER BY created_at DESC,id DESC LIMIT ?`).all(owner.accountId, ...(args.principalId === undefined ? [] : [args.principalId]), ...(position ? [position.createdAt, position.createdAt, position.id] : []), limit + 1);
          return { grants: rows.slice(0, limit).map(publicGrant), cursor: pageCursor(rows, limit, fingerprint) };
        }
        case 'access.grants.revoke':
          exact(args, ['expectedAccountId', 'grantId']); identifier(args.grantId);
          return revokeGrant(args.grantId, owner.accountId, time);
        case 'access.credentials.issue': {
          exact(args, ['expectedAccountId', 'grantId', 'audience', 'expiresAt']); identifier(args.grantId);
          const grant = chain(args.grantId, owner.accountId, time)[0];
          const expiresAt = args.expiresAt ?? grant.expires_at;
          integer(expiresAt, time + 1, grant.expires_at, 'expiry_invalid');
          const target = audience(args.audience);
          assert(db.prepare('SELECT count(*) AS n FROM cap_credentials WHERE account_id=?').get(owner.accountId).n < 10000, 'quota_exceeded');
          const id = newId('credential'); const token = TOKEN_PREFIX + randomBytes(32).toString('base64url');
          db.prepare('INSERT INTO cap_credentials(id,digest,account_id,client_id,principal_id,grant_id,audience,expires_at,created_at) VALUES(?,?,?,?,?,?,?,?,?)')
            .run(id, tokenDigest(token), owner.accountId, grant.client_id, grant.principal_id, grant.id, target, expiresAt, time);
          return { credential: { id, audience: target, expiresAt, createdAt: time, grantId: grant.id }, token };
        }
        case 'access.credentials.revoke': {
          exact(args, ['expectedAccountId', 'credentialId']); identifier(args.credentialId);
          const row = db.prepare('SELECT * FROM cap_credentials WHERE id=? AND account_id=?').get(args.credentialId, owner.accountId);
          assert(row, 'not_found');
          db.prepare('UPDATE cap_credentials SET revoked_at=COALESCE(revoked_at,?) WHERE id=?').run(time, row.id);
          db.prepare('UPDATE cap_grants SET policy_epoch=policy_epoch+1 WHERE id=(SELECT root_id FROM cap_grants WHERE id=?)').run(row.grant_id);
          return { credential: { id: row.id, revokedAt: row.revoked_at ?? time } };
        }
        case 'access.events.list': {
          exact(args, ['expectedAccountId', 'limit', 'cursor']);
          const limit = integer(args.limit ?? 40, 1, 100);
          let position = null;
          if (args.cursor !== undefined) {
            text(args.cursor, { max: 600 });
            try { position = JSON.parse(Buffer.from(args.cursor, 'base64url').toString('utf8')); }
            catch { assert(false, 'cursor_invalid'); }
            exact(position, ['accountId', 'createdAt', 'id'], 'cursor_invalid');
            assert(position.accountId === owner.accountId, 'cursor_invalid');
            integer(position.createdAt, 0, Number.MAX_SAFE_INTEGER, 'cursor_invalid'); identifier(position.id, 'cursor_invalid');
          }
          const rows = position
            ? db.prepare('SELECT * FROM cap_audit WHERE account_id=? AND (created_at<? OR (created_at=? AND id<?)) ORDER BY created_at DESC,id DESC LIMIT ?').all(owner.accountId, position.createdAt, position.createdAt, position.id, limit + 1)
            : db.prepare('SELECT * FROM cap_audit WHERE account_id=? ORDER BY created_at DESC,id DESC LIMIT ?').all(owner.accountId, limit + 1);
          const page = rows.slice(0, limit);
          const last = page.at(-1);
          return { events: page.map(row => ({ id: row.id, kind: row.kind, objectType: row.object_type, objectId: row.object_id, actorType: row.actor_type, actorId: row.actor_id, createdAt: row.created_at })),
            cursor: rows.length > limit ? Buffer.from(JSON.stringify({ accountId: owner.accountId, createdAt: last.created_at, id: last.id })).toString('base64url') : null };
        }
        default: assert(false, 'operation_unknown');
      } })();
      if (!op.endsWith('.list')) {
        const [objectType, object] = result.principal ? ['principal', result.principal] : result.grant ? ['grant', result.grant] : ['credential', result.credential];
        db.prepare('INSERT INTO cap_audit(id,account_id,kind,object_type,object_id,actor_type,actor_id,created_at) VALUES(?,?,?,?,?,?,?,?)')
          .run(newId('event'), owner.accountId, op, objectType, object.id, 'connect', owner.deviceId, time);
      }
      return result;
    });
  }

  function reserveBudget({ authorization, invocationId, attemptId, charges }) {
    assert(authorization && authorizationRefs.has(authorization), 'authorization_required');
    // Reservation is only called inside the service admission transaction.
    currentCredential(authorization);
    identifier(invocationId); identifier(attemptId);
    assert(Array.isArray(charges) && charges.length === 1, 'budget_unit_unsupported');
    exact(charges[0], ['unit', 'amount']);
    assert(charges[0].unit === 'invocations', 'budget_unit_unsupported');
    const amount = integer(charges[0].amount, 1, 1000000);
    const digest = canonicalHash({ rootGrantId: authorization.rootGrantId, invocationId, attemptId, charges });
    const existing = db.prepare('SELECT * FROM cap_budget_reservations WHERE root_grant_id=? AND invocation_id=? AND attempt_id=? AND unit=?')
      .get(authorization.rootGrantId, invocationId, attemptId, 'invocations');
    if (existing) {
      assert(existing.request_digest === digest, 'idempotency_conflict');
      return { reservationId: existing.id };
    }
    const changed = db.prepare('UPDATE cap_budgets SET reserved_amount=reserved_amount+? WHERE root_grant_id=? AND unit=? AND reserved_amount+spent_amount+?<=limit_amount')
      .run(amount, authorization.rootGrantId, 'invocations', amount);
    assert(changed.changes === 1, 'budget_exceeded');
    const id = newId('reservation'); const time = now(clock);
    db.prepare("INSERT INTO cap_budget_reservations(id,invocation_id,attempt_id,root_grant_id,unit,amount,disposition,request_digest,created_at,updated_at) VALUES(?,?,?,?,?,?,'reserved',?,?,?)")
      .run(id, invocationId, attemptId, authorization.rootGrantId, 'invocations', amount, digest, time, time);
    return { reservationId: id };
  }
  function settleBudgetCore({ reservationId, disposition, actualCharges }) {
    identifier(reservationId);
    assert(['spent', 'released', 'uncertain'].includes(disposition), 'settlement_invalid');
    const row = db.prepare('SELECT * FROM cap_budget_reservations WHERE id=?').get(reservationId);
    assert(row, 'not_found');
    let actual = disposition === 'spent' ? row.amount : 0;
    if (actualCharges !== undefined) {
      assert(Array.isArray(actualCharges) && (actualCharges.length === 1 || (actualCharges.length === 0 && disposition !== 'spent')), 'settlement_invalid');
      if (actualCharges.length === 1) {
        exact(actualCharges[0], ['unit', 'amount']);
        assert(actualCharges[0].unit === row.unit, 'settlement_invalid');
        actual = integer(actualCharges[0].amount, 0, row.amount, 'settlement_invalid');
      }
      assert(disposition === 'spent' || actual === 0, 'settlement_invalid');
    }
    if (['spent', 'released'].includes(row.disposition)) {
      assert(row.disposition === disposition && row.actual_amount === actual, 'settlement_conflict');
      return { reservationId, disposition, actualAmount: actual };
    }
    if (disposition === 'uncertain') {
      db.prepare("UPDATE cap_budget_reservations SET disposition='uncertain',updated_at=? WHERE id=?").run(now(clock), row.id);
      return { reservationId, disposition, heldAmount: row.amount };
    }
    db.prepare('UPDATE cap_budgets SET reserved_amount=reserved_amount-?,spent_amount=spent_amount+? WHERE root_grant_id=? AND unit=?')
      .run(row.amount, actual, row.root_grant_id, row.unit);
    db.prepare('UPDATE cap_budget_reservations SET disposition=?,actual_amount=?,updated_at=? WHERE id=?').run(disposition, actual, now(clock), row.id);
    return { reservationId, disposition, actualAmount: actual };
  }
  function settleBudget(args) {
    const row = db.prepare('SELECT invocation_id FROM cap_budget_reservations WHERE id=?').get(identifier(args.reservationId));
    assert(row, 'not_found');
    if (args.disposition !== 'uncertain') native.assertGeneric(row.invocation_id);
    return settleBudgetCore(args);
  }
  if (captureNativeSettlement !== undefined) {
    assert(typeof captureNativeSettlement === 'function', 'host_auth_required');
    // Captured only by the fixed coordinator, never returned as an Access API.
    captureNativeSettlement(({ invocationId, reservationId, disposition }) => {
      assert(db.isTransaction && native.find(invocationId), 'native_context_invalid');
      const row = db.prepare('SELECT invocation_id,unit,amount FROM cap_budget_reservations WHERE id=?').get(reservationId);
      assert(row?.invocation_id === invocationId && row.unit === 'invocations' && row.amount === 1
        && ['spent', 'released'].includes(disposition), 'settlement_invalid');
      return settleBudgetCore({ reservationId, disposition,
        ...(disposition === 'spent' ? { actualCharges: [{ unit: 'invocations', amount: 1 }] } : {}) });
    });
  }
  return Object.freeze({ operations: new Set(ACCESS_OPERATIONS), execute, authenticateCredential, authorize, authorizeInvocation, reserveBudget, settleBudget,
    verifyOwner({ actor, args }) { return hostOwner(actor, args); } });
}
