import { AccessError, canonicalHash, exact } from '../modules/capabilities/server/validation.mjs';
import { createAgentSourceCredentialStore } from './agent-source-credential-store.js';

const need = (value, code = 'access_denied') => { if (!value) throw new AccessError(code); };
const refKey = ref => `${ref.capabilityId}@${ref.version}:${ref.digest}`;
const operations = ['agent.sources.list', 'agent.sources.allow', 'agent.sources.revoke'];
const union = values => [...new Set(values.flat())].sort();
const publicState = value => value ? { state: value.state, mode: value.mode ?? null, expiresAt: value.expiresAt ?? null,
  grantId: value.grantId ?? null, requestId: value.requestId ?? null } : { state: 'none', mode: null, expiresAt: null, grantId: null, requestId: null };

/** Host-installed references only; signed Connect is the sole consent ingress.
 * No token is returned by execute, stored in Connect replay, or sent to the UI.
 * Two databases are not an atomic transaction: pending is durable before grants,
 * and remains denied after interruption. The user can revoke/replace it explicitly.
 */
export function createAgentSourceAccess({ entries, getConnect, getCapabilities, getApps, configuration, databasePath,
  audience, legacyResolver, clock = Date.now }) {
  const byApp = new Map(), byRef = new Map();
  if (configuration) { exact(configuration, ['key', 'keyId']); need(typeof audience === 'string' && audience.endsWith('/mcp'), 'agent_source_access_unavailable'); }
  for (const entry of entries) {
    byRef.set(refKey(entry.contract), entry);
    const group = byApp.get(entry.appId) ?? []; group.push(entry); byApp.set(entry.appId, group);
  }
  const store = configuration ? createAgentSourceCredentialStore({ databasePath, ...configuration }) : null;
  const call = (actor, op, args) => getCapabilities().execute({ actor, op, args: { ...args, expectedAccountId: actor.accountId } });
  const receiptKey = (appId, requestId) => `request:${appId}:${requestId}`;
  function persist(actor, appId, row) {
    store.writePair(actor, { id: appId, value: row }, { id: receiptKey(appId, row.requestId),
      value: { inputDigest: row.inputDigest, status: publicState(row) } });
  }
  function proposal(actor, appId, callback) {
    const all = byApp.get(appId); need(all?.length && all.length <= 16);
    return getApps().withAppAuthority({ actor, appId, mode: 'participant' }, app => {
      need(all.every(entry => entry.catalog.executionEnabled && entry.target.revision === app.target.revision && entry.target.digest === app.target.digest));
      // This release offers workspace consent, never an author's fixed private resource.
      const resources = union(all.map(entry => entry.catalog.resources)), recipients = union(all.map(entry => entry.catalog.recipients));
      need(resources.length === 1 && resources[0].startsWith(`workspace:${appId}:`) && recipients.length === 1 && recipients[0] === resources[0]);
      const functions = all.map(entry => ({ reference: entry.contract, title: entry.catalog.title, effects: entry.catalog.effects }));
      const value = { appId, title: app.title, resources, recipients, functions,
        target: { revision: app.target.revision, digest: app.target.digest } };
      return callback({ ...value, proposalDigest: canonicalHash(value) }, all);
    });
  }
  function revokePrincipal(actor, row) {
    for (const principalId of new Set([row?.principalId, row?.previousPrincipalId].filter(Boolean)))
      call(actor, 'access.principals.revoke', { principalId });
  }
  function credentialCurrent(actor, row) {
    need(row?.state === 'active' && row.expiresAt > clock());
    const credential = { token: row.token, audience: row.audience };
    const delegated = getCapabilities().authenticateCredential(credential);
    getCapabilities().withOwnerAuthority({ actor: delegated }, owner => need(owner.accountId === actor.accountId && owner.deviceId === actor.deviceId));
    return credential;
  }
  function resolve({ actor, reference }) {
    const entry = byRef.get(refKey(reference));
    if (!entry) {
      if (legacyResolver) return legacyResolver({ actor, reference });
      throw new AccessError('authorization_required');
    }
    const row = store?.read(actor, entry.appId);
    if (!row) {
      // Legacy resolver remains authoritative only where it actually has a mapping.
      if (legacyResolver) {
        const value = legacyResolver({ actor, reference });
        if (value != null) return value;
      }
      throw new AccessError('authorization_required');
    }
    need(row.references?.some(ref => refKey(ref) === refKey(reference)));
    return proposal(actor, entry.appId, value => {
      need(row.proposalDigest === value.proposalDigest);
      return credentialCurrent(actor, row);
    });
  }
  function execute({ actor, op, args }) {
    need(operations.includes(op), 'operation_unknown');
    need(store, 'agent_source_access_unavailable');
    need(args?.expectedAccountId === actor?.accountId, 'account_mismatch');
    return getConnect().withActorAuthorityFence(actor, () => {
      if (op === 'agent.sources.list') {
        exact(args, ['expectedAccountId']);
        const apps = [];
        for (const appId of byApp.keys()) {
          try { proposal(actor, appId, value => {
            let row = store.read(actor, appId), status = publicState(row);
            if (row?.state === 'active') {
              try { need(row.proposalDigest === value.proposalDigest); credentialCurrent(actor, row); }
              catch (error) { if (error.code === 'agent_source_storage_invalid') throw error; status = { ...status, state: 'unavailable' }; }
            }
            const { target, ...visible } = value; apps.push({ ...visible, status }); return true;
          }); } catch (error) {
            if (!['access_denied', 'apps_access_denied', 'app_unavailable', 'app_scoped_admission_required'].includes(error.code)) throw error;
          }
        }
        return { accountId: actor.accountId, deviceId: actor.deviceId, apps };
      }
      need(typeof args.appId === 'string' && byApp.has(args.appId));
      if (op === 'agent.sources.revoke') {
        exact(args, ['expectedAccountId', 'appId']);
        const row = store.read(actor, args.appId);
        // Persist deny before touching the grant: a failure never reactivates legacy.
        store.write(actor, args.appId, { ...row, state: 'revoked', token: null });
        revokePrincipal(actor, row);
        return { appId: args.appId, status: publicState({ ...row, state: 'revoked' }) };
      }
      exact(args, ['expectedAccountId', 'appId', 'proposalDigest', 'mode', 'durationSeconds', 'budget', 'requestId']);
      need(['read', 'write'].includes(args.mode) && [3600, 86400, 604800].includes(args.durationSeconds)
        && Number.isSafeInteger(args.budget) && args.budget >= 1 && args.budget <= 1000
        && typeof args.requestId === 'string' && /^[A-Za-z0-9_-]{16,128}$/u.test(args.requestId), 'invalid_arguments');
      const inputDigest = canonicalHash(args), receipt = store.read(actor, receiptKey(args.appId, args.requestId));
      if (receipt) {
        need(receipt.inputDigest === inputDigest, 'idempotency_conflict');
        return { appId: args.appId, status: receipt.status };
      }
      const previous = store.read(actor, args.appId);
      return proposal(actor, args.appId, (value, all) => {
        need(value.proposalDigest === args.proposalDigest, 'agent_source_proposal_stale');
        const selected = all.filter(entry => args.mode === 'write' || entry.catalog.effects.length === 0);
        need(selected.length > 0);
        const expiresAt = clock() + args.durationSeconds * 1000;
        let row = { state: 'pending', mode: args.mode, requestId: args.requestId, inputDigest, expiresAt,
          proposalDigest: value.proposalDigest, references: selected.map(entry => entry.contract), audience, token: null,
          previousPrincipalId: previous?.principalId ?? null };
        persist(actor, args.appId, row);
        try {
          revokePrincipal(actor, previous);
          const { principal } = call(actor, 'access.principals.create', { label: `Личный агент · ${value.title}`.slice(0, 100) });
          row = { ...row, principalId: principal.id }; persist(actor, args.appId, row);
          const { grant } = call(actor, 'access.grants.issue', { principalId: principal.id,
            capabilities: selected.map(entry => ({ capabilityId: entry.contract.capabilityId, version: entry.contract.version })),
            resources: value.resources, recipients: value.recipients, effects: union(selected.map(entry => entry.catalog.effects)),
            expiresAt, allowDelegation: false, maxDepth: 0, budget: { unit: 'invocations', limit: args.budget } });
          row = { ...row, grantId: grant.id }; persist(actor, args.appId, row);
          const issued = call(actor, 'access.credentials.issue', { grantId: grant.id, audience, expiresAt });
          row = { ...row, state: 'active', token: issued.token }; persist(actor, args.appId, row);
          return { appId: args.appId, status: publicState(row) };
        } catch (error) {
          try { persist(actor, args.appId, { ...row, state: 'failed', token: null }); revokePrincipal(actor, row); } catch { /* Durable pending/failed denies resolution; do not mask the original failure. */ }
          throw error;
        }
      });
    });
  }
  return Object.freeze({ operations: new Set(store ? operations : []), execute, sourceCredentialFor: resolve, close() { store?.close(); } });
}
