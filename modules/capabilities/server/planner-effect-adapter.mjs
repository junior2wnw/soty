import { snapshot } from '../../app-contract/json.mjs';
import { canonicalHash, freezeDeep } from './validation.mjs';
import { plannerToolsDigest, PlannerAdapterError } from './planner-adapter.mjs';
import { EXTERNAL_ADAPTER_PROFILE } from './external-adapters.mjs';

export const PLANNER_PROOF_PROTOCOL = 'planner.work-item-proof.v1';
const need = (ok, code = 'planner_effect_invalid') => {
  if (!ok) throw new PlannerAdapterError(code);
};
const closed = (value, keys) =>
  need(
    value &&
      typeof value === 'object' &&
      !Array.isArray(value) &&
      keys.every((key) => Object.hasOwn(value, key)) &&
      Object.keys(value).every((key) => keys.includes(key)),
  );
const id = (value) =>
  need(
    typeof value === 'string' &&
      /^[A-Za-z0-9][A-Za-z0-9_.:@/-]{0,159}$/u.test(value) &&
      !value.includes('..'),
  );
const sha = (value) => need(typeof value === 'string' && /^[a-f0-9]{64}$/u.test(value));
const number = (value) => need(Number.isSafeInteger(value) && value >= 0);
const pin = (value) => {
  closed(value, ['id', 'version', 'digest']);
  id(value.id);
  number(value.version);
  need(value.version > 0);
  sha(value.digest);
};

/** Host-owned bridge for the existing Capabilities delivery coordinator. The
 * binding commits to a closed destination/scope/protocol/release profile,
 * never an executable code hash, caller URL, secret or manifest command. */
export function createPlannerEffectAdapter({
  origin,
  resource,
  sourceScope,
  sourceRelease,
  expectedToolsDigest,
  expectedProofDigest,
  bindingId,
  bindingVersion = 1,
  withAuthority: hostAuthority,
  resolveCredential,
  assertDestination,
  allowLoopback = false,
  fetch: fetcher = globalThis.fetch,
} = {}) {
  const scope = snapshot(resource),
    originalScope = snapshot(sourceScope),
    release = snapshot(sourceRelease);
  closed(scope, [
    'registryId',
    'tenantId',
    'appId',
    'environmentId',
    'resourceId',
    'workspaceId',
    'sourceActorId',
  ]);
  Object.values(scope).forEach(id);
  closed(originalScope, ['workspaceIds', 'readOnly', 'keyId']);
  id(originalScope.keyId);
  need(
    originalScope.readOnly === false &&
      Array.isArray(originalScope.workspaceIds) &&
      originalScope.workspaceIds.length === 1 &&
      originalScope.workspaceIds[0] === scope.workspaceId,
  );
  pin(release);
  sha(expectedToolsDigest);
  sha(expectedProofDigest);
  id(bindingId);
  number(bindingVersion);
  need(bindingVersion > 0);
  const url = new URL(origin);
  need(
    origin === url.origin &&
      !url.username &&
      !url.password &&
      (url.protocol === 'https:' ||
        (allowLoopback &&
          url.protocol === 'http:' &&
          ['localhost', '127.0.0.1', '[::1]'].includes(url.hostname) &&
          Number(url.port) >= 1024 &&
          Number(url.port) <= 65535)),
    'planner_origin_invalid',
  );
  need(
    [hostAuthority, resolveCredential, assertDestination, fetcher].every(
      (value) => typeof value === 'function',
    ),
  );
  const scopeDigest = canonicalHash(originalScope);
  const binding = freezeDeep({
    id: bindingId,
    version: bindingVersion,
    digest: canonicalHash({
      profile: EXTERNAL_ADAPTER_PROFILE,
      sourceProtocol: PLANNER_PROOF_PROTOCOL,
      origin,
      resource: scope,
      sourceScope: originalScope,
      sourceRelease: release,
      providerSchema: { tools: expectedToolsDigest, proof: expectedProofDigest },
    }),
  });
  function selectedAuthorization(authorization) {
    need(authorization && typeof authorization === 'object');
    id(authorization.accountId);
    need(
      Array.isArray(authorization.resources) &&
        authorization.resources.length === 1 &&
        authorization.resources[0] === scope.resourceId &&
        Array.isArray(authorization.effects) &&
        authorization.effects.length === 1 &&
        authorization.effects[0] === 'create' &&
        Array.isArray(authorization.recipients) &&
        authorization.recipients.length === 1 &&
        authorization.recipients[0] === scope.resourceId,
      'planner_access_denied',
    );
  }
  function capture(request) {
    const value = snapshot(request);
    closed(value, ['requestId', 'inputDigest', 'input', 'authorization']);
    id(value.requestId);
    sha(value.inputDigest);
    selectedAuthorization(value.authorization);
    closed(value.input, ['title']);
    need(
      typeof value.input.title === 'string' &&
        value.input.title.length <= 500 &&
        value.input.title.trim().length > 0,
    );
    need(canonicalHash(value.input) === value.inputDigest, 'planner_input_digest_mismatch');
    const args = {
      requestId: value.requestId,
      operations: [
        {
          op: 'create',
          collection: 'objects',
          workspace: scope.workspaceId,
          key: 'workItem',
          data: { title: value.input.title },
        },
      ],
    };
    return freezeDeep({
      value,
      args,
      lookup: {
        protocol: PLANNER_PROOF_PROTOCOL,
        requestId: value.requestId,
        workspaceId: scope.workspaceId,
        inputDigest: value.inputDigest,
        providerInputDigest: canonicalHash(args),
        scopeDigest,
      },
    });
  }
  async function credential(request, purpose) {
    const lease = snapshot(
      await resolveCredential(request, freezeDeep({ resource: scope, purpose })),
    );
    closed(lease, [
      'sotyAccountId',
      'sourceActorId',
      'workspaceIds',
      'readOnly',
      'keyId',
      'expiresAt',
      'token',
    ]);
    need(
      lease.sotyAccountId === request.authorization.accountId &&
        lease.sourceActorId === scope.sourceActorId &&
        Array.isArray(lease.workspaceIds) &&
        lease.workspaceIds.length === 1 &&
        lease.workspaceIds[0] === scope.workspaceId,
      'planner_actor_mismatch',
    );
    id(lease.keyId);
    need(
      typeof lease.readOnly === 'boolean' &&
        Number.isFinite(Date.parse(lease.expiresAt)) &&
        Date.parse(lease.expiresAt) > Date.now() &&
        /^plnr_[A-Za-z0-9_-]{43}$/u.test(lease.token),
      'planner_credential_unavailable',
    );
    if (purpose === 'execute')
      need(
        !lease.readOnly &&
          canonicalHash({
            workspaceIds: lease.workspaceIds,
            readOnly: lease.readOnly,
            keyId: lease.keyId,
          }) === scopeDigest,
        'planner_source_scope_changed',
      );
    return lease;
  }
  async function http(path, body, lease, signal, beforeSend) {
    need(
      !signal?.aborted && (await assertDestination(origin)) === true,
      'planner_destination_denied',
    );
    // A previous Root check is not a lease across discovery/network awaits.
    // The synchronous fence returns before fetch: no network inside SQLite.
    if (beforeSend) beforeSend();
    const response = await fetcher(origin + path, {
      method: body === undefined ? 'GET' : 'POST',
      credentials: 'omit',
      redirect: 'error',
      referrerPolicy: 'no-referrer',
      signal,
      headers: {
        accept: 'application/json',
        authorization: 'Bearer ' + lease.token,
        ...(body === undefined ? {} : { 'content-type': 'application/json' }),
      },
      ...(body === undefined ? {} : { body: JSON.stringify(body) }),
    });
    need(
      !response.redirected && (!response.url || response.url === origin + path),
      'planner_redirect_denied',
    );
    need(
      /^application\/json(?:;|$)/iu.test(response.headers.get('content-type') || ''),
      'planner_response_invalid',
    );
    const declared = response.headers.get('content-length');
    need(
      !declared || (/^\d+$/u.test(declared) && Number(declared) <= 524288),
      'planner_response_limit',
    );
    need(response.body, 'planner_response_invalid');
    const reader = response.body.getReader(),
      parts = [];
    let size = 0;
    try {
      while (true) {
        const { done, value } = await reader.read();
        if (done) break;
        size += value.byteLength;
        if (size > 524288) {
          await reader.cancel();
          throw new PlannerAdapterError('planner_response_limit');
        }
        parts.push(value);
      }
    } finally {
      reader.releaseLock();
    }
    const bytes = new Uint8Array(size);
    let offset = 0;
    for (const part of parts) {
      bytes.set(part, offset);
      offset += part.byteLength;
    }
    const result = JSON.parse(new TextDecoder('utf-8', { fatal: true }).decode(bytes));
    if (!response.ok) throw new PlannerAdapterError('planner_source_unconfirmed');
    return result;
  }
  async function profile(lease, signal) {
    const data = await http('/api/agent/receipts/profile', undefined, lease, signal);
    closed(data, [
      'protocol',
      'inputSchema',
      'effect',
      'sourceAuthority',
      'receiptAuthority',
      'digest',
      'authority',
    ]);
    const { digest, authority, ...body } = data;
    need(
      data.protocol === PLANNER_PROOF_PROTOCOL &&
        digest === expectedProofDigest &&
        canonicalHash(body) === digest,
      'planner_proof_profile_changed',
    );
    closed(authority, ['sourceActorId', 'keyId', 'workspaceIds', 'readOnly', 'scopeDigest']);
    need(
      authority.sourceActorId === scope.sourceActorId &&
        authority.keyId === lease.keyId &&
        authority.readOnly === lease.readOnly &&
        authority.workspaceIds?.length === 1 &&
        authority.workspaceIds[0] === scope.workspaceId &&
        authority.scopeDigest ===
          canonicalHash({
            workspaceIds: lease.workspaceIds,
            readOnly: lease.readOnly,
            keyId: lease.keyId,
          }),
      'planner_source_actor_mismatch',
    );
  }
  const unknown = (request) => ({
    requestId: request.requestId,
    inputDigest: request.inputDigest,
    outcome: 'unknown',
  });
  const adapter = Object.freeze({
    profile: EXTERNAL_ADAPTER_PROFILE,
    withAuthority(request, callback) {
      if (request.phase === 'admit') {
        need(request.actor && typeof request.actor === 'object');
        id(request.actor.accountId);
      } else selectedAuthorization(request.authorization);
      return hostAuthority(request, freezeDeep({ resource: scope, binding }), callback);
    },
    async execute(request, signal) {
      const { value, args } = capture(request),
        lease = await credential(value, 'execute');
      await profile(lease, signal);
      const tools = await http('/api/agent/tools', undefined, lease, signal);
      need(plannerToolsDigest(tools.tools) === expectedToolsDigest, 'planner_schema_changed');
      const help = await http(
        '/api/agent/call',
        { name: 'planner_help', arguments: {} },
        lease,
        signal,
      );
      need(
        help.workspaces?.length === 1 && help.workspaces[0].id === scope.workspaceId,
        'planner_source_scope_mismatch',
      );
      await http(
        '/api/agent/call',
        { name: 'planner_apply', arguments: args },
        lease,
        signal,
        () => {
          need(!signal?.aborted, 'planner_source_unconfirmed');
          let active = true,
            entered = false;
          try {
            const result = hostAuthority(
              Object.freeze({ phase: 'execute', authorization: value.authorization }),
              freezeDeep({ resource: scope, binding }),
              () => {
                need(active && !entered, 'planner_authority_invalid');
                entered = true;
                return true;
              },
            );
            if (result && typeof result.then === 'function') {
              Promise.resolve(result).catch(() => {});
              need(false, 'planner_authority_invalid');
            }
            need(entered && result === true, 'planner_access_denied');
          } finally {
            active = false;
          }
        },
      );
    },
    async readProof(request, signal) {
      const { value, lookup } = capture(request);
      try {
        const lease = await credential(value, 'proof');
        await profile(lease, signal);
        const data = await http('/api/agent/receipts/lookup', lookup, lease, signal);
        closed(data, [
          'protocol',
          'requestId',
          'workspaceId',
          'sourceActorId',
          'inputDigest',
          'providerInputDigest',
          'scopeDigest',
          'outcome',
          ...(data.outcome === 'committed'
            ? ['objectId', 'objectRevision', 'revision', 'receiptDigest']
            : []),
        ]);
        need(
          ['committed', 'not_applied', 'unknown'].includes(data.outcome),
          'planner_proof_invalid',
        );
        for (const [key, expected] of Object.entries({
          ...lookup,
          sourceActorId: scope.sourceActorId,
        }))
          need(data[key] === expected, 'planner_proof_mismatch');
        if (data.outcome !== 'committed') return { ...unknown(value), outcome: data.outcome };
        id(data.objectId);
        number(data.objectRevision);
        number(data.revision);
        sha(data.receiptDigest);
        need(data.objectRevision === 1 && data.revision > 0, 'planner_proof_invalid');
        return {
          requestId: value.requestId,
          inputDigest: value.inputDigest,
          outcome: 'committed',
          effects: [
            {
              kind: 'created',
              resourceType: 'planner-object',
              resourceId: data.objectId,
              revision: data.objectRevision,
            },
          ],
          receipt: {
            verificationMethod: 'domain_read',
            artifacts: [
              { type: 'planner-object', id: data.objectId, revision: data.objectRevision },
            ],
          },
        };
      } catch {
        return unknown(value);
      }
    },
  });
  return Object.freeze({ binding, adapter });
}
