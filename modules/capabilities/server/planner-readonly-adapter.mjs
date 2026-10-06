import { snapshot } from '../../app-contract/json.mjs';
import { canonicalHash, freezeDeep } from './validation.mjs';
import { plannerToolsDigest, PlannerAdapterError } from './planner-adapter.mjs';
import {
  READONLY_QUERY_PROFILE,
  createTrustedReadonlyQueryAdapter,
  captureQueryData,
} from './readonly-queries.mjs';

export const PLANNER_READONLY_PROTOCOL = 'soty.planner-selected-object-query.v1';
const need = (ok, code = 'planner_query_invalid') => {
  if (!ok) throw new PlannerAdapterError(code);
};
const closed = (value, keys, optional = []) =>
  need(
    value &&
      typeof value === 'object' &&
      !Array.isArray(value) &&
      keys.every((key) => Object.hasOwn(value, key)) &&
      Object.keys(value).every((key) => [...keys, ...optional].includes(key)),
  );
const id = (value) =>
  need(
    typeof value === 'string' &&
      /^[A-Za-z0-9][A-Za-z0-9_.:@/-]{0,159}$/u.test(value) &&
      !value.includes('..') &&
      !value.includes('//'),
  );
const sha = (value) => need(typeof value === 'string' && /^[a-f0-9]{64}$/u.test(value));

/** Reviewed CODE path: fixed tools/help/objects-read + authenticated scope
 * projection only. It cannot call planner_apply. The live Source key must
 * independently report readOnly:true; metadata is not handler attestation. */
export function createPlannerReadonlyQueryAdapter({
  origin,
  resource,
  sourceScope,
  sourceRelease,
  expectedToolsDigest,
  expectedAuthorityProfileDigest,
  bindingId,
  bindingVersion = 1,
  withAuthority: hostAuthority,
  resolveCredential,
  assertDestination,
  allowLoopback = false,
  fetch: fetcher = globalThis.fetch,
} = {}) {
  const scope = snapshot(resource),
    selected = snapshot(sourceScope),
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
  closed(selected, ['workspaceIds', 'readOnly', 'keyId']);
  id(selected.keyId);
  need(
    selected.readOnly === true &&
      Array.isArray(selected.workspaceIds) &&
      selected.workspaceIds?.length === 1 &&
      selected.workspaceIds[0] === scope.workspaceId,
  );
  closed(release, ['id', 'version', 'digest']);
  id(release.id);
  sha(release.digest);
  need(Number.isSafeInteger(release.version) && release.version >= 1 && release.version <= 1000000);
  sha(expectedToolsDigest);
  sha(expectedAuthorityProfileDigest);
  id(bindingId);
  need(Number.isSafeInteger(bindingVersion) && bindingVersion >= 1 && bindingVersion <= 1000000);
  let url;
  try {
    url = new URL(origin);
  } catch {
    need(false, 'planner_origin_invalid');
  }
  need(
    origin === url.origin &&
      !url.username &&
      !url.password &&
      (url.protocol === 'https:' ||
        (allowLoopback &&
          url.protocol === 'http:' &&
          ['127.0.0.1', 'localhost', '[::1]'].includes(url.hostname) &&
          Number(url.port) >= 1024 &&
          Number(url.port) <= 65535)),
    'planner_origin_invalid',
  );
  need(
    [hostAuthority, resolveCredential, assertDestination, fetcher].every(
      (value) => typeof value === 'function',
    ) && hostAuthority.constructor?.name !== 'AsyncFunction',
  );
  const binding = freezeDeep({
    id: bindingId,
    version: bindingVersion,
    digest: canonicalHash({
      profile: READONLY_QUERY_PROFILE,
      protocol: PLANNER_READONLY_PROTOCOL,
      origin,
      resource: scope,
      sourceScope: selected,
      sourceRelease: release,
      toolsDigest: expectedToolsDigest,
      authorityProfileDigest: expectedAuthorityProfileDigest,
    }),
  });
  function authorization(value) {
    need(
      value &&
        value.resources?.length === 1 &&
        value.resources[0] === scope.resourceId &&
        value.effects?.length === 0 &&
        value.recipients?.length === 1 &&
        value.recipients[0] === scope.resourceId,
      'planner_access_denied',
    );
    id(value.accountId);
  }
  const adapter = createTrustedReadonlyQueryAdapter({
    withAuthority(request, callback) {
      if (request.authorization) authorization(request.authorization);
      else {
        id(request.actor?.accountId);
      }
      return hostAuthority(request, freezeDeep({ resource: scope, binding }), callback);
    },
    async query(raw, signal, currentAuthority) {
      const request = captureQueryData(raw);
      closed(request, ['requestId', 'input', 'authorization']);
      authorization(request.authorization);
      closed(request.input, [], ['query', 'limit', 'cursor']);
      need(
        typeof currentAuthority === 'function' &&
          currentAuthority.constructor?.name !== 'AsyncFunction',
      );
      const input = request.input;
      need(
        input.query === undefined || (typeof input.query === 'string' && input.query.length <= 500),
      );
      need(
        input.limit === undefined ||
          (Number.isSafeInteger(input.limit) && input.limit >= 1 && input.limit <= 20),
      );
      need(
        input.cursor === undefined ||
          (typeof input.cursor === 'string' && input.cursor.length <= 2000),
      );
      const fresh = () => {
        need(!signal?.aborted, 'planner_source_unconfirmed');
        need(currentAuthority() === true, 'planner_access_denied');
      };
      fresh();
      const lease = snapshot(
        await resolveCredential(request, freezeDeep({ resource: scope, purpose: 'query' })),
      );
      fresh();
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
          lease.keyId === selected.keyId &&
          lease.readOnly === true &&
          Array.isArray(lease.workspaceIds) &&
          lease.workspaceIds?.length === 1 &&
          lease.workspaceIds[0] === scope.workspaceId &&
          Number.isFinite(Date.parse(lease.expiresAt)) &&
          Date.parse(lease.expiresAt) > Date.now() &&
          /^plnr_[A-Za-z0-9_-]{43}$/u.test(lease.token),
        'planner_readonly_credential_required',
      );
      async function http(path, body) {
        fresh();
        need((await assertDestination(origin)) === true, 'planner_destination_denied');
        fresh();
        need(Date.parse(lease.expiresAt) > Date.now(), 'planner_credential_unavailable');
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
        fresh();
        need(
          !response.redirected &&
            (!response.url || response.url === origin + path) &&
            /^application\/json(?:;|$)/iu.test(response.headers.get('content-type') || ''),
          'planner_response_invalid',
        );
        const length = response.headers.get('content-length');
        need(
          !length || (/^\d+$/u.test(length) && Number(length) <= 524288),
          'planner_response_limit',
        );
        need(response.body, 'planner_response_invalid');
        const reader = response.body.getReader(),
          parts = [];
        let bytes = 0;
        try {
          for (;;) {
            const part = await reader.read();
            fresh();
            if (part.done) break;
            bytes += part.value.byteLength;
            if (bytes > 524288) {
              await reader.cancel();
              need(false, 'planner_response_limit');
            }
            parts.push(part.value);
          }
        } finally {
          reader.releaseLock();
        }
        let data;
        try {
          data = JSON.parse(
            new TextDecoder('utf-8', { fatal: true }).decode(
              Buffer.concat(
                parts.map((value) => Buffer.from(value)),
                bytes,
              ),
            ),
          );
        } catch {
          need(false, 'planner_response_invalid');
        }
        need(response.ok, 'planner_source_unconfirmed');
        fresh();
        return data;
      }
      async function profile() {
        const profile = await http('/api/agent/receipts/profile');
        closed(profile, [
          'protocol',
          'inputSchema',
          'effect',
          'sourceAuthority',
          'receiptAuthority',
          'digest',
          'authority',
        ]);
        const { digest, authority, ...body } = profile;
        need(
          digest === expectedAuthorityProfileDigest && canonicalHash(body) === digest,
          'planner_source_profile_changed',
        );
        closed(authority, ['sourceActorId', 'keyId', 'workspaceIds', 'readOnly', 'scopeDigest']);
        need(
          authority.sourceActorId === scope.sourceActorId &&
            authority.keyId === selected.keyId &&
            authority.readOnly === true &&
            Array.isArray(authority.workspaceIds) &&
            authority.workspaceIds?.length === 1 &&
            authority.workspaceIds[0] === scope.workspaceId &&
            authority.scopeDigest === canonicalHash(selected),
          'planner_source_scope_changed',
        );
      }
      await profile();
      const tools = await http('/api/agent/tools');
      need(plannerToolsDigest(tools.tools) === expectedToolsDigest, 'planner_schema_changed');
      const help = await http('/api/agent/call', { name: 'planner_help', arguments: {} });
      need(
        help.workspaces?.length === 1 && help.workspaces[0].id === scope.workspaceId,
        'planner_source_scope_changed',
      );
      const data = await http('/api/agent/call', {
        name: 'planner_read',
        arguments: {
          collection: 'objects',
          workspace: scope.workspaceId,
          limit: input.limit ?? 20,
          ...input,
        },
      });
      await profile();
      fresh();
      need(
        data.collection === 'objects' &&
          Number.isSafeInteger(data.revision) &&
          data.revision >= 0 &&
          Array.isArray(data.items) &&
          data.items.length <= (input.limit ?? 20) &&
          Number.isSafeInteger(data.total) &&
          data.total >= 0 &&
          (data.cursor === null || (typeof data.cursor === 'string' && data.cursor.length <= 2000)),
        'planner_response_invalid',
      );
      return captureQueryData({
        resourceId: scope.resourceId,
        revision: data.revision,
        total: data.total,
        ...(data.cursor === null ? {} : { cursor: data.cursor }),
        items: data.items.map((item) => {
          need(item.workspaceId === scope.workspaceId, 'planner_source_scope_changed');
          id(item.id);
          need(
            typeof item.title === 'string' &&
              item.title.length <= 1000 &&
              Number.isSafeInteger(item.version) &&
              item.version >= 0 &&
              typeof item.kind === 'string' &&
              item.kind.length <= 80 &&
              typeof item.status === 'string' &&
              item.status.length <= 80,
            'planner_response_invalid',
          );
          return {
            id: item.id,
            title: item.title,
            version: item.version,
            kind: item.kind,
            status: item.status,
          };
        }),
      });
    },
  });
  return Object.freeze({ binding, adapter });
}
