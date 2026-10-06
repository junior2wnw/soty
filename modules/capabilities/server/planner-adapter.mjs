import { createHash } from 'node:crypto';
import { snapshot } from '../../app-contract/json.mjs';
import { canonicalJson } from './validation.mjs';

export const PLANNER_ADAPTER_PROFILE = 'soty.planner-http-workspace.v1';
export class PlannerAdapterError extends Error {
  constructor(code, { status = 400, outcome = 'not-started' } = {}) {
    super(code);
    this.name = 'PlannerAdapterError';
    this.code = code;
    this.status = status;
    this.outcome = outcome;
  }
}
const requireThat = (ok, code = 'planner_adapter_invalid', details) => {
  if (!ok) throw new PlannerAdapterError(code, details);
};
const closed = (value, required, optional = []) =>
  requireThat(
    value &&
      typeof value === 'object' &&
      !Array.isArray(value) &&
      required.every((key) => Object.hasOwn(value, key)) &&
      Object.keys(value).every((key) => required.includes(key) || optional.includes(key)),
  );
const id = (value) => {
  requireThat(
    typeof value === 'string' &&
      /^[A-Za-z0-9][A-Za-z0-9._:@/-]{0,179}$/u.test(value) &&
      !value.includes('..') &&
      !value.includes('//'),
  );
  return value;
};
const integer = (value) => Number.isSafeInteger(value) && value >= 0;
const digest = (value) => createHash('sha256').update(canonicalJson(value)).digest('hex');
// Provider discovery is a separate wire document, not a U1 descriptor. Its
// schema can be deeper/larger and contain non-integer JSON Schema numbers.
// Keep a separate bounded data-only snapshot; never widen the core U1 limits.
function schemaData(input) {
  let nodes = 0;
  function capture(value, depth) {
    requireThat(++nodes <= 20000 && depth <= 32, 'planner_schema_limit');
    if (value === null || typeof value === 'boolean') return value;
    if (typeof value === 'number') {
      requireThat(Number.isFinite(value), 'planner_schema_invalid');
      return value;
    }
    if (typeof value === 'string') {
      requireThat(value.length <= 65536 && value.isWellFormed(), 'planner_schema_limit');
      return value;
    }
    requireThat(
      value && typeof value === 'object' && Object.getOwnPropertySymbols(value).length === 0,
      'planner_schema_invalid',
    );
    const properties = Object.getOwnPropertyDescriptors(value);
    if (Array.isArray(value)) {
      requireThat(
        value.length <= 1024 && Object.keys(properties).length === value.length + 1,
        'planner_schema_limit',
      );
      return Array.from({ length: value.length }, (_, index) => {
        requireThat(properties[index] && 'value' in properties[index], 'planner_schema_invalid');
        return capture(properties[index].value, depth + 1);
      });
    }
    requireThat(
      [Object.prototype, null].includes(Object.getPrototypeOf(value)),
      'planner_schema_invalid',
    );
    const result = Object.create(null);
    for (const [key, property] of Object.entries(properties)) {
      requireThat(
        key.length <= 256 && property.enumerable && 'value' in property,
        'planner_schema_invalid',
      );
      result[key] = capture(property.value, depth + 1);
    }
    return result;
  }
  const result = capture(input, 0);
  requireThat(Buffer.byteLength(canonicalJson(result)) <= 524288, 'planner_schema_limit');
  return result;
}

/** The three real transport-independent Planner schemas used by this adapter.
 * An approved source release supplies this pin; discovery does not approve it. */
export function plannerToolsDigest(input) {
  const tools = schemaData(input);
  requireThat(Array.isArray(tools) && tools.length <= 32, 'planner_schema_invalid');
  const selected = ['planner_help', 'planner_read', 'planner_apply'].map((name) => {
    const matches = tools.filter((tool) => tool.name === name);
    requireThat(
      matches.length === 1 && matches[0].inputSchema?.type === 'object',
      'planner_schema_invalid',
    );
    return { name, inputSchema: matches[0].inputSchema };
  });
  return digest(selected);
}

/** Trusted host-owned closure only. No endpoint, source key, workspace selector
 * or actor claim is accepted in tool arguments or installed from a descriptor.
 * The host broker retains its own admission/ledger/unknown-outcome semantics.
 * Every network call occurs outside its Apps/Connect SQLite transactions. */
export function createPlannerAdapter({
  origin,
  resource,
  expectedToolsDigest,
  authorize,
  resolveCredential,
  accountId,
  assertDestination,
  fetch: fetcher = globalThis.fetch,
  allowLoopback = false,
} = {}) {
  let parsed;
  try {
    parsed = new URL(origin);
  } catch {
    throw new PlannerAdapterError('planner_origin_invalid');
  }
  const local =
    allowLoopback &&
    parsed.protocol === 'http:' &&
    ['127.0.0.1', 'localhost', '[::1]'].includes(parsed.hostname) &&
    Number(parsed.port) >= 1024 &&
    Number(parsed.port) <= 65535;
  requireThat(
    origin === parsed.origin &&
      !parsed.username &&
      !parsed.password &&
      (parsed.protocol === 'https:' || local),
    'planner_origin_invalid',
  );
  const scope = snapshot(resource);
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
  Object.freeze(scope);
  requireThat(
    typeof expectedToolsDigest === 'string' && /^[a-f0-9]{64}$/u.test(expectedToolsDigest),
    'planner_schema_pin_required',
  );
  requireThat(
    [authorize, resolveCredential, accountId, assertDestination, fetcher].every(
      (value) => typeof value === 'function',
    ),
    'planner_host_required',
  );
  let stopped = false;
  async function check(context, effect) {
    requireThat(!stopped, 'planner_adapter_closed', { status: 503 });
    requireThat(
      (await authorize(context, Object.freeze({ resource: scope, effect }))) === true,
      'planner_access_denied',
      { status: 403 },
    );
    return id(accountId(context));
  }
  async function lease(context, effect) {
    const actor = await check(context, effect);
    const value = snapshot(
      await resolveCredential(context, Object.freeze({ resource: scope, effect })),
    );
    closed(value, [
      'sotyAccountId',
      'sourceActorId',
      'workspaceIds',
      'readOnly',
      'expiresAt',
      'token',
    ]);
    requireThat(
      value.sotyAccountId === actor && value.sourceActorId === scope.sourceActorId,
      'planner_actor_mismatch',
      { status: 403 },
    );
    requireThat(
      Array.isArray(value.workspaceIds) &&
        value.workspaceIds.length === 1 &&
        value.workspaceIds[0] === scope.workspaceId,
      'planner_scope_mismatch',
      { status: 403 },
    );
    requireThat(
      typeof value.readOnly === 'boolean' &&
        typeof value.expiresAt === 'string' &&
        Number.isFinite(Date.parse(value.expiresAt)) &&
        Date.parse(value.expiresAt) > Date.now(),
      'planner_credential_unavailable',
      { status: 401 },
    );
    requireThat(effect !== 'create' || value.readOnly === false, 'planner_read_only', {
      status: 403,
    });
    requireThat(
      typeof value.token === 'string' && /^plnr_[A-Za-z0-9_-]{43}$/u.test(value.token),
      'planner_credential_unavailable',
      { status: 401 },
    );
    return Object.freeze(value);
  }
  async function request(context, effect, credential, path, body, { mutation = false } = {}) {
    const actor = await check(context, effect);
    requireThat(
      actor === credential.sotyAccountId && Date.parse(credential.expiresAt) > Date.now(),
      'planner_actor_mismatch',
      { status: 403 },
    );
    requireThat((await assertDestination(origin)) === true, 'planner_destination_denied', {
      status: 403,
    });
    let response;
    try {
      response = await fetcher(origin + path, {
        method: body === undefined ? 'GET' : 'POST',
        credentials: 'omit',
        redirect: 'error',
        referrerPolicy: 'no-referrer',
        signal: AbortSignal.timeout(8000),
        headers: {
          accept: 'application/json',
          authorization: `Bearer ${credential.token}`,
          ...(body === undefined ? {} : { 'content-type': 'application/json' }),
        },
        ...(body === undefined ? {} : { body: JSON.stringify(body) }),
      });
    } catch {
      throw new PlannerAdapterError('planner_source_unconfirmed', {
        status: 503,
        outcome: mutation ? 'unknown' : 'not-started',
      });
    }
    requireThat(
      !response.redirected && (!response.url || response.url === origin + path),
      'planner_redirect_denied',
      { status: 502, outcome: mutation ? 'unknown' : 'not-started' },
    );
    requireThat(
      /^application\/json(?:;|$)/iu.test(response.headers.get('content-type') || ''),
      'planner_response_invalid',
      { status: 502, outcome: mutation ? 'unknown' : 'not-started' },
    );
    const declared = response.headers.get('content-length');
    requireThat(
      !declared || (/^\d+$/u.test(declared) && Number(declared) <= 524288),
      'planner_response_limit',
      { status: 502, outcome: mutation ? 'unknown' : 'not-started' },
    );
    requireThat(response.body, 'planner_response_invalid', {
      status: 502,
      outcome: mutation ? 'unknown' : 'not-started',
    });
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
          throw new PlannerAdapterError('planner_response_limit', {
            status: 502,
            outcome: mutation ? 'unknown' : 'not-started',
          });
        }
        parts.push(value);
      }
    } catch (error) {
      if (error instanceof PlannerAdapterError) throw error;
      throw new PlannerAdapterError('planner_source_unconfirmed', {
        status: 503,
        outcome: mutation ? 'unknown' : 'not-started',
      });
    } finally {
      reader.releaseLock();
    }
    const bytes = new Uint8Array(size);
    let offset = 0;
    for (const part of parts) {
      bytes.set(part, offset);
      offset += part.byteLength;
    }
    let data;
    try {
      data = JSON.parse(new TextDecoder('utf-8', { fatal: true }).decode(bytes));
    } catch {
      throw new PlannerAdapterError('planner_response_invalid', {
        status: 502,
        outcome: mutation ? 'unknown' : 'not-started',
      });
    }
    if (!response.ok) {
      const code =
        typeof data?.code === 'string' && /^[a-z_]{1,80}$/u.test(data.code)
          ? data.code
          : 'unavailable';
      throw new PlannerAdapterError('planner_source_' + code, {
        status: response.status,
        outcome: mutation && response.status >= 500 ? 'unknown' : 'not-started',
      });
    }
    try {
      requireThat((await check(context, effect)) === actor, 'planner_authority_changed', {
        status: 403,
      });
    } catch {
      throw new PlannerAdapterError('planner_authority_changed', {
        status: 403,
        outcome: mutation ? 'unknown' : 'not-started',
      });
    }
    return data;
  }
  const call = (context, effect, credential, name, args, mutation = false) =>
    request(
      context,
      effect,
      credential,
      '/api/agent/call',
      { name, arguments: args },
      { mutation },
    );
  async function prepare(context, effect) {
    const credential = await lease(context, effect);
    const tools = await request(context, effect, credential, '/api/agent/tools');
    requireThat(plannerToolsDigest(tools.tools) === expectedToolsDigest, 'planner_schema_changed', {
      status: 409,
    });
    const help = await call(context, effect, credential, 'planner_help', {});
    requireThat(
      Array.isArray(help.workspaces) &&
        help.workspaces.length === 1 &&
        help.workspaces[0].id === scope.workspaceId &&
        integer(help.revision),
      'planner_source_scope_mismatch',
      { status: 403 },
    );
    return credential;
  }
  return Object.freeze({
    profile: PLANNER_ADAPTER_PROFILE,
    // Authority/readiness are determined by the host and real source calls,
    // not by this local descriptor or the existence of a source directory.
    async discover(context) {
      const credential = await prepare(context, 'read');
      void credential;
      return Object.freeze({
        profile: PLANNER_ADAPTER_PROFILE,
        resourceId: scope.resourceId,
        schemaDigest: expectedToolsDigest,
        operations: ['readObjects', 'createWorkItem'],
        requiresHostAdmission: true,
      });
    },
    async readObjects(context, input = {}) {
      const args = snapshot(input);
      closed(args, [], ['query', 'limit', 'cursor']);
      requireThat(
        args.query === undefined || (typeof args.query === 'string' && args.query.length <= 500),
      );
      requireThat(
        args.limit === undefined ||
          (Number.isSafeInteger(args.limit) && args.limit >= 1 && args.limit <= 50),
      );
      requireThat(
        args.cursor === undefined ||
          (typeof args.cursor === 'string' && args.cursor.length <= 2000),
      );
      const credential = await prepare(context, 'read');
      const data = await call(context, 'read', credential, 'planner_read', {
        collection: 'objects',
        workspace: scope.workspaceId,
        limit: args.limit ?? 20,
        ...args,
      });
      requireThat(
        integer(data.revision) &&
          data.collection === 'objects' &&
          Array.isArray(data.items) &&
          data.items.length <= (args.limit ?? 20) &&
          integer(data.total) &&
          (data.cursor === null || (typeof data.cursor === 'string' && data.cursor.length <= 2000)),
        'planner_response_invalid',
      );
      for (const item of data.items)
        requireThat(
          item.workspaceId === scope.workspaceId &&
            typeof item.id === 'string' &&
            typeof item.title === 'string' &&
            item.title.length <= 1000 &&
            integer(item.version),
          'planner_response_invalid',
        );
      return Object.freeze({
        resourceId: scope.resourceId,
        revision: data.revision,
        total: data.total,
        cursor: data.cursor,
        items: data.items.map((item) => ({
          id: item.id,
          title: item.title,
          version: item.version,
          kind: item.kind,
          status: item.status,
          plan: item.plan,
        })),
      });
    },
    async createWorkItem(context, input) {
      const args = snapshot(input);
      closed(args, ['requestId', 'title']);
      id(args.requestId);
      requireThat(
        typeof args.title === 'string' && args.title.trim().length > 0 && args.title.length <= 500,
      );
      const credential = await prepare(context, 'create');
      // Keep the user's exact title and requestId on a retry. Do not invent a
      // schedule or reinterpret the source's undated-note semantics.
      const data = await call(
        context,
        'create',
        credential,
        'planner_apply',
        {
          requestId: args.requestId,
          operations: [
            {
              op: 'create',
              collection: 'objects',
              workspace: scope.workspaceId,
              key: 'workItem',
              data: { title: args.title },
            },
          ],
        },
        true,
      );
      requireThat(
        data.requestId === args.requestId &&
          integer(data.revision) &&
          Array.isArray(data.results) &&
          data.results.length === 1 &&
          typeof data.refs?.workItem === 'string' &&
          data.results[0].id === data.refs.workItem,
        'planner_receipt_invalid',
        { status: 502, outcome: 'unknown' },
      );
      return Object.freeze({
        resourceId: scope.resourceId,
        requestId: args.requestId,
        revision: data.revision,
        objectId: data.refs.workItem,
        sourceReceipt: true,
      });
    },
    close() {
      stopped = true;
    },
  });
}
