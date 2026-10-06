import test from 'node:test';
import assert from 'node:assert/strict';
import { mkdtempSync, realpathSync, rmSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { join, dirname, basename } from 'node:path';
import { DatabaseSync } from 'node:sqlite';
import { createCapabilitiesService } from '../server/index.mjs';
import { createCatalog } from '../server/catalog.mjs';
import {
  createTrustedReadonlyQueryAdapter,
  captureReadonlyQueryAdapters,
  READONLY_QUERY_PROFILE,
} from '../server/readonly-queries.mjs';
import { createExternalCapabilityOperations } from '../../../server/external-capabilities.js';
import { buildCapabilitiesOpenApi } from '../../../server/capabilities-openapi.js';

const CAP = {
  capabilityId: 'app.query:read',
  version: 1,
  appId: 'query',
  title: 'Read bounded Source data',
  description: 'Synthetic coordinator test; no actual Source claim.',
  visibility: 'private',
  executionEnabled: true,
  inputSchema: {
    type: 'object',
    properties: { query: { type: 'string', maxLength: 500 } },
    required: ['query'],
    additionalProperties: false,
  },
  outputSchema: {
    type: 'object',
    properties: { text: { type: 'string', maxLength: 1000 } },
    required: ['text'],
    additionalProperties: false,
  },
  resources: ['query:workspace'],
  effects: [],
  recipients: ['query:workspace'],
  executionBinding: {
    kind: 'registered',
    handler: 'app.query:read',
    version: 1,
    binding: { id: 'query:binding', version: 1, digest: 'a'.repeat(64) },
  },
};
const REF = {
    capabilityId: CAP.capabilityId,
    version: 1,
    digest: createCatalog([CAP]).get(CAP.capabilityId, 1).digest,
  },
  OWNER = { accountId: 'query_owner', deviceId: 'query_device' },
  AUDIENCE = 'https://query.test';
const fails = (code) => (error) => error.code === code;
function fixture(
  t,
  { limit = 10, timeout = 100, query = () => ({ text: 'private synthetic result' }), fence } = {},
) {
  const parent = realpathSync(tmpdir()),
    directory = realpathSync(mkdtempSync(join(parent, 'soty-query-'))),
    file = join(directory, 'capabilities.sqlite');
  let active = true,
    source = true,
    count = 0,
    paused = false,
    configured = true;
  const adapter = createTrustedReadonlyQueryAdapter({
    withAuthority(request, callback) {
      assert.equal(source, true, 'Source fence');
      return fence ? fence(callback) : callback();
    },
    query(...args) {
      count++;
      return query(...args);
    },
  });
  let service;
  const open = () =>
    createCapabilitiesService({
      databasePath: file,
      projectId: 'query-test',
      actorActive: (actor) =>
        active && actor.accountId === OWNER.accountId && actor.deviceId === OWNER.deviceId,
      catalog: [{ ...CAP, executionEnabled: !paused }],
      documentation: [],
      readonlyQueries: configured ? [{ contract: REF, adapter }] : [],
      readonlyQueryLimits: { callTimeoutMs: timeout },
    });
  service = open();
  const call = (op, args = {}) =>
    service.execute({
      op: 'access.' + op,
      actor: OWNER,
      args: { expectedAccountId: OWNER.accountId, ...args },
    });
  const principal = call('principals.create', { label: 'Synthetic reader' }).principal,
    grant = call('grants.issue', {
      principalId: principal.id,
      capabilities: [{ capabilityId: CAP.capabilityId, version: 1 }],
      resources: CAP.resources,
      effects: [],
      recipients: CAP.recipients,
      expiresAt: Date.now() + 600000,
      budget: { unit: 'invocations', limit },
    }).grant,
    credential = call('credentials.issue', { grantId: grant.id, audience: AUDIENCE });
  const actor = () =>
    service.authenticateCredential({ token: credential.token, audience: AUDIENCE });
  const request = (key = 'query-request-one', text = 'private synthetic input') => ({
    actor: actor(),
    reference: REF,
    idempotencyKey: key,
    input: { query: text },
  });
  const sql = (query) => {
    const db = new DatabaseSync(file, { readOnly: true });
    try {
      return db
        .prepare(query)
        .all()
        .map((row) => ({ ...row }));
    } finally {
      db.close();
    }
  };
  t.after(() => {
    service.close();
    assert.equal(dirname(realpathSync(directory)), parent);
    assert.match(basename(directory), /^soty-query-/u);
    rmSync(directory, { recursive: true, force: true, maxRetries: 5, retryDelay: 100 });
  });
  return {
    get service() {
      return service;
    },
    request,
    actor,
    call,
    grant,
    count: () => count,
    sql,
    revokeRoot() {
      active = false;
    },
    revokeSource() {
      source = false;
    },
    reopen({ disable = false, withoutReadonly = false } = {}) {
      service.close();
      paused = disable;
      configured = !withoutReadonly;
      service = open();
    },
  };
}
test('trusted readonly CODE admission cannot be fabricated by a profile annotation or JSON callback', () => {
  assert.throws(
    () =>
      captureReadonlyQueryAdapters([
        {
          contract: REF,
          adapter: {
            profile: READONLY_QUERY_PROFILE,
            withAuthority(_r, cb) {
              return cb();
            },
            query() {
              return {};
            },
          },
        },
      ]),
    fails('external_query_not_admitted'),
  );
  assert.throws(
    () => createTrustedReadonlyQueryAdapter({ withAuthority: 'callback', query: 'script' }),
    fails('external_query_invalid'),
  );
  assert.throws(
    () => buildCapabilitiesOpenApi({ queryConfigured: true }),
    /external_query_contract_mismatch/u,
  );
  const old = buildCapabilitiesOpenApi({ externalConfigured: true });
  assert.equal(old.paths['/api/capabilities/v1/app-actions/query'], undefined);
  const installed = buildCapabilitiesOpenApi({ externalConfigured: true, queryConfigured: true });
  assert.ok(installed.paths['/api/capabilities/v1/app-actions/query'].post);
});
test('one actual ledger dispatch charges1, retains no payload/output, and exact retry/restart never reads Source again', async (t) => {
  const f = fixture(t, { limit: 1 }),
    first = await f.service.external.query(f.request());
  assert.equal(first.result.text, 'private synthetic result');
  assert.equal(first.charge.amount, 1);
  assert.equal(first.resultUnavailable, false);
  const row = f.sql('SELECT input_json,authorization_json FROM cap_invocations')[0];
  assert.equal(row.input_json.includes('private synthetic input'), false);
  assert.equal(JSON.parse(row.input_json).schema, 'soty.read-query-intent.v1');
  const receipt = f.sql('SELECT value_json FROM cap_receipts')[0].value_json;
  assert.equal(receipt.includes('private synthetic result'), false);
  let replay = await f.service.external.query(f.request());
  assert.equal(replay.reused, true);
  assert.equal(replay.resultUnavailable, true);
  assert.equal(Object.hasOwn(replay, 'result'), false);
  assert.equal(JSON.stringify(replay).includes('inputDigest'), false);
  assert.equal(f.count(), 1);
  f.reopen();
  replay = await f.service.external.query(f.request());
  assert.equal(replay.invocation.invocationId, first.invocation.invocationId);
  assert.equal(f.count(), 1);
  await assert.rejects(
    f.service.external.query(f.request('query-new-key')),
    fails('budget_exceeded'),
  );
  await assert.rejects(
    f.service.external.query(f.request('query-request-one', 'changed input')),
    fails('invocation_request_conflict'),
  );
  assert.deepEqual(f.sql('SELECT reserved_amount,spent_amount FROM cap_budgets')[0], {
    reserved_amount: 0,
    spent_amount: 1,
  });
});
test('catalog/guidance metadata and typed query share authority; old invoke cannot pass a private marker as ordinary input', async (t) => {
  const f = fixture(t),
    actor = f.actor(),
    metadata = f.service.external.getContract({ actor, reference: REF });
  assert.deepEqual(metadata.effects, []);
  assert.equal(f.service.external.search({ actor }).items.length, 1);
  const ops = createExternalCapabilityOperations({ service: f.service, origin: AUDIENCE });
  assert.equal(
    ops.tools.some((tool) => tool.name === 'apps_query'),
    true,
  );
  assert.equal(
    ops.check('apps_query', {
      reference: REF,
      idempotencyKey: 'query-test',
      input: { query: 'value' },
      actor: OWNER,
    }),
    false,
  );
  await assert.rejects(
    f.service.external.invoke(f.request()),
    fails('readonly_query_api_required'),
  );
  assert.throws(
    () =>
      f.service.invocations.admit({
        actor,
        capabilityId: CAP.capabilityId,
        version: 1,
        idempotencyKey: 'marker-test',
        input: { schema: 'soty.read-query-intent.v1', inputDigest: 'b'.repeat(64) },
      }),
    fails('readonly_query_api_required'),
  );
  await assert.rejects(
    f.service.external.query({
      ...f.request(),
      input: { schema: 'soty.read-query-intent.v1', inputDigest: 'b'.repeat(64) },
    }),
  );
  assert.equal(f.sql('SELECT count(*) AS n FROM cap_invocations')[0].n, 0);
});
test('paused execution keeps exact metadata retry but refuses a fresh read', async (t) => {
  const f = fixture(t);
  await f.service.external.query(f.request());
  f.reopen({ disable: true });
  const replay = await f.service.external.query(f.request());
  assert.equal(replay.resultUnavailable, true);
  await assert.rejects(
    f.service.external.query(f.request('different-query')),
    fails('capability_disabled'),
  );
  assert.equal(f.count(), 1);
});
test('Root revoke during await blocks private response while the started unit stays spent', async (t) => {
  let entered, release;
  const reached = new Promise((done) => (entered = done)),
    gate = new Promise((done) => (release = done));
  const f = fixture(t, {
    query: async () => {
      entered();
      await gate;
      return { text: 'must not escape' };
    },
  });
  const pending = f.service.external.query(f.request());
  await reached;
  f.revokeRoot();
  release();
  await assert.rejects(pending);
  assert.equal(f.sql('SELECT spent_amount FROM cap_budgets')[0].spent_amount, 1);
  assert.equal(
    f.sql('SELECT value_json FROM cap_receipts')[0].value_json.includes('must not escape'),
    false,
  );
});
test('Source revoke or invalid output cannot leak data; failed exact retry is metadata only', async (t) => {
  let f;
  f = fixture(t, {
    query: () => {
      f.revokeSource();
      return { text: 'must not escape' };
    },
  });
  await assert.rejects(f.service.external.query(f.request()));
  assert.equal(f.count(), 1);
  const other = fixture(t, { query: () => ({ extra: 'private invalid payload' }) });
  await assert.rejects(
    other.service.external.query(other.request()),
    fails('external_query_output_invalid'),
  );
  const replay = await other.service.external.query(other.request());
  assert.equal(replay.resultUnavailable, true);
  assert.equal(replay.charge.amount, 1);
  assert.equal(other.count(), 1);
});
test('timeouts retain four actual unresolved promises; late output cannot fill a response or receipt', async (t) => {
  const releases = [];
  const f = fixture(t, { timeout: 15, query: () => new Promise((done) => releases.push(done)) });
  for (let i = 0; i < 4; i++)
    await assert.rejects(
      f.service.external.query(f.request('query-timeout-' + i)),
      fails('external_query_unconfirmed'),
    );
  await assert.rejects(
    f.service.external.query(f.request('query-fifth-key')),
    fails('external_adapter_capacity'),
  );
  assert.equal(f.count(), 4);
  const replay = await f.service.external.query(f.request('query-timeout-0'));
  assert.equal(replay.resultUnavailable, true);
  assert.equal(replay.charge.amount, 1);
  releases.forEach((done) => done({ text: 'late private result' }));
  await new Promise((done) => setImmediate(done));
  assert.equal(
    f
      .sql('SELECT value_json FROM cap_receipts')
      .some((row) => row.value_json.includes('late private result')),
    false,
  );
});
test('late/double/swallowed duplicate authority callbacks do not authorize a Source read', async (t) => {
  for (const mode of ['skip', 'double', 'async']) {
    const f = fixture(t, {
      fence: (callback) => {
        if (mode === 'skip') return {};
        if (mode === 'async') return Promise.resolve().then(callback);
        callback();
        try {
          callback();
        } catch {}
        return {};
      },
    });
    await assert.rejects(
      f.service.external.query(f.request()),
      fails('external_authority_invalid'),
    );
    assert.equal(f.count(), 0);
  }
});

test('raw query validation precedes private intent creation and captured caller data cannot change across await', async (t) => {
  let entered, release, received;
  const reached = new Promise((done) => (entered = done)),
    gate = new Promise((done) => (release = done));
  const f = fixture(t, {
    query: async (request) => {
      received = request;
      entered();
      await gate;
      return { text: request.input.query };
    },
  });
  let getter = false;
  const bad = {};
  Object.defineProperty(bad, 'query', {
    enumerable: true,
    get() {
      getter = true;
      return 'untrusted';
    },
  });
  await assert.rejects(f.service.external.query({ ...f.request(), input: bad }));
  assert.equal(getter, false);
  assert.equal(f.count(), 0);
  await assert.rejects(
    f.service.external.query({ ...f.request(), reference: { ...REF, digest: 'f'.repeat(64) } }),
    fails('external_query_not_admitted'),
  );
  await assert.rejects(f.service.external.query({ ...f.request(), actor: { ...f.actor() } }));
  assert.equal(f.count(), 0);
  assert.equal(f.sql('SELECT count(*) AS n FROM cap_invocations')[0].n, 0);
  const request = f.request('captured-query-key', 'original private query'),
    pending = f.service.external.query(request);
  await reached;
  request.input.query = 'changed private query';
  request.actor = { accountId: 'foreign', deviceId: 'foreign' };
  request.reference = { ...REF, digest: 'f'.repeat(64) };
  assert.equal(Object.isFrozen(received), true);
  assert.equal(Object.isFrozen(received.input), true);
  release();
  const result = await pending;
  assert.equal(result.result.text, 'original private query');
  assert.equal(f.count(), 1);
});

test('query-only persisted intents cannot be consumed by generic dispatch/result/recovery ports', async (t) => {
  const f = fixture(t),
    result = await f.service.external.query(f.request()),
    invocationId = result.invocation.invocationId;
  for (const name of [
    'inspectDispatch',
    'beginDispatch',
    'reconcileAuthorization',
    'markUncertain',
    'requestCancel',
  ])
    assert.throws(
      () => f.service.invocations[name]({ actor: f.actor(), invocationId }),
      fails('readonly_query_api_required'),
    );
  assert.throws(
    () =>
      f.service.invocations.recordResult({
        invocationId,
        status: 'failed',
        receipt: { verificationMethod: 'unverified', artifacts: [] },
        disposition: 'released',
      }),
    fails('readonly_query_api_required'),
  );
  assert.throws(
    () =>
      f.service.invocations.bindJob({
        invocationId,
        internalRequestId: 'query:internal',
        jobId: 'query:job',
      }),
    fails('readonly_query_api_required'),
  );
  await assert.rejects(
    f.service.external.reconcile({ reference: REF, invocationId }),
    fails('readonly_query_api_required'),
  );
  assert.deepEqual(f.sql('SELECT reserved_amount,spent_amount FROM cap_budgets')[0], {
    reserved_amount: 0,
    spent_amount: 1,
  });
  f.reopen({ withoutReadonly: true });
  assert.equal(f.service.external, null);
  assert.throws(
    () => f.service.invocations.inspectDispatch({ invocationId }),
    fails('readonly_query_api_required'),
  );
  assert.throws(
    () =>
      f.service.invocations.recordResult({
        invocationId,
        status: 'failed',
        receipt: { verificationMethod: 'unverified', artifacts: [] },
        disposition: 'released',
      }),
    fails('readonly_query_api_required'),
  );
});

test('a cancelled awaited read cannot publish data or use a retained authority closure for another read', async (t) => {
  let entered, release, current;
  const reached = new Promise((done) => (entered = done)),
    gate = new Promise((done) => (release = done));
  const f = fixture(t, {
    query: async (_request, _signal, currentAuthority) => {
      current = currentAuthority;
      entered();
      await gate;
      currentAuthority();
      return { text: 'cancelled private response' };
    },
  });
  const pending = f.service.external.query(f.request());
  await reached;
  const id = f.sql('SELECT id FROM cap_invocations')[0].id;
  assert.equal(
    f.service.external.cancel({ actor: f.actor(), invocationId: id }).invocation.status,
    'cancelled',
  );
  release();
  await assert.rejects(pending, fails('external_query_unconfirmed'));
  assert.throws(() => current(), fails('external_query_unconfirmed'));
  assert.equal(f.sql('SELECT spent_amount FROM cap_budgets')[0].spent_amount, 1);
  const replay = await f.service.external.query(f.request());
  assert.equal(replay.resultUnavailable, true);
  assert.equal(replay.charge.amount, 1);
});
