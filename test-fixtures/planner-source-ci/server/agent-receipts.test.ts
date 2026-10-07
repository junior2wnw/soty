import test from 'node:test';
import assert from 'node:assert/strict';
import { mkdtemp, rm } from 'node:fs/promises';
import { tmpdir } from 'node:os';
import { basename, dirname, join, resolve } from 'node:path';
import { randomUUID } from 'node:crypto';
import { createPlannerServer } from './main.ts';
import { proofHash, WORK_ITEM_PROOF_PROTOCOL } from './agent-receipts.ts';

async function fixture(t: test.TestContext) {
  const folder = await mkdtemp(join(tmpdir(), 'planner-recovery-'));
  const dbPath = join(folder, 'sandbox.sqlite');
  let app: Awaited<ReturnType<typeof createPlannerServer>> | undefined;
  let origin = '';
  t.after(async () => {
    if (app) {
      app.server.closeAllConnections();
      await app.close();
    }
    assert.equal(dirname(resolve(folder)), resolve(tmpdir()));
    assert.match(basename(folder), /^planner-recovery-/);
    await rm(folder, { recursive: true, force: true, maxRetries: 5, retryDelay: 100 });
  });
  const start = async () => {
    app = await createPlannerServer({ dbPath, port: 0, host: '127.0.0.1', scheduler: false });
    origin = `http://127.0.0.1:${await app.listen()}`;
  };
  await start();
  const http = async (
    path: string,
    body?: unknown,
    token?: string,
    method = body === undefined ? 'GET' : 'POST',
  ) => {
    const response = await fetch(origin + path, {
      method,
      redirect: 'error',
      headers: {
        origin,
        ...(body === undefined ? {} : { 'content-type': 'application/json' }),
        ...(token ? { authorization: 'Bearer ' + token } : {}),
      },
      ...(body === undefined ? {} : { body: JSON.stringify(body) }),
    });
    return { status: response.status, data: (await response.json()) as any };
  };
  const workspace = await http('/api/agent/call', {
    name: 'planner_apply',
    arguments: {
      requestId: randomUUID(),
      operations: [
        {
          op: 'create',
          collection: 'workspaces',
          key: 'workspace',
          data: { name: 'Только тест восстановления', timezone: 'UTC' },
        },
      ],
    },
  });
  assert.equal(workspace.status, 200);
  const workspaceId = workspace.data.refs.workspace;
  const key = await http('/api/agent/keys', {
    name: 'Synthetic proof writer',
    workspaceIds: [workspaceId],
    readOnly: false,
    expiresInDays: 1,
  });
  assert.equal(key.status, 201);
  const requestId = randomUUID(),
    input = { title: 'Синтетический объект без даты' },
    args = {
      requestId,
      operations: [
        {
          op: 'create',
          collection: 'objects',
          workspace: workspaceId,
          key: 'workItem',
          data: input,
        },
      ],
    };
  const lookup = {
    protocol: WORK_ITEM_PROOF_PROTOCOL,
    requestId,
    workspaceId,
    inputDigest: proofHash(input),
    providerInputDigest: proofHash(args),
    scopeDigest: proofHash({ workspaceIds: [workspaceId], readOnly: false, keyId: key.data.id }),
  };
  return {
    http,
    key: key.data,
    workspaceId,
    args,
    lookup,
    get app() {
      return app!;
    },
    async restart() {
      app!.server.closeAllConnections();
      await app!.close();
      app = undefined;
      await start();
    },
  };
}

test('actual HTTP receipt is read-only, atomic with one undated object, persistent, and bound to exact input/scope', async (t) => {
  const f = await fixture(t);
  const absent = await f.http('/api/agent/receipts/lookup', f.lookup, f.key.token);
  assert.equal(absent.status, 200);
  assert.equal(absent.data.outcome, 'not_applied');
  const applied = await f.http(
    '/api/agent/call',
    { name: 'planner_apply', arguments: f.args },
    f.key.token,
  );
  assert.equal(applied.status, 200);
  const before = f.app.store.read().revision;
  await f.restart();
  const found = await f.http('/api/agent/receipts/lookup', f.lookup, f.key.token);
  assert.equal(found.status, 200);
  assert.equal(found.data.outcome, 'committed');
  assert.equal(found.data.objectId, applied.data.refs.workItem);
  assert.equal(found.data.objectRevision, 1);
  assert.equal(found.data.receiptDigest, proofHash(applied.data));
  assert.equal(f.app.store.read().revision, before);
  assert.equal(f.app.store.read().entities.filter((e) => e.id === found.data.objectId).length, 1);
  for (const forbidden of [f.key.token, f.args.operations[0].data.title, 'operations', 'token'])
    assert.equal(JSON.stringify(found.data).includes(forbidden), false);
  for (const field of ['inputDigest', 'providerInputDigest', 'scopeDigest']) {
    const mismatch = await f.http(
      '/api/agent/receipts/lookup',
      { ...f.lookup, [field]: '0'.repeat(64) },
      f.key.token,
    );
    assert.equal(mismatch.status, 409);
    assert.equal(mismatch.data.code, 'idempotency_mismatch');
  }
});

test('current scoped read key can read historical proof; revoked keys and local fallback cannot', async (t) => {
  const f = await fixture(t);
  assert.equal(
    (await f.http('/api/agent/call', { name: 'planner_apply', arguments: f.args }, f.key.token))
      .status,
    200,
  );
  const reader = await f.http('/api/agent/keys', {
    name: 'Synthetic proof reader',
    workspaceIds: [f.workspaceId],
    readOnly: true,
    expiresInDays: 1,
  });
  assert.equal(reader.status, 201);
  assert.equal(
    (await f.http('/api/agent/receipts/lookup', f.lookup, reader.data.token)).data.outcome,
    'committed',
  );
  assert.equal((await f.http('/api/agent/receipts/lookup', f.lookup)).status, 401);
  assert.equal(
    (await f.http('/api/agent/keys/' + f.key.id, undefined, undefined, 'DELETE')).status,
    200,
  );
  const revoked = await f.http('/api/agent/receipts/lookup', f.lookup, f.key.token);
  assert.equal(revoked.status, 401);
  assert.equal(revoked.data.outcome, undefined);
  const readonlyWrite = await f.http(
    '/api/agent/call',
    { name: 'planner_apply', arguments: f.args },
    reader.data.token,
  );
  assert.equal(readonlyWrite.status, 403);
  assert.equal(readonlyWrite.data.code, 'read_only');
});

test('wrong workspace/current membership denial and legacy receipts never assert absence', async (t) => {
  const f = await fixture(t);
  assert.equal(
    (
      await f.http(
        '/api/agent/receipts/lookup',
        { ...f.lookup, workspaceId: 'foreign-space' },
        f.key.token,
      )
    ).status,
    403,
  );
  const other = {
    ...f.args,
    requestId: randomUUID(),
    operations: [{ ...f.args.operations[0], key: 'legacy' }],
  };
  assert.equal(
    (await f.http('/api/agent/call', { name: 'planner_apply', arguments: other }, f.key.token))
      .status,
    200,
  );
  const unknown = await f.http(
    '/api/agent/receipts/lookup',
    { ...f.lookup, requestId: other.requestId },
    f.key.token,
  );
  assert.equal(unknown.status, 200);
  assert.equal(unknown.data.outcome, 'unknown');
  // Synthetic state removal exercises the live ACL fence, not a user record.
  f.app.store.transaction((state) => {
    state.memberships = state.memberships.filter((m) => m.workspaceId !== f.workspaceId);
  });
  const denied = await f.http('/api/agent/receipts/lookup', f.lookup, f.key.token);
  assert.equal(denied.status, 403);
  assert.equal(denied.data.outcome, undefined);
});
