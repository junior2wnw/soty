import test from 'node:test';
import assert from 'node:assert/strict';
import { fork } from 'node:child_process';
import { mkdtempSync, realpathSync, rmSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { basename, dirname, join } from 'node:path';
import { DatabaseSync } from 'node:sqlite';
import { createCapabilitiesService } from '../server/index.mjs';
import { createCatalog } from '../server/catalog.mjs';
import { createTrustedReadonlyQueryAdapter } from '../server/readonly-queries.mjs';

const OWNER = Object.freeze({ accountId: 'process_owner', deviceId: 'process_device' });
const CAP = {
  capabilityId: 'app.process:read',
  version: 1,
  appId: 'process',
  title: 'Synthetic ledger-only process gate',
  description: 'Actual SQLite/ACL, no Source claim.',
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
    properties: { count: { type: 'integer', minimum: 0 } },
    required: ['count'],
    additionalProperties: false,
  },
  resources: ['process:workspace'],
  effects: [],
  recipients: ['process:workspace'],
  executionBinding: {
    kind: 'registered',
    handler: 'app.process:read',
    version: 1,
    binding: { id: 'process:binding', version: 1, digest: 'a'.repeat(64) },
  },
};
const REF = {
  capabilityId: CAP.capabilityId,
  version: 1,
  digest: createCatalog([CAP]).get(CAP.capabilityId, 1).digest,
};
const AUDIENCE = 'https://process-fixture.test';

async function worker(t, plan) {
  const child = fork(new URL('./support/readonly-ledger-worker.mjs', import.meta.url), [], {
    execPath: process.execPath,
    execArgv: [],
    windowsHide: true,
    stdio: ['ignore', 'ignore', 'ignore', 'ipc'],
  });
  let sequence = 0,
    ended = false;
  const waiting = new Map();
  const exited = new Promise((resolve) =>
    child.once('exit', (code) => {
      ended = true;
      for (const request of waiting.values()) {
        clearTimeout(request.timer);
        request.reject(new Error('fixture_child_closed'));
      }
      waiting.clear();
      resolve(code);
    }),
  );
  child.on('message', (value) => {
    const request = waiting.get(value.id);
    if (!request) return;
    clearTimeout(request.timer);
    waiting.delete(value.id);
    if (value.error) request.reject(new Error('fixture_' + value.error));
    else request.resolve(value.result);
  });
  const command = (command, args = {}) =>
    new Promise((resolve, reject) => {
      if (ended) return reject(new Error('fixture_child_closed'));
      const id = ++sequence,
        timer = setTimeout(() => {
          waiting.delete(id);
          child.kill();
          reject(new Error('fixture_child_timeout'));
        }, 8000);
      waiting.set(id, { resolve, reject, timer });
      child.send({ id, command, args });
    });
  t.after(async () => {
    if (!ended) {
      try {
        await command('close');
      } catch {
        child.kill();
      }
    }
    await exited;
  });
  const ready = await command('initialize', plan);
  return { command, exited, pid: ready.pid };
}

test('two OS processes make pending cancellation and charged dispatch mutually exclusive; crash cannot cause an automatic reread', async (t) => {
  const parent = realpathSync(tmpdir()),
    directory = realpathSync(mkdtempSync(join(parent, 'soty-query-process-'))),
    file = join(directory, 'capabilities.sqlite');
  let sourceCalls = 0;
  const adapter = createTrustedReadonlyQueryAdapter({
    withAuthority(_request, callback) {
      return callback();
    },
    query() {
      sourceCalls++;
      return { count: 1 };
    },
  });
  const service = createCapabilitiesService({
    databasePath: file,
    projectId: 'readonly-process',
    actorActive: (value) =>
      value.accountId === OWNER.accountId && value.deviceId === OWNER.deviceId,
    catalog: [CAP],
    documentation: [],
    readonlyQueries: [{ contract: REF, adapter }],
  });
  const call = (op, args) =>
    service.execute({
      op: 'access.' + op,
      actor: OWNER,
      args: { expectedAccountId: OWNER.accountId, ...args },
    });
  const principal = call('principals.create', { label: 'Synthetic process reader' }).principal;
  const grant = call('grants.issue', {
    principalId: principal.id,
    capabilities: [{ capabilityId: CAP.capabilityId, version: 1 }],
    resources: CAP.resources,
    effects: [],
    recipients: CAP.recipients,
    expiresAt: Date.now() + 600000,
    budget: { unit: 'invocations', limit: 10 },
  }).grant;
  const credential = call('credentials.issue', { grantId: grant.id, audience: AUDIENCE });
  const plan = {
    file,
    projectId: service.projectId,
    catalog: CAP,
    reference: REF,
    owner: OWNER,
    token: credential.token,
    audience: AUDIENCE,
  };
  const a = await worker(t, plan),
    b = await worker(t, plan);
  assert.notEqual(a.pid, b.pid);
  t.after(() => {
    service.close();
    assert.equal(dirname(realpathSync(directory)), parent);
    assert.match(basename(directory), /^soty-query-process-/u);
    rmSync(directory, { recursive: true, force: true, maxRetries: 5, retryDelay: 100 });
  });
  const actor = service.authenticateCredential({ token: credential.token, audience: AUDIENCE });
  const inspect = (invocationId) => {
    const db = new DatabaseSync(file, { readOnly: true });
    try {
      return db
        .prepare(
          'SELECT i.status,d.state,b.disposition,b.actual_amount FROM cap_invocations i ' +
            'JOIN cap_dispatch_intents d ON d.invocation_id=i.id JOIN cap_budget_reservations b ON b.id=i.reservation_id WHERE i.id=?',
        )
        .get(invocationId);
    } finally {
      db.close();
    }
  };
  for (let i = 0; i < 6; i++) {
    const idempotencyKey = 'process-race-' + i,
      input = { query: 'private fixture query ' + i };
    const accepted = await a.command('admit', { reference: REF, idempotencyKey, input });
    const claimArgs = { invocationId: accepted.invocationId, input },
      cancelArgs = { invocationId: accepted.invocationId };
    let claim, cancellation;
    if (i === 0) {
      cancellation = await b.command('cancel', cancelArgs);
      claim = await a.command('claim', claimArgs);
    } else if (i === 1) {
      claim = await a.command('claim', claimArgs);
      cancellation = await b.command('cancel', cancelArgs);
    } else
      [claim, cancellation] = await Promise.all([
        a.command('claim', claimArgs),
        b.command('cancel', cancelArgs),
      ]);
    assert.equal(cancellation.status, 'cancelled');
    const row = inspect(accepted.invocationId);
    assert.equal(row.status, 'cancelled');
    assert.equal(row.disposition, claim.claimed ? 'spent' : 'released');
    assert.equal(row.actual_amount, claim.claimed ? 1 : 0);
    assert.equal(row.state, claim.claimed ? 'dispatching' : 'cancelled');
    const replay = await service.external.query({ actor, reference: REF, idempotencyKey, input });
    assert.equal(replay.resultUnavailable, true);
    assert.equal(replay.charge.amount, claim.claimed ? 1 : 0);
  }
  const idempotencyKey = 'process-crash-before-source',
    input = { query: 'private crash query' };
  const accepted = await a.command('admit', { reference: REF, idempotencyKey, input });
  await assert.rejects(
    b.command('claim-crash', { invocationId: accepted.invocationId, input }),
    /fixture_child_closed/u,
  );
  assert.equal(await b.exited, 17);
  assert.equal(inspect(accepted.invocationId).disposition, 'spent');
  const replay = await service.external.query({ actor, reference: REF, idempotencyKey, input });
  assert.equal(replay.reused, true);
  assert.equal(replay.charge.amount, 1);
  assert.equal(replay.resultUnavailable, true);
  assert.equal(sourceCalls, 0, 'old keys never reach even the synthetic Source closure');
  const fresh = await service.external.query({
    actor,
    reference: REF,
    idempotencyKey: 'process-fresh-read',
    input,
  });
  assert.equal(fresh.result.count, 1);
  assert.equal(sourceCalls, 1);
});
