import test from 'node:test';
import assert from 'node:assert/strict';
import { actualSourceAvailable, readonlyAppHttpFixture } from './support/readonly-app-http.mjs';
const options = {
  skip: actualSourceAvailable ? false : 'Actual selected-workspace Planner source not installed.',
};
const args = (f, key = 'actual-query-one') => ({
  reference: f.reference,
  idempotencyKey: key,
  input: { query: 'Synthetic', limit: 10 },
});
test(
  'actual signed Root/App catalog exposes a Source readonly query; HTTP/MCP charge and metadata replay preserve domain rows',
  options,
  async (t) => {
    const f = await readonlyAppHttpFixture(t),
      before = f.domain();
    const catalog = await f.http('catalog', {});
    assert.equal(catalog.status, 200);
    assert.equal(catalog.value.items[0].reference.digest, f.reference.digest);
    assert.deepEqual(catalog.value.items[0].effects, []);
    const guide = await f.http('guidance-list', { reference: f.reference });
    assert.equal(guide.status, 200);
    assert.equal(guide.value.items.length, 1);
    const read = await f.http('query', args(f));
    assert.equal(read.status, 200);
    assert.equal(read.value.result.items.length, 1);
    assert.equal(read.value.result.items[0].title, 'Synthetic private object');
    assert.equal(read.value.charge.amount, 1);
    const replay = await f.mcp('apps_query', args(f));
    assert.equal(replay.status, 200);
    const data = replay.value.result.structuredContent;
    assert.equal(data.reused, true);
    assert.equal(data.resultUnavailable, true);
    assert.equal(Object.hasOwn(data, 'result'), false);
    assert.equal(f.readCalls(), 1);
    const fresh = await f.mcp('apps_query', args(f, 'actual-query-two'));
    assert.equal(fresh.status, 200);
    assert.equal(fresh.value.result.structuredContent.result.items.length, 1);
    assert.equal(f.readCalls(), 2);
    assert.deepEqual(
      f.domain(),
      before,
      'Source objects/revision/agent_requests unchanged; key last_used_at is operational metadata',
    );
    const missing = await f.http('query', args(f, 'no-bearer'), null);
    assert.equal(missing.status, 401);
    const wrongAudience = await f.http('query', args(f, 'wrong-audience'), f.mcpToken);
    assert.notEqual(wrongAudience.status, 200);
    assert.equal((await f.http('invoke', args(f, 'wrong-route'))).status, 400);
  },
);
test(
  'actual Source read key revoke before read denies data without a domain mutation',
  options,
  async (t) => {
    const f = await readonlyAppHttpFixture(t),
      before = f.domain();
    f.revokeSource();
    const denied = await f.http('query', args(f));
    assert.notEqual(denied.status, 200);
    assert.equal(JSON.stringify(denied.value).includes('Synthetic private object'), false);
    assert.equal(f.readCalls(), 0);
    assert.deepEqual(f.domain(), before);
  },
);
test(
  'actual current Root grant and App revoke during awaited Source call suppress private HTTP/MCP bytes',
  options,
  async (t) => {
    for (const kind of ['grant', 'app']) {
      const f = await readonlyAppHttpFixture(t),
        reached = f.hold(),
        before = f.domain();
      const pending = kind === 'grant' ? f.http('query', args(f)) : f.mcp('apps_query', args(f));
      await reached;
      if (kind === 'grant') await f.revokeGrant();
      else await f.revokeApp();
      f.release();
      const denied = await pending;
      if (kind === 'grant') assert.notEqual(denied.status, 200);
      else assert.equal(denied.value.result?.isError === true || Boolean(denied.value.error), true);
      assert.equal(JSON.stringify(denied.value).includes('Synthetic private object'), false);
      assert.deepEqual(f.domain(), before);
    }
  },
);
test(
  'a real writer Source key cannot be relabeled readonly by the host catalog',
  options,
  async (t) => {
    for (const lieReadOnlyLease of [false, true]) {
      const f = await readonlyAppHttpFixture(t, { writerInstead: true, lieReadOnlyLease }),
        before = f.domain();
      const denied = await f.http('query', args(f));
      assert.equal(denied.status, 403);
      assert.equal(f.readCalls(), 0);
      assert.equal(
        f.profileCalls(),
        lieReadOnlyLease ? 1 : 0,
        'actual Source claim independently rejects a mislabeled host lease',
      );
      assert.deepEqual(f.domain(), before);
    }
  },
);
