import test from 'node:test';
import assert from 'node:assert/strict';
import { runBoundedRuntimeTurn, createIsolatedContextStore } from '../../modules/personal-agent/runtime/vendor/agent-runtime-core.ts';

const call = (name, args = {}) => ({ choices: [{ message: { tool_calls: [{ id: 'call_' + name,
  function: { name, arguments: JSON.stringify(args) } }] } }] });
const evidence = id => ({ evidence: [{ id, title: id, observedAt: null, sourceStatus: 'current' }] });
function fixture(sequence, { discovery = ['source_catalog'], afterCatalog = () => {}, timeoutMs = 1000, results = {} } = {}) {
  const counts = { source_catalog: 0, source_write_1: 0, source_read_1: 0 }, offered = [];
  const adapter = { id: 'test', roles: ['human'], capabilities: Object.keys(counts), effectCapabilities: ['source_write_1'],
    discoveryCapabilities: discovery, applicability: 'own selected source', systemContext: () => '', isActionHrefAllowed: () => false };
  const tools = Object.fromEntries(Object.keys(counts).map(name => [name, { capability: name, description: name,
    inputSchema: { type: 'object' }, validate: input => input && Object.keys(input).length === 0,
    execute() { counts[name]++; if (name === 'source_catalog') afterCatalog(adapter);
      return typeof results[name] === 'function' ? results[name](counts[name]) : results[name] ?? evidence(name); } }]));
  let calls = 0;
  return { counts, offered, calls: () => calls, run: () => runBoundedRuntimeTurn({ adapter, tools, role: 'human', prompt: 'Use current source',
    contextStore: createIsolatedContextStore({ roles: ['human'] }), timeoutMs,
    async provider(messages, signal, available, options) {
      offered.push(Object.keys(available)); assert.equal(options.parallelToolCalls, false);
      const action = sequence[calls++]; if (typeof action === 'function') return action({ messages, signal, available });
      if (action === 'finish_answer') return call(action, { answer: 'Verified', selectedEvidenceIds: messages.filter(m => m.role === 'tool')
        .flatMap(m => JSON.parse(m.content).evidence.map(e => e.id)), claims: [] });
      return call(action);
    } }) };
}
test('one catalogue discovery precedes one write and preserves both evidence sets', async () => {
  const f = fixture(['source_catalog', 'source_write_1', 'finish_answer']); const result = await f.run();
  assert.equal(result.status, 'complete'); assert.deepEqual(result.evidence.map(e => e.id), ['source_catalog', 'source_write_1']);
  assert.equal(f.calls(), 3); assert.deepEqual(f.counts, { source_catalog: 1, source_write_1: 1, source_read_1: 0 });
  assert.ok(!f.offered[1].includes('source_catalog')); assert.deepEqual(f.offered[2], ['finish_answer']);
});
test('large skills reach the model without displacing the subsequent business evidence', async () => {
  const properties = Object.fromEntries(Array.from({ length: 25 }, (_, i) => ['field' + i,
    { type: 'string', description: 's'.repeat(1400) }]));
  const catalog = { evidence: [{ id: 'source_catalog', title: 'Catalog', observedAt: null, sourceStatus: 'current',
    data: { items: [{ inputSchema: { type: 'object', properties }, guidance: [{ reference: 'current_guide', summary: 'Use the project API.' }] }] } }],
    applicationGuidance: [{ authority: 'application-data', content: 'g'.repeat(15000) }] };
  const business = { evidence: [{ id: 'source_read_1', title: 'Saved result', observedAt: null, sourceStatus: 'current', data: { text: 'r'.repeat(14000) } }] };
  const f = fixture(['source_catalog', ({ messages }) => {
    assert.equal(JSON.parse(messages.at(-1).content).applicationGuidance[0].content.length, 15000);
    return call('source_read_1');
  }, 'finish_answer'], { results: { source_catalog: catalog, source_read_1: business } });
  const result = await f.run();
  assert.equal(result.status, 'complete'); assert.deepEqual(result.evidence.map(row => row.id), ['source_catalog', 'source_read_1']);
  assert.equal(Object.hasOwn(result, 'applicationGuidance'), false);
  assert.ok(Buffer.byteLength(JSON.stringify(result.evidence)) < 64000);
  assert.equal(f.counts.source_read_1, 1);
});

test('catalogue, separate preparatory reads and one write complete in one turn', async () => {
  const f = fixture(['source_catalog', 'source_read_1', 'source_read_1', 'source_write_1', 'finish_answer'],
    { results: { source_read_1: n => evidence('read_' + n) } });
  const result = await f.run();
  assert.equal(result.status, 'complete'); assert.equal(f.calls(), 5);
  assert.deepEqual(f.counts, { source_catalog: 1, source_read_1: 2, source_write_1: 1 });
  assert.deepEqual(result.evidence.map(e => e.id), ['source_catalog', 'read_1', 'read_2', 'source_write_1']);
  assert.ok(f.offered[2].includes('source_write_1')); assert.deepEqual(f.offered[4], ['finish_answer']);
});

test('read loops are bounded without executing a write or retrying it', async () => {
  const f = fixture(Array(9).fill('source_read_1'), { results: { source_read_1: n => evidence('read_' + n) } });
  await assert.rejects(f.run(), /provider-round-limit/);
  assert.equal(f.calls(), 8); assert.equal(f.counts.source_read_1, 8); assert.equal(f.counts.source_write_1, 0);
});

test('the last provider slot cannot execute an effect without a terminal slot', async () => {
  const f = fixture([...Array(7).fill('source_read_1'), 'source_write_1'],
    { results: { source_read_1: n => evidence('read_' + n) } });
  await assert.rejects(f.run(), /provider-round-limit/);
  assert.equal(f.calls(), 8); assert.equal(f.counts.source_write_1, 0);
});
test('a second write, even after discovery, is rejected before execution', async () => {
  const f = fixture(['source_catalog', 'source_write_1', 'source_write_1']);
  await assert.rejects(f.run(), /provider-tool-unknown/); assert.equal(f.counts.source_write_1, 1); assert.equal(f.calls(), 3);
});
test('catalogue loops are removed from the next offered tools and cannot execute', async () => {
  const f = fixture(['source_catalog', 'source_catalog']); await assert.rejects(f.run(), /provider-tool-unknown/);
  assert.equal(f.counts.source_catalog, 1); assert.equal(f.counts.source_write_1, 0);
});
test('current permission revoked after discovery still denies the business tool', async () => {
  const f = fixture(['source_catalog', 'source_write_1'], { afterCatalog: adapter => { adapter.capabilities = ['source_catalog']; } });
  await assert.rejects(f.run(), /tool-not-authorized/); assert.equal(f.counts.source_write_1, 0);
});
test('an effect capability cannot be declared as discovery', async () => {
  const f = fixture(['source_write_1'], { discovery: ['source_write_1'] });
  await assert.rejects(f.run(), /discovery-capability-invalid/); assert.equal(f.calls(), 0); assert.equal(f.counts.source_write_1, 0);
});
test('deadline cancellation during the post-discovery provider blocks the write', async () => {
  const f = fixture(['source_catalog', async ({ signal }) => { await new Promise(resolve => signal.addEventListener('abort', resolve, { once: true })); return call('source_write_1'); }], { timeoutMs: 100 });
  await assert.rejects(f.run(), /runtime-deadline/); assert.equal(f.counts.source_write_1, 0);
});
test('total provider attempts stay at eight across transient retries; writes are not repeated', async () => {
  const transient = () => { throw Object.assign(new Error('synthetic'), { status: 503 }); };
  const f = fixture([transient, 'source_catalog', transient, 'source_read_1', transient, 'source_write_1', transient, transient, 'finish_answer']);
  await assert.rejects(f.run(), /provider-round-limit/); assert.equal(f.calls(), 8); assert.equal(f.counts.source_write_1, 1);
});
