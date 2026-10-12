import test from 'node:test';
import assert from 'node:assert/strict';
import { runBoundedRuntimeTurn, createIsolatedContextStore } from '../../modules/personal-agent/runtime/vendor/agent-runtime-core.ts';

const call = (name, args = {}) => ({ choices: [{ message: { tool_calls: [{ id: 'call_' + name,
  function: { name, arguments: JSON.stringify(args) } }] } }] });
const evidence = id => ({ evidence: [{ id, title: id, observedAt: null, sourceStatus: 'current' }] });
function fixture(sequence, { discovery = ['source_catalog'], afterCatalog = () => {}, timeoutMs = 1000 } = {}) {
  const counts = { source_catalog: 0, source_write_1: 0, source_read_1: 0 }, offered = [];
  const adapter = { id: 'test', roles: ['human'], capabilities: Object.keys(counts), effectCapabilities: ['source_write_1'],
    discoveryCapabilities: discovery, applicability: 'own selected source', systemContext: () => '', isActionHrefAllowed: () => false };
  const tools = Object.fromEntries(Object.keys(counts).map(name => [name, { capability: name, description: name,
    inputSchema: { type: 'object' }, validate: input => input && Object.keys(input).length === 0,
    execute() { counts[name]++; if (name === 'source_catalog') afterCatalog(adapter); return evidence(name); } }]));
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
test('total provider attempts stay at four across transient retries; writes are not repeated', async () => {
  const transient = () => { throw Object.assign(new Error('synthetic'), { status: 503 }); };
  const f = fixture([transient, 'source_catalog', transient, 'source_write_1', 'finish_answer']);
  await assert.rejects(f.run(), /provider-round-limit/); assert.equal(f.calls(), 4); assert.equal(f.counts.source_write_1, 1);
});
