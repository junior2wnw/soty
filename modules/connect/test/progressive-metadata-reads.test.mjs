import test from 'node:test';
import assert from 'node:assert/strict';
import { resolve, dirname, join } from 'node:path';
import { pathToFileURL, fileURLToPath } from 'node:url';
const source = process.argv[2] ? resolve(process.argv[2]) : fileURLToPath(new URL('../browser/client.mjs', import.meta.url));
const repo = process.argv[3] ? resolve(process.argv[3]) : resolve(dirname(source), '../../..');
const { createClientWithStorage } = await import(pathToFileURL(source));
const { ConnectError, canonicalJson } = await import(new URL('./crypto.mjs', pathToFileURL(source)));
const { validateState } = await import(new URL('./storage.mjs', pathToFileURL(source)));
const { createConnectService } = await import(pathToFileURL(join(repo, 'modules/connect/server/index.mjs')));
const projectId = 'progressive-connect-test', origin = 'https://progressive.fixture.invalid', endpoint = origin + '/rpc', scope = { projectId, endpoint };
const reads = ['world.profile.get', 'world.community.list', 'world.directory.search', 'world.directory.resolve', 'world.field.get', 'apps.directory.search', 'apps.directory.resolve', 'apps.devices', 'contacts.list'];
const backgroundReads = ['call.inbox', 'notifications.status', 'apps.cover.read'];
const effects = ['world.profile.update', 'world.field.put', 'apps.launch', 'contacts.requestAccount', 'apps.directory.search.extra'];
const gate = () => { let resolve; const promise = new Promise(done => { resolve = done; }); return { promise, resolve }; };
const turn = () => new Promise(done => setTimeout(done, 5));
const outcome = promise => promise.then(value => ({ value }), error => ({ error }));
async function until(check) { const start = performance.now(); while (!check() && performance.now() - start < 800) await turn(); return check(); }
function memoryStorage() {
  let state = null, hook = null;
  const copy = () => state === null ? null : structuredClone(state);
  return {
    async read() { const value = copy(), next = hook; hook = null; if (next) await next(); return value; },
    async claim(value) { if (state === null) { validateState(value, scope); state = structuredClone(value); } return copy(); },
    async compareAndSwap(expected, value) { if (state?.localRevision !== expected) throw new ConnectError('LOCAL_CONFLICT', 'Synthetic conflict'); validateState(value, scope); state = structuredClone(value); return copy(); },
    inspect: copy, nextRead(next) { hook = next; },
  };
}
async function fixture(t) {
  const state = { traffic: [], executed: [], active: 0, peak: 0, before: null, after: null };
  const service = createConnectService({ databasePath: ':memory:', projectId, allowedOrigins: [origin], extensions: [{
    // Built-in operations must keep their genuine dispatcher and exact argument contracts.
    operations: new Set([...reads, ...backgroundReads, ...effects].filter(op => !['contacts.list', 'contacts.requestAccount'].includes(op))),
    execute({ op, args, actor }) { state.executed.push({ op, accountId: actor.accountId, args: structuredClone(args) }); return { operation: op, args, accountId: actor.accountId, deviceId: actor.deviceId }; },
  }], canRequestContact: () => true });
  const clients = [];
  const fetcher = async (_url, options) => {
    const body = JSON.parse(options.body); assert.equal(body.protocol, 1);
    state.traffic.push({ op: body.op, target: body.args?.operation, signed: Boolean(body.proof), challengeId: body.proof?.challengeId, publicJwk: body.proof?.publicJwk }); state.active++; state.peak = Math.max(state.peak, state.active);
    try {
      if (state.before) await state.before(body);
      const result = await service.handle({ ...body, origin });
      if (state.after) await state.after(body, result);
      return new Response(JSON.stringify(result), { status: result.ok ? 200 : 400 });
    } finally { state.active--; }
  };
  const client = store => { const value = createClientWithStorage({ projectId, endpoint, fetch: fetcher }, store); clients.push(value); return value; };
  t.after(() => { clients.forEach(value => value.dispose()); service.close(); });
  const store = memoryStorage(), secondStore = memoryStorage(), primary = client(store);
  const a = await primary.bootstrap('Synthetic A'), b = await client(secondStore).bootstrap('Synthetic B');
  const merged = store.inspect(); merged.installations.push(...secondStore.inspect().installations); merged.localRevision++;
  await store.compareAndSwap(merged.localRevision - 1, merged);
  const other = client(store); state.traffic.length = 0; state.executed.length = 0; state.peak = 0;
  return { primary, other, store, state, a: a.accountId, b: b.accountId, context: { expectedAccountId: a.accountId },
    signingPublicJwk: store.inspect().installations.find(value => value.accountId === a.accountId).signingPublicJwk };
}

test('signed profile, apps and local state finish while a community response is held', async t => {
  const f = await fixture(t), entered = gate(), release = gate();
  f.state.before = async body => { if (body.op === 'world.community.list' && body.proof) { entered.resolve(); await release.promise; } };
  const held = f.primary.extension('world.community.list', {}, f.context); await entered.promise;
  let profileDone = false, appsDone = false, localDone = false;
  const profile = f.primary.extension('world.profile.get', {}, f.context).then(value => { profileDone = true; return value; });
  const apps = f.primary.extension('apps.directory.search', {}, f.context).then(value => { appsDone = true; return value; });
  const local = f.primary.getLocalState().then(value => { localDone = true; return value; });
  let early; try { early = await until(() => profileDone && appsDone && localDone); } finally { release.resolve(); }
  const values = await Promise.all([held, profile, apps, local]);
  assert.equal(early, true, 'A read must release the browser state queue before awaiting its response'); assert.ok(values.every(value => value.accountId === f.a));
});

for (const operation of backgroundReads) {
  test(`field reads and local navigation do not wait for a held ${operation}`, async t => {
    const f = await fixture(t), entered = gate(), release = gate();
    f.state.before = async body => { if (body.op === operation && body.proof) { entered.resolve(); await release.promise; } };
    const held = f.primary.extension(operation, {}, f.context); await entered.promise;
    let profileDone = false, appsDone = false, localDone = false;
    const profile = f.primary.extension('world.profile.get', {}, f.context).then(value => { profileDone = true; return value; });
    const apps = f.primary.extension('apps.directory.resolve', {}, f.context).then(value => { appsDone = true; return value; });
    const local = f.primary.getLocalState().then(value => { localDone = true; return value; });
    let early; try { early = await until(() => profileDone && appsDone && localDone); } finally { release.resolve(); }
    await Promise.all([held, profile, apps, local]);
    assert.equal(early, true, 'An optional background response must not hold the field or identity queue');
  });
  for (const phase of ['challenge', 'response']) {
    test(`${operation} rejects an account round trip during its ${phase}`, async t => {
      const f = await fixture(t), entered = gate(), release = gate();
      f.state.after = async body => {
        if (phase === 'challenge' ? body.op === 'challenge' && body.args.operation === operation : body.op === operation && body.proof) {
          entered.resolve(); await release.promise;
        }
      };
      const pending = outcome(f.primary.extension(operation, {}, f.context)); await entered.promise;
      try { await f.other.switchProfile(f.b); await f.other.switchProfile(f.a); } finally { release.resolve(); }
      assert.equal((await pending).error?.code, 'ACTIVE_PROFILE_CHANGED');
      assert.equal(f.state.executed.length, phase === 'challenge' ? 0 : 1);
    });
  }
}

test('all nine exact bound reads overlap with unique real challenges and verified signatures', async t => {
  const f = await fixture(t), release = gate();
  f.state.before = async body => { if (reads.includes(body.op) && body.proof) await release.promise; };
  const pending = reads.map(op => f.primary.extension(op, op === 'contacts.list' ? {} : { marker: op }, f.context)); let early;
  try { early = await until(() => f.state.traffic.filter(row => row.signed).length === 9); } finally { release.resolve(); }
  const values = await Promise.all(pending), signed = f.state.traffic.filter(row => row.signed);
  assert.equal(early, true); assert.equal(signed.length, 9); assert.equal(new Set(signed.map(row => row.challengeId)).size, 9); assert.equal(f.state.executed.length, 8);
  assert.ok(f.state.executed.every(row => row.accountId === f.a)); assert.ok(values.slice(0, -1).every(value => value.accountId === f.a));
  assert.ok(Array.isArray(values.at(-1).contacts), 'The ninth read is the real contacts.list handler');
  assert.ok(signed.every(row => canonicalJson(row.publicJwk) === canonicalJson(f.signingPublicJwk)), 'Every real verified proof uses the captured A installation');
  assert.ok(f.state.peak > 1);
});

test('A to B to A during a challenge rejects the captured read before signing', async t => {
  const f = await fixture(t), entered = gate(), release = gate();
  f.state.after = async body => { if (body.op === 'challenge' && body.args.operation === 'apps.directory.search') { entered.resolve(); await release.promise; } };
  const pending = outcome(f.primary.extension('apps.directory.search', {}, f.context)); await entered.promise;
  await f.other.switchProfile(f.b); await f.other.switchProfile(f.a); release.resolve(); const result = await pending;
  assert.equal(result.error?.code, 'ACTIVE_PROFILE_CHANGED'); assert.equal(f.state.traffic.filter(row => row.signed).length, 0); assert.equal(f.state.executed.length, 0);
});

test('A to B to A after a signed read rejects its stale reply', async t => {
  const f = await fixture(t), entered = gate(), release = gate();
  f.state.after = async body => { if (body.op === 'apps.directory.search' && body.proof) { entered.resolve(); await release.promise; } };
  const pending = outcome(f.primary.extension('apps.directory.search', {}, f.context)); await entered.promise;
  await f.other.switchProfile(f.b); await f.other.switchProfile(f.a); release.resolve(); const result = await pending;
  assert.equal(result.error?.code, 'ACTIVE_PROFILE_CHANGED'); assert.equal(f.state.executed.length, 1); assert.equal(f.state.executed[0].accountId, f.a);
});

test('mismatched expected account and malformed contexts dispatch no challenge', async t => {
  const f = await fixture(t); await f.other.switchProfile(f.b);
  await assert.rejects(f.primary.extension('apps.directory.search', {}, f.context), { code: 'ACTIVE_PROFILE_CHANGED' });
  for (const context of [null, [], {}, { expectedAccountId: '' }, { expectedAccountId: f.a, extra: true }]) await assert.rejects(f.primary.extension('apps.directory.search', {}, context), { code: 'INVALID_ARGUMENT' });
  assert.equal(f.state.traffic.length, 0);
});

test('payload and expected-account strings are captured before queueing', async t => {
  const f = await fixture(t), entered = gate(), release = gate(); f.store.nextRead(async () => { entered.resolve(); await release.promise; });
  const held = f.primary.getLocalState(); await entered.promise;
  const args = { query: 'captured' }, context = { expectedAccountId: f.a }, pending = f.primary.extension('apps.directory.search', args, context);
  args.query = 'mutated'; context.expectedAccountId = f.b; release.resolve(); await held;
  const result = await pending; assert.equal(result.accountId, f.a); assert.deepEqual(result.args, { query: 'captured' }); assert.equal(Object.hasOwn(f.state.executed[0].args, 'expectedAccountId'), false);
});

test('mutations retain signed queue order and later metadata admission waits for an active write', async t => {
  const f = await fixture(t), entered = gate(), release = gate();
  f.state.before = async body => { if (body.op === 'world.profile.update' && body.proof) { entered.resolve(); await release.promise; } };
  const first = f.primary.extension('world.profile.update', {}, f.context); await entered.promise;
  const second = f.primary.extension('world.field.put', {}, f.context), read = f.primary.extension('apps.directory.search', {}, f.context); let early;
  try { await turn(); await turn(); early = f.state.traffic.filter(row => row.signed).map(row => row.op); } finally { release.resolve(); }
  await Promise.all([first, second, read]); assert.deepEqual(early, ['world.profile.update']); assert.deepEqual(f.state.executed.map(row => row.op), ['world.profile.update', 'world.field.put', 'apps.directory.search']);
});

test('effect operations and lookalike names do not enter the exact read allowlist', async t => {
  const f = await fixture(t);
  for (const op of ['world.field.put', 'apps.launch', 'contacts.requestAccount', 'apps.directory.search.extra']) {
    const entered = gate(), release = gate();
    f.state.before = async body => { if (body.op === op && body.proof) { entered.resolve(); await release.promise; } };
    const before = f.state.traffic.length, held = f.primary.extension(op, op === 'contacts.requestAccount' ? { accountId: f.b } : {}, f.context); await entered.promise;
    const next = f.primary.extension('world.profile.update', {}, f.context); let early;
    try { await turn(); await turn(); early = f.state.traffic.slice(before).filter(row => row.signed).map(row => row.op); } finally { release.resolve(); }
    await Promise.all([held, next]); assert.deepEqual(early, [op]);
  }
});

test('legacy two-argument reads keep their serialized behavior', async t => {
  const f = await fixture(t), entered = gate(), release = gate();
  f.state.before = async body => { if (body.op === 'world.community.list' && body.proof) { entered.resolve(); await release.promise; } };
  const held = f.primary.extension('world.community.list'); await entered.promise;
  let complete = false; const next = f.primary.extension('apps.directory.search').then(value => { complete = true; return value; }); let early;
  try { await turn(); await turn(); early = complete; } finally { release.resolve(); }
  await Promise.all([held, next]); assert.equal(early, false);
});

test('disposal during a held challenge prevents a later read signature', async t => {
  const f = await fixture(t), entered = gate(), release = gate();
  f.state.after = async body => { if (body.op === 'challenge' && body.args.operation === 'apps.directory.search') { entered.resolve(); await release.promise; } };
  const pending = outcome(f.primary.extension('apps.directory.search', {}, f.context)); await entered.promise; f.primary.dispose(); release.resolve(); const result = await pending;
  assert.equal(result.error?.code, 'CLIENT_DISPOSED'); assert.equal(f.state.executed.length, 0); assert.equal(f.state.traffic.filter(row => row.signed).length, 0);
});

for (const phase of ['before challenge', 'before signature', 'after response']) {
  test('disposal while the current-state transaction is pending ' + phase + ' rejects the read', async t => {
    const f = await fixture(t), entered = gate(), release = gate();
    const hold = async () => { entered.resolve(); await release.promise; };
    if (phase === 'before challenge') {
      // In the new path, capture revision, installation, then the first RPC fence
      // are three genuine storage snapshots. The legacy path also reaches its
      // third read, at its pre-signature fence, so the negative control settles.
      let count = 0;
      const next = async () => { if (++count === 3) await hold(); else f.store.nextRead(next); };
      f.store.nextRead(next);
    } else {
      f.state.after = body => {
        if (phase === 'before signature' && body.op === 'challenge' && body.args.operation === 'apps.directory.search'
          || phase === 'after response' && body.op === 'apps.directory.search' && body.proof) f.store.nextRead(hold);
      };
    }
    const pending = outcome(f.primary.extension('apps.directory.search', {}, f.context));
    await entered.promise; f.primary.dispose(); release.resolve(); const result = await pending;
    assert.equal(result.error?.code, 'CLIENT_DISPOSED', 'A snapshot taken before dispose cannot authorize continuation after it');
    assert.equal(f.state.traffic.filter(row => row.op === 'challenge').length, phase === 'before challenge' ? 0 : 1);
    assert.equal(f.state.traffic.filter(row => row.signed).length, phase === 'after response' ? 1 : 0);
    assert.equal(f.state.executed.length, phase === 'after response' ? 1 : 0);
  });
}
