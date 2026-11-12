import test from 'node:test';
import assert from 'node:assert/strict';
import { captureRuntimeExecutionPolicy as capture, findRuntimeExecutionCapabilities as find } from '../server/runtime-execution-policy.mjs';

const appId = 'app-' + 'a'.repeat(32), profile = 'soty.relay-restricted.v1';
const scope = () => ({ appId, policyEpoch: 2, target: { revision: 3, digest: 'b'.repeat(64), profile } });
const admission = () => ({ ...scope(), capabilities: { wasm: true, workers: 'self',
  connectSrc: ['https://tiles.publicsite.net', 'https://data.publicsite.org/public/'], imgSrc: ['https://images.publicsite.com'] } });
const policy = (...admissions) => ({ schema: 'soty.app-execution-policy.v1', admissions });
const invalid = { code: 'app_execution_policy_invalid', status: 503 };

test('absent and empty configuration deny every ordinary target', () => {
  for (const registry of [capture(), capture(policy())]) {
    assert.equal(find(registry, scope()), null); assert(Object.isFrozen(registry)); assert(Object.isFrozen(registry.admissions));
  }
});
test('the full current tuple selects only its captured capabilities', () => {
  const registry = capture(policy(admission())), expected = registry.admissions[0].capabilities;
  assert.equal(find(registry, scope()), expected);
  for (const mutate of [s => { s.appId = 'app-' + 'c'.repeat(32); }, s => { s.policyEpoch++; },
    s => { s.target.revision++; }, s => { s.target.digest = 'c'.repeat(64); }, s => { s.target.profile = 'soty.relay-self-ip.v1'; }]) {
    const changed = scope(); mutate(changed); assert.equal(find(registry, changed), null);
  }
});
test('input aliases cannot mutate the captured registry or returned capability arrays', () => {
  const input = admission(), registry = capture(policy(input)), result = find(registry, scope());
  input.target.digest = 'c'.repeat(64); input.capabilities.wasm = false; input.capabilities.connectSrc.push('https://changed.publicsite.net');
  assert.equal(find(registry, scope()), result); assert.equal(result.wasm, true); assert.equal(result.connectSrc.length, 2);
  assert.throws(() => result.connectSrc.push('https://changed.publicsite.net'), TypeError);
  assert.throws(() => { registry.admissions[0].target.revision = 100; }, TypeError);
  assert.throws(() => find(JSON.parse(JSON.stringify(registry)), scope()), invalid);
});
test('known profiles are captured without inventing a new transport profile', () => {
  for (const known of [profile, 'soty.relay-self-ip.v1', 'soty.selected-human-embed.v1', 'soty.selected-human-embed.v2']) {
    const input = admission(); input.target.profile = known; const registry = capture(policy(input));
    assert.equal(find(registry, { ...scope(), target: input.target }).wasm, true);
  }
  const input = admission(); input.target.profile = 'soty.browser-unrestricted.v1'; assert.throws(() => capture(policy(input)), invalid);
});
test('duplicate full tuples fail; independently approved epochs remain distinct', () => {
  assert.throws(() => capture(policy(admission(), admission())), invalid);
  const other = admission(); other.policyEpoch++; other.capabilities.wasm = false;
  const registry = capture(policy(admission(), other));
  assert.equal(find(registry, scope()).wasm, true); assert.equal(find(registry, { ...scope(), policyEpoch: 3 }).wasm, false);
});
test('provider-agnostic HTTPS origins and explicit path prefixes are admitted', () => {
  for (const value of ['https://maps.publicsite.net', 'https://tiles.publicsite.org/', 'https://maps.publicsite.net:8443/',
    'https://s3.amazonaws.com/elevation-tiles-prod/', 'https://tiles.openfreemap.org', 'https://tiles.maps.eox.at',
    'https://xn--e1afmkfd.xn--p1ai/maps/']) {
    const input = admission(); input.capabilities.connectSrc = [value];
    assert.deepEqual(find(capture(policy(input)), scope()).connectSrc, [value]);
  }
});
test('CSP injection, normalization, local/IP, wildcard and ambiguous sources are refused', () => {
  const attacks = ['http://maps.publicsite.net', 'https:', 'data:text/plain,x', 'blob:https://maps.publicsite.net/x',
    'https://user:secret@maps.publicsite.net', 'https://*.publicsite.net', 'https://maps.publicsite.net; script-src *',
    "https://maps.publicsite.net/'unsafe-eval'/", 'https://maps.publicsite.net/"/', 'https://maps.publicsite.net/a*/',
    'https://maps.publicsite.net/?x=1', 'https://maps.publicsite.net/#x', 'https://maps.publicsite.net/asset.wasm',
    'https://maps.publicsite.net/a/../', 'https://maps.publicsite.net/./', 'https://maps.publicsite.net/%2e%2e/',
    'https://maps.publicsite.net/.%2e/', 'https://maps.publicsite.net/a%2f..%2fb/', 'https://maps.publicsite.net/%252e%252e/',
    'https://maps.publicsite.net/%0a/', 'https://maps.publicsite.net/%3b/', 'https://maps.publicsite.net/%22/',
    'https://maps.publicsite.net/%FF/', 'https://maps.publicsite.net/a\\b/', 'https://maps.publicsite.net/\n/',
    'https://MAPS.publicsite.net', 'HTTPS://maps.publicsite.net', 'https://maps.publicsite.net:443', 'https://maps.publicsite.net.',
    'https://localhost', 'https://a.localhost', 'https://a.local', 'https://a.internal', 'https://a.test', 'https://a.invalid',
    'https://127.0.0.1', 'https://8.8.8.8', 'https://2130706433', 'https://0x7f000001', 'https://[::1]', 'https://[::ffff:127.0.0.1]',
    'https://singlelabel', 'https://maps.publicsite.net:0', 'https://пример.рф/'];
  for (const value of attacks) {
    const input = admission(); input.capabilities.connectSrc = [value]; assert.throws(() => capture(policy(input)), invalid, value);
  }
});
for (const [label, value] of [['raw', 'https://maps.publicsite.net/path,segment/'],
  ['encoded', 'https://maps.publicsite.net/path%2csegment/']]) {
  test(`CSP policy-list separators are refused: ${label} comma`, () => {
    const input = admission(); input.capabilities.connectSrc = [value];
    assert.throws(() => capture(policy(input)), invalid);
  });
}

test('closed records reject unknown fields, getters, sparse arrays and malformed primitive fields', () => {
  let invoked = false;
  const getter = admission(); Object.defineProperty(getter, 'policyEpoch', { enumerable: true, get() { invoked = true; return 2; } });
  const sparse = admission(); sparse.capabilities.connectSrc = new Array(1);
  for (const input of [null, {}, { ...policy(), other: true }, policy({ ...admission(), other: true }), policy(getter), policy(sparse),
    policy({ ...admission(), appId: 'other' }), policy({ ...admission(), policyEpoch: 0 }), policy({ ...admission(), policyEpoch: 1.5 }),
    policy({ ...admission(), target: { ...scope().target, digest: 'B'.repeat(64) } }),
    policy({ ...admission(), target: { ...scope().target, revision: Number.MAX_SAFE_INTEGER + 1 } }),
    policy({ ...admission(), target: { ...scope().target, other: true } }),
    policy({ ...admission(), capabilities: { ...admission().capabilities, wasm: 'true' } }),
    policy({ ...admission(), capabilities: { ...admission().capabilities, workers: 'blob:' } }),
    policy({ ...admission(), capabilities: { ...admission().capabilities, scriptSrc: ["'unsafe-eval'"] } })]) {
    assert.throws(() => capture(input), invalid);
  }
  assert.equal(invoked, false);
});
test('source and admission limits reject overflow rather than truncate', () => {
  const input = admission(); input.capabilities.connectSrc = Array.from({ length: 8 }, (_, i) => `https://s${i}.publicsite.net`);
  assert.equal(find(capture(policy(input)), scope()).connectSrc.length, 8);
  input.capabilities.connectSrc.push('https://overflow.publicsite.net'); assert.throws(() => capture(policy(input)), invalid);
  input.capabilities.connectSrc = ['https://maps.publicsite.net/', 'https://maps.publicsite.net/']; assert.throws(() => capture(policy(input)), invalid);
  const prefix = 'https://maps.publicsite.net/', maximum = prefix + 'a'.repeat(2048 - prefix.length - 1) + '/';
  input.capabilities.connectSrc = [maximum]; assert.equal(find(capture(policy(input)), scope()).connectSrc[0].length, 2048);
  input.capabilities.connectSrc = ['https://maps.publicsite.net/' + 'a'.repeat(2048) + '/']; assert.throws(() => capture(policy(input)), invalid);
  const entries = Array.from({ length: 128 }, (_, index) => ({ ...admission(), appId: 'app-' + index.toString(16).padStart(32, '0') }));
  assert.equal(capture(policy(...entries)).admissions.length, 128);
  entries.push({ ...admission(), appId: 'app-' + 'f'.repeat(32) }); assert.throws(() => capture(policy(...entries)), invalid);
});
test('malformed lookup binding is default-deny and cannot expand the captured match', () => {
  const registry = capture(policy(admission()));
  for (const binding of [null, {}, { ...scope(), extra: true }, { ...scope(), target: null }, { ...scope(), policyEpoch: '2' }]) {
    assert.equal(find(registry, binding), null);
  }
});
test('the mismatch oracle rejects an intentionally broken app-only selector', () => {
  const registry = capture(policy(admission())), changed = { ...scope(), policyEpoch: 100 };
  const oracle = lookup => assert.equal(lookup(registry, changed), null);
  oracle(find);
  assert.throws(() => oracle((value, binding) => value.admissions.find(entry => entry.appId === binding.appId)?.capabilities), assert.AssertionError);
});
