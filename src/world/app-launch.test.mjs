import test from 'node:test';
import assert from 'node:assert/strict';
import { createAppLauncher, formatAppLaunchRoute, normalizeAppLaunchTarget, parseAppLaunchRoute, validateAppLaunchPath, validateAppLaunchUrl } from './app-launch.mjs';

const appId = `app-${'a'.repeat(32)}`, domainId = `dom_${'b'.repeat(32)}`;
const shellUrl = 'https://soty.example/#mine';
const boot = (token = 'A') => `https://demo.apps.example/_soty/boot?path=%2Freader%3Fpage%3D2#${token.repeat(43)}`;
const target = Object.freeze({ appId, domainId, path: '/reader?page=2' });
const deferred = () => { let resolve, reject; const promise = new Promise((yes, no) => { resolve = yes; reject = no; }); return { promise, resolve, reject }; };
const popup = () => ({ opener: {}, closed: false, visits: [], location: { replace(url) { this.owner.visits.push(url); } }, close() { this.closed = true; } });
function makePopup() { const value = popup(); value.location.owner = value; return value; }
function fixture(request) {
  const state = { accountId: 'account-a', generation: 1, destroyed: false };
  const calls = [];
  const launcher = createAppLauncher({ target, accountId: state.accountId, shellUrl,
    isCurrent: accountId => !state.destroyed && state.accountId === accountId && state.generation === 1,
    request: async parameters => { calls.push(parameters); return request(parameters); },
  });
  return { state, calls, launcher };
}

test('named deep link round-trips its exact target, Unicode and query without treating them as return URLs', () => {
  const path = '/папка/файл?filter=a%2Fb&title=Привет%20мир&back=https%3A%2F%2Fother.example';
  const route = formatAppLaunchRoute({ appId, domainId, path });
  const intent = parseAppLaunchRoute(`#${route}`);
  assert.equal(intent.kind, 'launch');
  assert.deepEqual(intent.target, { appId, domainId, path });
  assert.equal(intent.route, route);
  assert.ok(Object.isFrozen(intent) && Object.isFrozen(intent.target));
  assert.equal(parseAppLaunchRoute('#notes/new'), null);
});

test('historical canonical and community routes retain context without manufacturing an alias', () => {
  assert.deepEqual(parseAppLaunchRoute(`#app/${appId}/community-123`), {
    kind: 'app', target: { appId }, communityId: 'community-123', route: `app/${appId}/community-123`,
  });
  assert.deepEqual(parseAppLaunchRoute(`#app/${appId}`).target, { appId });
  assert.equal(formatAppLaunchRoute({ appId, path: '/a?x=1' }, 'community-123'), `app/${appId}/community-123?path=%2Fa%3Fx%3D1`);
});

test('malformed app links remain errors, never become another screen or a weaker canonical launch', () => {
  for (const value of [
    '#launch', `#launch/${appId}`, `#launch/${appId}/`, `#launch/${appId}/${domainId}/extra`,
    `#launch/${appId}/other-domain`, `#launch/other-app/${domainId}`, `#app/${appId}/bad%2Fcontext`,
    `#launch/${appId}/${domainId}?path=%2Fa&path=%2Fb`, `#launch/${appId}/${domainId}?origin=https://other.example`,
    `#launch/${appId}/${domainId}?path=%2Fa#old-ticket`, `#app/${appId}?path=`,
  ]) assert.throws(() => parseAppLaunchRoute(value), { name: 'AppLaunchError' }, value);
});

test('path boundary rejects external targets, controls, reserved endpoints and normalized endpoint escapes', () => {
  for (const path of [
    'https://other.example/', '//other.example/', '/\\other.example', '/%2fother.example', '/%5cother.example',
    '/\nheader', '/%00header', '/bad%xx', '/_soty', '/_soty/boot', '/%5fsoty/session',
    '/folder/../_soty/session', '/folder/%2e%2e/_soty/session', '/a%23b/../_soty/session',
    '/a%3fb/../_soty/session', '/.//other.example', '/a/..//other.example', '/%2e//other.example', `/${'x'.repeat(8192)}`,
  ]) assert.throws(() => validateAppLaunchPath(path), { name: 'AppLaunchError' }, path);
  assert.equal(validateAppLaunchPath('/?q=%2F_soty%2Fsession'), '/?q=%2F_soty%2Fsession');
  assert.equal(validateAppLaunchPath('/%E2%9C%93?a=%23'), '/%E2%9C%93?a=%23');
});

test('legacy hash-router targets remain local and survive the outer shell fragment encoding', () => {
  for (const path of ['/#/dashboard', '/board?tag=a%2Bb#item']) {
    assert.equal(validateAppLaunchPath(path), path);
    const route = formatAppLaunchRoute({ appId, domainId, path });
    assert.ok(route.includes('%23'));
    assert.equal(parseAppLaunchRoute(`#${route}`).target.path, path);
  }
});

test('target copies only the exact app/domain/path contract and refuses injected request authority', () => {
  const source = { ...target }, copy = normalizeAppLaunchTarget(source);
  source.path = '/changed'; assert.equal(copy.path, '/reader?page=2'); assert.ok(Object.isFrozen(copy));
  for (const extra of [{ origin: 'https://other.example' }, { returnUrl: '/' }, { expectedAccountId: 'other' }])
    assert.throws(() => normalizeAppLaunchTarget({ ...target, ...extra }), { code: 'invalid_app_target' });
});

test('launch URL accepts the opaque boot query but never credentials, a same-origin page or a non-boot destination', () => {
  assert.equal(validateAppLaunchUrl(boot(), shellUrl), boot());
  assert.equal(validateAppLaunchUrl(`http://app.localhost:5301/_soty/boot#${'b'.repeat(43)}`, 'http://localhost:5300/'), `http://app.localhost:5301/_soty/boot#${'b'.repeat(43)}`);
  for (const value of [
    '/_soty/boot#ticket', `https://soty.example/_soty/boot#${'a'.repeat(43)}`, boot().replace('https:', 'http:'),
    boot().replace('https://', 'https://user:password@'), boot().replace('/_soty/boot', '/'),
    boot().replace('/_soty/boot', '/%5fsoty/boot'), boot().replace('#', '?ticket='), boot().slice(0, -1),
    'javascript:alert(1)', ` ${boot()}`, boot().replace('demo.apps', 'demo\\apps'),
  ]) assert.throws(() => validateAppLaunchUrl(value, shellUrl), { code: 'invalid_app_launch_url' }, value);
});

test('every fresh launch pins the original actor and target without sharing a ticket', async () => {
  let index = 0;
  const { launcher, calls } = fixture(async () => ({ url: boot(index++ === 0 ? 'A' : 'B') }));
  assert.equal(await launcher.launch(), boot('A'));
  const window = makePopup(); assert.equal(await launcher.openExternal(() => window), 'opened');
  assert.deepEqual(window.visits, [boot('B')]); assert.equal(window.opener, null);
  assert.equal(calls.length, 2); assert.deepEqual(calls[0], { ...target, expectedAccountId: 'account-a' });
  assert.ok(Object.isFrozen(calls[0]));
  launcher.dispose(); assert.equal(window.closed, false, 'a completed external tab belongs to the user');
});

test('popup is opened synchronously before asynchronous ticket issuance', async () => {
  const order = [], pending = deferred();
  const { launcher } = fixture(() => { order.push('request'); return pending.promise; });
  const window = makePopup();
  const result = launcher.openExternal(() => { order.push('popup'); return window; });
  assert.deepEqual(order, ['popup', 'request']); assert.equal(window.opener, null);
  assert.deepEqual(window.visits, []);
  pending.resolve({ url: boot() }); assert.equal(await result, 'opened');
});

test('blocked popup consumes no ticket and allows an explicit retry', async () => {
  const { launcher, calls } = fixture(async () => ({ url: boot() }));
  assert.equal(await launcher.openExternal(() => null), 'blocked'); assert.equal(calls.length, 0);
  const window = makePopup(); assert.equal(await launcher.openExternal(() => window), 'opened'); assert.equal(calls.length, 1);
});

test('repeated clicks while waiting neither create extra popups nor queue requests', async () => {
  const pending = deferred(), { launcher, calls } = fixture(() => pending.promise);
  const first = makePopup(), result = launcher.openExternal(() => first);
  let extra = 0;
  assert.equal(await launcher.openExternal(() => { extra++; return makePopup(); }), 'busy');
  assert.equal(extra, 0); assert.equal(calls.length, 1);
  pending.resolve({ url: boot() }); assert.equal(await result, 'opened');
});

for (const boundary of ['account', 'screen', 'destroy']) test(`late success after ${boundary} change cannot reach the iframe or pending popup`, async () => {
  const pending = deferred(), { launcher, state } = fixture(() => pending.promise);
  const window = makePopup(), frame = launcher.launch(), external = launcher.openExternal(() => window);
  if (boundary === 'account') state.accountId = 'account-b';
  if (boundary === 'screen') state.generation++;
  if (boundary === 'destroy') state.destroyed = true;
  pending.resolve({ url: boot() });
  assert.equal(await frame, null); assert.equal(await external, 'stale');
  assert.equal(window.closed, true); assert.deepEqual(window.visits, []);
  assert.equal(await launcher.launch(), null);
});

test('late failure after account switch is consumed without an error for the new screen', async () => {
  const pending = deferred(), { launcher, state } = fixture(() => pending.promise);
  const window = makePopup(), frame = launcher.launch(), external = launcher.openExternal(() => window);
  state.accountId = 'account-b'; pending.reject(new Error('old-account-denied'));
  assert.equal(await frame, null); assert.equal(await external, 'stale'); assert.equal(window.closed, true);
});

test('dispose immediately closes an owned blank popup even if the network never settles', async () => {
  const pending = deferred(), { launcher } = fixture(() => pending.promise);
  const window = makePopup(), result = launcher.openExternal(() => window);
  launcher.dispose(); assert.equal(window.closed, true);
  pending.resolve({ url: boot() }); assert.equal(await result, 'stale'); assert.deepEqual(window.visits, []);
});

test('a user-closed pending popup is not reopened by a successful response', async () => {
  const pending = deferred(), { launcher } = fixture(() => pending.promise);
  const window = makePopup(), result = launcher.openExternal(() => window);
  window.close(); pending.resolve({ url: boot() });
  assert.equal(await result, 'stale'); assert.deepEqual(window.visits, []);
});

test('current failures remain visible and retry gets a fresh response; no navigation to an invalid URL', async () => {
  let attempt = 0;
  const { launcher, calls } = fixture(async () => ({ url: attempt++ === 0 ? 'https://other.example/page' : boot('C') }));
  const rejected = makePopup();
  await assert.rejects(launcher.openExternal(() => rejected), { code: 'invalid_app_launch_url' });
  assert.equal(rejected.closed, true); assert.deepEqual(rejected.visits, []);
  const retried = makePopup(); assert.equal(await launcher.openExternal(() => retried), 'opened');
  assert.deepEqual(retried.visits, [boot('C')]); assert.equal(calls.length, 2);
});

test('a synchronous request failure closes the blank window and remains reportable', async () => {
  const failure = new Error('offline'), { launcher } = fixture(() => { throw failure; });
  const window = makePopup(); await assert.rejects(launcher.openExternal(() => window), failure);
  assert.equal(window.closed, true); assert.deepEqual(window.visits, []);
});
