import test from 'node:test';
import assert from 'node:assert/strict';
import { createAppLauncher, validateAppEntry, formatAppLaunchRoute, parseAppLaunchRoute, sameAppLaunchLocation } from './app-launch.mjs';

const appId = `app-${'c'.repeat(32)}`, domainId = `dom_${'d'.repeat(32)}`;
const origin = 'https://test.apps.example', shellUrl = 'https://shell.example/#mine';
const entry = path => ({ appId, domainId, origin, path });
const response = (path, ticket = 'a') => ({ url: `${origin}/_soty/boot?${new URLSearchParams({ path })}#${ticket.repeat(43)}`, entry: entry(path) });
const deferred = () => { let resolve, reject; const promise = new Promise((yes, no) => { resolve = yes; reject = no; }); return { promise, resolve, reject }; };
const turn = () => new Promise(done => setImmediate(done));
function popup() {
  const visits = [], window = { opener: {}, closed: false, visits, location: { replace: url => visits.push(url) }, close() { this.closed = true; } };
  return window;
}

test('discussion/archive routes preserve exact launch identity and reject unrelated query authority', () => {
  const conversationId = `conv_${'e'.repeat(32)}`, path = '/board?tag=a%2Bb#item';
  const original = parseAppLaunchRoute(formatAppLaunchRoute({ appId, domainId, path }));
  const presented = parseAppLaunchRoute(formatAppLaunchRoute(original.target, undefined, { panel: 'discussion', conversationId }));
  assert.deepEqual(presented.presentation, { panel: 'discussion', conversationId });
  assert.deepEqual(presented.target, original.target); assert.equal(sameAppLaunchLocation(original, presented, entry(path)), true);
  assert.equal(sameAppLaunchLocation(original, parseAppLaunchRoute(formatAppLaunchRoute({ appId, domainId, path: '/other' })), entry(path)), false);
  assert.equal(sameAppLaunchLocation(original, parseAppLaunchRoute(formatAppLaunchRoute({ appId, path })), entry(path)), false, 'a named address is never replaced by canonical');
  for (const query of ['panel=other', 'conversation=' + conversationId, 'panel=discussion&admin=0', 'panel=discussion&panel=discussion', 'panel=discussion&conversation=foreign'])
    assert.throws(() => parseAppLaunchRoute(`app/${appId}?${query}`), { code: 'invalid_app_route' });
  const admin = parseAppLaunchRoute(formatAppLaunchRoute({ appId }, undefined, { panel: 'discussion', administrative: true }));
  assert.equal(admin.presentation.administrative, true);
  assert.equal(sameAppLaunchLocation(parseAppLaunchRoute(formatAppLaunchRoute({ appId }, 'group-A')),
    parseAppLaunchRoute(formatAppLaunchRoute({ appId }, 'group-B')), entry(path)), false, 'another community cannot retain the former context');
});

test('entry validation binds the exact admission without changing Unicode, query or SPA hash', () => {
  for (const path of ['/#/dashboard', '/board?tag=a%2Bb#item', '/папка/файл?title=Привет%20мир#часть']) {
    const value = response(path), checked = validateAppEntry(value.entry, { appId }, shellUrl, value.url);
    assert.deepEqual(checked, entry(path)); assert.equal(Object.isFrozen(checked), true);
  }
  for (const invalid of [
    { ...entry('/A'), path: '/B' }, { ...entry('/A'), origin: 'https://other.apps.example' },
    { ...entry('/A'), domainId: `dom_${'e'.repeat(32)}` }, { ...entry('/A'), appId: `app-${'f'.repeat(32)}` },
    { ...entry('/A'), ticket: 'unexpected' }, { ...entry('/A'), origin: shellUrl.split('/#')[0] },
  ]) assert.throws(() => validateAppEntry(invalid, { appId, domainId, path: '/A' }, shellUrl, response('/A').url));
  assert.throws(() => validateAppEntry(entry('/A'), { appId }, shellUrl, response('/A').url.replace('?path=%2FA', '?path=%2FA&path=%2FB')));
});

test('initial iframe and concurrent external click resolve once, then use different tickets for the pinned original path', async () => {
  const pending = deferred(), calls = []; let defaultPath = '/A';
  const launcher = createAppLauncher({ target: { appId }, accountId: 'account_A', shellUrl, isCurrent: () => true,
    request: async args => { calls.push(args); return calls.length === 1 ? pending.promise : response(args.path ?? defaultPath, 'b'); } });
  const first = launcher.launch(), window = popup(), external = launcher.openExternal(() => window);
  assert.equal(window.opener, null); assert.equal(calls.length, 1); assert.deepEqual(window.visits, []);
  defaultPath = '/B'; pending.resolve(response('/A'));
  assert.equal(await first, response('/A').url); assert.equal(await external, 'opened');
  assert.equal(calls.length, 2); assert.deepEqual(calls[1], { appId, domainId, path: '/A', expectedAccountId: 'account_A' });
  assert.deepEqual(window.visits, [response('/A', 'b').url]); assert.deepEqual(launcher.entry(), entry('/A'));
  launcher.dispose(); assert.equal(launcher.entry(), null);
});

test('failed initial runtime may resolve an offline entry and retries cannot switch to a changed default', async () => {
  let available = false, path = '/initial', reads = 0; const calls = [];
  const launcher = createAppLauncher({ target: { appId }, accountId: 'account_A', shellUrl, isCurrent: () => true,
    request: async args => { calls.push(args); if (!available) throw Object.assign(new Error('app_offline'), { code: 'app_offline' }); return response(args.path ?? path); },
    resolveEntry: async () => { reads++; return { entry: entry(path) }; } });
  await assert.rejects(launcher.launch(), { code: 'app_offline' }); assert.equal(reads, 1);
  assert.deepEqual(launcher.entry(), entry('/initial')); path = '/changed'; available = true;
  assert.equal(await launcher.launch(), response('/initial').url); assert.equal(reads, 1);
  assert.equal(calls[1].path, '/initial');
});

test('a successful response missing its entry never performs a later lookup of the new default', async () => {
  let reads = 0;
  const launcher = createAppLauncher({ target: { appId }, accountId: 'account_A', shellUrl, isCurrent: () => true,
    request: async () => ({ url: response('/A').url }), resolveEntry: async () => { reads++; return { entry: entry('/B') }; } });
  await assert.rejects(launcher.launch(), { code: 'invalid_app_entry' });
  assert.equal(launcher.entry(), null); assert.equal(reads, 0);
});

test('a retired or different entry cannot replace the resolved launch location on retry', async () => {
  let first = true, reads = 0;
  const launcher = createAppLauncher({ target: { appId }, accountId: 'account_A', shellUrl, isCurrent: () => true,
    request: async () => { if (first) { first = false; return response('/A'); } return response('/B'); },
    resolveEntry: async () => { reads++; return { entry: entry('/B') }; } });
  await launcher.launch(); await assert.rejects(launcher.launch(), { code: 'invalid_app_entry' });
  assert.deepEqual(launcher.entry(), entry('/A')); assert.equal(reads, 0);
});

test('late offline resolution after account ABA never publishes the entry or navigates a pending popup', async () => {
  const pending = deferred(); let generation = 1, reads = 0;
  const launcher = createAppLauncher({ target: { appId }, accountId: 'account_A', shellUrl, isCurrent: () => generation === 1,
    request: async () => { throw new Error('offline'); }, resolveEntry: async () => { reads++; return pending.promise; } });
  const window = popup(), result = launcher.openExternal(() => window); await turn();
  assert.equal(reads, 1); generation = 3; pending.resolve({ entry: entry('/A') });
  assert.equal(await result, 'stale'); assert.equal(window.closed, true); assert.deepEqual(window.visits, []); assert.equal(launcher.entry(), null);
});
