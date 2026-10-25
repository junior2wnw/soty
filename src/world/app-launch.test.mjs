import test from 'node:test';
import assert from 'node:assert/strict';
import { createAppLauncher, formatAppLaunchRoute, normalizeAppLaunchTarget, parseAppLaunchRoute, validateAppEntry, validateAppLaunchPath, validateAppLaunchUrl } from './app-launch.mjs';
import { checkedScopedEntryPath, createScopedLaunchPath, createLaunchPath } from '../../modules/apps/server/launch-path.mjs';

const appId = `app-${'a'.repeat(32)}`, domainId = `dom_${'b'.repeat(32)}`;
const shellUrl = 'https://soty.example/#mine';
const boot = (token = 'A') => `https://demo.apps.example/_soty/boot?path=%2Freader%3Fpage%3D2#${token.repeat(43)}`;
const target = Object.freeze({ appId, domainId, path: '/reader?page=2' });
const scopedReply=()=>({url:boot(),runtimeProfile:'soty.selected-human-embed.v2',scopedCloseHandle:'C'.repeat(43),
  scopedSource:{id:'soty.standard-workspace',version:2,digest:'23061fc95610e3011ec42b3ecf229308bce44b0a1268408ca9b6ee138c98f7e9'}});
const deferred = () => { let resolve, reject; const promise = new Promise((yes, no) => { resolve = yes; reject = no; }); return { promise, resolve, reject }; };
const popup = () => ({ opener: {}, closed: false, visits: [], location: { replace(url) { this.owner.visits.push(url); } }, close() { this.closed = true; } });
function makePopup() { const value = popup(); value.location.owner = value; return value; }
function fixture(request) {
  const state = { accountId: 'account-a', generation: 1, destroyed: false };
  const calls = [];
  const launcher = createAppLauncher({ target, accountId: state.accountId, shellUrl,
    isCurrent: accountId => !state.destroyed && state.accountId === accountId && state.generation === 1,
    request: async parameters => { calls.push(parameters); const result = await request(parameters);
      return { ...result, entry: { ...target, origin: 'https://demo.apps.example' } }; },
  });
  return { state, calls, launcher };
}

test('discussion resolves an exact entry without admission, and a later runtime uses that pin', async () => {
  const entry = { ...target, origin: 'https://demo.apps.example' }, calls = [];
  const launcher = createAppLauncher({ target, accountId: 'account-a', shellUrl, isCurrent: () => true,
    resolveEntry: async args => { calls.push(['resolve', args]); return { entry }; },
    request: async args => { calls.push(['launch', args]); return { url: boot(), entry }; } });
  assert.deepEqual(await launcher.resolve(), entry);
  assert.equal(calls.length, 1); assert.equal(calls[0][0], 'resolve');
  assert.deepEqual(calls[0][1], { ...target, expectedAccountId: 'account-a' });
  await launcher.launch();
  assert.equal(calls[1][0], 'launch'); assert.deepEqual(calls[1][1], { ...target, expectedAccountId: 'account-a' });
});

test('an app launch racing a discussion resolve waits for the strict pin; stale profile results are discarded', async () => {
  const gate = deferred(), entry = { ...target, origin: 'https://demo.apps.example' };
  let current = true, admissions = 0;
  const launcher = createAppLauncher({ target, accountId: 'account-a', shellUrl, isCurrent: () => current,
    resolveEntry: () => gate.promise,
    request: async () => { admissions++; return { url: boot(), entry }; } });
  const resolving = launcher.resolve(), launching = launcher.launch();
  assert.equal(admissions, 0); current = false; gate.resolve({ entry });
  assert.equal(await resolving, null); assert.equal(await launching, null); assert.equal(admissions, 0); assert.equal(launcher.entry(), null);
});

test('a discussion resolve never accepts another named entry or a different path', async () => {
  for (const bad of [{ ...target, domainId: `dom_${'c'.repeat(32)}` }, { ...target, path: '/other' }]) {
    const launcher = createAppLauncher({ target, accountId: 'account-a', shellUrl, isCurrent: () => true,
      resolveEntry: async () => ({ entry: { ...bad, origin: 'https://demo.apps.example' } }),
      request: async () => { assert.fail('Discussion must not admit a runtime'); } });
    await assert.rejects(launcher.resolve(), error => error.code === 'invalid_app_entry');
    assert.equal(launcher.entry(), null);
  }
});

test('automatic scoped wake retains the approved Source and location without a relay binding',async()=>{
  const f=fixture(async()=>scopedReply());await f.launcher.launch();const old=f.launcher.scopedCapture();
  assert.equal(f.launcher.binding(),null);assert.equal(await f.launcher.launch({requireSameBinding:true}),boot());
  assert.deepEqual(f.launcher.scopedCapture().source,old.source);assert.notEqual(f.launcher.scopedCapture().slot,old.slot);
  assert.deepEqual(f.calls[1],f.calls[0]);f.launcher.dispose();
});

test('automatic wake denies a replaced Source pin or runtime and abandons only the new slot',async()=>{
  for(const changed of [{scopedSource:{...scopedReply().scopedSource,digest:'f'.repeat(64)}},{runtimeProfile:'soty.selected-human-embed.v1'}]){
    let calls=0,closed=0;const f=fixture(async()=>++calls===1?scopedReply():{...scopedReply(),...changed,scopedCleanup:async()=>{closed++;}});
    await f.launcher.launch();const old=f.launcher.scopedCapture();
    await assert.rejects(f.launcher.launch({requireSameBinding:true}),{code:'app_launch_source_changed'});
    assert.equal(f.launcher.scopedCapture().slot,old.slot);assert.equal(closed,1);f.launcher.dispose();
  }
});

test('a late wake reply cannot replace a newer scoped launch',async()=>{
  const held=deferred();let calls=0,closed=0;
  const f=fixture(async()=>++calls===2?held.promise:scopedReply());await f.launcher.launch();
  const late=f.launcher.launch({requireSameBinding:true});await f.launcher.launch({requireSameBinding:true});
  const current=f.launcher.scopedCapture();held.resolve({...scopedReply(),scopedCleanup:async()=>{closed++;}});
  await assert.rejects(late,{code:'app_launch_source_changed'});assert.equal(f.launcher.scopedCapture().slot,current.slot);
  assert.equal(closed,1);f.launcher.dispose();
});

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

test('approved capture pin gets a private per-launch slot; source mutation, refresh and account switch never retarget it',async()=>{
  const source={id:'planner.selected-workspace',version:1,digest:'a'.repeat(64)},fixtureReply={url:boot(),runtimeProfile:'soty.selected-human-embed.v1',scopedCloseHandle:'C'.repeat(43),scopedSource:source};
  const {launcher,state}=fixture(async()=>fixtureReply);await launcher.launch();const first=launcher.scopedCapture();
  assert.deepEqual(first.source,source);assert.deepEqual(first.slot,{});assert.ok(Object.isFrozen(first.slot));
  source.id='hive';assert.equal(first.source.id,'planner.selected-workspace');
  source.id='planner.selected-workspace';await launcher.launch();assert.notEqual(launcher.scopedCapture().slot,first.slot);
  state.accountId='account-b';assert.equal(launcher.scopedCapture(),null);launcher.dispose();
});

test('stable main/widget locator survives a new launch without becoming a route or capability', async () => {
  for(const instanceId of ['main','widget-11111111-1111-4111-8111-111111111111']){
    const calls=[];const launcher=createAppLauncher({target,accountId:'account-a',instanceId,shellUrl,isCurrent:()=>true,
      request:async args=>{calls.push(args);return {url:boot(),entry:{...target,origin:'https://demo.apps.example'}};}});
    await launcher.launch();await launcher.launch();
    assert.equal(calls.length,2);assert.ok(calls.every(value=>value.instanceId===instanceId&&value.expectedAccountId==='account-a'));
    assert.equal(formatAppLaunchRoute(target).includes(instanceId),false);launcher.dispose();
  }
});


test('saved root entry follows only exact workspace2 into embed and retains its identity and instance', async () => {
  for (const preResolved of [false, true]) {
    const calls = [], closed = [], origin = 'https://demo.apps.example'; let upgrade = !preResolved;
    const launcher = createAppLauncher({ target: { appId, domainId, path: '/' }, accountId: 'account-a', instanceId: 'main', shellUrl,
      isCurrent: () => true, request: async args => { calls.push(args); const path = upgrade ? '/embed' : '/';
        return { ...(upgrade ? scopedReply() : {}), url: `${origin}/_soty/boot?path=${encodeURIComponent(path)}#${'A'.repeat(43)}`,
          entry: { appId, domainId, origin, path }, ...(upgrade ? { scopedCleanup: () => { closed.push(true); } } : {}) }; } });
    if (preResolved) { await launcher.launch(); assert.equal(launcher.entry().path, '/'); upgrade = true; }
    await launcher.launch(); assert.deepEqual(launcher.entry(), { appId, domainId, origin, path: '/embed' });
    assert.deepEqual(calls.at(-1), { appId, domainId, path: '/', expectedAccountId: 'account-a', instanceId: 'main' });
    const pin = launcher.scopedCapture().source;
    await launcher.launch({ requireSameBinding: true });
    assert.deepEqual(calls.at(-1), { appId, domainId, path: '/embed', expectedAccountId: 'account-a', instanceId: 'main' });
    assert.deepEqual(launcher.scopedCapture().source, pin); launcher.dispose();
  }
});

test('saved root compatibility rejects older pins, changed locations and non-root deep links', async () => {
  const origin = 'https://demo.apps.example';
  for (const change of [ { scopedSource: { ...scopedReply().scopedSource, version: 1 } },
    { scopedSource: { ...scopedReply().scopedSource, digest: '0'.repeat(64) } }, { runtimeProfile: 'soty.selected-human-embed.v1' },
    { entry: { appId: `app-${'f'.repeat(32)}`, domainId, origin, path: '/embed' } },
    { entry: { appId, domainId: `dom_${'f'.repeat(32)}`, origin, path: '/embed' } }, { requestedPath: '/?x=1' }, { requestedPath: '/board' } ]) {
    let closed = 0; const { requestedPath = '/', ...response } = change;
    const launcher = createAppLauncher({ target: { appId, domainId, path: requestedPath }, accountId: 'account-a', shellUrl,
      isCurrent: () => true, request: async () => ({ ...scopedReply(), url: `${origin}/_soty/boot?path=%2Fembed#${'A'.repeat(43)}`,
        entry: { appId, domainId, origin, path: '/embed' }, scopedCleanup: () => { closed++; }, ...response }) });
    await assert.rejects(launcher.launch(), { code: 'invalid_app_entry' }); assert.equal(launcher.entry(), null);
    assert.equal(closed, 1); launcher.dispose();
  }
});

test('canonical guide is the sole reserved launch path and round-trips exactly', () => {
  assert.equal(validateAppLaunchPath('/_soty/guide'), '/_soty/guide');
  const value = { appId, path: '/_soty/guide' };
  assert.deepEqual(parseAppLaunchRoute(formatAppLaunchRoute(value)).target, value);
  for (const path of ['/_soty/guide?x', '/_soty/guide/', '/_soty/%67uide', '/guide/../_soty/guide', '/_soty/session', '/_soty/boot']) assert.throws(() => validateAppLaunchPath(path));
});

const privatePath='/embed#document=pag_private&block=hero&invite='+'z'.repeat(43);
const privateOrigin='https://demo.apps.example';
function privateReply(path=privatePath,token='A') {
  const generated=createScopedLaunchPath(path);
  return {...scopedReply(),url:privateOrigin+generated.bootPath+'#'+token.repeat(43),
    entry:{appId,domainId,origin:privateOrigin,path:generated.entryPath}};
}
function privateFixture(path=privatePath) {
  const state={current:true,change:()=>{},calls:[],abandoned:[]};
  const launcher=createAppLauncher({target:{appId,domainId,path},accountId:'account-a',instanceId:'main',shellUrl,
    isCurrent:()=>state.current,abandonScoped:value=>{state.abandoned.push(value);},request:async args=>{
      state.calls.push(args);const result=privateReply(args.path,String.fromCharCode(64+state.calls.length));state.change(result);return result;
    }});
  return{state,launcher};
}
function privateRenewal(path=privatePath) {
  const result=privateReply(path,'R'),url=new URL(result.url);url.searchParams.set('mode','renew');
  return{...result,url:url.href,scopedCloseHandle:'N'.repeat(43),scopedSlotExpiresAt:Date.now()+120000,scopedRenewalRequestId:'private_renewal_0001'};
}

test('actual Source server boot and client initial/external launcher keep private navigation off URL queries',async()=>{
  const f=privateFixture();
  assert.equal(await f.launcher.launch(),privateReply().url);
  assert.deepEqual(f.launcher.entry(),privateReply().entry);
  const old=f.launcher.scopedCapture(),window=makePopup();
  assert.equal(await f.launcher.openExternal(()=>window),'opened');
  assert.deepEqual(window.visits,[privateReply(privatePath,'B').url]);assert.equal(window.opener,null);
  assert.equal(f.launcher.scopedCapture().slot,old.slot,'external launch cannot replace the mounted slot');
  assert.ok(f.state.calls.every(args=>args.path===privatePath&&args.appId===appId&&args.domainId===domainId&&args.expectedAccountId==='account-a'&&args.instanceId==='main'));
  for(const url of window.visits){assert.equal(new URL(url).searchParams.get('path'),'/embed');assert.ok(!url.includes('invite')&&!url.includes('pag_private'));}
  f.launcher.dispose();assert.equal(window.closed,false);
});

test('private Source launch permits omitted initial path but pins the full returned path for future launches',async()=>{
  const calls=[];const launcher=createAppLauncher({target:{appId},accountId:'account-a',shellUrl,isCurrent:()=>true,
    request:async args=>{calls.push(args);return privateReply();}});
  await launcher.launch();await launcher.launch();assert.equal(calls[0].path,undefined);
  assert.deepEqual(calls[1],{appId,domainId,path:privatePath,expectedAccountId:'account-a'});launcher.dispose();
});

test('private Source renewal uses public embed boot but retains the exact old path, source and current slot',async()=>{
  const f=privateFixture();await f.launcher.launch();const previous=f.launcher.scopedCapture();
  const reply=privateRenewal(),next=f.launcher.prepareRenewal(reply);
  assert.equal(new URL(next.url).searchParams.get('path'),'/embed');assert.ok(!next.url.includes('invite'));
  assert.equal(f.launcher.entry().path,privatePath);assert.equal(f.launcher.scopedCapture().slot,previous.slot);
  assert.equal(f.launcher.commitRenewal(previous,next),true);assert.equal(f.launcher.scopedCapture().slot,next.slot);
  assert.deepEqual(f.launcher.scopedCapture().source,previous.source);assert.equal(f.launcher.entry().path,privatePath);
  assert.equal(f.launcher.commitRenewal(previous,next),false,'an old slot cannot commit again');f.launcher.dispose();
});

test('private boot matching is limited to known scoped runtime profiles and preserves ordinary full-path matching',()=>{
  const result=privateReply(),requested={appId,domainId,path:privatePath};
  for(const runtimeProfile of ['soty.selected-human-embed.v1','soty.selected-human-embed.v2'])
    assert.deepEqual(validateAppEntry(result.entry,requested,shellUrl,result.url,runtimeProfile),result.entry);
  for(const runtimeProfile of [undefined,null,'soty.relay-restricted.v1','soty.selected-human-embed.v3'])
    assert.throws(()=>validateAppEntry(result.entry,requested,shellUrl,result.url,runtimeProfile),{code:'invalid_app_entry'});
  const old=privateOrigin+createLaunchPath(privatePath).bootPath+'#'+'A'.repeat(43);
  assert.deepEqual(validateAppEntry(result.entry,requested,shellUrl,old),result.entry,'relay SPA fragments still require their full boot path');
});

test('client private fragment bounds agree with actual server admission, including Unicode and opaque values',()=>{
  for(const path of ['/embed','/embed#','/embed#раздел%20два','/embed#/reader/42?tab=comments',
    '/embed#next=https%3A%2F%2Fother.example%2F_soty%2Fsession','/embed#'+'a'.repeat(2048),'/embed#'+'界'.repeat(2048)]){
    const reply=privateReply(path);assert.deepEqual(validateAppEntry(reply.entry,{appId,domainId,path},shellUrl,reply.url,reply.runtimeProfile),reply.entry);
  }
  for(const path of ['/embed?x=1#document=pag_x','/embed?#x','/embed/#x','/other#x','/%65mbed#x','/a/../embed#x',
    '/_soty/guide','/embed%23document=x','/embed#x#y','/embed#x\\y','/embed#x y','/embed#%00','/embed#%0a',
    '/embed#%0D','/embed#%7f','/embed#%5c','/embed#%','/embed#%C0%AF','/embed#\ud800','/embed#'+'a'.repeat(2049)]){
    assert.equal(checkedScopedEntryPath(path),null);
    const entry={...privateReply().entry,path};
    assert.throws(()=>validateAppEntry(entry,{appId,domainId,path},shellUrl,privateReply().url,'soty.selected-human-embed.v2'),{name:'AppLaunchError'},path);
  }
});

test('private initial launches reject changed targets, destinations, ticket/query envelopes and unknown scope',async()=>{
  for(const change of [
    reply=>{reply.entry.path=privatePath.replace('pag_private','pag_other');},
    reply=>{reply.entry.appId='app-'+'f'.repeat(32);},reply=>{reply.entry.domainId='dom_'+'f'.repeat(32);},
    reply=>{reply.entry.origin='https://other.apps.example';},reply=>{reply.url=reply.url.replace('demo.apps.example','other.apps.example');},
    reply=>{reply.url=reply.url.replace('https:','http:');},reply=>{reply.url=reply.url.slice(0,-1);},
    reply=>{const url=new URL(reply.url);url.searchParams.append('path','/embed');reply.url=url.href;},
    reply=>{const url=new URL(reply.url);url.searchParams.set('path','/other');reply.url=url.href;},
    reply=>{const url=new URL(reply.url);url.searchParams.set('mode','renew');reply.url=url.href;},
    reply=>{delete reply.runtimeProfile;delete reply.scopedCloseHandle;delete reply.scopedSource;},
    reply=>{reply.runtimeProfile='soty.selected-human-embed.v3';}
  ]){
    const f=privateFixture();f.state.change=change;
    await assert.rejects(f.launcher.launch(),{name:'AppLaunchError'});assert.equal(f.launcher.entry(),null);
    assert.equal(f.launcher.scopedCapture(),null);f.launcher.dispose();
  }
});

test('private external redirects cannot replace a mounted document, domain, origin or admitted path',async()=>{
  for(const change of [reply=>{reply.entry.path=privatePath.replace('hero','other');},reply=>{reply.entry.domainId='dom_'+'e'.repeat(32);},
    reply=>{reply.url=reply.url.replace('demo.apps.example','other.apps.example');},
    reply=>{const url=new URL(reply.url);url.searchParams.set('path','/other');reply.url=url.href;}]){
    const f=privateFixture();await f.launcher.launch();const old=f.launcher.scopedCapture();f.state.change=change;
    const window=makePopup();await assert.rejects(f.launcher.openExternal(()=>window),{name:'AppLaunchError'});
    assert.equal(window.closed,true);assert.deepEqual(window.visits,[]);assert.equal(f.launcher.scopedCapture().slot,old.slot);
    assert.equal(f.launcher.entry().path,privatePath);assert.equal(f.state.abandoned.length,1);f.launcher.dispose();
  }
});

test('private renewal rejects new private paths and Source/session drift even when public embed query is identical',async()=>{
  const f=privateFixture();await f.launcher.launch();const old=f.launcher.scopedCapture();
  for(const change of [reply=>{reply.entry.path=privatePath.replace('pag_private','pag_other');},
    reply=>{reply.entry.appId='app-'+'f'.repeat(32);},reply=>{reply.entry.domainId='dom_'+'f'.repeat(32);},
    reply=>{reply.entry.origin='https://other.apps.example';},reply=>{reply.runtimeProfile='soty.selected-human-embed.v1';},
    reply=>{reply.scopedSource={...reply.scopedSource,id:'other.source'};},reply=>{reply.scopedSource={...reply.scopedSource,version:3};},
    reply=>{reply.scopedSource={...reply.scopedSource,digest:'f'.repeat(64)};},reply=>{reply.scopedCloseHandle='bad';},
    reply=>{reply.scopedSlotExpiresAt=Date.now()-1;},reply=>{reply.scopedSlotExpiresAt=Date.now()+400000;},
    reply=>{reply.scopedRenewalRequestId='bad';},reply=>{reply.url=reply.url.replace('demo.apps.example','other.apps.example');},
    reply=>{const url=new URL(reply.url);url.searchParams.set('path','/other');reply.url=url.href;},
    reply=>{const url=new URL(reply.url);url.searchParams.append('path','/embed');reply.url=url.href;},
    reply=>{const url=new URL(reply.url);url.searchParams.set('mode','unknown');reply.url=url.href;}]){
    const reply=privateRenewal();change(reply);assert.throws(()=>f.launcher.prepareRenewal(reply),{name:'AppLaunchError'});
    assert.equal(f.launcher.scopedCapture().slot,old.slot);assert.equal(f.launcher.entry().path,privatePath);
  }
  f.state.current=false;assert.throws(()=>f.launcher.prepareRenewal(privateRenewal()),{code:'invalid_app_scoped_launch'});f.launcher.dispose();
});
