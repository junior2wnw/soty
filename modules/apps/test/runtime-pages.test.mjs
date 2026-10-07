import test from 'node:test';
import assert from 'node:assert/strict';
import vm from 'node:vm';
import { renderBootPage, renderStatusPage } from '../server/runtime-pages.mjs';
import { createPalette } from '../../ui/palette.mjs';
import { roundedHexPath } from '../../ui/hex.mjs';

const shellUrl = 'https://soty.example/#app-entry?id=app-test&path=%2F';
const ticket = 'T'.repeat(43), sessionCheck = 'C'.repeat(43), nonce = Buffer.from('1234567890abcdef').toString('base64');
const response = (body, ok = true) => ({ ok, status: ok ? 200 : 403, json: async () => body });
const pending = () => { let resolve, reject; const promise = new Promise((a, b) => { resolve = a; reject = b; }); return { promise, resolve, reject }; };
const flush = () => new Promise(resolve => setImmediate(resolve));

function browser(markup, { framed = false, hash = '#' + ticket, responses = [], cleanThrows = false } = {}) {
  const scripts = [...markup.matchAll(/<script\b[^>]*>([\s\S]*?)<\/script>/gu)];
  assert.equal(scripts.length, 1);
  const elements = new Map(), calls = [], events = [], navigations = [], listeners = new Map(), timers = new Map(), parentMessages = [];
  let timerId = 0;
  const element = id => {
    if (!elements.has(id)) elements.set(id, { hidden: true, disabled: false, textContent: '', dataset: {}, attributes: new Map(), handlers: new Map(),
      setAttribute(key, value) { this.attributes.set(key, value); },
      removeAttribute(key) { this.attributes.delete(key); if (key === 'href') delete this.href; },
      addEventListener(type, callback) { this.handlers.set(type, callback); },
    });
    return elements.get(id);
  };
  const location = { hash, pathname: '/_soty/boot', search: '?path=%2Funtrusted-query',
    replace(path) { events.push(['navigate', path]); navigations.push(path); } };
  const window = { addEventListener(type, callback) { listeners.set(type, callback); } };
  window.self = window; window.top = framed ? {} : window;
  const parent = { postMessage(data, origin) { parentMessages.push({ data: structuredClone(data), origin }); } }; window.parent = framed ? parent : window;
  const context = { window, location, document: { title: '', body: element('body'), getElementById: element }, URL, AbortController,
    history: { replaceState(_state, _title, path) { events.push(['cleanup', path]); if (cleanThrows) throw new Error('blocked history'); location.hash = ''; } },
    setTimeout(callback, delay) { const id = ++timerId; timers.set(id, { callback, delay }); return id; }, clearTimeout(id) { timers.delete(id); },
    fetch(url, options) {
      events.push(['fetch', options.method]); calls.push({ url, options });
      const next = responses.shift();
      if (typeof next === 'function') return next(url, options);
      if (next instanceof Error) return Promise.reject(next);
      if (next !== undefined) return Promise.resolve(next);
      return Promise.reject(new Error('unexpected request'));
    },
  };
  vm.runInNewContext(scripts[0][1], context, { timeout: 1000 });
  return { context, element, calls, events, navigations, timers, parent, parentMessages,
    click: id => element(id).handlers.get('click')?.(),
    emit: (type, event = {}) => listeners.get(type)?.(event),
    expire: () => { for (const value of [...timers.values()]) value.callback(); },
  };
}

test('renderers use shared graphite palette and regular rounded hex geometry, with narrow and reduced-motion styles', () => {
  const page = renderBootPage({ shellUrl, nonce });
  assert.match(page, /^<!doctype html><html lang="ru">/u);
  assert.ok(page.includes(`--sw-bg:${createPalette('dark', 50)['--sw-bg']}`));
  assert.ok(page.includes(roundedHexPath(10, undefined, { x: 11, y: 10 })));
  assert.match(page, /viewBox="0 0 [0-9.]+ [0-9.]+"/u);
  assert.match(page, /height:auto/u); assert.match(page, /prefers-reduced-motion:reduce/u);
  assert.match(page, /max-width:359px/u); assert.match(page, /max-height:440px/u);
  assert.match(page, /@media\(max-height:280px\) and \(min-width:480px\)/u);
  assert.match(page, /body\[data-framed="true"\] \.brand\{display:none\}/u);
  assert.match(page, /min-height:44px/u);
  assert.match(page, /:focus-visible/u); assert.match(page, /min-height:46px/u);
  assert.match(page, /role="status" aria-live="polite"/u);
  assert.equal((page.match(/<script\b/gu) || []).length, 1); assert.ok(page.includes(`<script nonce="${nonce}">`));
  assert.doesNotMatch(page, /<script[^>]+src=|<link[^>]+href=|<img[^>]+src=/u);
});

test('only the recognized cookie-check failure sends one nonce-bound non-authoritative retry hint, before or after watch handshake', async () => {
  const appId='app-'+ 'a'.repeat(32), watch={schema:'soty.app-boot-watch.v1',appId,nonce:'n'.repeat(43)};
  for(const early of [true,false]){
    const env=browser(renderBootPage({shellUrl}),{framed:true,responses:[response({ok:true,entryPath:'/',sessionCheck}),response({ok:false,error:'app_session_check_failed'},false)]});
    const handshake=()=>env.emit('message',{data:watch,origin:'https://soty.example',source:env.parent,ports:[]});
    if(early)handshake();await flush();if(!early)handshake();await flush();
    assert.deepEqual(env.parentMessages,[{origin:'https://soty.example',data:{schema:'soty.app-boot-failure.v1',appId,nonce:watch.nonce,error:'app_session_check_failed'}}]);
    handshake();assert.equal(env.parentMessages.length,1);assert.equal(env.calls.length,2);assert.deepEqual(env.navigations,[]);
  }
});

test('wrong parent/origin/nonce/selectors and every other admission/check error cannot cause automatic recovery hints', async () => {
  const watch={schema:'soty.app-boot-watch.v1',appId:'app-'+ 'a'.repeat(32),nonce:'n'.repeat(43)};
  const env=browser(renderBootPage({shellUrl}),{framed:true,responses:[response({ok:true,entryPath:'/',sessionCheck}),response({ok:false,error:'app_session_check_failed'},false)]});await flush();
  for(const event of [{data:watch,origin:'https://evil.example',source:env.parent},
    {data:watch,origin:'https://soty.example',source:{}},{data:{...watch,accountId:'caller'},origin:'https://soty.example',source:env.parent},
    {data:{...watch,nonce:'bad'},origin:'https://soty.example',source:env.parent}])env.emit('message',event);
  assert.equal(env.parentMessages.length,0);
  for(const error of ['apps_access_denied','app_access_changed','app_access_expired','app_ticket_invalid','private_internal_error']){
    const child=browser(renderBootPage({shellUrl}),{framed:true,responses:[response({ok:true,entryPath:'/',sessionCheck}),response({ok:false,error},false)]});
    child.emit('message',{data:watch,origin:'https://soty.example',source:child.parent,ports:[]});await flush();assert.equal(child.parentMessages.length,0);
  }
});

test('pagehide/history restoration cannot revive a failure hint or reuse its cleared ticket', async()=>{
  const env=browser(renderBootPage({shellUrl}),{framed:true,responses:[response({ok:true,entryPath:'/',sessionCheck}),response({ok:false,error:'app_session_check_failed'},false)]});await flush();env.emit('pagehide');env.emit('pageshow',{persisted:true});
  env.emit('message',{data:{schema:'soty.app-boot-watch.v1',appId:'app-'+ 'a'.repeat(32),nonce:'n'.repeat(43)},origin:'https://soty.example',source:env.parent,ports:[]});assert.equal(env.parentMessages.length,0);assert.equal(env.calls.length,2);
});

test('parent origins are the existing exact embedding allowlist, with no wildcard or arbitrary callback URL',()=>{
  for(const parentOrigins of [[],['*'],['https://soty.example/path'],['https://user:pass@soty.example']])assert.throws(()=>renderBootPage({shellUrl,parentOrigins}));
  assert.doesNotThrow(()=>renderBootPage({shellUrl,parentOrigins:['https://soty.example','https://retained.example']}));
  assert.doesNotThrow(()=>renderBootPage({shellUrl,parentOrigins:Array.from({length:9},(_,index)=>'https://retained'+index+'.example')}),'existing trusted embedding settings have no eight-origin limit');
});

test('only HTTP(S) shell URLs, local runtime paths and canonical 16-byte nonce are accepted', () => {
  for (const bad of ['javascript:alert(1)', 'data:text/html,evil', '//evil.example', '/relative', 'https://user:pass@soty.example',
    'https:\\evil.example', ' https://soty.example', 'https://soty.example/\npath', 'https://soty.example/a b']) {
    assert.throws(() => renderBootPage({ shellUrl: bad }), /invalid_shell_url/u);
  }
  for (const bad of ['https://evil.example/', '//evil.example/', '/%2f%2fevil.example', '/\\evil', '/\n', '/%00', '/_soty/session', '/x/../_soty/session', '/%5fsoty/session', '/%ZZ',
    '/x/..//double', '/%2F_soty/session', '/x%2f..%2f_soty/session', '/x%2f..%2f%2fdouble', '/x%23/../_soty/session', '/bad%00/../safe']) {
    assert.throws(() => renderStatusPage({ publicResetPath: bad }), /invalid_public_reset_path/u);
  }
  for (const bad of ['', 'x" onload="evil', 'x'.repeat(24), 'A'.repeat(21) + 'B==']) {
    assert.throws(() => renderStatusPage({ nonce: bad }), /invalid_page_nonce/u);
  }
  assert.doesNotThrow(() => renderBootPage({ shellUrl: 'http://localhost:5300/#app-entry', publicResetPath: '/search?q=%23one&sort=two', nonce }));
  assert.doesNotThrow(() => renderBootPage({ shellUrl, publicResetPath: '/#/dashboard', nonce }));
});

test('script/HTML injection and prototype-key errors stay data, with no owner or raw technical error displayed', async () => {
  const malicious = '</script><script>globalThis.pwned=true</script>"&\u2028';
  for (const error of [malicious, '__proto__', 'constructor', 'toString', { code: malicious, owner: 'private-owner' }]) {
    const page = renderStatusPage({ error, shellUrl: `https://soty.example/#next=${malicious}`, publicResetPath: '/search?q=</script><svg/onload=evil>&quote="', nonce });
    assert.equal((page.match(/<script\b/gu) || []).length, 1);
    assert.equal((page.match(/<\/script>/gu) || []).length, 1);
    assert.doesNotMatch(page, /private-owner|<svg\/onload|<script>globalThis/u);
    const env = browser(page); await flush();
    assert.equal(env.context.pwned, undefined); assert.equal(env.element('status-title').textContent, 'Приложение недоступно');
    assert.equal(env.calls.length, 0);
  }
});

test('boot clears fragment before its only POST and proves the exact newly issued cookie before navigation', async () => {
  const env = browser(renderBootPage({ shellUrl, nonce }), { responses: [
    response({ ok: true, entryPath: '/project?tab=notes&tag=%23one#/dashboard', sessionCheck }), response({ ok: true, sessionCheck }),
  ] });
  assert.deepEqual(env.events.slice(0, 2), [['cleanup', '/_soty/boot?path=%2Funtrusted-query'], ['fetch', 'POST']]);
  assert.equal(env.context.location.hash, '');
  await flush();
  assert.deepEqual(env.calls.map(call => call.options.method), ['POST', 'GET']);
  assert.deepEqual(JSON.parse(env.calls[0].options.body), { ticket });
  assert.equal(env.calls[1].options.headers['X-Soty-Boot-Check'], sessionCheck);
  assert.equal(env.calls[1].options.body, undefined);
  for (const call of env.calls) {
    assert.equal(call.url, '/_soty/session'); assert.equal(call.options.credentials, 'same-origin');
    assert.equal(call.options.cache, 'no-store'); assert.equal(call.options.redirect, 'error'); assert.ok(call.options.signal);
  }
  assert.deepEqual(env.navigations, ['/project?tab=notes&tag=%23one#/dashboard']); assert.equal(env.timers.size, 0);
  assert.equal(env.element('status-title').textContent, 'Открываем приложение');
});

test('successful POST alone, bare GET and a different session check never count as confirmed entry', async () => {
  for (const checked of [{ ok: true }, { ok: true, sessionCheck: 'X'.repeat(43) }, { ok: false, error: 'constructor' }]) {
    const env = browser(renderBootPage({ shellUrl }), { responses: [response({ ok: true, entryPath: '/', sessionCheck }), response(checked)] });
    await flush();
    assert.equal(env.navigations.length, 0); assert.equal(env.calls.length, 2);
    assert.equal(env.element('status-title').textContent, 'Вход не подтвердился');
    assert.equal(env.element('shell-action').href, shellUrl); assert.equal(env.element('shell-action').hidden, false);
    await flush(); assert.equal(env.calls.length, 2, 'no automatic POST retry or reload');
  }
  const held = pending();
  const env = browser(renderBootPage({ shellUrl }), { responses: [response({ ok: true, entryPath: '/', sessionCheck }), () => held.promise] });
  await flush(); assert.equal(env.calls.length, 2); assert.equal(env.navigations.length, 0);
  held.resolve(response({ ok: true, sessionCheck })); await flush(); assert.deepEqual(env.navigations, ['/']);
});

test('untrusted successful response cannot navigate to another origin or the reserved control namespace', async () => {
  for (const entryPath of ['https://evil.example/', '//evil.example/', '/%2f%2fevil.example/', '/x/../_soty/session', '/%5fsoty/session', '/\\evil', '/%00',
    '/x/..//double', '/%2F_soty/session', '/x%2f..%2f_soty/session', '/x%2f..%2f%2fdouble', '/x%23/../_soty/session', '/bad%00/../safe']) {
    const env = browser(renderBootPage({ shellUrl }), { responses: [response({ ok: true, entryPath, sessionCheck })] });
    await flush(); assert.equal(env.calls.length, 1); assert.deepEqual(env.navigations, []);
  }
});

test('invalid ticket or failed fragment cleanup makes zero requests and offers only fresh top-level recovery', async () => {
  for (const options of [{ hash: '' }, { hash: '#bad' }, { cleanThrows: true }]) {
    const env = browser(renderBootPage({ shellUrl }), options); await flush();
    assert.equal(env.calls.length, 0); assert.equal(env.navigations.length, 0);
    assert.equal(env.element('status-title').textContent, 'Нужно открыть заново');
    assert.equal(env.element('shell-action').href, shellUrl);
  }
});

test('iframe errors never load shell, navigate top, open a popup or retry a consumed ticket', async () => {
  const env = browser(renderBootPage({ shellUrl }), { framed: true, responses: [response({ ok: false, error: 'app_ticket_invalid' }, false)] });
  await flush();
  assert.equal(env.element('shell-action').hidden, true); assert.equal(env.element('shell-action').href, undefined);
  assert.equal(env.element('frame-hint').hidden, false); assert.equal(env.element('actions').hidden, true);
  assert.deepEqual(env.navigations, []); assert.equal(env.calls.length, 1);
  const page = renderStatusPage({ error: 'app_offline', shellUrl });
  assert.ok(page.includes('Откройте отдельно кнопкой над приложением.'));
  assert.doesNotMatch(page, /target="_top"|target="_blank"|window\.open|top\.location|location\.reload/u);
});

test('framed recovery identifies its layout without hiding the public action or the outer-toolbar hint', () => {
  const page = renderStatusPage({ error: 'app_access_expired', shellUrl, publicResetPath: '/' });
  const framed = browser(page, { framed: true });
  assert.equal(framed.context.document.body.dataset.framed, 'true');
  assert.equal(framed.element('actions').hidden, false);
  assert.equal(framed.element('reset-action').hidden, false);
  assert.equal(framed.element('reset-action').disabled, false);
  assert.equal(framed.element('frame-hint').hidden, false);
  assert.equal(framed.element('shell-action').href, undefined);
  assert.equal(framed.calls.length, 0);
  const separate = browser(page);
  assert.equal(separate.context.document.body.dataset.framed, 'false');
  assert.equal(separate.element('shell-action').href, shellUrl);
  assert.equal(separate.element('frame-hint').hidden, true);
});

test('changed access with permitted public recovery is not presented as a permanent denial', async () => {
  const page = renderStatusPage({ error: 'app_access_changed', shellUrl, publicResetPath: '/' });
  assert.match(page, /<h1 id="status-title">Доступ изменился<\/h1>/u);
  const status = browser(page, { framed: true });
  assert.equal(status.element('status-title').textContent, 'Доступ изменился');
  assert.equal(status.element('status-detail').textContent, 'Откройте приложение заново.');
  assert.equal(status.element('reset-action').hidden, false);
  for (const publicResetPath of ['/', undefined]) {
    const env = browser(renderBootPage({ shellUrl, publicResetPath }), { framed: true, responses: [
      response({ ok: true, entryPath: '/', sessionCheck }), response({ ok: false, error: 'app_access_changed' }, false),
    ] });
    await flush();
    assert.equal(env.element('status-title').textContent, publicResetPath ? 'Доступ изменился' : 'Доступ закрыт');
    assert.equal(env.element('reset-action').hidden, !publicResetPath);
    assert.equal(env.calls.length, 2); assert.deepEqual(env.navigations, []);
  }
});

test('timeout aborts the request without retrying POST or discarding error recovery', async () => {
  const env = browser(renderBootPage({ shellUrl }), { responses: [(_url, options) => new Promise((_resolve, reject) =>
    options.signal.addEventListener('abort', () => reject(new Error('timeout'))))] });
  assert.equal(env.timers.size, 1); assert.equal([...env.timers.values()][0].delay, 10_000);
  env.expire(); await flush();
  assert.equal(env.calls.length, 1); assert.equal(env.calls[0].options.signal.aborted, true); assert.equal(env.timers.size, 0);
  assert.equal(env.element('status-title').textContent, 'Не удалось подключиться'); assert.equal(env.navigations.length, 0);
});

test('public reset requires explicit action, one DELETE acknowledgement and one local navigation, even on double click', async () => {
  const held = pending();
  const env = browser(renderStatusPage({ error: 'app_access_expired', shellUrl, publicResetPath: '/project?q=one#/dashboard' }),
    { framed: true, responses: [() => held.promise] });
  assert.equal(env.calls.length, 0); assert.equal(env.element('reset-action').hidden, false);
  const first = env.click('reset-action'), second = env.click('reset-action');
  assert.equal(env.calls.length, 1); assert.equal(env.calls[0].options.method, 'DELETE');
  assert.equal(env.calls[0].options.headers.Origin, undefined, 'the browser supplies its actual origin');
  assert.equal(env.element('reset-action').disabled, true); assert.equal(env.element('reset-action').hidden, false);
  assert.deepEqual(env.navigations, []);
  held.resolve(response({ ok: true })); await Promise.all([first, second]);
  assert.deepEqual(env.navigations, ['/project?q=one#/dashboard']);
  await env.click('reset-action'); assert.equal(env.calls.length, 1);
});

test('failed public reset stays on a recoverable page and never clears session automatically on an error', async () => {
  const env = browser(renderStatusPage({ error: 'apps_access_denied', shellUrl, publicResetPath: '/' }),
    { responses: [response({ ok: false, error: 'apps_access_denied' }, false), response({ ok: true })] });
  assert.equal(env.calls.length, 0);
  await env.click('reset-action'); assert.equal(env.calls.length, 1); assert.deepEqual(env.navigations, []);
  assert.equal(env.element('status-title').textContent, 'Доступ закрыт'); assert.equal(env.element('reset-action').disabled, false);
  await env.click('reset-action'); assert.equal(env.calls.length, 2); assert.deepEqual(env.navigations, ['/']);
  const privatePage = browser(renderStatusPage({ error: 'apps_access_denied', shellUrl }));
  await privatePage.click('reset-action'); assert.equal(privatePage.calls.length, 0); assert.equal(privatePage.element('reset-action').hidden, true);
});

test('leaving or restoring the page cannot complete a stale request or reuse a ticket from browser history', async () => {
  const late = pending(), reset = pending();
  const env = browser(renderBootPage({ shellUrl, publicResetPath: '/' }), { responses: [() => late.promise, () => reset.promise] });
  env.emit('pagehide'); assert.equal(env.calls[0].options.signal.aborted, true);
  env.emit('pageshow', { persisted: true });
  assert.equal(env.element('status-title').textContent, 'Нужно открыть заново');
  const firstReset = env.click('reset-action');
  late.resolve(response({ ok: true, entryPath: '/stale', sessionCheck })); await flush();
  await env.click('reset-action'); assert.equal(env.calls.length, 2, 'late boot must not clear the new action lock');
  assert.deepEqual(env.navigations, []);
  reset.resolve(response({ ok: true })); await firstReset;
  assert.deepEqual(env.navigations, ['/']);
});

test('status pages disclose only allowlisted human states for capacity and availability failures', async () => {
  for (const [error, expected] of [[429, 'Сейчас много подключений'], [503, 'Приложение недоступно'], ['app_offline', 'Устройство не в сети'],
    ['app_address_retired', 'Адрес недоступен'], ['totally_internal_failure', 'Приложение недоступно']]) {
    const env = browser(renderStatusPage({ error })); await flush();
    assert.equal(env.element('status-title').textContent, expected); assert.equal(env.calls.length, 0);
    assert.equal(env.element('shell-action').hidden, true); assert.equal(env.element('reset-action').hidden, true);
  }
});

test('an HTTP failure at admission is distinguished from a network failure without exposing its raw code', async () => {
  const env = browser(renderBootPage({ shellUrl }), { responses: [response({ ok: false, error: 'private_database_detail' }, false)] });
  await flush();
  assert.equal(env.element('status-title').textContent, 'Приложение недоступно');
  assert.equal(env.element('status-detail').textContent.includes('private_database_detail'), false);
  assert.equal(env.calls.length, 1); assert.deepEqual(env.navigations, []);
});
