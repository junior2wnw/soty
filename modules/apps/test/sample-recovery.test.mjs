import test from 'node:test';
import assert from 'node:assert/strict';
import vm from 'node:vm';
import { createServer, request } from 'node:http';
import { WebSocket } from 'ws';
import { createSampleApp } from '../examples/sample-app.mjs';

let assets;
test.before(async () => {
  const source = await createSampleApp();
  try {
    const origin = `http://127.0.0.1:${source.port}`;
    const [html, script, css] = await Promise.all(['/', '/app.js', '/styles.css'].map(async path => {
      const response = await fetch(origin + path); assert.equal(response.status, 200); return response.text();
    }));
    assets = { html, script, css };
  } finally { await source.close(); }
});

const flush = () => new Promise(resolve => setImmediate(resolve));
const INSTANCE = 'a'.repeat(32), OTHER_INSTANCE = 'b'.repeat(32);
const sample = (text = 'Synthetic list', done = false, revision = 0, instance = INSTANCE) => ({ instance, revision, items: [{ id: 'synthetic-item', text, done }] });
const response = (value, status = 200) => ({ ok: status >= 200 && status < 300, status, text: async () => JSON.stringify(value) });
const blocked = element => element.getAttribute('aria-disabled') === 'true';
async function until(check) {
  const deadline = Date.now() + 3000;
  while (!check()) {
    assert.ok(Date.now() < deadline, 'bounded fixture condition');
    await new Promise(resolve => setTimeout(resolve, 5));
  }
}

// DOM surface only: the controller is always the script actually returned by the sample HTTP server.
function browser(t, { fetchImpl, realWebSocket = false, obeyAbort = true, origin = 'http://synthetic.invalid' } = {}) {
  const elements = new Map(), sockets = [], calls = [], timers = new Map(), windowEvents = new Map();
  let clock = 0, timerId = 0;
  const document = { activeElement: null, body: null };
  class Element {
    constructor(tag) { this.tagName = tag.toUpperCase(); this.children = []; this.parentElement = null; this.listeners = new Map();
      this.attributes = new Map(); this.hidden = false; this._disabled = false; this.value = ''; this.checked = false; this.ownText = ''; this.selectionStart = 0; this.selectionEnd = 0; }
    get disabled() { return this._disabled; }
    set disabled(value) { this._disabled = value; if (value && document.activeElement === this) document.activeElement = document.body; }
    get isConnected() { let value = this; while (value) { if (value === document.body) return true; value = value.parentElement; } return false; }
    contains(value) { while (value) { if (value === this) return true; value = value.parentElement; } return false; }
    get textContent() { return this.ownText + this.children.map(child => child.textContent).join(''); }
    set textContent(value) { this.ownText = String(value); for (const child of [...this.children]) child.remove(); }
    setAttribute(key, value) { this.attributes.set(key, String(value)); if (key === 'id') elements.set(value, this);
      if (key === 'hidden' || key === 'disabled' || key === 'readonly') this[key === 'readonly' ? 'readOnly' : key] = true;
      if (key === 'type') this.type = value; }
    getAttribute(key) { return this.attributes.get(key) ?? null; }
    append(...values) { for (const value of values) this.insertBefore(value, null); }
    insertBefore(value, before) {
      if (value.parentElement) { const old = value.parentElement.children; old.splice(old.indexOf(value), 1); }
      const index = before === null ? this.children.length : this.children.indexOf(before);
      assert.ok(index >= 0); this.children.splice(index, 0, value); value.parentElement = this;
    }
    remove() { if (this.contains(document.activeElement)) document.activeElement = document.body;
      if (this.parentElement) { const old = this.parentElement.children; old.splice(old.indexOf(this), 1); this.parentElement = null; } }
    addEventListener(type, callback) { const values = this.listeners.get(type) ?? []; values.push(callback); this.listeners.set(type, values); }
    focus() { if (this.isConnected && !this.disabled) document.activeElement = this; }
    dispatch(type, values = {}, force = false) {
      if (this.disabled && !force) return;
      const event = { target: this, defaultPrevented: false, preventDefault() { this.defaultPrevented = true; }, ...values };
      for (const callback of this.listeners.get(type) ?? []) callback(event);
      return event;
    }
  }
  document.body = new Element('body'); document.activeElement = document.body;
  const stack = [document.body], voidTags = new Set(['meta', 'link', 'input', 'br']);
  for (const token of assets.html.matchAll(/<\/?([a-z][a-z0-9]*)([^>]*)>|([^<]+)/giu)) {
    if (!token[1]) { stack.at(-1).ownText += token[3] ?? ''; continue; }
    const name = token[1].toLowerCase();
    if (token[0].startsWith('</')) { if (stack.length > 1 && stack.at(-1).tagName === name.toUpperCase()) stack.pop(); continue; }
    const element = new Element(name); stack.at(-1).append(element);
    for (const attribute of token[2].matchAll(/([\w-]+)(?:="([^"]*)")?/gu)) element.setAttribute(attribute[1], attribute[2] ?? '');
    if (!voidTags.has(name)) stack.push(element);
  }
  document.createElement = tag => new Element(tag);
  document.querySelector = selector => selector.startsWith('#') ? elements.get(selector.slice(1)) : find(document.body, selector.toUpperCase());
  function find(element, tag) { if (element.tagName === tag) return element; for (const child of element.children) { const found = find(child, tag); if (found) return found; } return null; }
  class ControlledSocket {
    constructor(url) { this.url = url; this.readyState = 0; this.closeCalls = 0; sockets.push(this); }
    open() { this.readyState = 1; this.onopen?.({}); }
    message(items) { this.onmessage?.({ data: JSON.stringify(items) }); }
    remoteClose() { this.readyState = 3; this.onclose?.({ code: 1006 }); }
    close() { this.closeCalls++; this.readyState = 3; this.onclose?.({ code: 1000 }); }
  }
  class ObservedSocket extends WebSocket { constructor(url) { super(url); sockets.push(this); } }
  const env = {
    document, window: { addEventListener(type, callback) { windowEvents.set(type, callback); } },
    location: { origin }, TextEncoder, AbortController,
    WebSocket: realWebSocket ? ObservedSocket : ControlledSocket,
    setTimeout(callback, delay) { const id = ++timerId; timers.set(id, { callback, at: clock + delay, delay }); return id; },
    clearTimeout(id) { timers.delete(id); },
    fetch(url, options = {}) {
      const call = { url, options }; calls.push(call);
      if (fetchImpl) return fetchImpl(url, options);
      const promise = new Promise((resolve, reject) => { call.resolve = resolve; call.reject = reject; });
      if (obeyAbort) {
        if (options.signal?.aborted) call.reject(new Error('fixture_aborted'));
        else options.signal?.addEventListener('abort', () => call.reject(new Error('fixture_aborted')), { once: true });
      }
      return promise;
    }
  };
  vm.runInNewContext(assets.script, env, { timeout: 1000 });
  const id = name => { assert.ok(elements.has(name), `emitted HTML includes ${name}`); return elements.get(name); };
  const handle = { env, id, calls, sockets, timers, document,
    get socket() { return sockets.at(-1); },
    get posts() { return calls.filter(call => call.options.method === 'POST'); },
    get check() { return id('items').children[0]?.children[0]; },
    get listText() { return id('items').textContent; },
    draft(value, start = value.length, end = start) { id('item').value = value; id('item').selectionStart = start; id('item').selectionEnd = end; id('item').dispatch('input'); },
    submit() { return document.querySelector('form').dispatch('submit'); },
    click(name, force = false) { return id(name).dispatch('click', {}, force); },
    ready(items = sample()) { this.socket.open(); this.socket.message(items); },
    advance(ms) { const end = clock + ms; for (;;) { const next = [...timers.entries()].filter(([, value]) => value.at <= end).sort((a, b) => a[1].at - b[1].at)[0]; if (!next) break;
      clock = next[1].at; timers.delete(next[0]); next[1].callback(); } clock = end; },
    emit(type, event = {}) { windowEvents.get(type)?.(event); },
  };
  t.after(() => handle.emit('pagehide'));
  return handle;
}

test('emitted page has separate read-only recovery and waits for a valid WS snapshot before writes', async t => {
  const h = browser(t);
  assert.equal(h.id('reconnect').type, 'button'); assert.equal(h.id('reviewed').type, 'button');
  assert.equal(h.id('add').type, 'submit'); assert.equal(h.id('attempt').readOnly, true);
  assert.equal(h.id('item').getAttribute('aria-describedby'), 'draft-hint');
  assert.equal(h.id('status').getAttribute('role'), 'status');
  assert.equal(h.calls.length, 1); assert.equal(h.calls[0].options.method, 'GET');
  h.socket.open(); assert.equal(blocked(h.id('add')), true);
  h.calls[0].resolve(response(sample('Read preview'))); await flush();
  assert.equal(h.listText, 'Read preview'); assert.equal(blocked(h.id('add')), true);
  h.socket.message(sample('Live snapshot'));
  assert.equal(blocked(h.id('add')), false); assert.equal(h.timers.size, 0); assert.equal(h.posts.length, 0);
});

test('unsent draft, selection and focused input survive close and explicit reconnect with no POST', async t => {
  const h = browser(t, { obeyAbort: false }); h.ready();
  h.draft('Synthetic unsent 🌿', 2, 8); h.id('item').focus();
  const old = h.socket, lateMessage = old.onmessage, lateClose = old.onclose, input = h.id('item');
  old.remoteClose(); assert.equal(input.disabled, false); assert.equal(blocked(h.id('add')), true);
  h.click('reconnect'); h.click('reconnect', true); assert.equal(h.sockets.length, 2);
  h.ready(sample('Fresh list')); h.calls[0].resolve(response(sample('Old GET'))); await flush();
  lateMessage({ data: JSON.stringify(sample('Old WS')) }); lateClose({});
  assert.equal(h.listText, 'Fresh list'); assert.equal(blocked(h.id('add')), false);
  assert.equal(input.value, 'Synthetic unsent 🌿'); assert.equal(input.selectionStart, 2); assert.equal(input.selectionEnd, 8);
  assert.equal(h.document.activeElement, input); assert.equal(h.posts.length, 0); assert.equal(old.onmessage, null);
});

test('confirmed captured POST clears only its original draft and never posts twice while pending', async t => {
  const h = browser(t); h.ready(); h.draft('Original synthetic'); h.submit(); h.submit();
  assert.equal(h.posts.length, 1); assert.equal(h.id('item').value, 'Original synthetic'); assert.equal(h.id('item').disabled, false);
  assert.deepEqual(JSON.parse(h.posts[0].options.body), { text: 'Original synthetic' });
  h.socket.message(sample('Current list after accepted write', false, 2));
  h.posts[0].resolve(response(sample('POST snapshot', false, 1))); await flush();
  assert.equal(h.id('item').value, ''); assert.match(h.id('outcome').textContent, /Добавлено/u);
  assert.equal(h.listText, 'Current list after accepted write'); assert.equal(blocked(h.id('add')), false);
});

test('late ACK never clears newer typing, including A to B to A draft ABA', async t => {
  for (const edits of [['B'], ['B', 'A']]) {
    const h = browser(t); h.ready(); h.draft('A'); h.submit();
    for (const text of edits) h.draft(text);
    h.id('item').focus(); h.id('item').selectionStart = 0; h.id('item').selectionEnd = 1;
    h.socket.message(sample('Changed while pending', false, 2));
    h.posts[0].resolve(response(sample('Earlier response', false, 1))); await flush();
    assert.equal(h.id('item').value, edits.at(-1)); assert.equal(h.id('item').selectionEnd, 1); assert.equal(h.document.activeElement, h.id('item'));
  }
});

test('unknown preserves original A separately from draft B and requires fresh read plus explicit review', async t => {
  const h = browser(t); h.ready(); h.draft('A private synthetic'); h.submit(); h.draft('B later draft');
  h.posts[0].reject(new Error('lost_response')); await flush();
  assert.equal(h.id('attempt').value, 'A private synthetic'); assert.equal(h.id('item').value, 'B later draft');
  assert.equal(h.id('uncertain').hidden, false); assert.equal(blocked(h.id('reviewed')), true);
  h.socket.message(sample('A live update alone is not a new recovery read'));
  assert.equal(blocked(h.id('reviewed')), true);
  h.click('reviewed', true); h.submit(); assert.equal(h.posts.length, 1);
  h.click('reconnect'); h.ready(sample('The first write may be here'));
  assert.equal(h.id('uncertain').hidden, false); assert.equal(blocked(h.id('add')), true); assert.equal(h.posts.length, 1);
  h.click('reviewed'); assert.equal(h.id('item').value, 'B later draft'); assert.equal(h.document.activeElement, h.id('item'));
  assert.equal(h.id('add').textContent, 'Добавить как новое'); assert.match(h.id('outcome').textContent, /может повторить/u);
  h.submit(); assert.equal(h.posts.length, 2); assert.deepEqual(JSON.parse(h.posts[1].options.body), { text: 'B later draft' });
});

test('validation before fetch is not-sent; HTTP errors and malformed responses after dispatch are unknown', async t => {
  const local = browser(t); local.ready(); local.draft('  '); local.submit();
  assert.equal(local.posts.length, 0); assert.match(local.id('outcome').textContent, /Не отправлено/u);
  local.draft('x'.repeat(101)); local.submit(); assert.equal(local.posts.length, 0);
  for (const result of [response([], 403), response([], 500), { ok: true, status: 200, text: async () => '<not-json>' }, response({ items: [] }), response([{ id: 'x', text: 'x', done: 'false' }])]) {
    const h = browser(t); h.ready(); h.draft('Keep this synthetic'); h.submit();
    h.posts[0].resolve(result); await flush();
    assert.equal(h.id('item').value, 'Keep this synthetic'); assert.equal(h.id('uncertain').hidden, false);
    assert.match(h.id('outcome').textContent, /могло сохраниться/u); assert.equal(h.posts.length, 1);
  }
});

test('finite connection and POST deadlines abort work and ignore late successful callbacks', async t => {
  const connecting = browser(t, { obeyAbort: false }), stale = connecting.socket.onmessage;
  connecting.advance(12000); await flush();
  assert.equal(connecting.calls[0].options.signal.aborted, true); assert.equal(blocked(connecting.id('reconnect')), false);
  stale({ data: JSON.stringify(sample()) }); assert.equal(blocked(connecting.id('add')), true); assert.equal(connecting.timers.size, 0);
  const h = browser(t, { obeyAbort: false }); h.ready(); h.draft('Timeout draft'); h.submit();
  h.advance(12000); assert.equal(h.posts[0].options.signal.aborted, true); assert.equal(h.id('uncertain').hidden, false);
  h.posts[0].resolve(response(sample())); await flush();
  assert.equal(h.id('item').value, 'Timeout draft'); assert.equal(h.id('uncertain').hidden, false); assert.equal(h.timers.size, 0);
});

test('POST ACK is independent from connection close/reconnect and cannot replace the new connection snapshot', async t => {
  const h = browser(t); h.ready(); h.draft('Confirmed across disconnect'); h.submit();
  h.socket.remoteClose(); h.click('reconnect'); h.ready(sample('New socket state', false, 2));
  h.posts[0].resolve(response(sample('Old captured response', false, 1))); await flush();
  assert.equal(h.id('item').value, ''); assert.equal(h.listText, 'New socket state'); assert.equal(blocked(h.id('add')), false);
  const closed = browser(t); closed.ready(); closed.draft('Confirmed but offline'); closed.submit(); closed.socket.remoteClose();
  closed.posts[0].resolve(response(sample('Accepted offline', false, 1))); await flush();
  assert.equal(closed.id('item').value, ''); assert.match(closed.id('outcome').textContent, /Добавлено/u);
  assert.match(closed.id('status').textContent, /Связь прервалась/u); assert.equal(blocked(closed.id('add')), true);
});

test('keyed rows retain keyboard focus and successful POST waits for a later list snapshot before another write', async t => {
  const h = browser(t); h.ready(); const check = h.check; check.focus();
  h.socket.message(sample('Same item, fresh text')); assert.equal(h.check, check); assert.equal(h.document.activeElement, check);
  check.checked = true; check.dispatch('change'); check.dispatch('change', {}, true);
  assert.equal(h.posts.length, 1); assert.deepEqual(JSON.parse(h.posts[0].options.body), { id: 'synthetic-item' });
  h.posts[0].resolve(response(sample('Accepted', true, 1))); await flush();
  assert.equal(blocked(check), true); h.submit(); assert.equal(h.posts.length, 1); assert.match(h.id('outcome').textContent, /Ждём/u);
  h.socket.message(sample('Authoritative changed item', true, 1)); assert.equal(h.check, check); assert.equal(check.checked, true); assert.equal(blocked(check), false);
  assert.doesNotMatch(h.id('outcome').textContent, /Ждём/u);
});

test('temporarily unavailable controls remain focusable and synchronous handlers refuse duplicate actions', async t => {
  const h = browser(t); h.ready();
  const check = h.check; check.focus(); check.checked = true; check.dispatch('change');
  assert.equal(h.document.activeElement, check); assert.equal(check.disabled, false); assert.equal(blocked(check), true);
  assert.equal(check.dispatch('keydown', { key: ' ' }).defaultPrevented, true);
  assert.equal(check.dispatch('click').defaultPrevented, true);
  assert.equal(h.id('add').dispatch('click').defaultPrevented, true, 'pending submit does not trigger native form validation');
  check.dispatch('change'); h.submit(); assert.equal(h.posts.length, 1);
  h.posts[0].resolve(response(sample('Accepted checkbox', true, 1))); await flush();
  assert.equal(h.document.activeElement, check); assert.equal(blocked(check), true);
  h.socket.message(sample('Accepted checkbox', true, 1));
  assert.equal(h.document.activeElement, check); assert.equal(blocked(check), false);

  h.id('reconnect').focus(); h.click('reconnect');
  assert.equal(h.document.activeElement, h.id('reconnect')); assert.equal(h.id('reconnect').disabled, false);
  assert.equal(blocked(h.id('reconnect')), true); h.click('reconnect'); assert.equal(h.sockets.length, 2);
  h.ready(sample('Same current state', true, 1));
  assert.equal(h.document.activeElement, h.id('reconnect')); assert.equal(blocked(h.id('reconnect')), false);
  assert.equal(h.posts.length, 1);
});

test('only a same-instance snapshot at least as new as the ACK clears its barrier and older snapshots cannot roll back the list', async t => {
  const h = browser(t); h.ready(); h.check.dispatch('change');
  h.socket.message(sample('Other writer before our commit', false, 1));
  h.posts[0].resolve(response(sample('Our committed result', true, 2))); await flush();
  assert.equal(h.check.checked, false); assert.equal(blocked(h.check), true);
  h.socket.message(sample('An older delayed frame', false, 0));
  assert.equal(h.listText, 'Other writer before our commit'); assert.equal(blocked(h.check), true);
  h.socket.message(sample('A precommit frame delivered after ACK', false, 1));
  h.check.dispatch('change'); assert.equal(h.posts.length, 1); assert.equal(blocked(h.check), true);
  h.socket.message(sample('Our committed result', true, 2));
  assert.equal(h.check.checked, true); assert.equal(blocked(h.check), false);
  h.socket.message(sample('Late prior frame cannot undo the UI', false, 1));
  assert.equal(h.check.checked, true); assert.equal(h.listText, 'Our committed result');
  h.socket.message(sample('A genuinely later change', false, 3));
  assert.equal(h.check.checked, false); assert.equal(blocked(h.check), false); assert.equal(h.posts.length, 1);
});

test('a confirmed ACK from a different server instance requires fresh explicit recovery and is never called an unknown POST', async t => {
  const h = browser(t); h.ready(sample('Earlier lifetime', false, 40)); h.draft('Accepted in A'); h.submit(); h.draft('Keep newer draft B');
  h.click('reconnect'); h.ready(sample('New lifetime', false, 100, OTHER_INSTANCE));
  h.posts[0].resolve(response(sample('Old instance ACK', false, 41))); await flush();
  assert.match(h.id('outcome').textContent, /Изменение принято.*разным запускам/u);
  assert.doesNotMatch(h.id('outcome').textContent, /Не удалось подтвердить/u);
  assert.equal(h.id('item').value, 'Keep newer draft B'); assert.equal(h.id('attempt').value, 'Accepted in A');
  assert.equal(h.listText, 'New lifetime'); assert.equal(blocked(h.id('add')), true);
  h.click('reviewed'); h.socket.message(sample('Another current frame', false, 101, OTHER_INSTANCE));
  h.click('reviewed'); h.submit(); assert.equal(h.posts.length, 1); assert.equal(h.id('uncertain').hidden, false);
  h.click('reconnect'); h.ready(sample('Explicit read of the new lifetime', false, 101, OTHER_INSTANCE));
  assert.equal(blocked(h.id('reviewed')), false); h.click('reviewed');
  assert.equal(blocked(h.id('add')), false); assert.equal(h.id('item').value, 'Keep newer draft B'); assert.equal(h.posts.length, 1);
});

test('snapshot identities and revisions are strict scalars and a nonadvancing POST ACK cannot clear its draft', async t => {
  for (const invalid of [sample().items, { ...sample(), instance: [INSTANCE] }, { ...sample(), revision: '1' },
    { ...sample(), revision: -1 }, { ...sample(), revision: 0.5 }, { ...sample(), revision: Number.MAX_SAFE_INTEGER + 1 }]) {
    const h = browser(t); h.ready(invalid); assert.equal(blocked(h.id('add')), true); assert.equal(h.posts.length, 0);
  }
  const h = browser(t); h.ready(sample('Initial', false, 3)); h.draft('Must remain'); h.submit();
  h.posts[0].resolve(response(sample('Not an applied mutation', false, 3))); await flush();
  assert.equal(h.id('item').value, 'Must remain'); assert.equal(h.id('uncertain').hidden, false);
  assert.equal(blocked(h.id('add')), true);
  const switched = browser(t); switched.ready(); switched.socket.message(sample('Invalid in-socket restart', false, 100, OTHER_INSTANCE));
  assert.equal(blocked(switched.id('add')), true); assert.match(switched.id('status').textContent, /Запуск приложения изменился/u);
});

test('unknown toggle is never repeated by recovery and cannot mutate again until the list is reviewed', async t => {
  const h = browser(t); h.ready(); h.check.dispatch('change'); h.posts[0].reject(new Error('drop')); await flush();
  assert.equal(h.id('attempt').hidden, true); h.check.dispatch('change', {}, true); assert.equal(h.posts.length, 1);
  h.click('reconnect'); h.ready(sample('Toggled at origin', true)); assert.equal(h.posts.length, 1);
  h.click('reviewed'); assert.equal(h.check.checked, true); assert.equal(h.posts.length, 1);
});

test('pagehide closes transports and preserves pending unknown; BFCache return never automatically resends or reconnects', async t => {
  const h = browser(t, { obeyAbort: false }); h.ready(); h.draft('Keep on page return'); h.submit();
  h.emit('pagehide'); assert.equal(h.posts[0].options.signal.aborted, true); assert.equal(h.timers.size, 0);
  h.emit('pageshow', { persisted: true }); h.posts[0].resolve(response(sample())); await flush();
  assert.equal(h.id('item').value, 'Keep on page return'); assert.equal(h.id('uncertain').hidden, false);
  assert.equal(h.sockets.length, 1); assert.equal(h.posts.length, 1); assert.equal(blocked(h.id('reconnect')), false);
});

test('read access failure and malformed WS snapshots keep draft usable and expose no automatic write/navigation', async t => {
  const h = browser(t); h.draft('Copy before reopen'); h.calls[0].resolve(response({}, 403)); await flush();
  assert.match(h.id('status').textContent, /Откройте его заново/u); assert.equal(h.id('item').disabled, false);
  assert.equal(h.id('item').value, 'Copy before reopen'); assert.equal(h.posts.length, 0); assert.equal(h.timers.size, 0);
  h.click('reconnect'); h.ready(sample()); h.socket.onmessage({ data: JSON.stringify({ ...sample(), items: [...sample().items, ...sample().items] }) });
  assert.equal(blocked(h.id('add')), true); assert.equal(h.id('item').value, 'Copy before reopen'); assert.equal(h.posts.length, 0);
});

test('repeated explicit recoveries keep at most one live socket/deadline and never enqueue writes', async t => {
  const h = browser(t);
  for (let index = 0; index < 40; index++) {
    h.ready(); h.socket.remoteClose(); h.click('reconnect');
    assert.equal(h.timers.size, 1); assert.equal(h.sockets.filter(socket => socket.readyState !== 3).length, 1);
    assert.ok(h.sockets.slice(0, -1).every(socket => socket.onmessage === null));
    await flush();
  }
  h.ready(); assert.equal(h.timers.size, 0); assert.equal(h.posts.length, 0);
});

test('actual HTTP commit with a deliberately lost response is unknown and read-only recovery creates no duplicate or inverse toggle', async t => {
  const source = await createSampleApp(); let postCount = 0, dropResponse = true;
  const origin = `http://127.0.0.1:${source.port}`;
  const proxy = createServer((req, res) => {
    if (req.method === 'POST') postCount++;
    const upstream = request(origin + req.url, { method: req.method, headers: { 'content-type': 'application/json' } }, reply => {
      const chunks = []; reply.on('data', chunk => chunks.push(chunk));
      reply.on('end', () => {
        if (req.method === 'POST' && dropResponse) { res.destroy(); return; }
        res.writeHead(reply.statusCode, { 'content-type': 'application/json' }); res.end(Buffer.concat(chunks));
      });
    });
    upstream.on('error', () => res.destroy()); req.pipe(upstream);
  });
  await new Promise(resolve => proxy.listen(0, '127.0.0.1', resolve));
  const proxyOrigin = `http://127.0.0.1:${proxy.address().port}`;
  t.after(async () => { proxy.closeAllConnections(); await new Promise(resolve => proxy.close(resolve)); await source.close(); });
  const h = browser(t, { origin: proxyOrigin, fetchImpl: (url, options) => fetch(proxyOrigin + url, options) });
  const read = async () => (await fetch(origin + '/api/items?snapshot=1')).json();
  h.ready(await read()); h.draft('Synthetic committed without ACK'); h.submit();
  await until(() => !h.id('uncertain').hidden);
  assert.equal((await read()).items.filter(item => item.text === 'Synthetic committed without ACK').length, 1);
  assert.equal(postCount, 1); assert.equal(h.id('item').value, 'Synthetic committed without ACK');
  h.click('reconnect'); h.ready(await read()); h.click('reviewed'); await flush();
  assert.equal(postCount, 1); assert.equal(h.id('add').textContent, 'Добавить как новое');
  h.check.dispatch('change'); await until(() => !h.id('uncertain').hidden);
  assert.equal((await read()).items[0].done, true); assert.equal(postCount, 2);
  h.click('reconnect'); h.ready(await read()); h.click('reviewed'); await flush();
  assert.equal((await read()).items[0].done, true); assert.equal(postCount, 2);
  dropResponse = false;
});

test('actual emitted controller restores a real loopback WebSocket without losing draft or issuing POST', async t => {
  const source = await createSampleApp(), origin = `http://127.0.0.1:${source.port}`;
  t.after(() => source.close());
  const h = browser(t, { realWebSocket: true, origin, fetchImpl: (url, options) => fetch(origin + url, options) });
  await until(() => !blocked(h.id('add')));
  h.draft('Synthetic actual socket draft', 1, 4); h.sockets[0].terminate();
  await until(() => !blocked(h.id('reconnect')) && blocked(h.id('add')));
  h.click('reconnect'); await until(() => !blocked(h.id('add')));
  assert.equal(h.sockets.length, 2); assert.equal(h.id('item').value, 'Synthetic actual socket draft');
  assert.equal(h.id('item').selectionStart, 1); assert.equal(h.id('item').selectionEnd, 4); assert.equal(h.posts.length, 0);
});

test('actual versioned HTTP ACK and WS share one revision, including legacy writers, while legacy array and echo remain compatible', async t => {
  const source = await createSampleApp(), other = await createSampleApp();
  const origin = `http://127.0.0.1:${source.port}`;
  const legacy = new WebSocket(origin.replace('http:', 'ws:') + '/live');
  const versioned = new WebSocket(origin.replace('http:', 'ws:') + '/live?snapshot=1');
  const message = socket => new Promise((resolve, reject) => { socket.once('message', bytes => resolve(bytes.toString())); socket.once('error', reject); });
  t.after(async () => { legacy.terminate(); versioned.terminate(); await source.close(); await other.close(); });
  const [legacyInitial, versionedInitial] = await Promise.all([message(legacy), message(versioned)]);
  const initial = JSON.parse(versionedInitial); assert.equal(initial.revision, 0); assert.match(initial.instance, /^[a-f0-9]{32}$/u);
  assert.deepEqual(initial.items, JSON.parse(legacyInitial));
  assert.notEqual((await (await fetch(`http://127.0.0.1:${other.port}/api/items?snapshot=1`)).json()).instance, initial.instance);
  const firstLegacy = message(legacy), firstVersioned = message(versioned);
  const legacyAck = await (await fetch(origin + '/api/items', { method: 'POST', body: JSON.stringify({ text: 'Legacy writer' }) })).json();
  const first = JSON.parse(await firstVersioned); assert.equal(first.revision, 1); assert.equal(first.instance, initial.instance);
  assert.deepEqual(first.items, legacyAck); assert.deepEqual(JSON.parse(await firstLegacy), legacyAck);
  const secondLegacy = message(legacy), secondVersioned = message(versioned);
  const ack = await (await fetch(origin + '/api/items?snapshot=1', { method: 'POST', body: JSON.stringify({ id: 'bread' }) })).json();
  assert.equal(ack.revision, 2); assert.equal(ack.items[0].done, true); assert.deepEqual(JSON.parse(await secondVersioned), ack);
  assert.deepEqual(JSON.parse(await secondLegacy), ack.items);
  const echoed = message(legacy); legacy.send('legacy transport probe'); assert.equal(await echoed, 'legacy transport probe');
});
