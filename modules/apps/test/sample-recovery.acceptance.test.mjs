import test from 'node:test';
import assert from 'node:assert/strict';
import vm from 'node:vm';
import { WebSocket } from 'ws';
import { createSampleApp } from '../examples/sample-app.mjs';

async function until(predicate, label) {
  const deadline = Date.now() + 3000;
  while (!predicate()) {
    assert.ok(Date.now() < deadline, label);
    await new Promise(resolve => setTimeout(resolve, 5));
  }
}

const unavailable = element => element.disabled || element.getAttribute('aria-disabled') === 'true';

// This is a controller seam, not a browser/focus simulation. The script and
// snapshots come from the real sample HTTP/WS server; only callback delivery
// and admission of the first POST are deliberately held.
async function harness(t) {
  const source = await createSampleApp();
  const origin = `http://127.0.0.1:${source.port}`;
  const html = await (await fetch(origin)).text();
  const script = await (await fetch(origin + '/app.js')).text();
  const elements = new Map(), windowEvents = new Map(), sockets = [];
  const document = { activeElement: null, body: null };
  class Element {
    constructor(tag) {
      this.tagName = tag.toUpperCase(); this.children = []; this.parentElement = null;
      this.listeners = new Map(); this.attributes = new Map(); this.disabled = false; this.hidden = false;
      this.value = ''; this.checked = false; this.textContent = '';
    }
    get isConnected() { return this === document.body || !!this.parentElement?.isConnected; }
    contains(node) { return node === this || this.children.some(child => child.contains(node)); }
    append(...nodes) { for (const node of nodes) this.insertBefore(node, null); }
    insertBefore(node, reference) {
      node.remove();
      const index = reference === null ? this.children.length : this.children.indexOf(reference);
      assert.ok(index >= 0); this.children.splice(index, 0, node); node.parentElement = this;
    }
    remove() {
      if (this.parentElement) this.parentElement.children.splice(this.parentElement.children.indexOf(this), 1);
      this.parentElement = null;
    }
    addEventListener(type, callback) { this.listeners.set(type, callback); }
    setAttribute(name, value) { this.attributes.set(name, String(value)); }
    getAttribute(name) { return this.attributes.get(name) ?? null; }
    emit(type) { this.listeners.get(type)?.({ preventDefault() {} }); }
    focus() { document.activeElement = this; }
  }
  document.body = new Element('body'); document.activeElement = document.body;
  for (const match of html.matchAll(/<([a-z]+)\b[^>]*\bid="([^"]+)"[^>]*>/giu)) {
    const element = new Element(match[1]); elements.set(match[2], element); document.body.append(element);
  }
  const form = new Element('form'); document.body.append(form);
  document.createElement = tag => new Element(tag);
  document.querySelector = selector => selector === 'form' ? form : elements.get(selector.slice(1));
  let heldPost = null, postCount = 0, holdSnapshots = false;
  const waitingSnapshots = [];
  class Socket {
    constructor(url) {
      this.inner = new WebSocket(url); sockets.push(this);
      this.inner.on('open', () => this.onopen?.({}));
      this.inner.on('close', () => this.onclose?.({}));
      this.inner.on('error', () => this.onerror?.({}));
      this.inner.on('message', bytes => {
        const deliver = () => this.onmessage?.({ data: bytes.toString() });
        if (holdSnapshots) waitingSnapshots.push(deliver); else deliver();
      });
    }
    close() { this.inner.close(); }
  }
  const env = {
    document, location: { origin }, TextEncoder, AbortController, WebSocket: Socket,
    window: { addEventListener: (type, callback) => windowEvents.set(type, callback) },
    setTimeout, clearTimeout,
    fetch(path, options) {
      if (options.method === 'POST' && ++postCount === 1) {
        return new Promise((resolve, reject) => {
          heldPost = () => fetch(origin + path, options).then(resolve, reject);
        });
      }
      return fetch(origin + path, options);
    },
  };
  t.after(async () => {
    windowEvents.get('pagehide')?.({});
    for (const socket of sockets) socket.inner.terminate();
    await source.close();
  });
  vm.runInNewContext(script, env, { timeout: 1000 });
  await until(() => elements.get('items').children.length === 2 && !unavailable(elements.get('add')), 'initial real WS snapshot');
  return {
    origin, elements,
    get check() { return elements.get('items').children[0].children[0]; },
    get posts() { return postCount; },
    get queuedSnapshots() { return waitingSnapshots.length; },
    async releasePost() { assert.ok(heldPost); await heldPost(); },
    holdSnapshots() { holdSnapshots = true; },
    releaseOneSnapshot() { const deliver = waitingSnapshots.shift(); assert.ok(deliver); deliver(); },
    releaseSnapshots() { holdSnapshots = false; for (const deliver of waitingSnapshots.splice(0)) deliver(); },
  };
}

async function causalRace(t, otherSnapshotDelivery) {
  const h = await harness(t);
  assert.equal(h.check.checked, false);
  h.check.checked = true; h.check.emit('change');
  assert.equal(h.posts, 1); assert.equal(unavailable(h.check), true);

  // A real second writer changes another item while the first POST has not
  // reached the server. This broadcast is newer than dispatch, not its result.
  if (otherSnapshotDelivery === 'after ACK') h.holdSnapshots();
  await fetch(h.origin + '/api/items', {
    method: 'POST', headers: { 'content-type': 'application/json' },
    body: JSON.stringify({ text: 'Independent writer before the held toggle' }),
  });
  if (otherSnapshotDelivery === 'after ACK') {
    await until(() => h.queuedSnapshots === 1, 'other writer broadcast queued before commit');
  } else {
    await until(() => h.elements.get('items').children.length === 3, 'other writer broadcast delivered');
  }
  assert.equal(h.check.checked, false);
  h.holdSnapshots();
  await h.releasePost();
  await until(() => h.elements.get('outcome').textContent.includes('Изменение принято'), 'specific POST ACK accepted');
  if (otherSnapshotDelivery === 'after ACK') {
    h.releaseOneSnapshot();
    assert.equal(h.elements.get('items').children.length, 3, 'the unrelated old snapshot arrives only after ACK');
  }
  const current = await (await fetch(h.origin + '/api/items')).json();
  assert.equal(current[0].done, true, 'the held toggle really committed');
  assert.equal(h.check.checked, false, 'its WS snapshot is still withheld');
  const exposedAsUnavailable = unavailable(h.check);
  h.check.checked = true; h.check.emit('change');
  assert.equal(h.posts, 1, 'a prior unrelated snapshot must not permit an inverse second toggle');
  assert.equal(exposedAsUnavailable, true, 'the blocked control communicates its state');

  h.releaseSnapshots();
  await until(() => h.check.checked === true && !unavailable(h.check), 'matching current source state restores writes');
  assert.equal(h.posts, 1, 'delivering snapshots does not repeat the mutation');
}

for (const delivery of ['before ACK', 'after ACK']) {
  test(`a different writer snapshot delivered ${delivery} cannot unlock an unobserved successful toggle`, t => causalRace(t, delivery));
}
