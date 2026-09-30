import { createServer } from 'node:http';
import { WebSocketServer } from 'ws';
import { resolve } from 'node:path';
import { fileURLToPath } from 'node:url';
import { randomBytes } from 'node:crypto';

// A genuinely separate local project used by transport/browser acceptance.
// It has no dependency on Soty and can also run directly on its loopback port.
export async function createSampleApp({ port = 0 } = {}) {
  let items = [{ id: 'bread', text: 'Хлеб', done: false }, { id: 'milk', text: 'Молоко', done: true }];
  const instance = randomBytes(16).toString('hex');
  let revision = 0;
  const snapshots = () => ({ legacy: JSON.stringify(items), versioned: JSON.stringify({ instance, revision, items }) });
  const requests = [];
  const wss = new WebSocketServer({ noServer: true, maxPayload: 1024 * 1024 });
  const app = createServer(async (req, res) => {
    requests.push({ path: req.url, headers: { ...req.headers } });
    if (req.url === '/api/items' || req.url === '/api/items?snapshot=1') {
      if (req.method === 'POST') {
        const parts = []; for await (const chunk of req) parts.push(chunk);
        const input = JSON.parse(Buffer.concat(parts).toString('utf8'));
        const changes = input.id ? items.some(item => item.id === input.id) : typeof input.text === 'string' && !!input.text.trim();
        if (changes && revision === Number.MAX_SAFE_INTEGER) { res.writeHead(503); res.end(); return; }
        if (input.id) items = items.map(item => item.id === input.id ? { ...item, done: !item.done } : item);
        else if (changes) items.push({ id: `item-${items.length}`, text: input.text.trim().slice(0, 100), done: false });
        if (changes) revision++;
      }
      // One synchronous mutation/serialization step gives the response and all sockets the same revision.
      const snapshot = snapshots();
      if (req.method === 'POST') for (const ws of wss.clients) ws.send(ws.versionedSnapshot ? snapshot.versioned : snapshot.legacy);
      res.writeHead(200, { 'Content-Type': 'application/json', 'Cache-Control': 'no-store', 'Set-Cookie': 'local_secret=do-not-forward; Path=/' });
      res.end(req.url === '/api/items?snapshot=1' ? snapshot.versioned : snapshot.legacy); return;
    }
    if (req.url === '/large') { res.writeHead(200, { 'Content-Type': 'application/octet-stream' }); for (let i = 0; i < 64; i++) res.write(Buffer.alloc(64 * 1024, i)); res.end(); return; }
    if (req.url === '/slow') {
      res.writeHead(200, { 'Content-Type': 'text/plain' }); res.write('first\n');
      const timer = setInterval(() => res.write('next\n'), 100); res.on('close', () => clearInterval(timer)); return;
    }
    if (req.url === '/redirect') { res.writeHead(302, { Location: 'http://169.254.169.254/latest/meta-data/' }); res.end(); return; }
    if (req.url === '/app.js') { res.writeHead(200, { 'Content-Type': 'text/javascript' }); res.end(sampleScript); return; }
    if (req.url === '/styles.css') { res.writeHead(200, { 'Content-Type': 'text/css' }); res.end(sampleStyles); return; }
    // Keep the real query intact in requests while serving the same app shell.
    if (new URL(req.url, 'http://localhost').pathname !== '/') { res.writeHead(404); res.end(); return; }
    res.writeHead(200, { 'Content-Type': 'text/html; charset=utf-8' }); res.end(sampleHtml);
  });
  app.on('upgrade', (req, socket, head) => {
    if (req.url !== '/live' && req.url !== '/live?snapshot=1') { socket.destroy(); return; }
    wss.handleUpgrade(req, socket, head, ws => {
      ws.versionedSnapshot = req.url === '/live?snapshot=1';
      const snapshot = snapshots(); ws.send(ws.versionedSnapshot ? snapshot.versioned : snapshot.legacy);
      if (!ws.versionedSnapshot) ws.on('message', bytes => ws.send(bytes));
    });
  });
  await new Promise(resolve => app.listen(port, '127.0.0.1', resolve));
  return { port: app.address().port, requests, server: app, close: async () => { for (const ws of wss.clients) ws.terminate(); wss.close(); app.closeAllConnections(); await new Promise(resolve => app.close(resolve)); } };
}
const sampleHtml = `<!doctype html><html lang="ru"><meta charset="utf-8"><meta name="viewport" content="width=device-width,initial-scale=1"><title>Покупки</title><link rel="stylesheet" href="/styles.css"><main>
<span class="eyebrow">СЕМЕЙНЫЙ СПИСОК</span><h1>Покупки<span aria-hidden="true">◈</span></h1>
<div class="connection"><p id="status" role="status" aria-live="polite">Подключаемся…</p><button id="reconnect" class="secondary" type="button">Подключить снова</button></div>
<div id="items" tabindex="-1" aria-label="Общий список"></div>
<form><label for="item">Добавить в список</label><div class="input"><input id="item" type="text" placeholder="Что купить?" maxlength="100" required aria-describedby="draft-hint"><button id="add" type="submit">Добавить</button></div></form>
<p id="draft-hint" class="hint">Ввод хранится только в этой открытой странице. Перед повторным открытием приложения скопируйте его.</p>
<p id="outcome" role="status" aria-live="polite"></p>
<section id="uncertain" hidden aria-label="Проверка действия"><label id="attempt-label" for="attempt">Текст неподтверждённой попытки</label><textarea id="attempt" readonly rows="2"></textarea><p class="hint">Подключитесь снова и проверьте список. Затем можно убрать предупреждение и текст попытки; ввод в поле останется. Новая отправка может создать повтор.</p><button id="reviewed" class="secondary" type="button">Я проверил список</button></section>
</main><script src="/app.js"></script></html>`;
const sampleStyles = `html{color-scheme:light}*{box-sizing:border-box}[hidden]{display:none!important}body{margin:0;background:#f8f6ef;color:#262822;font:16px system-ui,sans-serif}main{max-width:620px;margin:auto;padding:40px 26px}.eyebrow{font-size:10px;letter-spacing:.22em;color:#78786b}h1{font-size:38px;font-weight:550;letter-spacing:-.055em;margin:15px 0 4px;display:flex;justify-content:space-between}h1 span{color:#b88735}.connection{display:flex;align-items:center;gap:12px;flex-wrap:wrap;margin-bottom:16px}#status{flex:1 1 180px;font-size:13px;color:#596451;margin:8px 0;overflow-wrap:anywhere}.item{display:flex;align-items:center;gap:14px;min-height:44px;padding:18px 0;border-bottom:1px solid #e6e2d7}.item input{width:22px;height:22px;flex:0 0 22px;accent-color:#ad843d}.item span{min-width:0;overflow-wrap:anywhere}.item input:checked+span{text-decoration:line-through;color:#72776b}label{font-size:13px;color:#565c50}form{margin-top:28px}.input{display:flex;flex-wrap:wrap;margin-top:10px;gap:1px;border:1px solid #ddd8c9;border-radius:12px;overflow:hidden}input[type=text]{font:inherit;border:0;background:transparent;padding:15px;min-width:0;flex:1 1 140px;width:100%}button{border:0;border-radius:8px;background:#d9b267;min-width:44px;min-height:44px;padding:10px 14px;font:inherit;font-size:14px;cursor:pointer;white-space:normal}.input button{flex:0 1 auto;border-radius:0}button:hover:not([aria-disabled=true]){background:#caa456}.secondary{border:1px solid #d0c7b3;background:#eee9db}[aria-disabled=true]{cursor:default;opacity:.6}input:focus-visible,button:focus-visible,textarea:focus-visible,#items:focus-visible{outline:3px solid #997938;outline-offset:2px}.hint{font-size:12px;line-height:1.5;color:#626658;overflow-wrap:anywhere}#outcome{font-size:14px;line-height:1.5;overflow-wrap:anywhere}#outcome:empty{display:none}#uncertain{padding:14px;border:1px solid #b59e72;border-radius:12px;background:#f3ead6}textarea{display:block;width:100%;min-height:66px;margin-top:8px;padding:10px;border:1px solid #c7bda8;border-radius:6px;background:#fffdf8;color:inherit;font:inherit;resize:vertical}@media(max-width:359px){main{padding:24px 16px}.connection{gap:6px}.input button{flex:1 1 auto}}@media(min-width:480px) and (max-height:440px){main{padding:18px 24px}h1{margin-top:8px}form{margin-top:18px}.item{padding:10px 0}}`;
const sampleScript = String.raw`(() => {
  const list = document.querySelector('#items'), status = document.querySelector('#status');
  const input = document.querySelector('#item'), form = document.querySelector('form');
  const add = document.querySelector('#add'), reconnect = document.querySelector('#reconnect');
  const outcome = document.querySelector('#outcome'), uncertain = document.querySelector('#uncertain');
  const attempt = document.querySelector('#attempt'), attemptLabel = document.querySelector('#attempt-label');
  const reviewed = document.querySelector('#reviewed');
  const rows = new Map(), encoder = new TextEncoder();
  const DEADLINE_MS = 12000, MAX_SNAPSHOT_BYTES = 1024 * 1024;
  let alive = true, connection = null, connectionId = 0, phase = 'disconnected';
  let connectionText = '', draftRevision = 0, snapshotRevision = 0, requiredSnapshot = null, mutation = null;
  let currentSnapshot = null, renderedSnapshot = null;
  let lastOutcome = '', afterUnknown = false;

  function parseSnapshot(text) {
    if (typeof text !== 'string' || text.length > MAX_SNAPSHOT_BYTES || encoder.encode(text).byteLength > MAX_SNAPSHOT_BYTES) throw new Error('invalid_snapshot');
    const value = JSON.parse(text), ids = new Set();
    if (!value || typeof value !== 'object' || Array.isArray(value)
      || Object.keys(value).some(key => !['instance', 'revision', 'items'].includes(key))
      || typeof value.instance !== 'string' || !/^[a-f0-9]{32}$/.test(value.instance)
      || !Number.isSafeInteger(value.revision) || value.revision < 0
      || !Array.isArray(value.items) || value.items.length > 10000) throw new Error('invalid_snapshot');
    for (const item of value.items) {
      if (!item || typeof item !== 'object' || Array.isArray(item) || Object.keys(item).some(key => !['id', 'text', 'done'].includes(key))
        || typeof item.id !== 'string' || !item.id.length || item.id.length > 100 || ids.has(item.id)
        || typeof item.text !== 'string' || item.text.length > 100 || typeof item.done !== 'boolean') throw new Error('invalid_snapshot');
      ids.add(item.id);
    }
    return value;
  }
  async function readResponse(response) {
    if (!response.ok) throw new Error(response.status === 401 || response.status === 403 ? 'access_unavailable' : 'request_failed');
    return parseSnapshot(await response.text());
  }
  function current(value) { return alive && connection === value && !value.ended; }
  function canWrite() { return alive && phase === 'ready' && requiredSnapshot === null && mutation === null; }
  function unavailable(element, value) { element.setAttribute('aria-disabled', String(value)); }
  function reviewable() {
    return alive && ['unknown', 'review'].includes(mutation?.state) && phase === 'ready'
      && snapshotRevision > mutation.reviewAfter && connection.id > mutation.reviewConnectionAfter;
  }
  function reconcileSnapshot() {
    if (!requiredSnapshot || !currentSnapshot) return;
    if (requiredSnapshot.instance === currentSnapshot.instance) {
      if (currentSnapshot.revision >= requiredSnapshot.revision) requiredSnapshot = null;
    } else {
      // This ACK is real, but it belongs to another in-memory server lifetime.
      // A subsequent explicit reconnect and review is required, never a numeric epoch comparison.
      const value = requiredSnapshot.attempt;
      value.state = 'review'; value.reviewAfter = snapshotRevision; value.reviewConnectionAfter = connection.id;
      mutation = value; requiredSnapshot = null;
    }
  }
  function controls() {
    unavailable(reconnect, !alive || phase === 'connecting');
    unavailable(add, !canWrite());
    add.textContent = afterUnknown ? 'Добавить как новое' : 'Добавить';
    for (const row of rows.values()) unavailable(row.check, !canWrite());
    status.textContent = connectionText;
    uncertain.hidden = !['unknown', 'review'].includes(mutation?.state);
    if (mutation?.state === 'pending') outcome.textContent = 'Отправляем… Ввод можно продолжать.';
    else if (mutation?.state === 'unknown' || mutation?.state === 'review') {
      outcome.textContent = mutation.state === 'unknown'
        ? 'Не удалось подтвердить изменение. Оно могло сохраниться. Обновите список перед новым действием.'
        : 'Изменение принято, но подтверждение и показанный список относятся к разным запускам приложения. Подключитесь снова и проверьте список перед новым действием.';
      const isText = mutation.kind === 'add';
      attempt.hidden = attemptLabel.hidden = !isText;
      attemptLabel.textContent = mutation.state === 'review' ? 'Текст принятой попытки другого запуска' : 'Текст неподтверждённой попытки';
      if (attempt.value !== (isText ? mutation.text : '')) attempt.value = isText ? mutation.text : '';
      unavailable(reviewed, !reviewable());
    } else outcome.textContent = lastOutcome + (lastOutcome && requiredSnapshot ? ' Ждём обновления списка; при необходимости подключитесь снова.' : '');
  }
  function render(snapshot) {
    const items = snapshot.items;
    renderedSnapshot = snapshot;
    const focused = document.activeElement;
    const focusedInList = list.contains(focused);
    const ids = new Set(items.map(item => item.id));
    for (const [id, row] of rows) if (!ids.has(id)) { row.element.remove(); rows.delete(id); }
    items.forEach((item, index) => {
      let row = rows.get(item.id);
      if (!row) {
        const element = document.createElement('label'), check = document.createElement('input'), text = document.createElement('span');
        element.className = 'item'; check.type = 'checkbox'; element.append(check, text);
        row = { element, check, text, item }; rows.set(item.id, row);
        check.addEventListener('click', event => { if (!canWrite()) event.preventDefault(); });
        check.addEventListener('keydown', event => { if (event.key === ' ' && !canWrite()) event.preventDefault(); });
        check.addEventListener('change', () => {
          check.checked = row.item.done;
          if (canWrite()) void send({ kind: 'toggle', itemId: row.item.id });
        });
      }
      row.item = item; row.check.checked = item.done; row.text.textContent = item.text;
      if (list.children[index] !== row.element) list.insertBefore(row.element, list.children[index] || null);
    });
    controls();
    if (focusedInList && !focused.isConnected && document.activeElement === document.body) list.focus();
  }
  function stopConnection(value) {
    if (!value || value.ended) return;
    value.ended = true; clearTimeout(value.timer); value.controller.abort();
    if (value.socket) {
      value.socket.onopen = value.socket.onmessage = value.socket.onclose = value.socket.onerror = null;
      try { value.socket.close(); } catch { /* Already closed. */ }
    }
  }
  function disconnected(value, reason) {
    if (!current(value)) return;
    stopConnection(value); phase = 'disconnected'; connectionText = reason;
    controls();
  }
  function connect() {
    if (!alive || phase === 'connecting') return;
    stopConnection(connection);
    const value = { id: ++connectionId, controller: new AbortController(), socket: null, timer: null, ended: false, websocketSnapshot: false };
    connection = value; currentSnapshot = null; phase = 'connecting'; connectionText = 'Подключаемся… Введённый текст остаётся здесь.'; controls();
    value.timer = setTimeout(() => disconnected(value, 'Не удалось обновить список. Попробуйте подключиться снова.'), DEADLINE_MS);
    try {
      const socket = value.socket = new WebSocket(location.origin.replace(/^http/, 'ws') + '/live?snapshot=1');
      socket.onopen = () => { if (current(value)) { connectionText = 'Получаем свежий список…'; controls(); } };
      socket.onmessage = event => {
        if (!current(value)) return;
        let snapshot;
        try { snapshot = parseSnapshot(event.data); }
        catch { disconnected(value, 'Не удалось прочитать список. Попробуйте подключиться снова.'); return; }
        if (value.instance && value.instance !== snapshot.instance) { disconnected(value, 'Запуск приложения изменился. Подключитесь снова.'); return; }
        if (renderedSnapshot?.instance === snapshot.instance && snapshot.revision < renderedSnapshot.revision) return;
        value.instance = snapshot.instance;
        value.websocketSnapshot = true; clearTimeout(value.timer); value.controller.abort();
        phase = 'ready'; connectionText = 'Общий список · подключено'; snapshotRevision++;
        currentSnapshot = snapshot; reconcileSnapshot(); render(snapshot);
      };
      socket.onclose = socket.onerror = () => disconnected(value, 'Связь прервалась. Введённый текст остаётся здесь. Подключитесь снова; если доступ не восстановится, откройте приложение заново.');
      void fetch('/api/items?snapshot=1', { method: 'GET', cache: 'no-store', redirect: 'error', signal: value.controller.signal })
        .then(readResponse).then(snapshot => { if (current(value) && !value.websocketSnapshot && !value.controller.signal.aborted && !renderedSnapshot) render(snapshot); })
        .catch(error => {
          if (!current(value) || value.websocketSnapshot || value.controller.signal.aborted) return;
          if (error.message === 'access_unavailable') disconnected(value, 'Приложение недоступно. Откройте его заново; перед этим скопируйте введённый текст.');
          // The current WS may still deliver its authoritative initial snapshot before the deadline.
        });
    } catch { disconnected(value, 'Не удалось подключиться. Введённый текст остаётся здесь.'); }
  }
  function unknown(value) {
    if (mutation !== value || value.state !== 'pending') return;
    clearTimeout(value.timer); value.controller.abort();
    value.state = 'unknown'; value.reviewAfter = snapshotRevision; value.reviewConnectionAfter = connection?.id ?? 0;
    controls();
  }
  async function send(action) {
    if (!canWrite()) return;
    const text = input.value;
    if (action.kind === 'add' && (!text.trim() || text.length > 100)) {
      lastOutcome = 'Не отправлено: введите от 1 до 100 символов.'; controls(); return;
    }
    const value = { ...action, text: action.kind === 'add' ? text : null, revision: draftRevision,
      payload: JSON.stringify(action.kind === 'add' ? { text } : { id: action.itemId }),
      controller: new AbortController(), timer: null, state: 'pending', reviewAfter: snapshotRevision,
      baseInstance: currentSnapshot.instance, baseRevision: currentSnapshot.revision };
    mutation = value; afterUnknown = false; lastOutcome = ''; controls();
    value.timer = setTimeout(() => { if (alive) unknown(value); }, DEADLINE_MS);
    try {
      const response = await fetch('/api/items?snapshot=1', { method: 'POST', headers: { 'Content-Type': 'application/json' },
        body: value.payload, cache: 'no-store', redirect: 'error', signal: value.controller.signal });
      const acknowledged = await readResponse(response);
      if (acknowledged.instance === value.baseInstance && acknowledged.revision <= value.baseRevision) throw new Error('invalid_ack_revision');
      if (!alive || mutation !== value || value.state !== 'pending' || value.controller.signal.aborted) return;
      clearTimeout(value.timer); mutation = null;
      if (value.kind === 'add' && draftRevision === value.revision && input.value === value.text) { input.value = ''; draftRevision++; }
      lastOutcome = value.kind === 'add' ? 'Добавлено.' : 'Изменение принято.';
      requiredSnapshot = { instance: acknowledged.instance, revision: acknowledged.revision, attempt: value };
      reconcileSnapshot();
      // Only the current WS owns the list. An older POST response must not replace a newer snapshot.
      controls();
    } catch { if (alive) unknown(value); }
    finally { clearTimeout(value.timer); }
  }
  input.addEventListener('input', () => { draftRevision++; });
  add.addEventListener('click', event => { if (!canWrite()) event.preventDefault(); });
  form.addEventListener('submit', event => { event.preventDefault(); void send({ kind: 'add' }); });
  reconnect.addEventListener('click', connect);
  reviewed.addEventListener('click', () => {
    if (!reviewable()) return;
    mutation = null; attempt.value = ''; afterUnknown = true;
    lastOutcome = 'Вы проверили список. Новое действие может повторить прежнее; автоматической повторной отправки не было.';
    controls(); input.focus();
  });
  window.addEventListener('pagehide', () => {
    if (mutation?.state === 'pending') unknown(mutation);
    alive = false; stopConnection(connection); phase = 'disconnected'; connectionText = 'Подключитесь снова. Введённый текст остаётся здесь.'; controls();
  });
  window.addEventListener('pageshow', event => { if (event.persisted) { alive = true; controls(); } });
  connect();
})();`;

if (process.argv[1] && resolve(process.argv[1]) === fileURLToPath(import.meta.url)) {
  const sample = await createSampleApp();
  console.log(`Покупки: http://127.0.0.1:${sample.port} — этот порт можно добавить в Соты.`);
  let closing = false;
  const close = () => { if (!closing) { closing = true; void sample.close(); } };
  process.once('SIGINT', close); process.once('SIGTERM', close);
}
