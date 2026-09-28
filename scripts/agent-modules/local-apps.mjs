export const localAppsSchema = 'soty.apps-channel.v1';
const localAppsChunkBytes = 48 * 1024;
const localAppsFrameBytes = 72 * 1024;

// Kept dependency-injected so the existing immutable connector release bundler
// can embed this module without adding a package manager to installed devices.
export function createLocalAppsRuntime(deps, options = {}) {
  const streams = new Map(), apps = new Map();
  const state = { running: false, connected: false, socket: null, retry: null, probe: null, claimCode: '', claimExpiresAt: 0, claimPending: null, claimAcknowledged: false, processing: 0 };
  const now = deps.now || Date.now;
  const blocked = [...new Set([49424, ...(options.blockedPorts || [])])];
  const makeSecret = () => deps.randomSecret();
  const identity = () => typeof options.identity === 'function' ? options.identity() : options.identity;
  const serverUrl = () => typeof options.serverUrl === 'function' ? options.serverUrl() : options.serverUrl;
  const token = () => typeof options.token === 'function' ? options.token() : options.token;
  const fail = (code) => Object.assign(new Error(code), { code });
  const send = frame => {
    const ws = state.socket;
    if (!ws || ws.readyState !== 1 || ws.bufferedAmount > 4 * 1024 * 1024) return false;
    ws.send(JSON.stringify(frame)); return true;
  };
  function rotateClaim() { state.claimCode = makeSecret(); state.claimExpiresAt = now() + 5 * 60_000; return deps.digest(state.claimCode); }
  async function claim() {
    if (!state.connected) throw fail('apps_connector_offline');
    // A code is generated only on the explicit local UI action. Repeated reads
    // use the same unexpired code, and successful claim invalidates it.
    if (!state.claimCode || state.claimExpiresAt <= now()) {
      const claimDigest = rotateClaim(); state.claimAcknowledged = false;
      let resolveClaim, rejectClaim;
      const promise = new Promise((resolve, reject) => { resolveClaim = resolve; rejectClaim = reject; });
      const timer = setTimeout(() => { state.claimPending = null; state.claimExpiresAt = 0; rejectClaim(fail('apps_claim_timeout')); }, 5000); timer.unref?.();
      state.claimPending = { promise, resolve: resolveClaim, reject: rejectClaim, timer, digest: claimDigest };
      if (!send({ type: 'claim', claimDigest })) { clearTimeout(timer); state.claimPending = null; state.claimExpiresAt = 0; throw fail('apps_connector_offline'); }
    }
    if (state.claimPending) await state.claimPending.promise;
    if (!state.claimAcknowledged) throw fail('apps_claim_unavailable');
    const target = identity();
    return { ok: true, hostDeviceId: target.hostDeviceId, connectorId: target.connectorId, claimCode: state.claimCode, expiresAt: state.claimExpiresAt };
  }
  function closeStream(stream, error = 'app_stream_closed', notify = true) {
    if (stream.closed) return;
    stream.closed = true; streams.delete(stream.id); clearTimeout(stream.timer);
    stream.request?.destroy(); stream.response?.destroy(); stream.socket?.destroy();
    for (const pending of stream.pending.values()) { clearTimeout(pending.timer); pending.reject(fail(error)); }
    stream.pending.clear();
    if (notify) send({ type: 'cancel', id: stream.id, error });
  }
  function stop() {
    state.running = false; state.connected = false; clearTimeout(state.retry); clearInterval(state.probe);
    for (const stream of streams.values()) closeStream(stream, 'app_offline', false);
    if (state.claimPending) { clearTimeout(state.claimPending.timer); state.claimPending.reject(fail('apps_connector_offline')); state.claimPending = null; }
    state.socket?.close(); state.socket = null;
  }
  function connect() {
    if (!state.running) return;
    let url;
    try {
      const base = new URL(serverUrl());
      if (base.protocol !== 'https:' && !(base.protocol === 'http:' && ['localhost', '127.0.0.1', '[::1]'].includes(base.hostname))) throw fail('apps_server_requires_https');
      url = new URL('/api/apps/channel', base); url.protocol = base.protocol === 'https:' ? 'wss:' : 'ws:';
    } catch { scheduleReconnect(); return; }
    const ws = deps.createWebSocket(url.href); state.socket = ws;
    ws.addEventListener('open', () => {
      const target = identity();
      state.claimCode = ''; state.claimExpiresAt = 0; state.claimAcknowledged = false;
      send({ type: 'auth', schema: localAppsSchema, linkId: target.linkId, hostDeviceId: target.hostDeviceId, connectorId: target.connectorId,
        name: target.name || target.hostDeviceId, token: token() });
    });
    ws.addEventListener('message', event => {
      if (typeof event.data !== 'string' || event.data.length > localAppsFrameBytes || state.processing > 64) { ws.close(); return; }
      state.processing += 1;
      void onFrame(event.data).catch(() => ws.close()).finally(() => { state.processing -= 1; });
    });
    ws.addEventListener('error', () => {});
    ws.addEventListener('close', () => {
      if (state.socket !== ws) return;
      state.connected = false; state.socket = null; apps.clear();
      if (state.claimPending) { clearTimeout(state.claimPending.timer); state.claimPending.reject(fail('apps_connector_offline')); state.claimPending = null; }
      for (const stream of streams.values()) closeStream(stream, 'app_offline', false);
      scheduleReconnect();
    });
  }
  function scheduleReconnect() { clearTimeout(state.retry); if (state.running) { state.retry = setTimeout(connect, 2000); state.retry.unref?.(); } }
  async function onFrame(text) {
    const frame = JSON.parse(text);
    if (frame.type === 'ready' && frame.schema === localAppsSchema) { state.connected = true; return; }
    if (!state.connected) throw fail('apps_not_authenticated');
    if (frame.type === 'claim-ready') {
      if (state.claimPending && frame.claimDigest === state.claimPending.digest) {
        clearTimeout(state.claimPending.timer); state.claimAcknowledged = true; state.claimPending.resolve(); state.claimPending = null;
      }
      return;
    }
    if (frame.type === 'claimed') { state.claimCode = ''; state.claimExpiresAt = 0; state.claimAcknowledged = false; return; }
    if (frame.type === 'sync') {
      if (!Array.isArray(frame.apps) || frame.apps.length > 100) throw fail('apps_bad_configuration');
      const next = new Map();
      for (const item of frame.apps) {
        if (!/^app-[a-f0-9]{32}$/u.test(item.id || '')) throw fail('apps_bad_configuration');
        next.set(item.id, { id: item.id, port: localAppsPort(item.port, blocked), entryPath: localAppsPath(item.entryPath) });
      }
      apps.clear(); for (const [key, value] of next) apps.set(key, value);
      for (const stream of streams.values()) if (!apps.has(stream.appId)) closeStream(stream, 'app_access_revoked');
      void probeAll(); return;
    }
    if (frame.type === 'open') { openStream(frame); return; }
    const stream = streams.get(frame.id); if (!stream) return;
    if (frame.type === 'cancel') { closeStream(stream, 'app_cancelled', false); return; }
    if (frame.type === 'ack') {
      const pending = stream.pending.get(frame.seq); if (!pending) throw fail('apps_bad_ack');
      clearTimeout(pending.timer); stream.pending.delete(frame.seq); pending.resolve(); return;
    }
    if (frame.type === 'end') { (stream.socket || stream.request)?.end(); return; }
    if (frame.type === 'data') {
      if (stream.receiving || frame.seq !== stream.recvSeq + 1 || typeof frame.data !== 'string' || frame.data.length > localAppsChunkBytes * 4 / 3 + 4 || !/^[A-Za-z0-9+/]*={0,2}$/u.test(frame.data)) throw fail('apps_bad_data');
      const bytes = deps.decodeBase64(frame.data);
      stream.received += bytes.length;
      if (bytes.length > localAppsChunkBytes || (stream.kind === 'http' && stream.received > 8 * 1024 * 1024)) { closeStream(stream, 'app_request_too_large'); return; }
      stream.receiving = true; stream.recvSeq = frame.seq; stream.timer.refresh();
      await write(stream.socket || stream.request, bytes); stream.receiving = false;
      send({ type: 'ack', id: stream.id, seq: frame.seq }); return;
    }
    throw fail('apps_bad_frame');
  }
  function openStream(frame) {
    if (!/^[a-f0-9]{32}$/u.test(frame.id || '') || streams.has(frame.id) || streams.size >= 32 || !['http', 'ws'].includes(frame.kind)) throw fail('apps_bad_open');
    const app = apps.get(frame.appId); if (!app) { send({ type: 'cancel', id: frame.id, error: 'app_access_revoked' }); return; }
    const path = localAppsPath(frame.path);
    if (!['GET', 'HEAD', 'POST', 'PUT', 'PATCH', 'DELETE', 'OPTIONS'].includes(frame.method)) throw fail('apps_bad_method');
    const headers = localAppsHeaders(frame.headers, frame.kind === 'ws' ? 'ws-request' : 'request');
    if (frame.kind === 'ws') { headers.connection = 'Upgrade'; headers.upgrade = 'websocket'; }
    const stream = { id: frame.id, appId: app.id, kind: frame.kind, received: 0, sent: 0, recvSeq: 0, sendSeq: 0, pending: new Map(), receiving: false, closed: false, head: false };
    streams.set(stream.id, stream);
    stream.timer = setTimeout(() => closeStream(stream, 'app_response_timeout'), 30_000); stream.timer.unref?.();
    const request = deps.httpRequest({ hostname: '127.0.0.1', port: app.port, path, method: frame.method, headers, agent: false });
    stream.request = request;
    request.on('error', () => { send({ type: 'observation', appId: app.id, state: 'stopped' }); closeStream(stream, 'app_stopped'); });
    request.on('response', response => {
      if (stream.kind === 'ws') { response.destroy(); closeStream(stream, 'app_upgrade_failed'); return; }
      stream.response = response; stream.head = true;
      clearTimeout(stream.timer); stream.timer = setTimeout(() => closeStream(stream, 'app_idle_timeout'), 120_000); stream.timer.unref?.();
      const responseHeaders = localAppsHeaders(response.headers, 'response'); let location;
      if (response.headers.location) {
        try {
          const next = new URL(response.headers.location, `http://127.0.0.1:${app.port}${path}`);
          if (next.origin !== `http://127.0.0.1:${app.port}`) throw fail('app_external_redirect');
          location = localAppsPath(next.pathname + next.search + next.hash);
        } catch { closeStream(stream, 'app_external_redirect'); return; }
      }
      send({ type: 'observation', appId: app.id, state: 'ready' });
      send({ type: 'head', id: stream.id, status: response.statusCode, headers: responseHeaders, ...(location ? { location } : {}) });
      void (async () => {
        try {
          for await (const bytes of response) { stream.sent += bytes.length; if (stream.sent > 64 * 1024 * 1024) throw fail('app_response_too_large'); await sendChunks(stream, bytes); }
          send({ type: 'end', id: stream.id }); closeStream(stream, 'app_stream_complete', false);
        } catch (error) { closeStream(stream, error.code || 'app_upstream_failed'); }
      })();
    });
    request.on('upgrade', (response, socket, head) => {
      if (stream.kind !== 'ws' || response.statusCode !== 101) { socket.destroy(); closeStream(stream, 'app_upgrade_failed'); return; }
      stream.socket = socket; stream.head = true;
      clearTimeout(stream.timer); stream.timer = setTimeout(() => closeStream(stream, 'app_idle_timeout'), 120_000); stream.timer.unref?.();
      socket.on('error', () => closeStream(stream, 'app_upstream_failed'));
      send({ type: 'head', id: stream.id, status: 101, headers: localAppsHeaders(response.headers, 'ws-response') });
      void (async () => {
        try { if (head.length) await sendChunks(stream, head); for await (const bytes of socket) await sendChunks(stream, bytes); send({ type: 'end', id: stream.id }); closeStream(stream, 'app_stream_complete', false); }
        catch { closeStream(stream, 'app_upstream_failed'); }
      })();
    });
    if (stream.kind === 'ws') request.end();
  }
  async function sendChunks(stream, bytes) {
    for (let offset = 0; offset < bytes.length; offset += localAppsChunkBytes) {
      if (stream.closed) throw fail('app_stream_closed');
      const seq = ++stream.sendSeq; stream.timer.refresh();
      await new Promise((resolve, reject) => {
        const timer = setTimeout(() => { stream.pending.delete(seq); reject(fail('app_ack_timeout')); }, 30_000); timer.unref?.();
        stream.pending.set(seq, { resolve, reject, timer });
        if (!send({ type: 'data', id: stream.id, seq, data: deps.encodeBase64(bytes.subarray(offset, offset + localAppsChunkBytes)) })) { clearTimeout(timer); stream.pending.delete(seq); reject(fail('app_offline')); }
      });
    }
  }
  async function probeAll() {
    if (!state.connected) return;
    for (const app of apps.values()) {
      const ready = await probeLocalApp(deps, { port: app.port, entryPath: app.entryPath, blockedPorts: blocked });
      if (state.connected && apps.has(app.id)) send({ type: 'observation', appId: app.id, state: ready ? 'ready' : 'stopped' });
    }
  }
  return {
    start() { if (state.running) return; state.running = true; connect(); state.probe = setInterval(() => void probeAll(), 20_000); state.probe.unref?.(); },
    stop, claim,
    status() { return { connected: state.connected, apps: apps.size, streams: streams.size }; },
  };
}

export function normalizeLocalAppManifest(value, blockedPorts = []) {
  if (!value || typeof value !== 'object' || Array.isArray(value) || value.schema !== 'soty.local-app.v1' || !Object.keys(value).every(key => ['schema', 'name', 'port', 'entryPath'].includes(key))) throw new Error('invalid_app_manifest');
  if (typeof value.name !== 'string' || !value.name.trim() || value.name.trim().length > 64 || /[\u0000-\u001f\u007f]/u.test(value.name)) throw new Error('invalid_app_name');
  const entryPath = localAppsPath(value.entryPath ?? '/'); if (entryPath.startsWith('/_soty/')) throw new Error('reserved_app_path');
  return { schema: 'soty.local-app.v1', name: value.name.trim(), port: localAppsPort(value.port, blockedPorts), entryPath };
}

/** A new app receives its own directory by default. Resolve existing parents before
 * each mkdir so a pre-existing symlink/junction cannot redirect writes outside it. */
export async function prepareLocalAppWorkspace(deps, { workspace, allowedRoots, jobId, create = false } = {}) {
  const base = await deps.realpath(workspace);
  const roots = await Promise.all(allowedRoots.map(root => deps.realpath(root)));
  if (!roots.some(root => deps.isWithin(root, base))) throw new Error('app_workspace_not_allowed');
  if (!(await deps.lstat(base)).isDirectory()) throw new Error('app_workspace_not_directory');
  if (!create) return base;
  if (!/^job_[A-Za-z0-9_-]{8,160}$/u.test(jobId)) throw new Error('app_workspace_invalid_job');
  let parent = base;
  for (const segment of ['Soty Apps', jobId]) {
    const candidate = deps.join(parent, segment);
    try { await deps.mkdir(candidate, { mode: 0o700 }); }
    catch (error) { if (error.code !== 'EEXIST') throw error; }
    const information = await deps.lstat(candidate);
    if (information.isSymbolicLink() || !information.isDirectory()) throw new Error('app_workspace_not_directory');
    const actual = await deps.realpath(candidate);
    if (!deps.isWithin(base, actual)) throw new Error('app_workspace_not_allowed');
    parent = actual;
  }
  return parent;
}

export function readLocalAppProposal(deps, { workspace, allowedRoots, jobId, blockedPorts = [], completedAfter = 0 } = {}) {
  return (async () => {
    const realWorkspace = await deps.realpath(workspace);
    const roots = await Promise.all(allowedRoots.map(root => deps.realpath(root)));
    if (!roots.some(root => deps.isWithin(root, realWorkspace))) throw new Error('app_workspace_not_allowed');
    const requested = deps.join(realWorkspace, '.soty', 'app.json'), realFile = await deps.realpath(requested);
    if (!deps.isWithin(realWorkspace, realFile)) throw new Error('app_manifest_outside_workspace');
    const stat = await deps.stat(realFile);
    if (!stat.isFile() || stat.size > 4096 || stat.size < 2 || (completedAfter && stat.mtimeMs < completedAfter)) throw new Error('app_manifest_invalid_file');
    const manifest = normalizeLocalAppManifest(JSON.parse(await deps.readFile(realFile, 'utf8')), blockedPorts);
    if (!await probeLocalApp(deps, { ...manifest, blockedPorts })) throw new Error('app_not_ready');
    return { ...manifest, sourceJobId: jobId };
  })();
}

export function probeLocalApp(deps, { port, entryPath = '/', blockedPorts = [] }) {
  return new Promise(resolve => {
    let request;
    try {
      request = deps.httpRequest({ hostname: '127.0.0.1', port: localAppsPort(port, blockedPorts), path: localAppsPath(entryPath), method: 'HEAD', agent: false });
      const timer = setTimeout(() => { request.destroy(); resolve(false); }, 3000); timer.unref?.();
      request.on('response', response => { clearTimeout(timer); const ok = response.statusCode >= 200 && response.statusCode < 500; response.destroy(); resolve(ok); });
      request.on('error', () => { clearTimeout(timer); resolve(false); }); request.end();
    } catch { request?.destroy(); resolve(false); }
  });
}
function localAppsPort(value, blocked = []) { if (!Number.isSafeInteger(value) || value < 1024 || value > 65535 || [49424, ...blocked].includes(value)) throw new Error('invalid_app_port'); return value; }
function localAppsPath(value) {
  if (typeof value !== 'string' || value.length > 8192 || !value.startsWith('/') || value.startsWith('//') || /[\\\u0000-\u0020\u007f]/u.test(value)) throw new Error('invalid_app_path');
  const decoded = decodeURIComponent(value.split('?')[0]); if (decoded.startsWith('//') || /[\\\u0000-\u001f\u007f]/u.test(decoded)) throw new Error('invalid_app_path'); return value;
}
function localAppsHeaders(value, direction) {
  const allow = new Set(direction === 'response' ? ['content-type', 'content-encoding', 'content-language', 'content-disposition', 'etag', 'last-modified', 'accept-ranges', 'content-range', 'vary']
    : direction === 'ws-request' ? ['sec-websocket-key', 'sec-websocket-version', 'sec-websocket-protocol', 'sec-websocket-extensions', 'origin']
      : direction === 'ws-response' ? ['sec-websocket-accept', 'sec-websocket-protocol', 'sec-websocket-extensions']
        : ['accept', 'accept-language', 'content-type', 'content-encoding', 'if-none-match', 'if-modified-since', 'range', 'if-range']);
  const result = {}; let bytes = 0;
  for (const [key, raw] of Object.entries(value || {})) {
    if (!allow.has(key.toLowerCase()) || typeof raw !== 'string' || /[\r\n\u0000]/u.test(raw)) continue;
    bytes += key.length + raw.length; if (bytes > 16 * 1024) throw new Error('app_headers_too_large'); result[key.toLowerCase()] = raw;
  }
  return result;
}
function write(stream, chunk) { return new Promise((resolve, reject) => { if (!stream || stream.destroyed) return reject(new Error('app_stream_closed')); stream.write(chunk, error => error ? reject(error) : resolve()); }); }
