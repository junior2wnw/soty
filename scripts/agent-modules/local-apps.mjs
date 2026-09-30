export const localAppsSchema = 'soty.apps-channel.v1';
const localAppsChunkBytes = 48 * 1024;
const localAppsFrameBytes = 72 * 1024;
const localAppsProfile = 'soty.relay-restricted.v1';
const localAppsEncoder = new TextEncoder();
const localAppsFail = code => Object.assign(new Error(code), { code });
const localAppsCheck = (condition, code = 'apps_bad_frame') => { if (!condition) throw localAppsFail(code); };
const localAppsExact = (value, keys) => localAppsCheck(value && typeof value === 'object' && !Array.isArray(value)
  && Object.keys(value).every(key => keys.includes(key)));
const localAppsNonce = value => typeof value === 'string' && /^[A-Za-z0-9_-]{43}$/u.test(value);
const localAppsId = value => typeof value === 'string' && /^app-[a-f0-9]{32}$/u.test(value);
const localAppsText = value => typeof value === 'string' && /^[A-Za-z0-9_.:-]{1,180}$/u.test(value);
const localAppsPins = target => ({ appId: target.appId, revision: target.revision, digest: target.digest, profile: target.profile });

// Dependency injection is preserved for the immutable single-file release.
// Every asynchronous operation owns its actual connection and binding object.
export function createLocalAppsRuntime(deps, options = {}) {
  const state = { running: false, context: null, retry: null, interval: null };
  const now = deps.now || Date.now;
  const blocked = [...new Set([49424, ...(options.blockedPorts || [])])];
  const option = name => typeof options[name] === 'function' ? options[name]() : options[name];
  const current = context => state.running && state.context === context && !context.closed;
  const connected = context => current(context) && context.connected && context.ws.readyState === 1;
  const streamCurrent = stream => connected(stream.context) && !stream.closed
    && stream.context.streams.get(stream.id) === stream && stream.context.bindings.get(stream.appId) === stream.binding;
  function send(context, frame) {
    if (!current(context) || context.ws.readyState !== 1) return false;
    try {
      const text = JSON.stringify(frame), bytes = localAppsEncoder.encode(text).byteLength;
      if (bytes > localAppsFrameBytes || context.ws.bufferedAmount + bytes > 4 * 1024 * 1024) {
        disconnect(context); return false;
      }
      context.ws.send(text); return true;
    } catch { disconnect(context); return false; }
  }
  function sendStream(stream, frame) {
    localAppsCheck(streamCurrent(stream), 'app_stream_closed');
    localAppsCheck(send(stream.context, frame), 'app_offline');
  }
  function closeStream(stream, error = 'app_stream_closed', notify = true) {
    if (stream.closed) return;
    stream.closed = true; clearTimeout(stream.timer);
    if (stream.context.streams.get(stream.id) === stream) stream.context.streams.delete(stream.id);
    for (const pending of stream.pending.values()) { clearTimeout(pending.timer); pending.reject(localAppsFail(error)); }
    stream.pending.clear();
    stream.request?.destroy(); stream.response?.destroy(); stream.socket?.destroy();
    if (notify) send(stream.context, { type: 'cancel', id: stream.id, error });
  }
  function removeBinding(context, id, reason = 'app_access_changed') {
    const binding = context.bindings.get(id);
    context.bindings.delete(id); context.probeQueue.delete(id);
    if (!binding) return;
    context.probes.get(binding)?.cancel();
    for (const stream of context.streams.values()) if (stream.binding === binding) closeStream(stream, reason);
  }
  function dropPreparation(context, entry) {
    clearTimeout(entry.timer); entry.cancelled = true; entry.probe?.cancel();
    if (context.preparations.get(entry.nonce) === entry) context.preparations.delete(entry.nonce);
  }
  function disconnect(context, closeSocket = true) {
    if (context.closed) return;
    const wasCurrent = state.context === context;
    context.closed = true; context.connected = false; clearTimeout(context.authTimer);
    if (wasCurrent) state.context = null;
    if (context.claimPending) {
      clearTimeout(context.claimPending.timer); context.claimPending.reject(localAppsFail('apps_connector_offline')); context.claimPending = null;
    }
    for (const stream of context.streams.values()) closeStream(stream, 'app_offline', false);
    for (const probe of context.probes.values()) probe.cancel();
    for (const entry of context.preparations.values()) dropPreparation(context, entry);
    context.bindings.clear(); context.configs.clear(); context.probeQueue.clear();
    if (closeSocket) { try { context.ws.close(); } catch {} }
    if (wasCurrent) scheduleReconnect();
  }
  function scheduleReconnect() {
    clearTimeout(state.retry);
    if (state.running) { state.retry = setTimeout(connect, 2000); state.retry.unref?.(); }
  }
  function connect() {
    if (!state.running) return;
    let url, identity, authToken, ws;
    try {
      const base = new URL(option('serverUrl'));
      localAppsCheck(base.protocol === 'https:' || (base.protocol === 'http:' && ['localhost', '127.0.0.1', '[::1]'].includes(base.hostname)), 'apps_server_requires_https');
      url = new URL('/api/apps/channel', base); url.protocol = base.protocol === 'https:' ? 'wss:' : 'ws:';
      const value = option('identity');
      localAppsCheck(value && [value.linkId, value.hostDeviceId, value.connectorId].every(localAppsText), 'apps_bad_identity');
      identity = Object.freeze({ linkId: value.linkId, hostDeviceId: value.hostDeviceId, connectorId: value.connectorId,
        name: typeof value.name === 'string' ? value.name.slice(0, 80) : value.hostDeviceId });
      authToken = option('token'); ws = deps.createWebSocket(url.href);
    } catch { scheduleReconnect(); return; }
    const context = { ws, identity, key: [identity.linkId, identity.hostDeviceId, identity.connectorId].join('|'),
      closed: false, connected: false, version: 0, channelId: null, bindings: new Map(), configs: new Map(), streams: new Map(),
      probes: new Map(), probeQueue: new Map(), probeScheduled: false, preparations: new Map(), preparing: 0, asyncWrites: 0,
      claimCode: '', claimExpiresAt: 0, claimAcknowledged: false, claimPending: null };
    state.context = context;
    context.authTimer = setTimeout(() => disconnect(context), 5000); context.authTimer.unref?.();
    ws.addEventListener('open', () => {
      if (!current(context)) return;
      send(context, { type: 'auth', schema: localAppsSchema, ...identity, token: authToken,
        capabilities: { targetBindingVersions: [1, 2] } });
      authToken = undefined;
    });
    ws.addEventListener('message', event => {
      if (!current(context)) return;
      try {
        localAppsCheck(typeof event.data === 'string' && localAppsEncoder.encode(event.data).byteLength <= localAppsFrameBytes);
        const frame = JSON.parse(event.data);
        localAppsCheck(frame && typeof frame === 'object' && !Array.isArray(frame));
        // Control handlers are synchronous. A genuine 100-frame parser burst
        // must not be counted as 100 outstanding promise continuations.
        onFrame(context, frame);
      } catch { disconnect(context); }
    });
    ws.addEventListener('error', () => {});
    ws.addEventListener('close', () => disconnect(context, false));
  }
  async function claim() {
    const context = state.context;
    localAppsCheck(context && connected(context), 'apps_connector_offline');
    if (!context.claimCode || context.claimExpiresAt <= now()) {
      context.claimCode = deps.randomSecret(); context.claimExpiresAt = now() + 5 * 60_000; context.claimAcknowledged = false;
      const claimDigest = deps.digest(context.claimCode);
      let resolveClaim, rejectClaim;
      const promise = new Promise((resolve, reject) => { resolveClaim = resolve; rejectClaim = reject; });
      const pending = { promise, resolve: resolveClaim, reject: rejectClaim, digest: claimDigest };
      pending.timer = setTimeout(() => {
        if (context.claimPending !== pending) return;
        context.claimPending = null; context.claimExpiresAt = 0; rejectClaim(localAppsFail('apps_claim_timeout'));
      }, 5000); pending.timer.unref?.();
      context.claimPending = pending;
      if (!send(context, { type: 'claim', claimDigest })) {
        // disconnect rejects the owned promise; observe it before returning.
        await promise; throw localAppsFail('apps_connector_offline');
      }
    }
    if (context.claimPending) await context.claimPending.promise;
    localAppsCheck(connected(context), 'apps_connector_offline');
    localAppsCheck(context.claimAcknowledged, 'apps_claim_unavailable');
    return { ok: true, hostDeviceId: context.identity.hostDeviceId, connectorId: context.identity.connectorId,
      claimCode: context.claimCode, expiresAt: context.claimExpiresAt };
  }
  function channelFrame(context, frame, keys) {
    localAppsExact(frame, ['type', 'channelId', ...keys]);
    localAppsCheck(context.version === 2 && frame.channelId === context.channelId, 'apps_bad_binding_channel');
  }
  function target(context, value) {
    localAppsExact(value, ['appId', 'revision', 'digest', 'ownerAccountId', 'port', 'entryPath', 'profile']);
    localAppsCheck(localAppsId(value.appId) && Number.isSafeInteger(value.revision) && value.revision >= 1
      && typeof value.digest === 'string' && /^[a-f0-9]{64}$/u.test(value.digest) && localAppsText(value.ownerAccountId)
      && Number.isSafeInteger(value.port) && typeof value.entryPath === 'string'
      && typeof value.profile === 'string' && value.profile.length > 0 && value.profile.length <= 128, 'apps_bad_target');
    const normalized = Object.freeze({ appId: value.appId, revision: value.revision, digest: value.digest,
      ownerAccountId: value.ownerAccountId, port: value.port, entryPath: value.entryPath, profile: value.profile });
    localAppsCheck(deps.digest(JSON.stringify(['soty.runtime-target.v1', normalized.appId, normalized.revision,
      normalized.ownerAccountId, context.key, normalized.port, normalized.entryPath, normalized.profile])) === normalized.digest, 'apps_target_digest_mismatch');
    return normalized;
  }
  function policyError(value) {
    if (value.profile !== localAppsProfile) return 'unsupported_profile';
    try { localAppsPort(value.port, blocked); } catch { return 'invalid_app_port'; }
    try { localAppsHttpPath(value.entryPath, value.port); } catch { return 'invalid_app_path'; }
    return null;
  }
  function setBinding(context, frame) {
    channelFrame(context, frame, ['syncId', 'target']); localAppsCheck(localAppsNonce(frame.syncId), 'apps_bad_binding');
    const value = target(context, frame.target), previous = context.configs.get(value.appId);
    for (const record of context.configs.values()) if (record.syncId === frame.syncId)
      localAppsCheck(record.target.appId === value.appId && record.target.digest === value.digest, 'apps_binding_id_conflict');
    if (previous?.syncId === frame.syncId) { send(context, previous.reply); return; }
    localAppsCheck(previous || context.configs.size < 100, 'apps_bad_configuration');
    const error = policyError(value);
    removeBinding(context, value.appId);
    const reply = { type: error ? 'binding-rejected' : 'binding-ack', channelId: context.channelId, syncId: frame.syncId,
      ...localAppsPins(value), ...(error ? { error } : {}) };
    const binding = error ? null : Object.freeze({ appId: value.appId, port: value.port, entryPath: value.entryPath,
      target: value, syncId: frame.syncId });
    context.configs.set(value.appId, { target: localAppsPins(value), syncId: frame.syncId, reply });
    if (binding) context.bindings.set(value.appId, binding);
    if (send(context, reply) && binding) queueProbe(context, binding);
  }
  function removeBindings(context, frame) {
    channelFrame(context, frame, ['bindings']); localAppsCheck(Array.isArray(frame.bindings) && frame.bindings.length <= 100);
    for (const item of frame.bindings) {
      localAppsExact(item, ['appId', 'syncId']); localAppsCheck(localAppsId(item.appId) && localAppsNonce(item.syncId));
    }
    for (const item of frame.bindings) if (context.configs.get(item.appId)?.syncId === item.syncId) {
      context.configs.delete(item.appId); removeBinding(context, item.appId, 'app_access_revoked');
    }
  }
  function legacySync(context, frame) {
    localAppsCheck(context.version === 1 && Array.isArray(frame.apps) && frame.apps.length <= 100, 'apps_bad_configuration');
    const next = new Map();
    for (const item of frame.apps) {
      localAppsCheck(item && localAppsId(item.id) && !next.has(item.id), 'apps_bad_configuration');
      const port = localAppsPort(item.port, blocked), entryPath = localAppsPath(item.entryPath), previous = context.bindings.get(item.id);
      next.set(item.id, previous && previous.port === port && previous.entryPath === entryPath ? previous
        : Object.freeze({ appId: item.id, port, entryPath }));
    }
    for (const [id, binding] of context.bindings) if (next.get(id) !== binding) removeBinding(context, id);
    for (const [id, binding] of next) context.bindings.set(id, binding);
    for (const binding of next.values()) queueProbe(context, binding);
  }
  function sweepPreparations(context) {
    const timestamp = now();
    for (const entry of context.preparations.values()) if (timestamp < entry.createdAt || timestamp >= entry.expiresAt) dropPreparation(context, entry);
  }
  function prepareTarget(context, frame) {
    channelFrame(context, frame, ['nonce', 'target']); localAppsCheck(localAppsNonce(frame.nonce), 'apps_bad_preparation');
    const value = target(context, frame.target); sweepPreparations(context);
    const existing = context.preparations.get(frame.nonce);
    if (existing) {
      localAppsCheck(existing.digest === value.digest, 'apps_preparation_id_conflict');
      if (existing.reply) send(context, existing.reply);
      return;
    }
    const rejected = error => send(context, { type: 'target-rejected', channelId: context.channelId, nonce: frame.nonce,
      ...localAppsPins(value), error });
    // Completed nonce replies are bounded separately from four in-flight HEADs.
    if (context.preparations.size >= 256) { rejected('app_prepare_busy'); return; }
    const error = policyError(value);
    let sameApp = 0;
    for (const entry of context.preparations.values()) if (entry.pending && entry.pins.appId === value.appId) sameApp++;
    if (!error && (context.preparing >= 4 || sameApp >= 2)) { rejected('app_prepare_busy'); return; }
    const timestamp = now(), entry = { nonce: frame.nonce, digest: value.digest, pins: localAppsPins(value),
      createdAt: timestamp, expiresAt: timestamp + 30_000, pending: !error, cancelled: false, reply: null, probe: null };
    context.preparations.set(entry.nonce, entry);
    entry.timer = setTimeout(() => dropPreparation(context, entry), 30_000); entry.timer.unref?.();
    if (error) {
      entry.reply = { type: 'target-rejected', channelId: context.channelId, nonce: entry.nonce, ...entry.pins, error };
      send(context, entry.reply); return;
    }
    context.preparing++;
    entry.probe = startLocalAppProbe(deps, { port: value.port, entryPath: value.entryPath, blockedPorts: blocked });
    void entry.probe.promise.then(result => {
      if (!connected(context) || entry.cancelled || context.preparations.get(entry.nonce) !== entry) return;
      if (now() < entry.createdAt || now() >= entry.expiresAt) { dropPreparation(context, entry); return; }
      entry.reply = { type: 'target-prepared', channelId: context.channelId, nonce: entry.nonce, ...entry.pins,
        state: result.state, httpStatus: result.httpStatus };
      send(context, entry.reply);
    }).finally(() => { context.preparing--; entry.pending = false; entry.probe = null; });
  }
  function observation(context, binding, result) {
    if (!connected(context) || context.bindings.get(binding.appId) !== binding) return;
    send(context, context.version === 2
      ? { type: 'bound-observation', channelId: context.channelId, syncId: binding.syncId, ...localAppsPins(binding.target),
        state: result.state, httpStatus: result.httpStatus }
      : { type: 'observation', appId: binding.appId, state: result.state === 'responding' ? 'ready' : 'stopped' });
  }
  function queueProbe(context, binding) {
    if (!connected(context) || context.bindings.get(binding.appId) !== binding) return;
    if (!context.probes.has(binding)) context.probeQueue.set(binding.appId, binding);
    if (context.probeScheduled) return;
    context.probeScheduled = true;
    queueMicrotask(() => { context.probeScheduled = false; pumpProbes(context); });
  }
  function pumpProbes(context) {
    if (!connected(context)) return;
    while (context.probes.size < 4 && context.probeQueue.size) {
      const [id, binding] = context.probeQueue.entries().next().value; context.probeQueue.delete(id);
      if (context.bindings.get(id) !== binding || context.probes.has(binding)) continue;
      let probe;
      try { probe = startLocalAppProbe(deps, { port: binding.port, entryPath: binding.entryPath, blockedPorts: blocked }); }
      catch { observation(context, binding, { state: 'unreachable', httpStatus: null }); continue; }
      context.probes.set(binding, probe);
      void probe.promise.then(result => observation(context, binding, result)).finally(() => {
        if (context.probes.get(binding) === probe) context.probes.delete(binding);
        pumpProbes(context);
      });
    }
  }
  function onFrame(context, frame) {
    if (frame.type === 'ready') {
      localAppsExact(frame, ['type', 'schema', 'bindingVersion', 'channelId']);
      localAppsCheck(!context.connected && frame.schema === localAppsSchema, 'apps_bad_negotiation');
      if (frame.bindingVersion === 2) {
        localAppsCheck(localAppsNonce(frame.channelId), 'apps_bad_negotiation'); context.version = 2; context.channelId = frame.channelId;
      } else {
        localAppsCheck((frame.bindingVersion === undefined || frame.bindingVersion === 1) && frame.channelId === undefined, 'apps_bad_negotiation'); context.version = 1;
      }
      context.connected = true; clearTimeout(context.authTimer); return;
    }
    localAppsCheck(connected(context), 'apps_not_authenticated');
    if (frame.type === 'claim-ready') {
      const pending = context.claimPending;
      if (pending && frame.claimDigest === pending.digest) {
        clearTimeout(pending.timer); context.claimAcknowledged = true; context.claimPending = null; pending.resolve();
      }
      return;
    }
    if (frame.type === 'claimed') { context.claimCode = ''; context.claimExpiresAt = 0; context.claimAcknowledged = false; return; }
    if (frame.type === 'binding-set') { setBinding(context, frame); return; }
    if (frame.type === 'binding-remove') { removeBindings(context, frame); return; }
    if (frame.type === 'target-prepare') { prepareTarget(context, frame); return; }
    if (frame.type === 'sync') { legacySync(context, frame); return; }
    if (frame.type === 'open' || frame.type === 'bound-open') { openStream(context, frame); return; }
    localAppsCheck(['cancel', 'ack', 'end', 'data'].includes(frame.type), 'apps_bad_frame');
    localAppsCheck(typeof frame.id === 'string' && /^[a-f0-9]{32}$/u.test(frame.id), 'apps_bad_stream');
    const stream = context.streams.get(frame.id); if (!stream) return;
    try {
      localAppsCheck(streamCurrent(stream), 'app_stream_closed');
      if (frame.type === 'cancel') { closeStream(stream, 'app_cancelled', false); return; }
      if (frame.type === 'ack') {
        const pending = stream.pending.get(frame.seq); localAppsCheck(pending, 'apps_bad_ack');
        clearTimeout(pending.timer); stream.pending.delete(frame.seq); pending.resolve(); return;
      }
      if (frame.type === 'end') {
        localAppsCheck(!stream.receiving, 'apps_bad_end'); (stream.socket || stream.request)?.end(); return;
      }
      localAppsCheck(!stream.receiving && frame.seq === stream.recvSeq + 1 && typeof frame.data === 'string'
        && frame.data.length <= localAppsChunkBytes * 4 / 3 + 4 && /^[A-Za-z0-9+/]*={0,2}$/u.test(frame.data), 'apps_bad_data');
      const bytes = deps.decodeBase64(frame.data); stream.received += bytes.length;
      localAppsCheck(bytes.length <= localAppsChunkBytes && (stream.kind !== 'http' || stream.received <= 8 * 1024 * 1024), 'app_request_too_large');
      localAppsCheck(context.asyncWrites < 64, 'apps_busy');
      stream.receiving = true; stream.recvSeq = frame.seq; stream.timer.refresh(); context.asyncWrites++;
      void write(stream.socket || stream.request, bytes).then(() => {
        localAppsCheck(streamCurrent(stream), 'app_stream_closed'); stream.receiving = false;
        sendStream(stream, { type: 'ack', id: stream.id, seq: frame.seq });
      }).catch(error => closeStream(stream, error.code || 'app_upstream_failed')).finally(() => { context.asyncWrites--; });
    } catch (error) { closeStream(stream, error.code || 'app_upstream_failed'); }
  }
  function openStream(context, frame) {
    if (frame.type === 'bound-open') {
      channelFrame(context, frame, ['syncId', 'appId', 'revision', 'digest', 'profile', 'id', 'kind', 'path', 'method', 'headers']);
      localAppsCheck(localAppsNonce(frame.syncId) && localAppsId(frame.appId) && Number.isSafeInteger(frame.revision) && frame.revision >= 1
        && typeof frame.digest === 'string' && /^[a-f0-9]{64}$/u.test(frame.digest) && typeof frame.profile === 'string', 'apps_bad_open');
    } else localAppsCheck(context.version === 1, 'apps_bad_open');
    localAppsCheck(typeof frame.id === 'string' && /^[a-f0-9]{32}$/u.test(frame.id)
      && !context.streams.has(frame.id) && ['http', 'ws'].includes(frame.kind), 'apps_bad_open');
    const binding = context.bindings.get(frame.appId);
    if (!binding || (context.version === 2 && (binding.syncId !== frame.syncId
      || ['appId', 'revision', 'digest', 'profile'].some(name => binding.target[name] !== frame[name])))) {
      send(context, { type: 'cancel', id: frame.id, error: 'app_access_changed' }); return;
    }
    if (context.streams.size >= 32) { send(context, { type: 'cancel', id: frame.id, error: 'app_device_busy' }); return; }
    let stream;
    try {
      const path = context.version === 2 ? localAppsHttpPath(frame.path, binding.port) : localAppsPath(frame.path);
      localAppsCheck(['GET', 'HEAD', 'POST', 'PUT', 'PATCH', 'DELETE', 'OPTIONS'].includes(frame.method), 'apps_bad_method');
      const headers = localAppsHeaders(frame.headers, frame.kind === 'ws' ? 'ws-request' : 'request');
      if (frame.kind === 'ws') { headers.connection = 'Upgrade'; headers.upgrade = 'websocket'; }
      stream = { id: frame.id, appId: binding.appId, kind: frame.kind, binding, context, received: 0, sent: 0,
        recvSeq: 0, sendSeq: 0, pending: new Map(), receiving: false, closed: false, head: false };
      context.streams.set(stream.id, stream);
      stream.timer = setTimeout(() => closeStream(stream, 'app_response_timeout'), 30_000); stream.timer.unref?.();
      localAppsCheck(streamCurrent(stream), 'app_stream_closed');
      const request = deps.httpRequest({ hostname: '127.0.0.1', port: binding.port, path, method: frame.method, headers, agent: false });
      stream.request = request;
      request.on('error', () => {
        if (!streamCurrent(stream)) return;
        observation(context, binding, { state: 'unreachable', httpStatus: null }); closeStream(stream, 'app_stopped');
      });
      request.on('response', response => {
        if (!streamCurrent(stream)) { response.destroy(); return; }
        if (stream.kind === 'ws') { response.destroy(); closeStream(stream, 'app_upgrade_failed'); return; }
        stream.response = response;
        try {
          localAppsCheck(Number.isInteger(response.statusCode) && response.statusCode >= 200 && response.statusCode <= 599, 'app_bad_head');
          const responseHeaders = localAppsHeaders(response.headers, 'response'); let location;
          if (response.headers.location) {
            const next = new URL(response.headers.location, `http://127.0.0.1:${binding.port}${path}`);
            localAppsCheck(next.origin === `http://127.0.0.1:${binding.port}`, 'app_external_redirect');
            location = context.version === 2 ? localAppsRuntimePath(next.pathname + next.search + next.hash) : localAppsPath(next.pathname + next.search + next.hash);
          }
          stream.head = true; idle(stream);
          observation(context, binding, { state: response.statusCode < 500 ? 'responding' : 'unreachable', httpStatus: response.statusCode });
          sendStream(stream, { type: 'head', id: stream.id, status: response.statusCode, headers: responseHeaders, ...(location ? { location } : {}) });
          void (async () => {
            try {
              for await (const bytes of response) {
                localAppsCheck(streamCurrent(stream), 'app_stream_closed'); stream.sent += bytes.length;
                localAppsCheck(stream.sent <= 64 * 1024 * 1024, 'app_response_too_large'); await sendChunks(stream, bytes);
              }
              sendStream(stream, { type: 'end', id: stream.id }); closeStream(stream, 'app_stream_complete', false);
            } catch (error) { closeStream(stream, error.code || 'app_upstream_failed'); }
          })();
        } catch (error) { closeStream(stream, error.code || 'app_upstream_failed'); }
      });
      request.on('upgrade', (response, socket, head) => {
        if (!streamCurrent(stream)) { socket.destroy(); return; }
        if (stream.kind !== 'ws' || response.statusCode !== 101) { socket.destroy(); closeStream(stream, 'app_upgrade_failed'); return; }
        stream.socket = socket; stream.head = true; idle(stream);
        socket.on('error', () => closeStream(stream, 'app_upstream_failed'));
        try { sendStream(stream, { type: 'head', id: stream.id, status: 101, headers: localAppsHeaders(response.headers, 'ws-response') }); }
        catch (error) { closeStream(stream, error.code || 'app_upstream_failed'); return; }
        void (async () => {
          try {
            if (head.length) await sendChunks(stream, head);
            for await (const bytes of socket) await sendChunks(stream, bytes);
            sendStream(stream, { type: 'end', id: stream.id }); closeStream(stream, 'app_stream_complete', false);
          } catch (error) { closeStream(stream, error.code || 'app_upstream_failed'); }
        })();
      });
      if (stream.kind === 'ws') request.end();
    } catch (error) {
      if (stream) closeStream(stream, error.code || 'app_upstream_failed');
      else send(context, { type: 'cancel', id: frame.id, error: error.code || 'app_upstream_failed' });
    }
  }
  function idle(stream) {
    clearTimeout(stream.timer); stream.timer = setTimeout(() => closeStream(stream, 'app_idle_timeout'), 120_000); stream.timer.unref?.();
  }
  async function sendChunks(stream, bytes) {
    for (let offset = 0; offset < bytes.length; offset += localAppsChunkBytes) {
      localAppsCheck(streamCurrent(stream), 'app_stream_closed');
      const seq = ++stream.sendSeq; stream.timer.refresh();
      await new Promise((resolve, reject) => {
        const pending = { resolve, reject };
        pending.timer = setTimeout(() => {
          if (stream.pending.get(seq) === pending) stream.pending.delete(seq);
          reject(localAppsFail('app_ack_timeout'));
        }, 30_000); pending.timer.unref?.(); stream.pending.set(seq, pending);
        try { sendStream(stream, { type: 'data', id: stream.id, seq, data: deps.encodeBase64(bytes.subarray(offset, offset + localAppsChunkBytes)) }); }
        catch (error) { clearTimeout(pending.timer); stream.pending.delete(seq); reject(error); }
      });
      localAppsCheck(streamCurrent(stream), 'app_stream_closed');
    }
  }
  return {
    start() {
      if (state.running) return;
      state.running = true; connect();
      state.interval = setInterval(() => {
        const context = state.context; if (!context || !connected(context)) return;
        for (const binding of context.bindings.values()) queueProbe(context, binding);
      }, 20_000); state.interval.unref?.();
    },
    stop() { state.running = false; clearTimeout(state.retry); clearInterval(state.interval); if (state.context) disconnect(state.context); },
    claim,
    status() { const context = state.context; return { connected: Boolean(context && connected(context)),
      apps: context?.bindings.size ?? 0, streams: context?.streams.size ?? 0 }; },
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
  try { return startLocalAppProbe(deps, { port, entryPath, blockedPorts }).promise.then(result => result.state === 'responding'); }
  catch { return Promise.resolve(false); }
}
function startLocalAppProbe(deps, { port, entryPath, blockedPorts }) {
  const validatedPort = localAppsPort(port, blockedPorts), path = localAppsHttpPath(entryPath, validatedPort);
  let request, timer, settled = false, resolveResult;
  const promise = new Promise(resolve => { resolveResult = resolve; });
  const finish = httpStatus => {
    if (settled) return;
    settled = true; clearTimeout(timer);
    try { request?.destroy(); } catch {}
    const valid = Number.isInteger(httpStatus) && httpStatus >= 200 && httpStatus <= 599;
    resolveResult({ state: valid && httpStatus < 500 ? 'responding' : 'unreachable', httpStatus: valid ? httpStatus : null });
  };
  try {
    request = deps.httpRequest({ hostname: '127.0.0.1', port: validatedPort, path, method: 'HEAD', agent: false });
    timer = setTimeout(() => finish(null), 3000); timer.unref?.();
    request.on('response', response => { const status = response.statusCode; response.destroy(); finish(status); });
    request.on('error', () => finish(null)); request.end();
  } catch { finish(null); }
  return { promise, cancel: () => finish(null) };
}
function localAppsPort(value, blocked = []) { if (!Number.isSafeInteger(value) || value < 1024 || value > 65535 || [49424, ...blocked].includes(value)) throw localAppsFail('invalid_app_port'); return value; }
function localAppsPath(value) {
  if (typeof value !== 'string' || value.length > 8192 || !value.startsWith('/') || value.startsWith('//') || /[\\\u0000-\u0020\u007f]/u.test(value)) throw localAppsFail('invalid_app_path');
  let decoded;
  try { decoded = decodeURIComponent(value.split('?')[0]); } catch { throw localAppsFail('invalid_app_path'); }
  if (decoded.startsWith('//') || /[\\\u0000-\u001f\u007f]/u.test(decoded)) throw localAppsFail('invalid_app_path'); return value;
}
function localAppsRuntimePath(value) {
  const path = localAppsPath(value);
  try {
    const decoded = decodeURIComponent(new URL(path, 'https://runtime.invalid').pathname);
    const normalized = new URL(decodeURIComponent(path.split('?', 1)[0]), 'https://runtime.invalid').pathname;
    if ([decoded, normalized].some(item => item.startsWith('//') || item.startsWith('/_soty/') || item === '/_soty')) throw localAppsFail('invalid_app_path');
    return path;
  } catch { throw localAppsFail('invalid_app_path'); }
}
function localAppsHttpPath(original, port) {
  localAppsRuntimePath(original);
  const url = new URL(original, `http://127.0.0.1:${port}`);
  return localAppsRuntimePath(url.pathname + url.search);
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
