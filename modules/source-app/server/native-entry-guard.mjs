import { request as httpRequest } from 'node:http';
import { request as httpsRequest } from 'node:https';
import { performance } from 'node:perf_hooks';

const fail = (code, status = 403) => Object.assign(new Error(code), { code, status });
const need = (value, code = 'native_entry_configuration_invalid', status = 503) => { if (!value) throw fail(code, status); };
const exact = (value, fields) => value && typeof value === 'object' && !Array.isArray(value)
  && Object.keys(value).every(key => fields.includes(key)) && fields.every(key => Object.hasOwn(value, key));
const hash = value => typeof value === 'string' && /^[a-f0-9]{64}$/u.test(value);
const positive = value => Number.isSafeInteger(value) && value >= 1;
const bodyFields = ['schema','appId','origin','primaryTargetRevision','primaryTargetDigest',
  'nativeTargetRevision','nativeTargetDigest','policyEpoch','admissionKeyHash','leaseMs'];

/** Fixed OPERATOR configuration, not request arguments. This is only a short
 * app-publication lease. Native MUST still check its own Human/ACL/CSRF boundary.
 * No cookie, token, browser header, redirect or caller supplied proof is used. */
export function createNativeEntryGuard(configuration) {
  need(configuration && typeof configuration === 'object' && !Array.isArray(configuration)
    && Object.keys(configuration).every(key => ['rootCheckUrl','appId','origin','primaryTargetDigest','nativeTargetDigest',
      'timeoutMs','heartbeatMs','leaseMs','maxStreams','maxRequests'].includes(key)));
  const { rootCheckUrl, appId, origin, primaryTargetDigest, nativeTargetDigest,
    timeoutMs = 750, heartbeatMs = 250, leaseMs = 1000, maxStreams = 128, maxRequests = 128 } = configuration;
  need(/^app-[a-f0-9]{32}$/u.test(appId) && hash(primaryTargetDigest) && hash(nativeTargetDigest));
  let endpoint, native;
  try { endpoint = new URL(rootCheckUrl); native = new URL(origin); } catch { throw fail('native_entry_configuration_invalid', 503); }
  need(native.origin === origin && native.protocol === 'https:' && !native.username && !native.password
    && !endpoint.username && !endpoint.password && !endpoint.search && !endpoint.hash
    && endpoint.pathname === '/_soty/ingress-check' && (endpoint.protocol === 'https:'
      || endpoint.protocol === 'http:' && ['127.0.0.1','[::1]'].includes(endpoint.hostname)));
  for (const [value, maximum] of [[timeoutMs,1000],[heartbeatMs,250],[leaseMs,1000],[maxStreams,128],[maxRequests,128]])
    need(Number.isSafeInteger(value) && value >= 1 && value <= maximum);
  need(heartbeatMs < leaseMs);
  const leases = new WeakMap(), streams = new Set(), requests = new Set(); let closed = false;
  const timestamp = () => performance.now();
  function assertCurrent(lease) {
    const state = leases.get(lease), time = timestamp();
    need(!closed && state && !state.revoked && time >= state.started && time < state.deadline, 'native_entry_lease_closed', 403);
    return lease;
  }
  async function acquire({ signal } = {}) {
    need(!closed && !signal?.aborted, 'native_entry_closed', 403);
    need(requests.size < maxRequests, 'native_entry_request_capacity', 503);
    const started = timestamp(), controller = new AbortController(); requests.add(controller);
    const abort = () => controller.abort(); signal?.addEventListener('abort', abort, { once: true });
    try {
      const result = await new Promise((resolve, reject) => {
        let settled = false, request;
        const finish = (error, value) => { if (settled) return; settled = true; clearTimeout(timer); error ? reject(error) : resolve(value); };
        const timer = setTimeout(() => { request?.destroy(); finish(fail('native_entry_policy_timeout', 503)); }, timeoutMs);
        const transport = endpoint.protocol === 'https:' ? httpsRequest : httpRequest;
        request = transport(endpoint, { method: 'GET', signal: controller.signal, headers: {
          Host: native.host, Accept: 'application/json', 'x-soty-ingress-surface': 'native',
          'x-soty-ingress-app': appId, 'x-soty-ingress-primary': primaryTargetDigest, 'x-soty-ingress-target': nativeTargetDigest,
        } }, response => {
          const chunks = []; let bytes = 0;
          if (response.statusCode !== 200 || !/^application\/json(?:;|$)/iu.test(response.headers['content-type'] ?? '')) {
            response.destroy(); request.destroy(); finish(fail('native_entry_policy_denied', 403)); return;
          }
          response.on('data', chunk => { bytes += chunk.length; if (bytes > 4096) {
            response.destroy(); request.destroy(); finish(fail('native_entry_policy_invalid', 503));
          } else chunks.push(chunk); });
          response.once('error', () => finish(fail('native_entry_policy_unavailable', 503)));
          response.once('end', () => {
            let body; try { body = JSON.parse(Buffer.concat(chunks).toString('utf8')); }
            catch { finish(fail('native_entry_policy_invalid', 503)); return; }
            finish(null, body);
          });
        });
        request.once('error', () => finish(fail('native_entry_policy_unavailable', 503))); request.end();
      });
      need(exact(result, bodyFields) && result.schema === 'soty.native-entry-policy.v1' && result.appId === appId && result.origin === origin
        && result.primaryTargetDigest === primaryTargetDigest && result.nativeTargetDigest === nativeTargetDigest
        && positive(result.primaryTargetRevision) && positive(result.nativeTargetRevision) && positive(result.policyEpoch)
        && hash(result.admissionKeyHash) && positive(result.leaseMs) && result.leaseMs <= 1000,
      'native_entry_policy_invalid', 503);
      const lease = Object.freeze({ schema: 'soty.native-entry-lease.v1', appId, policyEpoch: result.policyEpoch,
        admissionKeyHash: result.admissionKeyHash, primaryTargetDigest, nativeTargetDigest });
      leases.set(lease, { started, deadline: started + Math.min(leaseMs, result.leaseMs), revoked: false });
      need(!signal?.aborted, 'native_entry_closed', 403); return assertCurrent(lease);
    } finally { signal?.removeEventListener('abort', abort); requests.delete(controller); }
  }
  async function openStream({ onClose } = {}) {
    need(typeof onClose === 'function' && !closed, 'native_entry_closed', 403);
    need(streams.size < maxStreams, 'native_entry_stream_capacity', 503);
    const state = { lease: null, refresh: null, closed: false, timer: null, expiry: null, controller: new AbortController() };
    streams.add(state); // Reserve BEFORE asynchronous Root I/O.
    const stop = (reason = 'native_entry_stream_closed') => {
      if (state.closed) return; state.closed = true; clearTimeout(state.timer); clearTimeout(state.expiry);
      state.controller.abort(); streams.delete(state);
      if (state.lease) leases.get(state.lease).revoked = true;
      try { onClose(reason); } catch { /* A broken consumer cannot retain its lease. */ }
    };
    state.stop = stop;
    const refresh = () => {
      need(!state.closed, 'native_entry_lease_closed', 403);
      if (state.refresh) return state.refresh;
      const flight = (async () => {
        const next = await acquire({ signal: state.controller.signal });
        need(!state.closed, 'native_entry_lease_closed', 403);
        if (state.lease) need(next.policyEpoch === state.lease.policyEpoch && next.admissionKeyHash === state.lease.admissionKeyHash,
          'native_entry_policy_changed', 403);
        if (state.lease) leases.get(state.lease).revoked = true;
        state.lease = next; clearTimeout(state.expiry);
        state.expiry = setTimeout(() => stop('native_entry_lease_expired'), Math.max(1, leases.get(next).deadline - timestamp()));
        state.expiry.unref?.(); return next;
      })().catch(error => { stop(error.code ?? 'native_entry_policy_unavailable'); throw error; });
      state.refresh = flight;
      flight.finally(() => { if (state.refresh === flight) state.refresh = null; }).catch(() => {});
      return flight;
    };
    const schedule = () => {
      if (state.closed) return;
      state.timer = setTimeout(() => { refresh().then(schedule, () => {}); }, heartbeatMs); state.timer.unref?.();
    };
    try { await refresh(); schedule(); }
    catch (error) { stop(error.code); throw error; }
    return Object.freeze({
      beforeEffect: refresh,
      assertCurrent() { need(!state.closed, 'native_entry_lease_closed', 403); return assertCurrent(state.lease); },
      close: stop,
    });
  }
  return Object.freeze({ acquire, assertCurrent, openStream,
    close() { if (closed) return; closed = true; for (const controller of requests) controller.abort();
      for (const state of [...streams]) state.stop('native_entry_guard_closed'); },
  });
}
