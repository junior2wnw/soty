import { createHash } from 'node:crypto';
import { AppsError, assertApps, appName, connectorKey, normalizeManifest, runtimePath } from './protocol.mjs';

// Browsing is a bounded, read-only request to one currently authenticated
// connector. The location is an opaque root digest plus relative segments;
// neither the browser nor Root supplies an absolute filesystem path.
export function createPreparedBrowse({ channels, send, secret, assertActor, resolveOwnedDevice, authenticateConnector, connectorCurrent, blockedPorts }) {
  const pending = new Map(); let closed = false;
  const segment = value => typeof value === 'string' && value.length > 0 && value.length <= 255
    && value.isWellFormed() && value !== '.' && value !== '..' && !/[\\/\u0000-\u001f\u007f]/u.test(value);
  const keys = (value, names) => value && typeof value === 'object' && !Array.isArray(value)
    && Object.keys(value).length === names.length && Object.keys(value).every(key => names.includes(key));
  const locationValid = value => keys(value, ['rootId', 'segments']) && typeof value.rootId === 'string' && /^[a-f0-9]{64}$/u.test(value.rootId)
    && Array.isArray(value.segments) && value.segments.length <= 32 && value.segments.every(segment)
    && Buffer.byteLength(JSON.stringify(value), 'utf8') <= 4096;
  function checkedResult(value, location, after) {
    if (location === null) {
      assertApps(keys(value, ['schema', 'roots']) && value.schema === 'soty.prepared-directories.v1'
        && Array.isArray(value.roots) && value.roots.length > 0 && value.roots.length <= 16, 'prepared_bad_response', 502);
      const ids = new Set();
      for (const root of value.roots) {
        assertApps(keys(root, ['id', 'label']) && typeof root.id === 'string' && /^[a-f0-9]{64}$/u.test(root.id) && !ids.has(root.id)
          && typeof root.label === 'string' && root.label.length > 0 && root.label.length <= 255
          && root.label.isWellFormed() && !/[\u0000-\u001f\u007f]/u.test(root.label), 'prepared_bad_response', 502);
        ids.add(root.id);
      }
      return value;
    }
    assertApps(keys(value, ['schema', 'location', 'entries', 'next', 'project', 'projectError']) && value.schema === 'soty.prepared-directories.v1'
      && locationValid(value.location) && value.location.rootId === location.rootId
      && JSON.stringify(value.location.segments) === JSON.stringify(location.segments)
      && Array.isArray(value.entries) && value.entries.length <= 100, 'prepared_bad_response', 502);
    let last = after;
    for (const name of value.entries) { assertApps(segment(name) && name > last, 'prepared_bad_response', 502); last = name; }
    assertApps(value.next === null || value.entries.length > 0 && value.next === last, 'prepared_bad_response', 502);
    assertApps(value.projectError === null || value.projectError === 'unreadable' && value.project === null, 'prepared_bad_response', 502);
    if (value.project !== null) {
      const project = value.project;
      assertApps(keys(project, ['name', 'digest', 'responding']) && /^[a-f0-9]{64}$/u.test(project.digest || '')
        && typeof project.responding === 'boolean', 'prepared_bad_response', 502);
      assertApps(appName(project.name) === project.name, 'prepared_bad_response', 502);
    }
    return value;
  }
  function checkedManifest(value) {
    assertApps(keys(value, ['schema', 'state', 'name', 'digest']) && value.schema === 'soty.prepared-manifest.v1'
      && ['present', 'missing'].includes(value.state), 'prepared_bad_response', 502);
    if (value.state === 'missing') assertApps(value.name === '' && value.digest === '', 'prepared_bad_response', 502);
    else assertApps(appName(value.name) === value.name && /^[a-f0-9]{64}$/u.test(value.digest || ''), 'prepared_bad_response', 502);
    return value;
  }
  const fail = () => new AppsError('prepared_device_unavailable', 503);
  function finish(entry, error, frame) {
    if (pending.get(entry.nonce) !== entry) return;
    pending.delete(entry.nonce); clearTimeout(entry.timer);
    if (error) entry.reject(error); else entry.resolve(frame);
  }
  function drop(channel) { for (const item of pending.values()) if (item.channel === channel) finish(item, fail()); }
  function handle(channel, frame) {
    if (frame.type !== 'prepared-browse-result') return false;
    assertApps(channel.bindingVersion === 2 && (channel.preparedBrowse || channel.preparedInspect || channel.preparedManifest) && frame.channelId === channel.channelId
      && typeof frame.nonce === 'string' && /^[A-Za-z0-9_-]{43}$/u.test(frame.nonce)
      && typeof frame.ok === 'boolean' && (frame.ok ? keys(frame, ['type', 'channelId', 'nonce', 'ok', 'result'])
        : keys(frame, ['type', 'channelId', 'nonce', 'ok']) || keys(frame, ['type', 'channelId', 'nonce', 'ok', 'code']) && frame.code === 'prepared_busy'), 'app_bad_frame');
    const item = pending.get(frame.nonce);
    if (item?.channel === channel) finish(item, frame.ok ? null : new AppsError(frame.code === 'prepared_busy' ? 'prepared_busy' : 'prepared_browse_failed', frame.code === 'prepared_busy' ? 429 : 422), frame.result);
    return true;
  }
  async function request(actor, args, kind = 'browse') {
    const inspect = kind === 'inspect', manifestOnly = kind === 'manifest';
    assertApps(!closed && args && typeof args === 'object' && !Array.isArray(args)
      && Object.keys(args).every(key => (inspect ? ['hostDeviceId', 'connectorId', 'location', 'digest'] : manifestOnly ? ['hostDeviceId', 'connectorId', 'location']
        : ['hostDeviceId', 'connectorId', 'location', 'after']).includes(key)), 'prepared_invalid_request');
    const location = args.location ?? null, after = args.after ?? '';
    assertApps((inspect ? locationValid(location) && typeof args.digest === 'string' && /^[a-f0-9]{64}$/u.test(args.digest) : manifestOnly ? locationValid(location)
      : (location === null || locationValid(location)) && (after === '' || segment(after)) && (location !== null || after === '')),
    'prepared_invalid_request');
    const identity = resolveOwnedDevice(actor, args.hostDeviceId, args.connectorId), key = connectorKey(identity), channel = channels.get(key);
    assertApps(channel && channel.bindingVersion === 2 && (inspect ? channel.preparedInspect : manifestOnly ? channel.preparedManifest : channel.preparedBrowse) === true && channel.ws.readyState === 1,
      'prepared_connector_unsupported', 503);
    const recheck = () => {
      assertApps(!closed, 'apps_closed', 503); assertActor(actor);
      assertApps(connectorKey(resolveOwnedDevice(actor, args.hostDeviceId, args.connectorId)) === key
        && channels.get(key) === channel && channel.ws.readyState === 1, 'prepared_device_unavailable', 503);
    };
    recheck(); assertApps(await authenticateConnector(channel.auth), 'prepared_device_unavailable', 503); recheck();
    assertApps(pending.size < 32 && [...pending.values()].filter(value => value.channel === channel).length < 4, 'prepared_browse_busy', 429);
    const nonce = secret();
    const response = await new Promise((resolve, reject) => {
      const item = { nonce, channel, resolve, reject, timer: null };
      item.timer = setTimeout(() => finish(item, fail()), 8000); item.timer.unref(); pending.set(nonce, item);
      if (!send(channel, inspect ? { type: 'prepared-inspect', channelId: channel.channelId, nonce, location, digest: args.digest }
        : manifestOnly ? { type: 'prepared-manifest', channelId: channel.channelId, nonce, location }
        : { type: 'prepared-browse', channelId: channel.channelId, nonce, location, after })) finish(item, fail());
    });
    recheck(); assertApps(await authenticateConnector(channel.auth), 'prepared_device_unavailable', 503); recheck();
    if (manifestOnly) return { result: checkedManifest(response), assertCurrent() { recheck(); assertApps(connectorCurrent(channel.auth) === true, 'prepared_device_unavailable', 503); } };
    if (!inspect) return checkedResult(response, location, after);
    assertApps(keys(response, ['schema', 'name', 'port', 'entryPath']), 'prepared_bad_response', 502);
    const manifest = normalizeManifest(response, blockedPorts);
    assertApps(runtimePath(manifest.entryPath) === manifest.entryPath && JSON.stringify(manifest) === JSON.stringify(response)
      && createHash('sha256').update(JSON.stringify(manifest)).digest('hex') === args.digest, 'prepared_bad_response', 502);
    return { manifest, assertCurrent() { recheck(); assertApps(connectorCurrent(channel.auth) === true, 'prepared_device_unavailable', 503); } };
  }
  return Object.freeze({ browse: (actor, args) => request(actor, args), inspect: (actor, args) => request(actor, args, 'inspect'),
    readManifest: (actor, args) => request(actor, args, 'manifest'), handle, drop,
    close() { closed = true; for (const item of pending.values()) finish(item, fail()); } });
}
