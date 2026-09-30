import { readSync, writeSync } from 'node:fs';
import { createServer } from 'node:http';
import { createHash } from 'node:crypto';
import path from 'node:path';
import express from 'express';
import { createNotesService } from '../../../modules/notes/server/index.mjs';
import { createCapabilitiesService, BUILTIN_CAPABILITIES } from '../../../modules/capabilities/server/index.mjs';
import { attachConnectModule } from '../../connect-module.js';
import { createOAuthHostProfile } from '../../capabilities-oauth-profile.js';
import { attachCapabilitiesOAuth } from '../../capabilities-oauth.js';
import { attachCapabilitiesActions } from '../../capabilities-actions.js';

// A test-only forwarding facade. Every pause follows a completed real sync
// domain call. Dedicated pipes avoid making the production upsert port async.
const sha = value => createHash('sha256').update(value).digest('hex');
const safeCode = error => typeof error?.code === 'string' && /^[a-z][a-z0-9_]{0,79}$/u.test(error.code)
  ? error.code : 'fixture_failure';
let server, connect, notes, caps, gate = [], eventCount = 0, closing = false;
function event(value) {
  if (++eventCount > 512) throw new Error('fixture_event_limit');
  const bytes = Buffer.from(JSON.stringify(value) + '\n');
  if (bytes.length > 2048) throw new Error('fixture_event_bytes');
  writeSync(4, bytes); // private parent/child pipe, never test stdout or a log
}
function checkpoint(method, args, result) {
  const index = gate.findIndex(item => item.method === method && item.model === args.model
    && (item.hash === undefined || item.hash === sha(args.id))
    && (method !== 'find' || result !== undefined && result.consumed === undefined)
    && (method !== 'consume' || result.status === 'consumed'));
  if (index < 0) return;
  const [item] = gate.splice(index, 1);
  event({ kind: 'barrier', point: item.point, method, model: args.model,
    ...(args.id ? { token: args.id } : {}) });
  const permit = Buffer.alloc(1);
  // No transaction remains here. The parent either sends one release byte or
  // forcibly kills this OS process by its bounded test deadline.
  const read = readSync(0, permit, 0, 1, null);
  if (read !== 1 || permit[0] !== 82) throw new Error('fixture_bad_release');
}
function forwardingStore(store) {
  return Object.freeze(Object.fromEntries(Object.entries(store).map(([name, method]) => [name, (...args) => {
    try {
      const value = method(...args);
      if (value && typeof value.then === 'function') throw new Error('fixture_async_domain_port');
      if (['upsert', 'consume', 'destroy', 'revokeByGrantId'].includes(name)) event({ kind: 'operation',
        method: name, model: args[0]?.model ?? 'family', status: value?.status ?? 'ok',
        ...(typeof args[0]?.id === 'string' ? { idHash: sha(args[0].id) } : {}) });
      checkpoint(name, args[0] ?? {}, value);
      return value;
    } catch (error) {
      event({ kind: 'operation', method: name, model: args[0]?.model ?? 'family', status: 'refused', code: safeCode(error),
        ...(typeof args[0]?.id === 'string' ? { idHash: sha(args[0].id) } : {}) });
      throw error;
    }
  }])));
}
async function close() {
  if (closing) return; closing = true;
  if (server?.listening) { server.closeAllConnections(); await new Promise(resolve => server.close(resolve)); }
  caps?.close(); notes?.close(); connect?.close();
}
async function initialize({ directory, origin, keys, distDir }) {
  const profile = createOAuthHostProfile({ enabled: true, issuer: origin + '/oauth',
    jwks: keys.jwks, cookieKeys: keys.cookieKeys, artifactKey: Buffer.from(keys.artifactKey, 'base64url'),
    artifactKeyId: keys.artifactKeyId }, { shellOrigins: [origin], audience: origin });
  const app = express();
  app.disable('x-powered-by');
  app.use((_req, res, next) => {
    res.set('Content-Security-Policy', "default-src 'none'; form-action 'self'"); next();
  });
  notes = createNotesService({ databasePath: path.join(directory, 'notes', 'notes.sqlite'), projectId: 'soty',
    verifyNativeContext: (token, mode) => caps.nativeNotes.verifyContext(token, mode) });
  caps = createCapabilitiesService({ databasePath: path.join(directory, 'capabilities', 'capabilities.sqlite'), projectId: 'soty',
    actorActive: actor => connect?.isActorActive(actor) === true,
    catalog: BUILTIN_CAPABILITIES.map(entry => ({ ...entry, executionEnabled: entry.capabilityId === 'notes.createDraft' && entry.version === 1 })),
    nativeNotes: { notes: notes.native, withAuthorityFence: action => connect.withAuthorityFence(action) },
    oauth: profile.domainConfiguration(action => connect.withAuthorityFence(action)) });
  connect = attachConnectModule(app, { dataDir: directory, origins: [origin], extensions: [notes, caps] });
  if (caps.nativeNotes.readiness().ready !== true || caps.oauth.readiness().available !== true) throw new Error('fixture_real_readiness_failed');
  const service = Object.freeze({ ...caps, oauth: Object.freeze({ ...caps.oauth,
    artifactStore: forwardingStore(caps.oauth.artifactStore) }) });
  attachCapabilitiesActions(app, { service, audience: origin,
    resourceMetadata: origin + '/.well-known/oauth-protected-resource' });
  attachCapabilitiesOAuth(app, { profile, service, distDir });
  app.use((_req, res) => res.status(404).json({ error: 'not_found' }));
  server = createServer(app);
  await new Promise((resolve, reject) => { server.once('error', reject); server.listen(0, '127.0.0.1', resolve); });
  return { port: server.address().port, pid: process.pid };
}

process.on('message', async message => {
  try {
    let result;
    if (message.command === 'initialize') result = await initialize(message.args);
    else if (message.command === 'arm') {
      if (!Array.isArray(message.args) || message.args.length > 4 || gate.length) throw new Error('fixture_bad_gate');
      gate = message.args;
      result = { armed: gate.length };
    } else if (message.command === 'close') { await close(); result = { closed: true }; }
    else throw new Error('fixture_unknown_command');
    process.send({ requestId: message.requestId, result }, () => {
      if (message.command === 'close') process.disconnect();
    });
  } catch (error) {
    process.send({ requestId: message.requestId, error: safeCode(error) });
    if (message.command === 'initialize') { await close().catch(() => {}); process.exitCode = 1; process.disconnect(); }
  }
});
