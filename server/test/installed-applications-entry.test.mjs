import test from 'node:test';
import assert from 'node:assert/strict';
import { createHash } from 'node:crypto';
import { mkdtemp, mkdir, writeFile, readFile, stat, rm } from 'node:fs/promises';
import { tmpdir } from 'node:os';
import { join, dirname, resolve, basename } from 'node:path';
import { pathToFileURL } from 'node:url';
import { fork } from 'node:child_process';
import { once } from 'node:events';
import { loadInstalledApplicationsEntry } from '../installed-applications-entry.js';
import { createHttpApp, waitForRejectedHttpStart } from '../http-app.js';
import { createClientWithStorage } from '../../modules/connect/browser/client.mjs';

const root = new URL('../../', import.meta.url);
const ORIGIN = 'https://installed-entry.fixture.test';
const digest = bytes => createHash('sha256').update(bytes).digest('hex');
async function fixture(t) {
  const folder = await mkdtemp(join(tmpdir(), 'installed-app-entry-'));
  const owner = {};
  t.after(async () => {
    await owner.stop?.();
    assert.equal(dirname(resolve(folder)), resolve(tmpdir())); assert.match(basename(folder), /^installed-app-entry-/u);
    await rm(folder, { recursive: true, force: true, maxRetries: 5, retryDelay: 20 });
  });
  const file = join(folder, 'operator.mjs'), data = join(folder, 'data'), marker = join(folder, 'events.json');
  const code = async text => {
    await writeFile(file, text);
    return { SOTY_INSTALLED_APPLICATIONS_MODULE: file, SOTY_INSTALLED_APPLICATIONS_SHA256: digest(Buffer.from(text)) };
  };
  return { folder, file, data, marker, code, owner };
}
function memory() {
  let value;
  return { async read() { return structuredClone(value ?? null); }, async claim(next) { value ??= structuredClone(next); return structuredClone(value); },
    async compareAndSwap(revision, next) { assert.equal(value.localRevision, revision); value = structuredClone(next); return structuredClone(value); } };
}
test('installed operator code can compose the existing OTsov tools without issuing user authority', async t => {
  const f = await fixture(t), scope = {
    appId: 'app-' + 'a'.repeat(32), target: { revision: 2, digest: 'b'.repeat(64) },
    sourceProfile: { id: 'soty.standard-workspace', version: 2, digest: '23061fc95610e3011ec42b3ecf229308bce44b0a1268408ca9b6ee138c98f7e9' },
    resource: { registryId: 'soty', environmentId: 'test', tenantId: 'synthetic-publisher', appId: 'app-' + 'a'.repeat(32),
      resourceId: 'otsov', selection: { kind: 'soty.workspace.v1', nativeId: 'otsov', incarnationId: 'one' } },
  };
  const config = await f.code(`export default async api => ({...await api.loadInstalledExistingApplications({
    createRegistry: () => api.createSelectedOtsovSourceApplications(${JSON.stringify(scope)}) }), close() {}});`);
  const installed = await loadInstalledApplicationsEntry(config); t.after(() => installed.close());
  assert.equal(installed.sourceReferences.length, 5);
  assert.equal(installed.sourceCredentialFor, undefined, 'registry metadata is not a delegated user credential');
  assert.deepEqual(installed.httpOptions.externalApplications.map(row => row.catalog.effects), [[], [], ['create'], [], ['update']]);
});
test('installed operator code can append bounded HIVE workspace2 tools without a delegated credential', async t => {
  const f = await fixture(t), appId = 'app-' + 'c'.repeat(32), scope = {
    appId, target: { revision: 2, digest: 'd'.repeat(64) },
    sourceProfile: { id: 'soty.standard-workspace', version: 2, digest: '23061fc95610e3011ec42b3ecf229308bce44b0a1268408ca9b6ee138c98f7e9' },
    resource: { registryId: 'soty', environmentId: 'test', tenantId: 'synthetic-publisher', appId,
      resourceId: 'hive', selection: { kind: 'soty.workspace.v1', nativeId: 'hive', incarnationId: 'one' } },
  };
  const config = await f.code(`export default async api => ({...await api.loadInstalledExistingApplications({
    createRegistry: () => api.createHiveWorkspaceSourceApplications(${JSON.stringify(scope)}) }), close() {}});`);
  const installed = await loadInstalledApplicationsEntry(config); t.after(() => installed.close());
  assert.equal(installed.sourceReferences.length, 2);
  assert.deepEqual(installed.sourceReferences.map(row => row.capabilityId), [
    `app.${appId}:hive.workspace.cells.context`, `app.${appId}:hive.workspace.additive.batch`,
  ]);
  assert.equal(installed.sourceCredentialFor, undefined);
  assert.deepEqual(installed.httpOptions.externalApplications.map(row => row.catalog.effects), [[], ['update']]);
});
async function boot(t, f, entryEnv = {}) {
  // The actual current index source is held; only dependency import URLs are
  // adapted in this OWN temporary copy. Its maintained installed-entry callsite
  // is executed directly; no proposed hookup or tracked/private state is changed.
  const index = await readFile(new URL('../index.js', import.meta.url), 'utf8');
  let source = index.replace(/from (["'])([^"']+)\1/gu, (original, _quote, specifier) => {
    if (specifier.startsWith('node:')) return original;
    const url = specifier.startsWith('.') ? new URL(specifier, new URL('../index.js', import.meta.url)).href : import.meta.resolve(specifier);
    return 'from ' + JSON.stringify(url);
  });
  const entry = join(f.folder, 'entry.mjs'), dist = join(f.folder, 'dist');
  await writeFile(entry, source); await mkdir(dist); await writeFile(join(dist, 'index.html'), '<title>Synthetic installed startup</title>');
  const env = {};
  for (const key of ['PATH', 'SystemRoot', 'WINDIR', 'TEMP', 'TMP']) if (process.env[key]) env[key] = process.env[key];
  Object.assign(env, { PORT: '0', HOST: '127.0.0.1', DATA_DIR: f.data, SOTY_DIST_DIR: dist, SOTY_CONNECT_ORIGINS: ORIGIN,
    SOTY_PERSONAL_AGENT_ENABLED: '0', SOTY_OAUTH_ENABLED: '0', SOTY_UNIVERSAL_OPERATOR_ENABLED: '0', ...entryEnv });
  const child = fork(entry, [], { execPath: process.execPath, execArgv: [], cwd: f.folder, env, stdio: ['ignore', 'pipe', 'pipe', 'ipc'] });
  let output = ''; child.stdout.on('data', bytes => { output += bytes; }); child.stderr.on('data', bytes => { output += bytes; });
  let stopped = false;
  const stop = async () => {
    if (stopped) return; stopped = true;
    if (child.exitCode === null && child.signalCode === null) {
      const exited = once(child, 'exit');
      if (child.connected) child.disconnect(); else child.kill('SIGTERM');
      const result = await Promise.race([exited, new Promise((_, reject) => { const timer = setTimeout(() => reject(Error('owned_startup_stop_timeout')), 10000); timer.unref(); })]);
      assert.equal(result[0], 0);
    }
  };
  f.owner.stop = stop;
  t.after(stop);
  const ready = await Promise.race([
    new Promise((done, reject) => { child.on('message', message => { if (message?.type === 'soty:ready') done(message); }); child.once('exit', () => reject(Error(output.includes('ERR_MODULE_NOT_FOUND') ? 'owned_startup_dependency_resolution' : 'owned_startup_exited_before_ready'))); }),
    new Promise((_, reject) => { const timer = setTimeout(() => reject(Error('owned_startup_ready_timeout')), 15000); timer.unref(); }),
  ]);
  assert.ok(ready.port > 0); assert.equal(output.includes('PRIVATE_ERROR_CANARY'), false);
  const origin = 'http://127.0.0.1:' + ready.port;
  const client = createClientWithStorage({ projectId: 'soty', endpoint: origin + '/api/connect/rpc',
    fetch: (url, init) => fetch(url, { ...init, headers: { ...init.headers, origin: ORIGIN } }) }, memory());
  t.after(() => client.dispose());
  return { child, origin, client, stop };
}
const options = dataDir => ({ dataDir, appHosting: { domainProfile: 'separate-site' }, namedAppZone: '', discoveryOrigin: '', universalAppsEnabled: true });

test('defaultOFF actual Root startup keeps signed Connect/static routes and no module/config/state read', async t => {
  const disabled = await loadInstalledApplicationsEntry({}); assert.deepEqual(disabled.httpOptions, {}); assert.deepEqual(disabled.sourceReferences, []);
  const f = await fixture(t), root = await boot(t, f);
  assert.equal(await (await fetch(root.origin + '/')).text(), '<title>Synthetic installed startup</title>');
  const actor = await root.client.bootstrap('Synthetic default Root'); assert.ok(actor.accountId);
  await root.stop(); await assert.rejects(stat(f.marker), error => error.code === 'ENOENT');
});

test('bad private path/hash/file preflight rejects before module factory or Root data/effects', async t => {
  const f = await fixture(t), env = await f.code('globalThis.__entryMustNotRun = true; export default () => {};');
  const directory = join(f.folder, 'directory.mjs'); await mkdir(directory);
  const cases = [{ SOTY_INSTALLED_APPLICATIONS_MODULE: f.file }, { SOTY_INSTALLED_APPLICATIONS_SHA256: env.SOTY_INSTALLED_APPLICATIONS_SHA256 },
    { ...env, SOTY_INSTALLED_APPLICATIONS_MODULE: 'relative.mjs' }, { ...env, SOTY_INSTALLED_APPLICATIONS_SHA256: 'a'.repeat(64) },
    { ...env, SOTY_INSTALLED_APPLICATIONS_MODULE: directory }, { ...env, SOTY_INSTALLED_APPLICATIONS_SHA256: 'A'.repeat(64) }];
  for (const bad of cases) await assert.rejects(loadInstalledApplicationsEntry(bad), error => error.code === 'installed_applications_entry_invalid');
  await writeFile(f.file, Buffer.alloc(131073, 65)); await assert.rejects(loadInstalledApplicationsEntry(env), error => error.code === 'installed_applications_entry_invalid');
  await writeFile(f.file, Buffer.from([0xff])); await assert.rejects(loadInstalledApplicationsEntry({ ...env, SOTY_INSTALLED_APPLICATIONS_SHA256: digest(Buffer.from([0xff])) }), error => error.code === 'installed_applications_entry_invalid');
  let getters = 0, traps = 0; const accessor = {}; Object.defineProperty(accessor, 'SOTY_INSTALLED_APPLICATIONS_MODULE', { get() { getters++; } });
  const proxy = new Proxy({}, { getOwnPropertyDescriptor() { traps++; } });
  for (const bad of [accessor, proxy]) await assert.rejects(loadInstalledApplicationsEntry(bad), error => error.code === 'installed_applications_entry_invalid');
  assert.equal(getters, 0); assert.equal(traps, 0); assert.equal(globalThis.__entryMustNotRun, undefined);
  await assert.rejects(stat(f.data), error => error.code === 'ENOENT');
});

test('one hash-approved private factory reaches actual Root HTTP and Connect; shutdown shared close runs once', async t => {
  const f = await fixture(t), module = `import {writeFileSync} from 'node:fs';
export default async api {
  let calls=0,closed=0;const save=()=>writeFileSync(${JSON.stringify(f.marker)},JSON.stringify({calls,closed}));
  const close=()=>{closed++;save();};api.ownClose(close);calls++;save();
  const composition=await api.loadInstalledExistingApplications();
  return {...composition,httpOptions:{...composition.httpOptions,nativeSourceHandlers:[{
    async handle(req,res){if(req.url!=='/__fixture/installed')return false;res.status(200).json({installed:true});return true;},close
  }]},close};
}`.replace('async api {', 'async function(api) {');
  const env = await f.code(module), root = await boot(t, f, env);
  assert.deepEqual(await (await fetch(root.origin + '/__fixture/installed', { method: 'POST', body: '{}' })).json(), { installed: true });
  const account = await root.client.bootstrap('Synthetic installed Root');
  assert.ok((await root.client.extension('access.principals.create', { expectedAccountId: account.accountId, label: 'Synthetic existing principal' })).principal.id);
  await root.stop(); assert.deepEqual(JSON.parse(await readFile(f.marker, 'utf8')), { calls: 1, closed: 1 });
});

test('factory throws opaque errors after registered effect: safe unavailable, cleanup once, no error getters', async t => {
  const f = await fixture(t);
  globalThis.__entryOwned = { effects: 0, closed: 0, getters: 0 };
  t.after(() => { delete globalThis.__entryOwned; });
  const env = await f.code(`export default function(api){const p=globalThis.__entryOwned;api.ownClose(()=>{p.closed++;});p.effects++;
    const error={};Object.defineProperty(error,'message',{get(){p.getters++;throw Error('PRIVATE_ERROR_CANARY');}});throw error;}`);
  await assert.rejects(loadInstalledApplicationsEntry(env), error => error.code === 'installed_applications_entry_unavailable' && error.cause === undefined);
  assert.deepEqual(globalThis.__entryOwned, { effects: 1, closed: 1, getters: 0 }); await assert.rejects(stat(f.data), error => error.code === 'ENOENT');
});

test('factory cannot add fixture/IAM configuration or forged Source references; owned effects close', async t => {
  const f = await fixture(t); globalThis.__entryRejectedClosed = 0; t.after(() => { delete globalThis.__entryRejectedClosed; });
  for (const tail of ["httpOptions:{...c.httpOptions,nativeNotesEnabled:true}", "sourceReferences:[{capabilityId:'fake',version:1,digest:'a'.repeat(64)}]", 'httpOptions:{...c.httpOptions,nativeSourceHandlers:null}']) {
    const env = await f.code(`export default async function(api){const close=()=>{globalThis.__entryRejectedClosed++;};api.ownClose(close);const c=await api.loadInstalledExistingApplications();return {...c,close,${tail}};}`);
    await assert.rejects(loadInstalledApplicationsEntry(env), error => error.code === 'installed_applications_entry_unavailable');
  }
  assert.equal(globalThis.__entryRejectedClosed, 3); await assert.rejects(stat(f.data), error => error.code === 'ENOENT');
});

test('accepted entry resources close on actual Root constructor rejection before storage, then repeated close is inert', async t => {
  const f = await fixture(t); globalThis.__entryRejectedStartClosed = 0; t.after(() => { delete globalThis.__entryRejectedStartClosed; });
  const env = await f.code(`export default async function(api){const close=()=>{globalThis.__entryRejectedStartClosed++;};api.ownClose(close);return {...await api.loadInstalledExistingApplications(),close};}`);
  const entry = await loadInstalledApplicationsEntry(env); let caught;
  try { createHttpApp(f.folder, { ...options(f.data), ...entry.httpOptions, universalAppsEnabled: 'invalid' }); } catch (error) { caught = error; }
  assert.equal(caught.code, 'universal_configuration_invalid'); await waitForRejectedHttpStart(caught); await entry.close(); await entry.close();
  assert.equal(globalThis.__entryRejectedStartClosed, 1); await assert.rejects(stat(f.data), error => error.code === 'ENOENT');
});

test('captured module bytes and callback refs survive later operator-file rewrite; own resources close once', async t => {
  const f = await fixture(t); globalThis.__entryCapturedClosed = 0; t.after(() => { delete globalThis.__entryCapturedClosed; });
  const env = await f.code(`export default async function(api){const close=()=>{globalThis.__entryCapturedClosed++;};return {...await api.loadInstalledExistingApplications(),close};}`);
  const entry = await loadInstalledApplicationsEntry(env); await writeFile(f.file, 'throw Error("PRIVATE_ERROR_CANARY");');
  assert.deepEqual(entry.sourceReferences, []); await entry.close(); await entry.close(); assert.equal(globalThis.__entryCapturedClosed, 1);
});

test('private CODE Source callback is sync, captured and cannot supply references or HTTP/billing options', async t => {
  const f = await fixture(t); globalThis.__entrySourceCallbacks = { calls: 0, closed: 0, getters: 0 };
  t.after(() => { delete globalThis.__entrySourceCallbacks; });
  const env = await f.code(`export default async function(api){const state=globalThis.__entrySourceCallbacks;
    const close=()=>{state.closed++;};api.ownClose(close);const c=await api.loadInstalledExistingApplications();
    const callback=()=>{state.calls++;return {token:'synthetic-only',audience:'https://fixture.invalid/mcp'};};
    const result={...c,close,sourceCredentialFor:callback};state.result=result;return result;}`);
  const entry = await loadInstalledApplicationsEntry(env), held = entry.sourceCredentialFor;
  assert.equal(globalThis.__entrySourceCallbacks.calls, 0); assert.deepEqual(entry.sourceReferences, []);
  assert.deepEqual(Object.keys(entry.httpOptions).sort(), ['externalApplications', 'nativeSourceHandlers']);
  globalThis.__entrySourceCallbacks.result.sourceCredentialFor = () => { throw Error('PRIVATE_ERROR_CANARY'); };
  assert.equal(entry.sourceCredentialFor, held); assert.equal(held().token, 'synthetic-only');
  await entry.close(); assert.equal(globalThis.__entrySourceCallbacks.closed, 1);
  for (const callback of ['async()=>({})', 'function*(){}', 'new Proxy(()=>({}),{get(){throw Error("PRIVATE_ERROR_CANARY")}})']) {
    const bad = await f.code(`export default async function(api){const state=globalThis.__entrySourceCallbacks;
      const close=()=>{state.closed++;};api.ownClose(close);return {...await api.loadInstalledExistingApplications(),close,sourceCredentialFor:${callback}};}`);
    await assert.rejects(loadInstalledApplicationsEntry(bad), error => error.code === 'installed_applications_entry_unavailable');
  }
  const getter = await f.code(`export default async function(api){const state=globalThis.__entrySourceCallbacks;
    const close=()=>{state.closed++;};api.ownClose(close);const result={...await api.loadInstalledExistingApplications(),close};
    Object.defineProperty(result,'sourceCredentialFor',{enumerable:true,get(){state.getters++;throw Error('PRIVATE_ERROR_CANARY');}});return result;}`);
  await assert.rejects(loadInstalledApplicationsEntry(getter), error => error.code === 'installed_applications_entry_unavailable');
  assert.equal(globalThis.__entrySourceCallbacks.closed, 5); assert.equal(globalThis.__entrySourceCallbacks.getters, 0);
  const returnedClose = await f.code(`export default async function(api){const state=globalThis.__entrySourceCallbacks;
    return {...await api.loadInstalledExistingApplications(),close:()=>{state.closed++;},sourceCredentialFor:async()=>({})};}`);
  await assert.rejects(loadInstalledApplicationsEntry(returnedClose), error => error.code === 'installed_applications_entry_unavailable');
  assert.equal(globalThis.__entrySourceCallbacks.closed, 6, 'known returned close is retained before callback validation');
});
