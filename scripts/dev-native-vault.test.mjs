import test from 'node:test';
import assert from 'node:assert/strict';
import { createServer } from 'node:net';
import { mkdtemp, mkdir, readFile, rm, stat } from 'node:fs/promises';
import { tmpdir } from 'node:os';
import { dirname, join, resolve } from 'node:path';
import { fileURLToPath } from 'node:url';
import { parseArgs } from 'node:util';
import { DatabaseSync } from 'node:sqlite';
import vm from 'node:vm';
import ts from 'typescript';
import { startDevelopment } from './dev.mjs';
import { migrateNotes } from '../modules/notes/server/schema.mjs';
import { openMaterialSchema } from '../modules/materials/server/schema.mjs';
import { initializeVaultStorage } from '../modules/canvas-vault/storage.mjs';

const rootDir = fileURLToPath(new URL('../', import.meta.url));
const source = await readFile(new URL('./dev.mjs', import.meta.url), 'utf8');
const parsed = ts.createSourceFile('dev.mjs', source, ts.ScriptTarget.Latest, true, ts.ScriptKind.JS);
const printer = ts.createPrinter();
const print = node => printer.printNode(ts.EmitHint.Unspecified, node, parsed);
const named = name => parsed.statements.find(node => ts.isFunctionDeclaration(node) && node.name?.text === name);
const stores = [
  { name: 'notes', path: ['notes', 'notes.sqlite'], meta: 'notes_meta', version: 3,
    initialize: db => migrateNotes(db, 'soty', { allowVaultMigration: true }) },
  { name: 'materials', path: ['materials', 'materials.sqlite'], meta: 'material_meta', version: 2,
    initialize: db => openMaterialSchema(db, 'soty', { allowVaultMigration: true }) },
  { name: 'vault', path: ['canvas', 'vault.sqlite'], meta: 'vault_meta', version: 2,
    initialize: db => initializeVaultStorage(db, 'soty', { allowFormat2Migration: true }) },
];
const directoryClosers = new Map();

async function freePorts() {
  const servers = [];
  try {
    for (let i = 0; i < 3; i++) {
      const server = createServer(); servers.push(server);
      await new Promise((done, reject) => { server.once('error', reject); server.listen(0, '127.0.0.1', done); });
    }
    return servers.map(server => server.address().port);
  } finally { await Promise.all(servers.map(server => new Promise(done => server.close(done)))); }
}
async function reusable(port) {
  const server = createServer();
  try { await new Promise((done, reject) => { server.once('error', reject); server.listen(port, '127.0.0.1', done); }); }
  finally { if (server.listening) await new Promise(done => server.close(done)); }
}
async function ownedDirectory(t) {
  const dataDir = await mkdtemp(join(tmpdir(), 'soty-dev-native-'));
  const closers = new Set(); directoryClosers.set(dataDir, closers);
  t.after(async () => {
    for (const close of closers) await close();
    assert.equal(dirname(resolve(dataDir)), resolve(tmpdir()));
    assert.match(dataDir, /soty-dev-native-/u);
    await rm(dataDir, { recursive: true, force: true, maxRetries: 5, retryDelay: 100 });
    directoryClosers.delete(dataDir);
  });
  return dataDir;
}
function physical(dataDir, store) {
  const db = new DatabaseSync(join(dataDir, ...store.path), { readOnly: true });
  try {
    return { version: Number(db.prepare('PRAGMA user_version').get().user_version),
      meta: Object.fromEntries(db.prepare(`SELECT key,value FROM ${store.meta} ORDER BY key`).all().map(row => [row.key, row.value])),
      layout: db.prepare("SELECT name,sql FROM sqlite_schema WHERE name NOT GLOB 'sqlite_*' ORDER BY name").all().map(row => [row.name, row.sql]),
      integrity: db.prepare('PRAGMA quick_check(1)').get().quick_check };
  } finally { db.close(); }
}
async function running(t, dataDir, nativeVault, ports = undefined) {
  ports ??= await freePorts(); let dev;
  dev = await startDevelopment({ port: ports[0], apiPort: ports[1], connectorPort: ports[2], dataDir,
    ...(nativeVault === undefined ? {} : { nativeVault }), quiet: process.env.SOTY_DEV_NATIVE_TEST_OUTPUT !== '1', signal: t.signal });
  const stop = async () => {
    if (!dev) return;
    await dev.close(); assert.equal(await dev.done, null);
    await Promise.all(ports.slice(0, 2).map(reusable)); dev = null;
    directoryClosers.get(dataDir).delete(stop);
  };
  directoryClosers.get(dataDir).add(stop);
  return { dev, ports, stop };
}

test('nativeVault rejects non-booleans before reading paths or creating child resources', async () => {
  for (const value of ['true', 'false', 1, 0, null, {}, []]) {
    let pathReads = 0;
    await assert.rejects(startDevelopment({ nativeVault: value,
      get dataDir() { pathReads++; throw new Error('PATH_WAS_READ'); } }),
    error => error instanceof TypeError && /nativeVault.*boolean/u.test(error.message));
    assert.equal(pathReads, 0);
  }
});

test('actual child environment ignores inherited Native and production settings', () => {
  const declaration = parsed.statements.find(node => ts.isVariableStatement(node)
    && node.declarationList.declarations.some(value => value.name.getText(parsed) === 'platformEnvironment'));
  const context = { process: { env: { PATH: 'synthetic-path', SOTY_NATIVE_VAULT_ENABLED: '1',
    NODE_OPTIONS: '--import=untrusted', DATA_DIR: 'other-data', SOTY_GONKA_API_KEY: 'synthetic-production-key',
    SOTY_CONNECT_ORIGINS: 'https://production.invalid', SOTY_WORLD7_ENABLED: '1' } }, join, rootDir };
  vm.createContext(context);
  vm.runInContext(print(declaration) + '\n' + print(named('developmentEnvironment'))
    + '\nthis.environment = developmentEnvironment;', context);
  for (const nativeVault of [false, true]) {
    const env = context.environment({ apiPort: 53413, connectorPort: 53414,
      origin: 'http://127.0.0.1:53412', dataDir: 'synthetic-local-data', nativeVault });
    assert.equal(env.SOTY_NATIVE_VAULT_ENABLED, nativeVault ? '1' : '0');
    assert.equal(env.DATA_DIR, 'synthetic-local-data');
    assert.equal(env.SOTY_CONNECT_ORIGINS, 'http://127.0.0.1:53412');
    for (const key of ['NODE_OPTIONS', 'SOTY_GONKA_API_KEY', 'SOTY_WORLD7_ENABLED']) assert.equal(env[key], undefined);
  }
});

test('CLI opt-in and API option have the same strict boolean meaning, independent of ambient env', async () => {
  for (const args of [[], ['--native-vault']]) {
    let seen;
    const context = { process: { env: { SOTY_NATIVE_VAULT_ENABLED: '1' }, once() {}, off() {} },
      parseArgs: options => parseArgs({ ...options, args }), AbortController, join, rootDir,
      startDevelopment: async input => { seen = input; return { done: Promise.resolve(null) }; } };
    vm.createContext(context);
    vm.runInContext(print(named('main')) + '\nthis.invoke = main;', context);
    await context.invoke(); assert.equal(seen.nativeVault, args.length > 0);
  }
});

test('real default child keeps fresh Notes1 Materials1 Vault1 even with inherited native flag', { timeout: 35000 }, async t => {
  const previous = process.env.SOTY_NATIVE_VAULT_ENABLED; process.env.SOTY_NATIVE_VAULT_ENABLED = '1';
  t.after(() => { if (previous === undefined) delete process.env.SOTY_NATIVE_VAULT_ENABLED; else process.env.SOTY_NATIVE_VAULT_ENABLED = previous; });
  const dataDir = await ownedDirectory(t), runtime = await running(t, dataDir);
  assert.equal((await (await fetch(`${runtime.dev.origin}/health`, { signal: t.signal })).json()).ok, true);
  assert.deepEqual(stores.map(store => physical(dataDir, store).version), [1, 1, 1]);
  for (const store of stores) assert.equal(physical(dataDir, store).integrity, 'ok');
  await runtime.stop();
});

test('real explicit child creates Notes3 Materials2 Vault2 and a default reopen preserves them', { timeout: 35000 }, async t => {
  const dataDir = await ownedDirectory(t), first = await running(t, dataDir, true);
  assert.equal((await fetch(first.dev.origin, { signal: t.signal })).status, 200);
  const states = stores.map(store => physical(dataDir, store));
  assert.deepEqual(states.map(state => state.version), [3, 2, 2]);
  assert.equal(states[0].meta.lineage, 'soty.notes.sqlite.v3');
  assert.equal(states[1].meta.lineage, 'soty.materials.sqlite.v2');
  assert.ok(states[2].layout.some(([name]) => name === 'vault_cipher_chunks'));
  await first.stop();
  // Apps pins the exact registered origin template to its DATA. A reopen
  // retains the same ports; changing both flags and origin is a different case.
  const second = await running(t, dataDir, false, first.ports);
  assert.deepEqual(stores.map(store => physical(dataDir, store)), states);
  await second.stop();
});

for (const store of stores) for (const damage of ['future-format', 'extra-schema']) {
  test(`real child rejects ${store.name} ${damage} without rewriting its unsupported store`, { timeout: 35000 }, async t => {
    const dataDir = await ownedDirectory(t);
    for (const item of stores) {
      const file = join(dataDir, ...item.path); await mkdir(dirname(file), { recursive: true });
      const db = new DatabaseSync(file); try { item.initialize(db); } finally { db.close(); }
    }
    const file = join(dataDir, ...store.path), db = new DatabaseSync(file);
    try { db.exec(damage === 'future-format' ? 'PRAGMA user_version=100' : 'CREATE TABLE unknown_future_state(id TEXT PRIMARY KEY) STRICT'); }
    finally { db.close(); }
    const before = physical(dataDir, store), ports = await freePorts();
    await assert.rejects(startDevelopment({ nativeVault: true, port: ports[0], apiPort: ports[1],
      connectorPort: ports[2], dataDir, quiet: true, signal: t.signal }), /Cannot start development API/u);
    assert.deepEqual(physical(dataDir, store), before);
    assert.equal((await stat(file)).isFile(), true);
    await Promise.all(ports.slice(0, 2).map(reusable));
  });
}
