import test from 'node:test';
import assert from 'node:assert/strict';
import vm from 'node:vm';
import { readFileSync } from 'node:fs';
import { dirname, resolve, join } from 'node:path';
import { fileURLToPath, pathToFileURL } from 'node:url';
import { createRequire } from 'node:module';
const sourcePath = process.argv[2] ? resolve(process.argv[2]) : fileURLToPath(new URL('./field-directory.ts', import.meta.url));
const repo = process.argv[3] ? resolve(process.argv[3]) : resolve(dirname(sourcePath), '../..');
const ts = createRequire(pathToFileURL(join(repo, 'package.json')))('typescript');
const compile = source => ts.transpileModule(source, { compilerOptions: { target: ts.ScriptTarget.ES2022, module: ts.ModuleKind.CommonJS } }).outputText;
const contractCode = compile(readFileSync(join(repo, 'modules/field/contract.mjs'), 'utf8'));
const directoryCode = compile(readFileSync(sourcePath, 'utf8'));
const plain = value => JSON.parse(JSON.stringify(value));
const gate = () => { let resolve; const promise = new Promise(done => { resolve = done; }); return { promise, resolve }; };
const settle = async () => { for (let n = 0; n < 10; n++) await new Promise(done => setImmediate(done)); };
const group = i => ({ communityId: `group_${i}`, name: `Группа ${i}`, description: '', symbol: 'cells', color: '#abcdef', revision: 1, memberCount: 0, previewMembers: [], membership: null, unreadCount: 0 });
const person = i => ({ profileId: `person_${i}`, displayName: `Человек ${i}`, bio: '', avatarColor: '#abcdef', revision: 1 });
const app = i => ({ id: 'app-' + String(i).padStart(32, '0'), name: `Приложение ${i}`, ownerAccountId: 'owner', state: 'ready', access: 'owner', canManage: true, createdAt: i, updatedAt: i,
  entry: { appId: 'app-' + String(i).padStart(32, '0'), domainId: 'dom_' + String(i).padStart(32, '0'), origin: 'https://example.invalid', path: '/' } });
function fixture({ groups = 20, people = 20, apps = 20, waits = {}, failed = '', invalid = '', shortApps = false } = {}) {
  let current = true, active = 0;
  const state = { calls: [], maxActive: 0, progress: [] }, values = { communities: Array.from({ length: groups }, (_, i) => group(i)), people: Array.from({ length: people }, (_, i) => person(i)), apps: Array.from({ length: apps }, (_, i) => app(i)) };
  const api = { async request(op, args) {
    const kind = op === 'apps.devices' ? 'devices' : op === 'contacts.list' ? 'contacts' : op.startsWith('apps.') ? 'apps' : args.kind;
    state.calls.push({ kind, op, args: plain(args) }); active++; state.maxActive = Math.max(state.maxActive, active);
    try {
      if (op.includes('.directory.')) assert.equal(args.expectedAccountId, 'owner');
      if (waits[kind]) await waits[kind].promise;
      if (kind === invalid) throw Object.assign(new Error('Synthetic invalid cursor'), { code: 'invalid_directory_cursor' });
      if (kind === failed) throw Object.assign(new Error('Synthetic source offline'), { code: 'NETWORK_ERROR' });
      if (kind === 'devices') return { devices: [] };
      if (kind === 'contacts') return { contacts: [] };
      const rows = values[kind], after = Number(args.cursor ?? 0), batch = rows.slice(after, after + args.limit);
      const nextCursor = after + batch.length < rows.length ? String(after + batch.length) : null;
      if (kind === 'apps') return shortApps ? { apps: [], nextCursor: '1' } : { apps: batch, nextCursor };
      return { people: kind === 'people' ? batch : [], communities: kind === 'communities' ? batch : [], nextCursor };
    } finally { active--; }
  } };
  const environment = { TextEncoder, crypto };
  vm.createContext(environment);
  const module = (source, key) => { vm.runInContext(`globalThis.${key}={};\n(function(exports,require){\n${source}\n})(globalThis.${key},globalThis.require);`, environment); return environment[key]; };
  const contract = module(contractCode, 'contractExports');
  environment.require = specifier => { assert.equal(specifier, '../../modules/field/contract.mjs'); return contract; };
  const directory = module(directoryCode, 'directoryExports').createFieldDirectory({ api, accountId: 'owner', isCurrent: () => current });
  const onProgress = value => state.progress.push(plain(value));
  return { directory, state, onProgress, stop() { current = false; } };
}

test('fast app metadata is published while both world sources remain pending; final source order and page size stay deterministic', async () => {
  const communities = gate(), people = gate(), f = fixture({ waits: { communities, people } });
  const pending = f.directory.search({ limit: 6, onProgress: f.onProgress }); let early;
  try { await settle(); early = { progress: [...f.state.progress], calls: [...f.state.calls], max: f.state.maxActive }; }
  finally { communities.resolve(); people.resolve(); }
  const page = await pending;
  assert.ok(early.progress.some(value => value.source === 'apps' && value.items.length === 2));
  assert.ok(early.max <= 3); assert.equal(early.calls.filter(value => value.kind === 'apps').length, 1);
  assert.deepEqual(plain(page.items.map(item => item.entity.kind)), ['community', 'community', 'person', 'person', 'app', 'app']); assert.equal(page.items.length, 6);
  assert.equal(page.status, 'ready'); assert.ok(page.cursor); assert.equal(f.state.calls.length, 3);
});

test('small one-result budgets preserve fair source rotation and do not prefetch extra source identities', async () => {
  const f = fixture(); const seen = []; let cursor;
  for (let n = 0; n < 6; n++) { const before = f.state.calls.length; const page = await f.directory.search({ limit: 1, ...(cursor ? { cursor } : {}), onProgress: f.onProgress }); assert.equal(f.state.calls.length - before, 1); seen.push(page.items[0].entity.kind); cursor = page.cursor; }
  assert.deepEqual(seen, ['community', 'person', 'app', 'community', 'person', 'app']); assert.ok(f.state.progress.every(value => value.items.length === 1));
});

test('empty neighboring sources release their fair remainder without losing or duplicating app cursor positions', async () => {
  const f = fixture({ groups: 0, people: 0, apps: 100 });
  const first = await f.directory.search({ limit: 12, onProgress: f.onProgress });
  assert.equal(first.items.length, 12); assert.deepEqual(plain(first.items.map(item => item.entity.id)), Array.from({ length: 12 }, (_, i) => app(i).id));
  const cursor = JSON.parse(first.cursor); assert.equal(cursor.version, 2); assert.equal(cursor.accountId, 'owner'); assert.equal(cursor.sources.apps.cursor, '12');
  assert.deepEqual(f.state.calls.filter(c => c.kind === 'apps').map(c => c.args.limit), [4, 8]);
  const next = await f.directory.search({ limit: 12, cursor: first.cursor, onProgress: f.onProgress });
  assert.deepEqual(plain(next.items.map(item => item.entity.id)), Array.from({ length: 12 }, (_, i) => app(i + 12).id)); assert.ok(f.state.maxActive <= 3);
});

test('a short server page with a live cursor is neither EOF nor permission to scan another page immediately', async () => {
  const f = fixture({ groups: 0, people: 0, apps: 100, shortApps: true });
  const page = await f.directory.search({ limit: 12, onProgress: f.onProgress });
  assert.equal(page.items.length, 0); assert.ok(page.cursor); assert.equal(JSON.parse(page.cursor).sources.apps.done, false); assert.equal(f.state.calls.filter(c => c.kind === 'apps').length, 1);
});

test('one failed source leaves successful app progress and explicit errors that persist through later cursor pages', async () => {
  const f = fixture({ failed: 'people' });
  const first = await f.directory.search({ limit: 6, onProgress: f.onProgress }); assert.equal(first.status, 'partial'); assert.equal(first.errors[0].source, 'people');
  assert.ok(f.state.progress.some(value => value.source === 'apps' && value.items.length));
  const second = await f.directory.search({ limit: 6, cursor: first.cursor, onProgress: f.onProgress }); assert.equal(second.status, 'partial'); assert.equal(second.errors[0].source, 'people');
  assert.equal(f.state.calls.filter(value => value.kind === 'people').length, 1); assert.ok(f.state.maxActive <= 3);
});

test('the account boundary rejects held old metadata and prevents new progress after disposal', async () => {
  const communities = gate(), people = gate(), f = fixture({ waits: { communities, people } });
  const pending = f.directory.search({ limit: 6, onProgress: f.onProgress }); await settle(); const before = f.state.progress.length; f.stop(); communities.resolve(); people.resolve();
  await assert.rejects(pending, { code: 'directory_account_changed' }); await settle(); assert.equal(f.state.progress.length, before);
  assert.throws(() => f.directory.getRecord({ kind: 'app', id: app(0).id }), { code: 'directory_account_changed' });
});

test('a fatal first source closes progress before late neighboring responses settle', async () => {
  const apps = gate(), people = gate(), f = fixture({ invalid: 'communities', waits: { apps, people } });
  let settled = false;
  const pending = f.directory.search({ limit: 6, onProgress: f.onProgress }).then(value => { settled = true; return { value }; }, error => { settled = true; return { error }; });
  await settle(); const before = f.state.progress.length; const earlySettled = settled;
  apps.resolve(); people.resolve(); const outcome = await pending; await settle();
  assert.equal(earlySettled, false, 'A failed page must retain its read slot until held neighboring RPCs settle');
  assert.equal(outcome.error?.code, 'invalid_directory_cursor'); assert.equal(f.state.progress.length, before);
});

test('Mine keeps apps among the bounded first three concurrent sources without exposing devices in public search', async () => {
  const communities = gate(), contacts = gate(), f = fixture({ waits: { communities, contacts } });
  const pending = f.directory.loadMine({ limit: 8, onProgress: f.onProgress }); let early;
  try { await settle(); early = [...f.state.progress]; assert.ok(f.state.maxActive <= 3); }
  finally { communities.resolve(); contacts.resolve(); }
  await pending; assert.ok(early.some(value => value.source === 'apps' && value.items.length));
  const publicOnly = fixture(); await publicOnly.directory.search({ limit: 6, onProgress: publicOnly.onProgress }); assert.equal(publicOnly.state.calls.some(value => ['contacts', 'devices'].includes(value.kind)), false);
});


test('a fatal nonfirst prefetched source suppresses later success while the first ordered source is still held', async () => {
  const communities = gate(), people = gate(), f = fixture({ invalid: 'apps', waits: { communities, people } });
  let settled = false;
  const pending = f.directory.search({ limit: 6, onProgress: f.onProgress }).then(value => { settled = true; return { value }; }, error => { settled = true; return { error }; });
  await settle(); people.resolve(); await settle(); const early = { count: f.state.progress.length, settled };
  communities.resolve(); const outcome = await pending;
  assert.equal(early.count, 0, 'A fatal prefetched source closes progress immediately, before ordered consumption reaches it');
  assert.equal(early.settled, false, 'The read slot remains owned until started neighboring RPCs settle');
  assert.equal(outcome.error?.code, 'invalid_directory_cursor'); assert.equal(f.state.progress.length, 0);
});
