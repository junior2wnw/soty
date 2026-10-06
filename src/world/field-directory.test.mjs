import test from 'node:test';
import assert from 'node:assert/strict';
import { createFieldDirectory } from './field-directory.ts';

const group = index => ({ communityId: `group_${index}`, name: `Группа ${index}`, description: '', symbol: '✦', color: '#abcdef',
  joinPolicy: 'open', revision: 1, memberCount: 0, previewMembers: [], membership: null, permissions: { canWrite: false, canManage: false, canModerate: false }, unreadCount: 0 });
const person = index => ({ profileId: `profile_${index}`, displayName: `Человек ${index}`, bio: '', interests: [], avatarColor: '#abcdef', avatarRevision: 4, revision: 1 });
const app = index => ({ id: `app-${String(index).padStart(32, '0')}`, name: `Приложение ${index}`, ownerAccountId: 'other_account', state: 'offline',
  createdAt: index, updatedAt: index, access: 'public', canManage: false, entry: { appId: `app-${String(index).padStart(32, '0')}`, domainId: `dom_${String(index).padStart(32, '0')}`, origin: 'https://example.invalid', path: '/' } });
const failure = code => Object.assign(new Error(code), { code });
function fixture({ groups = 0, people = 0, apps = 0, failed = '' } = {}) {
  const calls = [], values = { communities: Array.from({ length: groups }, (_, index) => group(index)), people: Array.from({ length: people }, (_, index) => person(index)), apps: Array.from({ length: apps }, (_, index) => app(index)) };
  return { calls, api: { async request(op, args) {
    calls.push({ op, args: structuredClone(args) });
    if (op.includes('.directory.')) assert.equal(args.expectedAccountId, 'owner');
    if (op === 'apps.devices') { assert.deepEqual(args, {}); return { devices: [] }; }
    const kind = op.startsWith('apps.') ? 'apps' : args.kind;
    if (kind === failed) throw failure('network_offline');
    const after = Number(args.cursor ?? 0), rows = values[kind], page = rows.slice(after, after + args.limit);
    const nextCursor = after + page.length < rows.length ? String(after + page.length) : null;
    return kind === 'apps' ? { apps: page, nextCursor } : { people: kind === 'people' ? page : [], communities: kind === 'communities' ? page : [], nextCursor };
  } } };
}

test('fair federation surfaces people/groups/apps on the first page and never exhausts a large world before applications', async () => {
  const f = fixture({ groups: 100, people: 100, apps: 100 }), directory = createFieldDirectory({ api: f.api, accountId: 'owner' });
  const first = await directory.search({ limit: 6 });
  assert.equal(first.items.length, 6); assert.deepEqual(first.items.map(item => item.entity.kind), ['community', 'community', 'person', 'person', 'app', 'app']);
  assert.equal(f.calls.length, 3); assert.ok(first.cursor);
  const second = await directory.search({ limit: 6, cursor: first.cursor });
  assert.equal(second.items.length, 6); assert.equal(second.items.some(item => first.items.some(before => before.entity.id === item.entity.id)), false);
  assert.equal(second.status, 'ready'); directory.dispose();
});

test('a one-result budget rotates sources rather than starving later kinds', async () => {
  const f = fixture({ groups: 20, people: 20, apps: 20 }), directory = createFieldDirectory({ api: f.api, accountId: 'owner' });
  let cursor; const kinds = [];
  for (let index = 0; index < 6; index++) { const result = await directory.search({ limit: 1, ...(cursor ? { cursor } : {}) }); kinds.push(result.items[0].entity.kind); cursor = result.cursor; }
  assert.deepEqual(kinds, ['community', 'person', 'app', 'community', 'person', 'app']); directory.dispose();
});

test('source errors remain explicit on later pages while successful sources are still searchable', async () => {
  const f = fixture({ groups: 20, people: 20, apps: 20, failed: 'people' }), directory = createFieldDirectory({ api: f.api, accountId: 'owner' });
  const first = await directory.search({ limit: 4 }); assert.equal(first.status, 'partial'); assert.equal(first.errors[0].source, 'people');
  const second = await directory.search({ limit: 4, cursor: first.cursor }); assert.equal(second.status, 'partial'); assert.equal(second.errors[0].source, 'people');
  assert.ok(second.items.some(item => item.entity.kind === 'app')); directory.dispose();
});

test('federation cursor binds query/kinds/mode/account; app raw record stays separate from entity projection', async () => {
  const f = fixture({ groups: 10, apps: 10 }), directory = createFieldDirectory({ api: f.api, accountId: 'owner' });
  const page = await directory.search({ limit: 2 });
  await assert.rejects(directory.search({ query: 'changed', cursor: page.cursor }), error => error.code === 'invalid_directory_cursor');
  await assert.rejects(directory.search({ kinds: ['app'], cursor: page.cursor }), error => error.code === 'invalid_directory_cursor');
  await assert.rejects(directory.loadMine({ cursor: page.cursor }), error => error.code === 'invalid_directory_cursor');
  const appEntity = page.items.find(item => item.entity.kind === 'app');
  assert.equal(appEntity.online, undefined); assert.equal(appEntity.entry, undefined); assert.equal(directory.getRecord(appEntity.entity).entry.path, '/'); directory.dispose();
});

test('delayed old-account metadata cannot appear and raw records are purged when admission changes', async () => {
  const gate = {}; gate.promise = new Promise(done => { gate.resolve = done; }); let active = true;
  const f = fixture({ apps: 1 });
  const directory = createFieldDirectory({ accountId: 'owner', isCurrent: () => active, api: { async request(op, args) { await gate.promise; return f.api.request(op, args); } } });
  const pending = directory.search({ kinds: ['app'] }); active = false; gate.resolve();
  await assert.rejects(pending, error => error.code === 'directory_account_changed');
  assert.throws(() => directory.getRecord({ kind: 'app', id: app(0).id }), error => error.code === 'directory_account_changed'); directory.dispose();
});

test('hidden roster projection has no invented bio/interests or human online state; avatars retain actual revision', async () => {
  const directory = createFieldDirectory({ accountId: 'owner', api: { async request(op, args) {
    assert.equal(op, 'world.directory.resolve'); assert.equal(args.entities.length, 1);
    return { items: [{ ref: args.entities[0], available: true, access: 'member', person: { profileId: 'hidden_person', displayName: 'Участник', avatarColor: '#abcdef', avatarRevision: 2, revision: 3 } }] };
  } } });
  const result = await directory.resolve([{ kind: 'person', id: 'hidden_person' }]);
  assert.equal(result.items[0].source, 'member'); assert.equal(result.items[0].description, undefined); assert.equal(result.items[0].online, undefined);
  assert.equal(result.items[0].avatarRevision, 2); assert.equal(directory.getRecord(result.items[0].entity).bio, undefined); directory.dispose();
});

test('unknown legacy shortcut identity stays neutral without invalidating valid entries in its resolve batch', async () => {
  const actual = app(1), calls = [];
  const directory = createFieldDirectory({ accountId: 'owner', api: { async request(op, args) {
    calls.push({ op, args }); assert.deepEqual(args.appIds, [actual.id]);
    return { items: [{ ref: { kind: 'app', id: actual.id }, available: true, app: actual }] };
  } } });
  const result = await directory.resolve([{ kind: 'app', id: 'legacy-name' }, { kind: 'app', id: actual.id }]);
  assert.equal(calls.length, 1); assert.equal(result.items[0].entity.id, actual.id); assert.deepEqual(result.unavailable, [{ kind: 'app', id: 'legacy-name' }]);
  assert.deepEqual(result.errors, []); directory.dispose();
});

test('valid astral emoji and Cyrillic queries pass unchanged; unpaired surrogate is rejected before any RPC', async () => {
  const f = fixture({}), directory = createFieldDirectory({ api: f.api, accountId: 'owner' });
  for (const query of ['👩‍💻', 'Музыка ё']) {
    await directory.search({ query, kinds: ['app'] }); assert.equal(f.calls.at(-1).args.query, query);
  }
  const before = f.calls.length;
  await assert.rejects(directory.search({ query: '\ud83d', kinds: ['app'] }), error => error.code === 'invalid_directory_query');
  assert.equal(f.calls.length, before); directory.dispose();
});

test('large own contact lists do not evict loaded apps; contact cache remains thin and a later block removes fallback authority', async () => {
  let contacts = Array.from({ length: 1000 }, (_, index) => ({ relationshipId: `relationship_${index}`, peerAccountId: `private_peer_${index}`,
    label: `Друг ${index}`, createdAt: index, privateBio: 'Extra core response fields are never cached' }));
  const actual = app(1), directory = createFieldDirectory({ accountId: 'owner', api: { async request(op, args) {
    if (op === 'contacts.list') { assert.deepEqual(args, {}); return { contacts }; }
    if (op === 'apps.devices') return { devices: [] };
    if (op === 'apps.directory.search') return { apps: [actual], nextCursor: null };
    if (op === 'world.directory.search') return { people: [], communities: [], nextCursor: null };
    if (op === 'world.directory.resolve') return { items: args.entities.map(ref => ({ ref, available: false })) };
    throw new Error(`Unexpected operation ${op}`);
  } } });
  const mine = await directory.loadMine({ limit: 8 }); assert.ok(mine.items.some(value => value.entity.id === actual.id));
  const ref = { kind: 'person', id: 'private_peer_999' }, found = await directory.resolve([ref]);
  assert.equal(found.items[0].title, 'Друг 999'); assert.equal(directory.getRecord({ kind: 'app', id: actual.id }).entry.appId, actual.id);
  assert.deepEqual(Object.keys(directory.getRecord(ref)).sort(), ['createdAt', 'kind', 'label', 'peerAccountId', 'relationshipId']);
  contacts = contacts.filter(value => value.peerAccountId !== ref.id);
  const blocked = await directory.resolve([ref]); assert.deepEqual(blocked.unavailable, [ref]); assert.equal(directory.getRecord(ref), null);
  assert.ok(directory.getRecord({ kind: 'app', id: actual.id })); directory.dispose();
});
