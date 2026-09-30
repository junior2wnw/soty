import test from 'node:test';
import assert from 'node:assert/strict';
import { DatabaseSync } from 'node:sqlite';
import { AppsError } from '../server/protocol.mjs';
import { createLaunchPath } from '../server/launch-path.mjs';
import { createEngagementEntryResolver } from '../server/engagement-access.mjs';

const id = `app-${'a'.repeat(32)}`, canonical = `dom_${'c'.repeat(32)}`, alias = `dom_${'d'.repeat(32)}`;
const actor = { accountId: 'reader', deviceId: 'reader-device' };

// This small resolver fixture isolates projection/error handling. The separate
// acceptance suite covers real schema, policy, World and signed HTTP together.
function fixture(t) {
  const db = new DatabaseSync(':memory:'); t.after(() => db.close());
  db.exec(`CREATE TABLE local_apps(id TEXT,name TEXT,state TEXT,owner_account_id TEXT);
    CREATE TABLE app_domains(id TEXT,app_id TEXT,origin TEXT,state TEXT,role TEXT);`);
  db.prepare('INSERT INTO local_apps VALUES (?,?,?,?)').run(id, 'Личный проект', 'enabled', 'owner');
  db.prepare('INSERT INTO app_domains VALUES (?,?,?,?,?)').run(canonical, id, 'https://canonical.example', 'bound', 'canonical');
  db.prepare('INSERT INTO app_domains VALUES (?,?,?,?,?)').run(alias, id, 'https://named.example', 'bound', 'alias');
  const observations = [], calls = [];
  let error = null, state = 'offline';
  const resolver = createEngagementEntryResolver({ db,
    assertActor: value => { if (value.accountId !== actor.accountId) throw new AppsError('apps_authentication_required', 401); },
    publications: { decideAccess(value) {
      calls.push(value); if (error) throw error;
      return { targetRevision: 7, targetDigest: 'private-digest', policyEpoch: 8,
        route: { connectorKey: 'private-connector', entryPath: '/board?tag=one#today', port: 7000 } };
    } },
    inspectSource: value => { observations.push(value); return { state }; },
  });
  const read = (args = {}) => resolver({ actor, appId: id, ...args });
  db.exec('BEGIN IMMEDIATE');
  return { db, read, observations, calls, fail: value => { error = value; }, state: value => { state = value; } };
}

test('saved and launch paths share the actual encoded bootstrap bound and reserved-path rules', () => {
  const value = '/board?text=%D1%81%D0%BE%D1%82%D1%8B#/row';
  assert.equal(createLaunchPath(value).entryPath, value);
  assert.equal(new URL(createLaunchPath(value).bootPath, 'https://runtime.example').searchParams.get('path'), value);
  const ascii = '/' + 'a'.repeat(8172);
  assert.equal(createLaunchPath(ascii).bootPath.length, 8192);
  for (const path of [ascii + 'a', '/' + 'я'.repeat(1400), '//elsewhere/path', '/%5Cevil', '/_soty/boot', '/a/../_soty/session']) {
    assert.throws(() => createLaunchPath(path), error => ['invalid_app_path', 'app_reserved_path'].includes(error.code));
  }
});

test('entry projection keeps the selected address and minimal current fields while offline', t => {
  const f = fixture(t);
  assert.deepEqual(f.read({ domainId: alias, path: '/notes?day=2#item' }), {
    appId: id, domainId: alias, origin: 'https://named.example', path: '/notes?day=2#item',
    name: 'Личный проект', status: 'offline', canManage: false,
  });
  for (const [state, status] of [['unknown', 'starting'], ['responding', 'ready'], ['unreachable', 'stopped']]) {
    f.state(state); assert.equal(f.read().status, status);
  }
  assert.equal(f.calls.at(-1).domainId, canonical);
  assert.deepEqual(f.observations[0].target, { connectorKey: 'private-connector', revision: 7, digest: 'private-digest' });
});

test('private, missing and retired entries are unavailable without alias substitution or observations', t => {
  const f = fixture(t);
  f.fail(new AppsError('apps_access_denied', 403));
  assert.equal(f.read(), null);
  assert.equal(f.calls.length, 1); assert.equal(f.calls[0].domainId, canonical);
  assert.equal(f.observations.length, 0);
  f.db.prepare("UPDATE app_domains SET state='tombstone' WHERE id=?").run(alias);
  assert.equal(f.read({ domainId: alias }), null);
  assert.equal(f.read({ domainId: `dom_${'f'.repeat(32)}` }), null);
  f.db.prepare("UPDATE local_apps SET state='revoked' WHERE id=?").run(id);
  assert.equal(f.read(), null); assert.equal(f.calls.length, 1);
});

test('authentication, broken storage and observation failures stay errors; a transaction is required', t => {
  const f = fixture(t), failure = new Error('synthetic storage failure');
  f.fail(failure); assert.throws(() => f.read(), error => error === failure);
  f.fail(new AppsError('apps_authentication_required', 401));
  assert.throws(() => f.read(), { code: 'apps_authentication_required' });
  f.fail(null); f.state('unexpected');
  assert.throws(() => f.read(), { code: 'apps_source_observation_invalid' });
  f.db.exec('COMMIT');
  assert.throws(() => f.read(), { code: 'apps_transaction_required' });
});
