import test from 'node:test';
import assert from 'node:assert/strict';
import { mkdtempSync, rmSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { join, dirname, resolve, basename } from 'node:path';
import { createHash, randomBytes } from 'node:crypto';
import { PlannerStore } from './store.ts';
import { createPlannerRpSessions, type RootContext } from './soty-rp.ts';
import {
  installPlannerRpFormat,
  inspectPlannerRpFormat,
  assertPlannerRpStartup,
} from './soty-rp-format.ts';
import { createPlannerServer } from './main.ts';
import { DatabaseSync } from 'node:sqlite';
// Independent literal pre-start fixtures deliberately do not import production reader.
// @ts-ignore executable independent JS fixture
import { readLegacyPlannerRp } from '../scripts/fixtures/planner-rp-v1-reader.literal.mjs';
// @ts-ignore executable independent JS fixture
import { readCompatiblePlannerRp } from '../scripts/fixtures/planner-rp-v2-reader.literal.mjs';
const hash = (value: string) => createHash('sha256').update(value).digest('hex');
const random = () => randomBytes(32).toString('base64url');
function fixture(t: test.TestContext) {
  const directory = mkdtempSync(join(tmpdir(), 'planner-rp-native-')),
    path = join(directory, 'planner.sqlite'),
    store = new PlannerStore(path);
  let time = Date.now();
  const clock = () => time,
    workspaceId = store.read().workspaces[0].id,
    userId = store.localUserId;
  store.db
    .exec(`CREATE TABLE planner_soty_links(issuer TEXT NOT NULL,subject TEXT NOT NULL,workspace_id TEXT NOT NULL,user_id TEXT NOT NULL,created_at INTEGER NOT NULL,PRIMARY KEY(issuer,subject,workspace_id));
    CREATE TABLE planner_soty_grants(issuer TEXT NOT NULL,subject TEXT NOT NULL,workspace_id TEXT NOT NULL,binding_digest TEXT NOT NULL,created_at INTEGER NOT NULL,PRIMARY KEY(issuer,subject,workspace_id,binding_digest));`);
  const issuer = 'https://root.fixture/human-identity',
    subject = 'fixture-immutable-human',
    bindingDigest = hash('selected-semantic-scope');
  store.db
    .prepare('INSERT INTO planner_soty_links VALUES(?,?,?,?,?)')
    .run(issuer, subject, workspaceId, userId, time);
  store.db
    .prepare('INSERT INTO planner_soty_grants VALUES(?,?,?,?,?)')
    .run(issuer, subject, workspaceId, bindingDigest, time);
  let sends = 0;
  const protocol = {
    async ready() {},
    async start() {
      return {
        verifier: random(),
        state: random(),
        nonce: random(),
        location: 'https://root.fixture/human-identity/authorize',
      };
    },
    async exchange() {
      throw new Error('not-used');
    },
    async renew(input: { nonce: string }) {
      sends++;
      return {
        accessToken: random(),
        refreshToken: random(),
        nonce: input.nonce,
        expiresAt: time + 300000,
      };
    },
    async currentSubject(_at: string, sub: string) {
      return sub;
    },
  };
  const options = {
    profileDigest: hash('reviewed-oidc-profile'),
    bindingDigest,
    issuer,
    clientId: 'source-client',
    workspaceId,
    key: randomBytes(32),
    allowMigration: true,
    protocol,
    clock,
  };
  const context: RootContext = {
    reference: { id: random(), version: 1, digest: hash('slot-one') },
    rootPrincipal: { accountId: 'Root-original-A', deviceId: 'Root-original-device' },
    humanPrincipal: {
      issuer,
      subject,
      clientId: 'source-client',
      clientProfileDigest: hash('actual-reviewed-client'),
      clientGeneration: 1,
    },
    expiresAt: time + 300000,
  };
  const source = createPlannerRpSessions(store, options),
    initial = {
      issuer,
      subject,
      accessToken: random(),
      refreshToken: random(),
      nonce: random(),
      expiresAt: time + 300000,
    };
  t.after(() => {
    store.close();
    assert.equal(dirname(resolve(directory)), resolve(tmpdir()));
    assert.match(basename(directory), /^planner-rp-native-/u);
    rmSync(directory, { recursive: true, force: true, maxRetries: 3, retryDelay: 100 });
  });
  return {
    store,
    path,
    directory,
    source,
    options,
    context,
    initial,
    userId,
    workspaceId,
    clock,
    sends: () => sends,
    advance(ms: number) {
      time += ms;
    },
  };
}
test('explicit additive format2 preserves native baseline rows/FKs and independent preSTART reader1 refuses', async (t) => {
  const directory = mkdtempSync(join(tmpdir(), 'planner-rp-format-')),
    path = join(directory, 'planner.sqlite'),
    store = new PlannerStore(path);
  t.after(() => {
    store.close();
    rmSync(directory, { recursive: true, force: true, maxRetries: 3, retryDelay: 100 });
  });
  const rows = store.db.prepare('SELECT document FROM state').get();
  assert.equal(inspectPlannerRpFormat(store.db), 1);
  assert.throws(
    () => installPlannerRpFormat(store.db, false),
    (e: any) => e.code === 'planner_rp_migration_required',
  );
  installPlannerRpFormat(store.db, true);
  assert.equal(inspectPlannerRpFormat(store.db), 2);
  assert.deepEqual(store.db.prepare('SELECT document FROM state').get(), rows);
  assert.throws(
    () => assertPlannerRpStartup(path, 1),
    (e: any) => e.code === 'planner_rp_reader_incompatible',
  );
  assert.equal(assertPlannerRpStartup(path, 2), 2);
  assert.throws(
    () => readLegacyPlannerRp(path),
    (e: any) => e.code === 'planner_rp_reader_incompatible',
  );
  assert.equal(readCompatiblePlannerRp(path).version, 2);
  assert.equal(store.db.prepare('PRAGMA foreign_key_check').all().length, 0);
});
test('actual Planner encrypted RP anchor renews without creating native link/grant and resumes exact device', async (t) => {
  const f = fixture(t),
    created = f.clock(),
    marker = await f.source.create({
      proof: f.initial,
      userId: f.userId,
      context: f.context,
      loginStartedAt: created,
    });
  const ciphertext = f.store.db
    .prepare('SELECT payload_cipher FROM planner_soty_rp_anchors')
    .get() as { payload_cipher: string };
  assert.equal(ciphertext.payload_cipher.includes(f.initial.refreshToken), false);
  f.advance(280000);
  const nextContext = {
    ...f.context,
    reference: { id: random(), version: 1, digest: hash('slot-two') },
    expiresAt: f.clock() + 300000,
  };
  const resumed = await f.source.resume(nextContext);
  assert.equal(resumed.marker.sessionIdHash === marker.sessionIdHash, true);
  assert.equal(f.sends(), 1);
  assert.equal(resumed.marker.sessionExpiresAt, created + 86400000);
  assert.equal(
    (f.store.db.prepare('SELECT count(*) AS n FROM planner_soty_grants').get() as { n: number }).n,
    1,
  );
  await assert.rejects(
    f.source.resume({
      ...nextContext,
      rootPrincipal: { ...nextContext.rootPrincipal, deviceId: 'foreign-device' },
    }),
    (e: any) => e.status === 401,
  );
  await assert.rejects(
    f.source.resume({
      ...nextContext,
      humanPrincipal: { ...nextContext.humanPrincipal, subject: 'same-label-different-sub' },
    }),
    (e: any) => e.status === 401,
  );
});
test('Source restart/rebind receipt ACK recovery returns same alias and conflicting continuation cannot reuse intent', async (t) => {
  const f = fixture(t),
    marker = await f.source.create({
      proof: f.initial,
      userId: f.userId,
      context: f.context,
      loginStartedAt: f.clock(),
    }),
    requestId = random(),
    token = random();
  await f.source.commitReceipt({
    requestId,
    context: f.context,
    marker,
    token,
    expiresAt: f.clock() + 200000,
  });
  const restarted = createPlannerRpSessions(f.store, { ...f.options, allowMigration: false }),
    receipt = await restarted.readReceipt(requestId, f.context);
  assert.equal(receipt?.token === token, true);
  assert.equal(receipt?.marker.sessionIdHash === marker.sessionIdHash, true);
  assert.equal(f.sends(), 0);
  await assert.rejects(
    restarted.readReceipt(requestId, {
      ...f.context,
      reference: { id: random(), version: 1, digest: hash('other-intent') },
    }),
    (e: any) => e.status === 409,
  );
});
test('native selected grant revoke/regrant and semantic scope change refuse durable anchor rather than linking another account', async (t) => {
  const f = fixture(t),
    marker = await f.source.create({
      proof: f.initial,
      userId: f.userId,
      context: f.context,
      loginStartedAt: f.clock(),
    });
  f.store.db.prepare('DELETE FROM planner_soty_grants').run();
  await assert.rejects(f.source.current(marker, f.context), (e: any) => e.status === 403);
  f.store.db
    .prepare('INSERT INTO planner_soty_grants VALUES(?,?,?,?,?)')
    .run(
      f.options.issuer,
      f.context.humanPrincipal.subject,
      f.workspaceId,
      f.options.bindingDigest,
      f.clock() + 1,
    );
  await assert.rejects(f.source.resume(f.context), (e: any) => e.status === 403);
  const other = createPlannerRpSessions(f.store, {
    ...f.options,
    bindingDigest: hash('widened-authority'),
    allowMigration: false,
  });
  await assert.rejects(other.resume(f.context), (e: any) => e.status === 401);
});
test('compatible reader2 baseline serves native app after format2 while missing key requires explicit login', async (t) => {
  const f = fixture(t);
  await f.source.create({
    proof: f.initial,
    userId: f.userId,
    context: f.context,
    loginStartedAt: f.clock(),
  });
  const app = await createPlannerServer({ dbPath: f.path, port: 0, scheduler: false });
  const port = await app.listen();
  const result = await fetch('http://127.0.0.1:' + port + '/api/state');
  assert.equal(result.status, 200);
  app.server.closeAllConnections();
  await app.close();
  const rotated = createPlannerRpSessions(f.store, {
    ...f.options,
    key: randomBytes(32),
    allowMigration: false,
  });
  await assert.rejects(rotated.resume(f.context), (e: any) => e.status === 503);
});

test('production and independent reader2 refuse matching column names with missing CAS/FK/marker constraints', (t) => {
  const directory = mkdtempSync(join(tmpdir(), 'planner-rp-shape-'));
  t.after(() =>
    rmSync(directory, { recursive: true, force: true, maxRetries: 3, retryDelay: 100 }),
  );
  for (const mutation of ['primary-key', 'cascade', 'marker-check']) {
    const path = join(directory, mutation + '.sqlite'),
      db = new DatabaseSync(path);
    installPlannerRpFormat(db, true);
    const tables = db
      .prepare(
        "SELECT name,sql FROM sqlite_master WHERE name LIKE 'planner_soty_rp_%' OR name='planner_soty_rebind_receipts'",
      )
      .all() as { name: string; sql: string }[];
    db.exec('PRAGMA foreign_keys=OFF');
    for (const table of [...tables].reverse()) db.exec('DROP TABLE ' + table.name);
    for (const table of tables) {
      let sql = table.sql;
      if (mutation === 'primary-key' && table.name === 'planner_soty_rp_anchors')
        sql = sql.replace('session_hash TEXT PRIMARY KEY', 'session_hash TEXT');
      if (mutation === 'cascade' && table.name === 'planner_soty_rp_heads')
        sql = sql.replace(' ON DELETE CASCADE', '');
      if (mutation === 'marker-check' && table.name === 'planner_soty_rp_format')
        sql = sql.replace(" CHECK(format='planner.source-rp.v2')", '');
      db.exec(sql);
    }
    db.exec("INSERT INTO planner_soty_rp_format VALUES(1,'planner.source-rp.v2',2)");
    assert.throws(
      () => inspectPlannerRpFormat(db),
      (e: any) => e.code === 'planner_rp_storage_unknown',
      mutation,
    );
    assert.throws(
      () => readCompatiblePlannerRp(path),
      (e: any) => e.code === 'planner_rp_reader_incompatible',
      mutation,
    );
    db.close();
  }
});

test('bounded native GC deletes expired encrypted heads/receipts without changing selected membership/link/grant', async (t) => {
  const f = fixture(t),
    created = f.clock(),
    marker = await f.source.create({
      proof: f.initial,
      userId: f.userId,
      context: f.context,
      loginStartedAt: created,
    });
  await f.source.commitReceipt({
    requestId: random(),
    context: f.context,
    marker,
    token: random(),
    expiresAt: f.clock() + 200000,
  });
  const grants = f.store.db.prepare('SELECT * FROM planner_soty_grants').all(),
    links = f.store.db.prepare('SELECT * FROM planner_soty_links').all();
  f.advance(86400001);
  await f.source.service.compactExpired();
  for (const name of [
    'planner_soty_rp_anchors',
    'planner_soty_rp_heads',
    'planner_soty_rebind_receipts',
  ])
    assert.equal(f.store.db.prepare('SELECT count(*) AS n FROM ' + name).get()!.n, 0);
  assert.deepEqual(f.store.db.prepare('SELECT * FROM planner_soty_grants').all(), grants);
  assert.deepEqual(f.store.db.prepare('SELECT * FROM planner_soty_links').all(), links);
});
