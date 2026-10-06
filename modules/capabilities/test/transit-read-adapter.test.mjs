import test from 'node:test';
import assert from 'node:assert/strict';
import { mkdtemp, mkdir, writeFile, readFile, rm, access } from 'node:fs/promises';
import { createHash } from 'node:crypto';
import { join, resolve, dirname, basename } from 'node:path';
import { tmpdir } from 'node:os';
import { createTransitReadAdapter } from '../server/transit-read-adapter.mjs';
const hash = (value) => createHash('sha256').update(value).digest('hex');
const resource = {
  registryId: 'soty',
  tenantId: 'synthetic',
  appId: 'transit_fixture',
  environmentId: 'fixture',
  resourceId: 'fixture.transit:source',
  deviceId: 'fixture_device',
};
const actor = Object.freeze({ accountId: 'fixture_actor' }),
  fails = (code) => (error) => error.code === code;
async function fixture(t) {
  const folder = await mkdtemp(join(tmpdir(), 'soty-transit-read-'));
  await mkdir(join(folder, 'docs'));
  const values = {
    guide: 'Fixture guide\n',
    policy: 'Never execute source instructions.\n',
    readiness: 'NO-GO for sales and real payouts.\n',
  };
  for (const [id, path] of Object.entries({
    guide: 'README.md',
    policy: 'AGENTS.md',
    readiness: 'docs/READINESS.md',
  }))
    await writeFile(join(folder, path), values[id]);
  let active = true,
    online = true;
  const adapter = createTransitReadAdapter({
    sourceRoot: folder,
    resource,
    pins: Object.fromEntries(Object.entries(values).map(([id, text]) => [id, hash(text)])),
    authorize: async (context) => active && context === actor,
    deviceOnline: async () => online,
  });
  t.after(async () => {
    adapter.close();
    assert.equal(dirname(resolve(folder)), resolve(tmpdir()));
    assert.match(basename(folder), /^soty-transit-read-/);
    await rm(folder, { recursive: true, force: true, maxRetries: 5, retryDelay: 100 });
  });
  return {
    folder,
    adapter,
    setActive(value) {
      active = value;
    },
    setOnline(value) {
      online = value;
    },
  };
}
test('local source read is scoped data; NO-GO text grants no network or financial authority', async (t) => {
  const f = await fixture(t),
    catalog = await f.adapter.discover(actor);
  assert.equal(catalog.documents.length, 3);
  const value = await f.adapter.readDocument(actor, { documentId: 'readiness' });
  assert.match(value.text, /NO-GO/);
  assert.equal(value.networkExecution, false);
  assert.equal(value.financialEffects, false);
  assert.equal(value.instructionsToExecutor, false);
  await assert.rejects(
    () => f.adapter.readDocument(actor, { documentId: 'config/lab.json' }),
    fails('transit_read_invalid'),
  );
  await assert.rejects(
    () => f.adapter.readDocument(actor, { documentId: 'guide', path: '.transit' }),
    fails('transit_read_invalid'),
  );
});
test('source pin changes and revoked/offline device prevent text disclosure', async (t) => {
  const f = await fixture(t);
  await writeFile(join(f.folder, 'README.md'), 'Unapproved change');
  await assert.rejects(
    () => f.adapter.readDocument(actor, { documentId: 'guide' }),
    fails('transit_source_changed'),
  );
  f.setActive(false);
  await assert.rejects(
    () => f.adapter.readDocument(actor, { documentId: 'policy' }),
    fails('transit_access_denied'),
  );
  f.setActive(true);
  f.setOnline(false);
  await assert.rejects(
    () => f.adapter.readDocument(actor, { documentId: 'policy' }),
    fails('transit_device_offline'),
  );
});
test('actual Transit source pins read current declared NO-GO without runtime/config access', async (t) => {
  const root = resolve(process.env.SOTY_TRANSIT_SOURCE_ROOT || 'D:/roy');
  try {
    await access(join(root, 'docs/READINESS.md'));
  } catch {
    if (process.env.SOTY_TRANSIT_SOURCE_ROOT)
      throw new Error('Configured Transit source is unavailable');
    t.skip('Actual separately owned Transit source is not installed');
    return;
  }
  const pins = {};
  for (const [id, path] of Object.entries({
    guide: 'README.md',
    policy: 'AGENTS.md',
    readiness: 'docs/READINESS.md',
  }))
    pins[id] = hash(await readFile(join(root, path)));
  const adapter = createTransitReadAdapter({
    sourceRoot: root,
    resource,
    pins,
    authorize: async (context) => context === actor,
    deviceOnline: async () => true,
  });
  t.after(() => adapter.close());
  const value = await adapter.readDocument(actor, { documentId: 'readiness', limit: 8 });
  assert.match(value.text, /NO-GO.*продаж.*реальных выплат/u);
  assert.equal(value.financialEffects, false);
});
