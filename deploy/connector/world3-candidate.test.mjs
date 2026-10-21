import test from 'node:test';
import assert from 'node:assert/strict';
import { mkdtemp, mkdir, readFile, cp, copyFile, symlink, rm } from 'node:fs/promises';
import { tmpdir } from 'node:os';
import { join, resolve, dirname, basename } from 'node:path';
import { fileURLToPath } from 'node:url';
import { spawnSync } from 'node:child_process';
import { DatabaseSync } from 'node:sqlite';
import { migrateWorld, SCHEMA_VERSION } from '../../modules/world/server/schema.mjs';
import { migrateField } from '../../modules/world/server/field.mjs';
import { migrateAppsSchema } from '../../modules/apps/server/schema.mjs';

const root = fileURLToPath(new URL('../..', import.meta.url));
async function folder(t) {
  const directory = await mkdtemp(join(tmpdir(), 'soty-world3-candidate-'));
  t.after(async () => {
    assert.equal(dirname(resolve(directory)), resolve(tmpdir()));
    assert.match(basename(directory), /^soty-world3-candidate-/u);
    await rm(directory, { recursive: true, force: true });
  });
  return directory;
}
const readerLabel = text => JSON.parse(JSON.parse(text.match(/^LABEL io\.soty\.storage\.readers=(".*")$/mu)[1]));

test('full candidate declares exactly serving Reader7/World3 and preserves populated genuine World3/field1/Apps8 on cold reopen', async t => {
  const recipe = await readFile(join(root, 'deploy/connector/Dockerfile.world3'), 'utf8');
  assert.deepEqual(readerLabel(recipe), { version: 7, readers: {
    rooms: [1, 2], apps: [1, 2, 3, 4, 5, 6, 7, 8], notes: [1, 2], capabilities: [1, 2, 3],
    appRegistration: [1], feedback: [1], humanIdentity: [1, 2, 3], connect: [3, 4], world: [3],
  } });
  assert.equal(SCHEMA_VERSION, 3);
  const directory = await folder(t), path = join(directory, 'world.sqlite'), appsPath = join(directory, 'apps.sqlite');
  let world = new DatabaseSync(path), apps = new DatabaseSync(appsPath);
  try {
    migrateWorld(world, 'soty'); migrateField(world);
    world.prepare('INSERT INTO profiles(account_id,display_name,search_text,created_at,updated_at) VALUES(?,?,?,?,?)')
      .run('synthetic-world3-owner', 'Saved profile', 'saved profile', 1, 1);
    migrateAppsSchema(apps, { legacyTemplate: 'https://{appId}.legacy.example', allowSelectedResourceMigration: true });
    assert.equal(apps.prepare('PRAGMA user_version').get().user_version, 8);
  } finally { world.close(); apps.close(); }
  world = new DatabaseSync(path); apps = new DatabaseSync(appsPath);
  try {
    const layout = world.prepare("SELECT type,name,sql FROM sqlite_schema WHERE name NOT LIKE 'sqlite_%' ORDER BY type,name").all();
    migrateWorld(world, 'soty'); migrateField(world);
    assert.deepEqual(world.prepare("SELECT type,name,sql FROM sqlite_schema WHERE name NOT LIKE 'sqlite_%' ORDER BY type,name").all(), layout);
    assert.equal(world.prepare('PRAGMA user_version').get().user_version, 3);
    assert.equal(world.prepare("SELECT value FROM world_meta WHERE key='field_schema_version'").get().value, '1');
    assert.equal(world.prepare('SELECT display_name FROM profiles WHERE account_id=?').get('synthetic-world3-owner').display_name, 'Saved profile');
    assert.equal(migrateAppsSchema(apps, { legacyTemplate: 'https://{appId}.legacy.example' }).migrated, false);
    assert.equal(apps.prepare('PRAGMA user_version').get().user_version, 8);
  } finally { world.close(); apps.close(); }
});

test('exact final full-build copy set resolves real startup imports and the feedback asset without a parent Root image', { timeout: 30000 }, async t => {
  const recipe = await readFile(join(root, 'deploy/connector/Dockerfile.world3'), 'utf8');
  const directory = await folder(t);
  const copies = [...recipe.matchAll(/^COPY --from=build \/app\/(\S+) \.\/(\S+)$/gmu)];
  assert.ok(copies.length >= 6);
  for (const [, source, destination] of copies) {
    const target = join(directory, destination);
    if (source === 'node_modules') { await symlink(join(root, source), target, 'junction'); continue; }
    await mkdir(dirname(target), { recursive: true });
    if (source.endsWith('.json') || source.endsWith('.mjs')) await copyFile(join(root, source), target);
    else await cp(join(root, source), target, { recursive: true });
  }
  const result = spawnSync(process.execPath, ['--input-type=module', '-e',
    "await import('./server/startup.js'); const fs=await import('node:fs/promises'); if(!(await fs.readFile('./src/world/feedback-media.mjs','utf8')).includes('export'))throw Error('feedback_asset_missing')"],
    { cwd: directory, encoding: 'utf8', windowsHide: true, timeout: 15000 });
  assert.equal(result.status, 0, result.stderr);
});
