import test from 'node:test';
import assert from 'node:assert/strict';
import { existsSync, mkdtempSync, mkdirSync, readFileSync, rmSync, symlinkSync, writeFileSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { dirname, basename, join, resolve } from 'node:path';
import { fileURLToPath } from 'node:url';
import { syncPublicDeploySkill, deploySkillFiles } from './sync-public-deploy-skill.mjs';

const repository = fileURLToPath(new URL('../', import.meta.url));
function fixture(t) {
  const parent = resolve(tmpdir()), root = mkdtempSync(join(parent, 'soty-public-skill-'));
  t.after(() => { assert.equal(dirname(root), parent); assert.match(basename(root), /^soty-public-skill-/u); rmSync(root, { recursive: true, force: true }); });
  for (const name of deploySkillFiles) { const file = join(root, name); mkdirSync(dirname(file), { recursive: true }); writeFileSync(file, readFileSync(join(repository, name))); }
  return root;
}

test('public deploy package is a byte-exact projection of the one canonical skill and every reference is discoverable', () => {
  assert.equal(syncPublicDeploySkill(repository, { check: true }).files, 10);
  const index = JSON.parse(readFileSync(join(repository, 'public/skills/index.json'))), entry = index.skills.find(skill => skill.name === 'soty-app-deploy');
  assert.equal(entry.entrypoint, '/skills/soty-app-deploy/SKILL.md');
  assert.deepEqual([...entry.references].sort(), deploySkillFiles.filter(name => name.endsWith('.md') && !name.endsWith('/SKILL.md')).map(name => '/' + name).sort());
});

test('sync never changes canonical files; check detects stale public copies and source edits before a build', t => {
  const root = fixture(t), name = 'skills/soty-app-deploy/references/authentication.md', source = readFileSync(join(root, name));
  syncPublicDeploySkill(root); syncPublicDeploySkill(root, { check: true });
  writeFileSync(join(root, 'public', name), 'stale copy');
  assert.throws(() => syncPublicDeploySkill(root, { check: true }), { code: 'public_skill_out_of_sync' });
  syncPublicDeploySkill(root); assert.deepEqual(readFileSync(join(root, name)), source);
  writeFileSync(join(root, name), Buffer.concat([source, Buffer.from('\nReviewed source update.\n')]));
  assert.throws(() => syncPublicDeploySkill(root, { check: true }), { code: 'public_skill_out_of_sync' });
  syncPublicDeploySkill(root); assert.deepEqual(readFileSync(join(root, 'public', name)), readFileSync(join(root, name)));
});

test('unpackaged relative references fail before any public copy is written', t => {
  const root = fixture(t), entry = join(root, 'skills/soty-app-deploy/SKILL.md');
  writeFileSync(entry, Buffer.concat([readFileSync(entry), Buffer.from('\n[unpackaged](references/private.md)\n')]));
  assert.throws(() => syncPublicDeploySkill(root), { code: 'public_skill_reference_missing' });
  assert.equal(existsSync(join(root, 'public')), false);
});

test('unexpected public payload is refused and preserved, not silently published or deleted', t => {
  const root = fixture(t); syncPublicDeploySkill(root);
  const privateFile = join(root, 'public/skills/soty-app-deploy/references/private.json'); writeFileSync(privateFile, 'synthetic-private-value');
  assert.throws(() => syncPublicDeploySkill(root), { code: 'public_skill_unexpected_file' });
  assert.equal(readFileSync(privateFile, 'utf8'), 'synthetic-private-value');
});

test('a source-directory symlink cannot import a different instruction tree', t => {
  const parent = resolve(tmpdir()), root = mkdtempSync(join(parent, 'soty-public-skill-'));
  t.after(() => { assert.equal(dirname(root), parent); assert.match(basename(root), /^soty-public-skill-/u); rmSync(root, { recursive: true, force: true }); });
  const source = join(root, 'skills/soty-app-deploy'), foreign = join(root, 'foreign');
  mkdirSync(source, { recursive: true }); mkdirSync(foreign);
  writeFileSync(join(source, 'SKILL.md'), readFileSync(join(repository, 'skills/soty-app-deploy/SKILL.md')));
  symlinkSync(foreign, join(source, 'references'), process.platform === 'win32' ? 'junction' : 'dir');
  assert.throws(() => syncPublicDeploySkill(root), { code: 'public_skill_symlink' });
  assert.equal(existsSync(join(root, 'public')), false);
});
