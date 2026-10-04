import test from 'node:test';
import assert from 'node:assert/strict';
import { mkdtemp, rm, appendFile, readFile } from 'node:fs/promises';
import { tmpdir } from 'node:os';
import { join, basename, dirname, resolve } from 'node:path';
import { installSotyDeploySkill } from '../../scripts/install-soty-deploy-skill.mjs';

test('skill installation is repeatable and protects local changes from an update', async t => {
  const root = await mkdtemp(join(tmpdir(), 'soty-skill-test-'));
  t.after(async () => {
    assert.equal(dirname(resolve(root)), resolve(tmpdir())); assert.match(basename(root), /^soty-skill-test-/u);
    await rm(root, { recursive: true, force: true });
  });
  const target = join(root, 'soty-app-deploy');
  assert.equal((await installSotyDeploySkill(target)).ok, true);
  assert.equal((await installSotyDeploySkill(target)).ok, true);
  const file = join(target, 'SKILL.md'); await appendFile(file, '\nLocal accepted instruction.\n');
  const before = await readFile(file, 'utf8');
  await assert.rejects(installSotyDeploySkill(target), { code: 'skill_local_changes' });
  assert.equal(await readFile(file, 'utf8'), before);
});
