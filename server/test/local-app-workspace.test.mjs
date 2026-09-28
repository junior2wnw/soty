import test from 'node:test';
import assert from 'node:assert/strict';
import { mkdtemp, mkdir, lstat, readFile, realpath, rm, symlink, writeFile } from 'node:fs/promises';
import { tmpdir } from 'node:os';
import { dirname, isAbsolute, join, relative, resolve, sep } from 'node:path';
import { prepareLocalAppWorkspace } from '../../scripts/agent-modules/local-apps.mjs';

function isWithin(root, target) {
  const difference = relative(root, target);
  return difference === '' || difference !== '..' && !difference.startsWith('..' + sep) && !isAbsolute(difference);
}
const deps = { realpath, lstat, mkdir, join, isWithin };
async function fixture(t) {
  const parent = resolve(tmpdir()), temporary = await mkdtemp(join(parent, 'soty-workspace-test-'));
  const workspace = join(temporary, 'workspace'), outside = join(temporary, 'outside');
  await mkdir(workspace); await mkdir(outside);
  await writeFile(join(workspace, 'keep.txt'), 'existing work');
  await writeFile(join(outside, 'keep.txt'), 'outside work');
  t.after(async () => {
    assert.equal(dirname(resolve(temporary)), parent);
    assert.ok(resolve(temporary).startsWith(join(parent, 'soty-workspace-test-')));
    await rm(temporary, { recursive: true, force: true });
  });
  return { workspace, outside, async unchanged() {
    assert.equal(await readFile(join(workspace, 'keep.txt'), 'utf8'), 'existing work');
    assert.equal(await readFile(join(outside, 'keep.txt'), 'utf8'), 'outside work');
  } };
}

test('auto app workspaces are separated by server job ID and lease retries preserve earlier files', async t => {
  const f = await fixture(t);
  const options = { workspace: f.workspace, allowedRoots: [f.workspace], jobId: 'job_firstabcdefgh', create: true };
  const first = await prepareLocalAppWorkspace(deps, options);
  assert.equal(first, await realpath(join(f.workspace, 'Soty Apps', options.jobId)));
  await writeFile(join(first, 'index.html'), 'previous lease output');
  assert.equal(await prepareLocalAppWorkspace(deps, options), first);
  assert.equal(await readFile(join(first, 'index.html'), 'utf8'), 'previous lease output');
  const second = await prepareLocalAppWorkspace(deps, { ...options, jobId: 'job_secondabcdefgh' });
  assert.notEqual(first, second);
  assert.equal(await prepareLocalAppWorkspace(deps, { ...options, workspace: first, create: false }), first);
  await f.unchanged();
});

test('auto workspace rejects a disallowed base, traversal ID and a junction in either generated path component', async t => {
  const f = await fixture(t);
  const options = { workspace: f.workspace, allowedRoots: [f.workspace], jobId: 'job_junctionabcdefgh', create: true };
  await assert.rejects(prepareLocalAppWorkspace(deps, { ...options, workspace: f.outside }), /app_workspace_not_allowed/);
  await assert.rejects(prepareLocalAppWorkspace(deps, { ...options, jobId: '../outside' }), /app_workspace_invalid_job/);
  const parent = join(f.workspace, 'Soty Apps');
  await symlink(f.outside, parent, process.platform === 'win32' ? 'junction' : 'dir');
  await assert.rejects(prepareLocalAppWorkspace(deps, options), /app_workspace_not_directory/);
  // Removing exactly this link never enumerates or deletes its target directory.
  assert.equal((await lstat(parent)).isSymbolicLink(), true);
  await rm(parent);
  await mkdir(parent);
  const leaf = join(parent, options.jobId);
  await symlink(f.outside, leaf, process.platform === 'win32' ? 'junction' : 'dir');
  await assert.rejects(prepareLocalAppWorkspace(deps, options), /app_workspace_not_directory/);
  await f.unchanged();
});
