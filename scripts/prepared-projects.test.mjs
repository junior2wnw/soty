import test from 'node:test';
import assert from 'node:assert/strict';
import { createHash } from 'node:crypto';
import { createServer, request } from 'node:http';
import { lstat, mkdir, mkdtemp, open, opendir, realpath, rm, symlink, writeFile } from 'node:fs/promises';
import { tmpdir } from 'node:os';
import { basename, dirname, isAbsolute, join, relative, resolve, sep } from 'node:path';
import { createPreparedProjectBrowser } from './agent-modules/prepared-projects.mjs';
import { normalizeLocalAppManifest, probeLocalApp } from './agent-modules/local-apps.mjs';
import { parseSourceJson } from '../modules/source-app/shared/strict-json.mjs';

const digest = value => createHash('sha256').update(value).digest('hex');
async function fixture(t) {
  const parent = await mkdtemp(join(tmpdir(), 'soty-prepared-directory-'));
  const root = join(parent, 'allowed'), outside = join(parent, 'outside'), project = join(root, 'project');
  await mkdir(join(project, '.soty'), { recursive: true }); await mkdir(outside); await mkdir(join(root, 'second'));
  let appRequests = 0, directoryReads = 0;
  const server = createServer((_req, res) => { appRequests++; res.writeHead(204).end(); });
  await new Promise(done => server.listen(0, '127.0.0.1', done));
  const manifest = { schema: 'soty.local-app.v1', name: 'Учебный проект', port: server.address().port, entryPath: '/' };
  await writeFile(join(project, '.soty', 'app.json'), JSON.stringify(manifest));
  let allowed = [root];
  const browser = createPreparedProjectBrowser({ allowedRoots: () => allowed, lstat, open, opendir: async path => { directoryReads++; return opendir(path); }, realpath,
    basename, isAbsolute, join, relative, sep, digest, normalizeManifest: normalizeLocalAppManifest, parseJson: parseSourceJson,
    blockedPorts: [], probe: value => probeLocalApp({ httpRequest: request }, value) });
  t.after(async () => { server.closeAllConnections(); if (server.listening) await new Promise(done => server.close(done));
    assert.equal(dirname(resolve(parent)), resolve(tmpdir())); assert.match(basename(parent), /^soty-prepared-directory-/u);
    await rm(parent, { recursive: true, force: true, maxRetries: 5, retryDelay: 100 }); });
  return { browser, root, outside, project, manifest, get appRequests() { return appRequests; }, get directoryReads() { return directoryReads; }, restrict(next) { allowed = next; },
    async stop() { server.closeAllConnections(); await new Promise(done => server.close(done)); } };
}

test('owned allowed root exposes a bounded folder choice and the same validated running project', async t => {
  const f = await fixture(t), root = (await f.browser.list()).roots[0];
  assert.match(root.id, /^[a-f0-9]{64}$/u); assert.equal(root.label, 'allowed');
  const location = { rootId: root.id, segments: [] }, first = await f.browser.list({ location });
  assert.deepEqual(first.entries, ['project', 'second']); assert.equal(first.project, null);
  const project = { rootId: root.id, segments: ['project'] }, preview = await f.browser.list({ location: project });
  assert.equal(preview.project.name, 'Учебный проект'); assert.equal(preview.project.responding, true);
  const accepted = await f.browser.inspect({ location: project, digest: preview.project.digest });
  assert.deepEqual(accepted, f.manifest);
  assert.equal((await f.browser.list({ location, after: 'project' })).entries[0], 'second');
});

test('manifest-only read checks the selected file without any request to the app or directory listing', async t => {
  const f = await fixture(t), root = (await f.browser.list()).roots[0];
  const location = { rootId: root.id, segments: ['project'] };
  const result = await f.browser.readManifest({ location });
  assert.deepEqual(result, { schema: 'soty.prepared-manifest.v1', state: 'present', name: f.manifest.name,
    digest: digest(JSON.stringify(f.manifest)) });
  assert.equal(f.appRequests, 0, 'even a HEAD request is not read-only for an arbitrary app');
  assert.equal(f.directoryReads, 0, 'the selected project does not authorize listing its neighbors');
  const missing = await f.browser.readManifest({ location: { rootId: root.id, segments: ['second'] } });
  assert.deepEqual(missing, { schema: 'soty.prepared-manifest.v1', state: 'missing', name: '', digest: '' });
  assert.equal(f.appRequests, 0);
  assert.equal(f.directoryReads, 0);
  await assert.rejects(f.browser.readManifest({ location: { rootId: root.id, segments: ['..', 'outside'] } }), { code: 'prepared_location_invalid' });
  f.restrict([f.outside]);
  await assert.rejects(f.browser.readManifest({ location }), { code: 'prepared_root_changed' });
  assert.equal(f.appRequests, 0);
});

test('path traversal, junction escape, root replacement and a changed manifest fail closed', async t => {
  const f = await fixture(t), root = (await f.browser.list()).roots[0];
  for (const segment of ['..', '../outside', 'x\\y', '', 'a\0b']) {
    await assert.rejects(f.browser.list({ location: { rootId: root.id, segments: [segment] } }), { code: 'prepared_location_invalid' });
  }
  await symlink(f.outside, join(f.root, 'escape'), process.platform === 'win32' ? 'junction' : 'dir');
  await assert.rejects(f.browser.list({ location: { rootId: root.id, segments: ['escape'] } }), { code: 'prepared_directory_invalid' });
  const location = { rootId: root.id, segments: ['project'] }, selected = await f.browser.list({ location });
  await writeFile(join(f.project, '.soty', 'app.json'), JSON.stringify({ ...f.manifest, name: 'Другая версия' }));
  await assert.rejects(f.browser.inspect({ location, digest: selected.project.digest }), { code: 'prepared_manifest_changed' });
  f.restrict([f.outside]); await assert.rejects(f.browser.list({ location }), { code: 'prepared_root_changed' });
});

test('duplicate keys, oversized or symlinked manifests and stopped endpoints are not accepted', async t => {
  const f = await fixture(t), root = (await f.browser.list()).roots[0], location = { rootId: root.id, segments: ['project'] };
  const file = join(f.project, '.soty', 'app.json');
  await writeFile(file, '{"schema":"soty.local-app.v1","name":"first","name":"second","port":3000,"entryPath":"/"}');
  assert.equal((await f.browser.list({ location })).projectError, 'unreadable');
  await assert.rejects(f.browser.inspect({ location, digest: 'a'.repeat(64) }));
  await writeFile(file, 'x'.repeat(4097)); assert.equal((await f.browser.list({ location })).projectError, 'unreadable');
  await assert.rejects(f.browser.inspect({ location, digest: 'a'.repeat(64) }), { code: 'prepared_manifest_invalid' });
  await writeFile(file, JSON.stringify(f.manifest)); await f.stop();
  const stopped = await f.browser.list({ location }); assert.equal(stopped.project.responding, false);
  await assert.rejects(f.browser.inspect({ location, digest: stopped.project.digest }), { code: 'prepared_app_not_responding' });
  await rm(file); await writeFile(join(f.outside, 'app.json'), JSON.stringify(f.manifest));
  try { await symlink(join(f.outside, 'app.json'), file, 'file'); }
  catch (error) { if (process.platform === 'win32' && ['EPERM', 'EACCES'].includes(error.code)) return; throw error; }
  assert.equal((await f.browser.list({ location })).projectError, 'unreadable');
  await assert.rejects(f.browser.inspect({ location, digest: 'a'.repeat(64) }), { code: 'prepared_manifest_invalid' });
});

test('directory pages stay within the wire bound even for long Unicode names', async t => {
  const f = await fixture(t), names = Array.from({ length: 120 }, (_, index) => 'п'.repeat(200) + String(index).padStart(3, '0'));
  const browser = createPreparedProjectBrowser({ allowedRoots: () => [f.root], lstat, open,
    opendir: async () => ({ async *[Symbol.asyncIterator]() { for (const name of names) yield { name, isDirectory: () => true, isSymbolicLink: () => false }; } }),
    realpath, basename, isAbsolute, join, relative, sep, digest, normalizeManifest: normalizeLocalAppManifest, parseJson: parseSourceJson,
    blockedPorts: [], probe: value => probeLocalApp({ httpRequest: request }, value) });
  const root = (await browser.list()).roots[0], location = { rootId: root.id, segments: [] }, actual = [];
  let after = '';
  do { const page = await browser.list({ location, after }); assert.ok(Buffer.byteLength(JSON.stringify(page.entries)) < 17000);
    actual.push(...page.entries); after = page.next || ''; } while (after);
  assert.deepEqual(actual, names);
});
