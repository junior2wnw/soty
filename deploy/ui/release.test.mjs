import test from 'node:test';
import assert from 'node:assert/strict';
import { mkdtempSync, rmSync, readFileSync, writeFileSync, symlinkSync } from 'node:fs';
import { tmpdir } from 'node:os';
import path from 'node:path';
import { createUiRelease, writeUiRelease, createCaddyOverlay, verifyStructuralDelta } from './release.mjs';
import { fixtureDist } from './test-support.mjs';

function fixture(t) {
  const root = mkdtempSync(path.join(tmpdir(), 'soty-ui-unit-'));
  t.after(() => rmSync(root, { recursive: true, force: true }));
  return { root, dist: fixtureDist(path.join(root, 'dist')) };
}
test('inventory is deterministic and excludes secrets, source maps, agents and connected application entries', t => {
  const f = fixture(t);
  fixtureDist(f.dist, { '.env': 'synthetic-secret', 'assets/index-abcdefgh.js.map': 'source',
    'agent/manifest.json': 'installer', 'projects/example/index.html': 'other app', 'hive/index.html': 'connected app' });
  const release = createUiRelease(f.dist);
  assert.equal(release.id, createUiRelease(f.dist).id); assert.equal(release.files.size, 5);
  for (const forbidden of ['.env', 'assets/index-abcdefgh.js.map', 'agent/manifest.json', 'projects/example/index.html', 'hive/index.html']) assert.equal(release.files.has(forbidden), false);
  writeUiRelease(release, path.join(f.root, release.id));
  assert.equal(readFileSync(path.join(f.root, release.id, 'index.html'), 'utf8'), release.files.get('index.html').bytes.toString());
  assert.throws(() => writeUiRelease(release, path.join(f.root, release.id)), { code: 'EEXIST' });
});
test('old hashed assets survive; reusing an immutable URL with different bytes refuses the release', t => {
  const f = fixture(t), old = fixtureDist(path.join(f.root, 'old'), { 'assets/old-12345678.js': 'old lazy import' });
  const release = createUiRelease(f.dist, [old]); assert.equal(release.files.get('assets/old-12345678.js').bytes.toString(), 'old lazy import');
  writeFileSync(path.join(old, 'assets/index-abcdefgh.js'), 'different bytes');
  assert.throws(() => createUiRelease(f.dist, [old]), { code: 'ui_immutable_url_conflict' });
});

test('only the public cover skill package is delivered and its documents update without becoming immutable', t => {
  const f = fixture(t), selected = ['skills/index.json', 'skills/index.html', 'skills/soty-cover-art/SKILL.md',
    'skills/soty-cover-art/references/runtime.md', 'skills/soty-cover-art/references/operator.md'];
  const forbidden = ['skills/private/SKILL.md', 'skills/soty-cover-art/registry.json',
    'skills/soty-cover-art/references/.env', 'skills/soty-cover-art/references/extra.md', 'agent/soty-cover-art/SKILL.md'];
  fixtureDist(f.dist, Object.fromEntries([...selected, ...forbidden].map(name => [name, `document:${name}`])));
  const skillIndex = JSON.stringify({skills:[{entrypoint:'/skills/soty-cover-art/SKILL.md',references:['/skills/soty-cover-art/references/runtime.md','/skills/soty-cover-art/references/operator.md']}]});
  writeFileSync(path.join(f.dist, 'skills/index.json'), skillIndex);
  const release = createUiRelease(f.dist);
  for (const name of selected) { assert.equal(release.files.get(name).immutable, false); assert.equal(release.files.get(name).bytes.toString(), name==='skills/index.json'?skillIndex:`document:${name}`); }
  for (const name of forbidden) assert.equal(release.files.has(name), false);
  const old = fixtureDist(path.join(f.root, 'old'), {'skills/index.json':'old skill index'});
  assert.equal(createUiRelease(f.dist, [old]).files.get('skills/index.json').bytes.toString(), skillIndex);
  rmSync(path.join(f.dist, 'skills/soty-cover-art/references/runtime.md'));
  assert.throws(() => createUiRelease(f.dist), {code:'ui_reference_not_in_release'});
});
test('a missing worker, HTML or manifest dependency cannot publish an incomplete shell', t => {
  for (const [name, text] of [['sw.js', 'const buildAssets = ["/agent/install.js"];\nconst classicAssets = [];\nconst shell = ["/"];'],
    ['index.html', '<script src="/assets/missing-12345678.js"></script>'], ['manifest.webmanifest', '{"icons":[{"src":"/icons/missing.png"}]}']]) {
    const f = fixture(t); writeFileSync(path.join(f.dist, name), text);
    assert.throws(() => createUiRelease(f.dist), { code: 'ui_reference_not_in_release' });
  }
  const f = fixture(t); writeFileSync(path.join(f.dist, 'sw.js'), 'unrecognized worker');
  assert.throws(() => createUiRelease(f.dist), { code: 'ui_worker_graph_missing' });
});
test('symlinked public files and a changed staged buffer fail closed', t => {
  const f = fixture(t); rmSync(path.join(f.dist, 'icon.svg'));
  symlinkSync(process.platform === 'win32' ? f.dist : path.join(f.dist, 'index.html'), path.join(f.dist, 'icon.svg'), process.platform === 'win32' ? 'junction' : 'file');
  assert.throws(() => createUiRelease(f.dist), { code: 'ui_dist_not_regular' });
  rmSync(path.join(f.dist, 'icon.svg')); writeFileSync(path.join(f.dist, 'icon.svg'), '<svg/>');
  const release = createUiRelease(f.dist); release.files.get('index.html').bytes[0] = 0;
  assert.throws(() => writeUiRelease(release, path.join(f.root, release.id)), { code: 'ui_release_changed' });
});
test('forged inventory and mutable/nonliteral roots are refused before Caddy is called', async t => {
  const f = fixture(t), release = createUiRelease(f.dist);
  const adapt = () => assert.fail('Caddy must not receive invalid release input');
  for (const root of ['/srv/soty-ui/current', `/srv/soty-ui//${release.id}`, `/srv/soty-ui/../${release.id}`, `/srv/soty-ui/${release.id};bad`]) {
    await assert.rejects(createCaddyOverlay({ caddyfile: '', release, root, adapt }), { code: 'ui_release_root_invalid' });
  }
  await assert.rejects(createCaddyOverlay({ caddyfile: '', release: { ...release, id: 'a'.repeat(64) }, root: '/srv/' + 'a'.repeat(64), adapt }), { code: 'ui_release_manifest_invalid' });
});

test('structural verification permits only generated group renaming and preserves partitions and all other values', () => {
  const routes = [{ group: 'group1', match: [{ path: ['/'] }], handle: [{ handler: 'reverse_proxy', upstreams: [{ dial: '127.0.0.1:18182' }], transport: { protocol: 'http' }, headers: { request: { set: { Authorization: ['synthetic-value'] } } } }] },
    { group: 'group1', handle: [{ handler: 'file_server', hide: ['/etc/caddy/Caddyfile', '/private'] }] },
    { group: 'group2', handle: [] }, { group: 'manual', handle: [] }];
  const config = { apps: { http: { servers: { srv0: { routes } } } }, notARoute: { group: 'group3', handle: [] } };
  const renamed = structuredClone(config), renamedRoutes = renamed.apps.http.servers.srv0.routes;
  renamedRoutes[0].group = renamedRoutes[1].group = 'group51'; renamedRoutes[2].group = 'group77';
  assert.equal(verifyStructuralDelta(config, renamed), true);
  for (const mutate of [
    value => { value.routes[1].group = 'group3'; }, value => { value.routes[2].group = 'group1'; },
    value => { value.routes[3].group = 'another-manual'; }, value => { value.routes[0].match[0].path = ['/changed']; },
    value => { value.routes[0].handle[0].headers.request.set.Authorization = ['changed']; },
    value => { value.routes[0].handle[0].upstreams[0].dial = '127.0.0.1:18183'; },
    value => { value.routes[0].handle[0].transport.protocol = 'fastcgi'; },
    value => { value.routes[1].handle[0].hide = ['/tmp/Caddyfile', '/private']; },
  ]) { const changed = structuredClone(config); mutate(changed.apps.http.servers.srv0); assert.throws(() => verifyStructuralDelta(config, changed), { code: 'ui_structure_changed' }); }
  const changed = structuredClone(config); changed.notARoute.group = 'group4';
  assert.throws(() => verifyStructuralDelta(config, changed), { code: 'ui_structure_changed' });
});
