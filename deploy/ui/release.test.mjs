import test from 'node:test';
import assert from 'node:assert/strict';
import { mkdtempSync, rmSync, readFileSync, writeFileSync, symlinkSync } from 'node:fs';
import { tmpdir } from 'node:os';
import path from 'node:path';
import { createUiRelease, writeUiRelease, createCaddyOverlay, verifyStructuralDelta, PUBLIC_SKILL_FILES } from './release.mjs';
import { fixtureDist } from './test-support.mjs';
import {sdkFormatFixture} from '../../modules/source-app/test/support/public-release.mjs';
import {sourceSdkReleaseIndex,sourceSdkHash} from '../../modules/source-app/public-release.mjs';
import {SOURCE_SDK_LIMITS} from '../../modules/source-app/package-contract.mjs';
import {mkdir,writeFile} from 'node:fs/promises';

function fixture(t) {
  const parent = path.resolve(tmpdir()), root = mkdtempSync(path.join(parent, 'soty-ui-unit-'));
  t.after(() => { assert.equal(path.dirname(root), parent); assert.match(path.basename(root), /^soty-ui-unit-/u); rmSync(root, { recursive: true, force: true }); });
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

test('finite SDK index and hash-named capsule publish through the existing UI release without private neighbours',async t=>{
  const f=fixture(t),sdk=await sdkFormatFixture(f.root),archive=sdk.archive(),index=sourceSdkReleaseIndex(archive),name=index.artifact.url.slice(1);
  await mkdir(path.join(f.dist,'sdk/source-app'),{recursive:true});await writeFile(path.join(f.dist,name),archive);
  await writeFile(path.join(f.dist,'sdk/source-app/index.json'),sdk.json(index));
  const release=createUiRelease(f.dist);assert.equal(release.files.get(name).immutable,true);assert.equal(release.files.get('sdk/source-app/index.json').immutable,false);
  const output=path.join(f.root,'staged-sdk');writeUiRelease(release,output);assert.deepEqual(readFileSync(path.join(output,name)),archive);
  const privatePath=path.join(f.dist,'sdk/source-app/operator.json');writeFileSync(privatePath,'synthetic-private');
  assert.throws(()=>createUiRelease(f.dist),{code:'ui_sdk_entry_denied'});assert.equal(readFileSync(privatePath,'utf8'),'synthetic-private');rmSync(privatePath);
  writeFileSync(path.join(f.dist,name),'changed immutable bytes');assert.throws(()=>createUiRelease(f.dist),{code:'ui_sdk_hash_name_mismatch'});
});

test('retained SDK union counts unique immutable URLs and refuses the next archive before a manifest',async t=>{
  const f=fixture(t),sdk=await sdkFormatFixture(f.root),old=fixtureDist(path.join(f.root,'old-sdk'));
  // Format-only capsules keep this inventory-bound test small; they are not runnable SDKs.
  const provenance=JSON.parse(sdk.files.get('dist/provenance.json'));
  for(const name of ['server.mjs','browser.mjs','example.mjs','install.mjs']){
    const bytes=Buffer.from('// Synthetic format-only runtime\n');sdk.files.set('dist/'+name,bytes);
    provenance.outputs.find(item=>item.path===name).sha256=sourceSdkHash(bytes);
  }
  sdk.files.set('dist/provenance.json',sdk.json(provenance));
  const sample=sdk.archive(),half=SOURCE_SDK_LIMITS.archives/2;
  assert.ok(sample.length*(SOURCE_SDK_LIMITS.archives+1)<SOURCE_SDK_LIMITS.retainedBytes);
  await mkdir(path.join(f.dist,'sdk/source-app'),{recursive:true});await mkdir(path.join(old,'sdk/source-app'),{recursive:true});
  let first,current;
  for(let i=0;i<=SOURCE_SDK_LIMITS.archives;i++){
    // Gzip MTIME changes the transport hash while retaining a valid identical capsule.
    const archive=Buffer.from(sample);archive.writeUInt32LE(i+1,4);
    const index=sourceSdkReleaseIndex(archive),name=index.artifact.url.slice(1);
    if(i===0)first={archive,name};
    if(i===half-1)current=index;
    if(i===SOURCE_SDK_LIMITS.archives){
      await writeFile(path.join(f.dist,'sdk/source-app/index.json'),sdk.json(current));
      await writeFile(path.join(old,first.name),first.archive);
      const release=createUiRelease(f.dist,[old]);
      assert.equal(release.manifest.filter(file=>file.name.endsWith('.tgz')).length,SOURCE_SDK_LIMITS.archives);
    }
    await writeFile(path.join(i<half?f.dist:old,name),archive);
  }
  assert.throws(()=>createUiRelease(f.dist,[old]),{code:'source_sdk_public_capacity'});
});

test('SDK metadata cannot point outside the finite archive URL, omit its capsule or publish a symlink',async t=>{
  const f=fixture(t),sdk=await sdkFormatFixture(f.root),archive=sdk.archive(),index=sourceSdkReleaseIndex(archive),name=index.artifact.url.slice(1);
  await mkdir(path.join(f.dist,'sdk/source-app'),{recursive:true});await writeFile(path.join(f.dist,name),archive);
  const file=path.join(f.dist,'sdk/source-app/index.json');writeFileSync(file,JSON.stringify({...index,artifact:{...index.artifact,url:'/agent/private.tgz'}}));
  assert.throws(()=>createUiRelease(f.dist),{code:'ui_sdk_archive_missing'});
  rmSync(file);assert.throws(()=>createUiRelease(f.dist),{code:'ui_sdk_index_missing'});
  writeFileSync(file,JSON.stringify(index));rmSync(path.join(f.dist,name));
  symlinkSync(process.platform==='win32'?f.root:file,path.join(f.dist,name),process.platform==='win32'?'junction':'file');
  assert.throws(()=>createUiRelease(f.dist),{code:'ui_dist_not_regular'});
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
function skillDist(t) {
  const f = fixture(t), names = [...PUBLIC_SKILL_FILES, 'help/interaction.html', 'sdk/source-app/index.json'];
  fixtureDist(f.dist, Object.fromEntries(names.map(name => [name, readFileSync(new URL('../../public/' + name, import.meta.url), 'utf8')])));
  const name=JSON.parse(readFileSync(path.join(f.dist,'sdk/source-app/index.json'))).artifact.url.slice(1);
  writeFileSync(path.join(f.dist,name),readFileSync(new URL('../../public/'+name,import.meta.url)));
  return { ...f, names };
}

test('complete deploy and cover skill packages plus user help publish byte-exact with finite neighbouring files closed', t => {
  const f = skillDist(t), forbidden = ['skills/soty-app-deploy/agents/openai.yaml', 'skills/soty-app-deploy/references/private.md',
    'skills/soty-app-deploy/references/.env', 'skills/soty-app-deploy/references/nested/key.json',
    'skills/soty-other/SKILL.md', 'help/private.json'];
  fixtureDist(f.dist, Object.fromEntries(forbidden.map(name => [name, 'synthetic-private-value'])));
  const release = createUiRelease(f.dist);
  for (const name of f.names) {
    assert.equal(release.files.get(name).immutable, false);
    assert.deepEqual(release.files.get(name).bytes, readFileSync(path.join(f.dist, name)));
  }
  for (const name of forbidden) assert.equal(release.files.has(name), false);
  const staged = path.join(f.root, 'staged'); writeUiRelease(release, staged);
  for (const name of f.names) assert.deepEqual(readFileSync(path.join(staged, name)), readFileSync(path.join(f.dist, name)));
  for (const name of forbidden) assert.equal(release.manifest.some(file => file.name === name), false);
});

test('missing deploy reference, forbidden catalogue reference or broken user-guide link cannot publish', t => {
  const missing = skillDist(t); rmSync(path.join(missing.dist, 'skills/soty-app-deploy/references/authentication.md'));
  assert.throws(() => createUiRelease(missing.dist), { code: 'ui_reference_not_in_release' });
  const forbidden = skillDist(t), index = JSON.parse(readFileSync(path.join(forbidden.dist, 'skills/index.json')));
  index.skills.find(skill => skill.name === 'soty-app-deploy').references.push('/skills/soty-app-deploy/references/private.md');
  fixtureDist(forbidden.dist, { 'skills/soty-app-deploy/references/private.md': 'synthetic-private-value' });
  writeFileSync(path.join(forbidden.dist, 'skills/index.json'), JSON.stringify(index));
  assert.throws(() => createUiRelease(forbidden.dist), { code: 'ui_reference_not_in_release' });
  const sdk = skillDist(t), catalogue = JSON.parse(readFileSync(path.join(sdk.dist, 'skills/index.json')));
  catalogue.resources[0].index='/sdk/source-app/private.json';
  writeFileSync(path.join(sdk.dist, 'skills/index.json'), JSON.stringify(catalogue));
  assert.throws(() => createUiRelease(sdk.dist), { code: 'ui_reference_not_in_release' });
  const help = skillDist(t); writeFileSync(path.join(help.dist, 'help/interaction.html'), '<a href="/help/private.json">Unpublished</a>');
  assert.throws(() => createUiRelease(help.dist), { code: 'ui_reference_not_in_release' });
});

test('retained immutable assets cannot restore an obsolete deploy instruction over the current document', t => {
  const f = skillDist(t), name = 'skills/soty-app-deploy/SKILL.md';
  const old = fixtureDist(path.join(f.root, 'old'), { [name]: 'old instruction', 'assets/retained-12345678.js': 'retained import' });
  const release = createUiRelease(f.dist, [old]);
  assert.deepEqual(release.files.get(name).bytes, readFileSync(path.join(f.dist, name)));
  assert.equal(release.files.get('assets/retained-12345678.js').immutable, true);
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
test('forged inventory and uncontrolled/nonliteral roots are refused before Caddy is called', async t => {
  const f = fixture(t), release = createUiRelease(f.dist);
  const adapt = () => assert.fail('Caddy must not receive invalid release input');
  for (const root of ['/srv/current', `/srv/current/${release.id}`, `/srv/soty-ui//${release.id}`, `/srv/soty-ui/../${release.id}`, `/srv/soty-ui/${release.id};bad`]) {
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
