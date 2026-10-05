import test from 'node:test';
import assert from 'node:assert/strict';
import { mkdtemp, mkdir, readFile, writeFile, rm, realpath, rename, symlink, readdir, copyFile, stat } from 'node:fs/promises';
import { dirname, basename, join } from 'node:path';
import { tmpdir } from 'node:os';
import { execFile } from 'node:child_process';
import { promisify } from 'node:util';
import { fileURLToPath } from 'node:url';
import sharp from 'sharp';
import { build as viteBuild, createLogger } from 'vite';
import { appArtBindingKey } from '../../src/world/app-art-identity.mjs';
import { REPO_ROOT, prepareArtwork, importArtwork, bindArtwork, rollbackArtwork, validateArtwork, validatePublicArtwork, queueArtwork, migrateArtworkHistory, readJson, sha256, validateDefinition, definitionsFromCatalog, withArtLock } from './pipeline.mjs';

const execute = promisify(execFile), cli = fileURLToPath(new URL('./cli.mjs', import.meta.url));

const fixtureDefinition = { key: 'test-canvas', appId: 'app-00000000000000000000000000000000', name: 'Тестовый Canvas', purpose: 'Собирать идеи', subject: 'One sculptural glass plate', alt: 'Стеклянная пластина', palette: { base: '#252C2B', accent: '#D5D9BF', ink: '#F4F4EB' }, focalPoint: { x: 0.5, y: 0.42 }, kind: 'app' };
async function fixture(t) {
  const root = await mkdtemp(join(tmpdir(), 'soty-app-art-'));
  t.after(async () => {
    const actual = await realpath(root), temp = await realpath(tmpdir());
    assert.equal(dirname(actual).toLowerCase(), temp.toLowerCase());
    assert.ok(basename(actual).startsWith('soty-app-art-'));
    await rm(actual, { recursive: true, force: true });
  });
  await mkdir(join(root, 'scripts', 'app-art'), { recursive: true });
  await mkdir(join(root, 'public', 'app-art'), { recursive: true });
  await writeFile(join(root, 'scripts', 'app-art', 'registry.json'), JSON.stringify({ schemaVersion: 1, styleVersion: 'test-1', assets: {}, bindings: {} }));
  await writeFile(join(root, 'public', 'app-art', 'manifest.json'), JSON.stringify({ schemaVersion: 1, styleVersion: 'test-1', assets: {}, pending: {}, bindings: {} }));
  return root;
}
async function syntheticSource(root, name = 'fixture.png', color = '#BCA987', width = 1200, height = 800) {
  const source = join(root, name);
  await sharp({ create: { width, height, channels: 3, background: color } }).png().toFile(source);
  return source;
}

test('prepare is idempotent and keeps private pending metadata outside the public manifest', async t => {
  const root = await fixture(t), prompt = 'Use case: stylized-concept.\nOne beautiful glass plate. No text or UI.';
  const first = await prepareArtwork(root, { definitions: [fixtureDefinition], prompt });
  const second = await prepareArtwork(root, { keys: [fixtureDefinition.key], prompt });
  assert.deepEqual(first.prepared, second.prepared);
  const job = await readJson(join(root, first.prepared[0].job));
  assert.equal(job.prompt, prompt); assert.equal(job.promptHash, sha256(prompt));
  assert.equal(job.provider, 'codex-builtin-image-gen');
  const manifest = await readJson(join(root, 'public', 'app-art', 'manifest.json'));
  assert.deepEqual(manifest.bindings, {});
  assert.equal(manifest.pending, undefined);
  assert.ok(!JSON.stringify(manifest).includes(fixtureDefinition.alt));
  assert.deepEqual(manifest.assets, {});
});

test('catalog key uses actual app identity rather than its editable name', () => {
  const base = { appId: fixtureDefinition.appId, name: 'Первое имя', description: 'Собирать идеи' };
  const first = definitionsFromCatalog({ apps: [base] })[0];
  const second = definitionsFromCatalog([{ ...base, name: 'Другое имя' }])[0];
  assert.equal(first.key, second.key); assert.equal(first.appId, second.appId);
  assert.throws(() => definitionsFromCatalog([{ ...base, appId: 'https://tracker.invalid/' }]), /Invalid stable app id/u);
  for (const key of ['../cover', '/etc/cover', 'Bad Cover', '__proto__']) assert.throws(() => validateDefinition({ ...fixtureDefinition, key }), /Invalid cover key/u);
});

test('the CLI prepares a new app and imports its selected local output without UI edits', async t => {
  const root = await fixture(t), definitionPath = join(root, 'new-app.json');
  await writeFile(definitionPath, JSON.stringify({ ...fixtureDefinition, appId: undefined }));
  const prepared = await execute(process.execPath, [cli, 'prepare', '--root', root, '--definition', definitionPath, '--app-id', fixtureDefinition.appId]);
  assert.equal(JSON.parse(prepared.stdout).prepared[0].key, fixtureDefinition.key);
  const source = await syntheticSource(root), imported = await execute(process.execPath, [cli, 'import', '--root', root, '--key', fixtureDefinition.key, '--source', source, '--visual-approved', '--reviewer', 'test-fixture']);
  assert.equal(JSON.parse(imported.stdout).version, 1);
  const manifest = await readJson(join(root, 'public', 'app-art', 'manifest.json'));
  assert.equal(manifest.bindings[appArtBindingKey(fixtureDefinition.appId)], JSON.parse(imported.stdout).publicKey);
  const checked = await execute(process.execPath, [cli, 'validate', '--root', root]);
  assert.equal(JSON.parse(checked.stdout).valid, true);
});

test('accepted imports are versioned, responsive, hash-verified, idempotent and reversible', async t => {
  const root = await fixture(t);
  await prepareArtwork(root, { definitions: [fixtureDefinition] });
  const source = await syntheticSource(root), first = await importArtwork(root, { key: fixtureDefinition.key, source, visualApproved: true, reviewer: 'test-fixture' });
  assert.equal(first.version, 1); assert.equal(first.unchanged, false);
  assert.equal(first.renditionBytes.length, 4);
  assert.equal((await validateArtwork(root)).valid, true);
  const repeated = await importArtwork(root, { key: fixtureDefinition.key, source, visualApproved: true });
  assert.equal(repeated.unchanged, true); assert.equal(repeated.version, 1);
  const before = await readJson(join(root, 'output', 'app-art-history', 'index.json'));
  const immutableSource = join(root, before.assets[fixtureDefinition.key].original.path);
  const originalHash = sha256(await readFile(immutableSource));
  const nextSource = await syntheticSource(root, 'fixture-next.png', '#33445A');
  assert.equal((await importArtwork(root, { key: fixtureDefinition.key, source: nextSource, visualApproved: true })).version, 2);
  assert.equal(sha256(await readFile(immutableSource)), originalHash);
  await bindArtwork(root, { appId: 'app-11111111111111111111111111111111', key: fixtureDefinition.key });
  assert.equal((await readJson(join(root, 'public', 'app-art', 'manifest.json'))).bindings[appArtBindingKey('app-11111111111111111111111111111111')], first.publicKey);
  assert.equal((await rollbackArtwork(root, { key: fixtureDefinition.key, version: 1 })).restored, true);
  assert.equal((await validateArtwork(root)).checked[0].version, 1);
  assert.deepEqual((await prepareArtwork(root, { all: true })).ready, [fixtureDefinition.key]);
});

test('missing visual acceptance, external source and undersized output fail before activation', async t => {
  const root = await fixture(t);
  await prepareArtwork(root, { definitions: [fixtureDefinition] });
  const source = await syntheticSource(root, 'tiny.png', '#333333', 320, 200);
  await assert.rejects(importArtwork(root, { key: fixtureDefinition.key, source }), /visual-approved/u);
  await assert.rejects(importArtwork(root, { key: fixtureDefinition.key, source: 'https://tracker.invalid/cover.png', visualApproved: true }), /local image path/u);
  await assert.rejects(importArtwork(root, { key: fixtureDefinition.key, source, visualApproved: true }), /at least 960/u);
  const disguised = join(root, 'not-an-image.png');
  await writeFile(disguised, '<svg><text>Not an accepted source</text></svg>');
  await assert.rejects(importArtwork(root, { key: fixtureDefinition.key, source: disguised, visualApproved: true }), /Source signature/u);
  assert.deepEqual((await readJson(join(root, 'public', 'app-art', 'manifest.json'))).assets, {});
});

test('a stale job or altered prompt cannot be imported into another definition', async t => {
  const root = await fixture(t), first = await prepareArtwork(root, { definitions: [fixtureDefinition] });
  const source = await syntheticSource(root);
  const jobPath = join(root, first.prepared[0].job), job = await readJson(jobPath);
  await writeFile(jobPath, JSON.stringify({ ...job, prompt: 'tampered' }));
  await assert.rejects(importArtwork(root, { key: fixtureDefinition.key, source, job: jobPath, visualApproved: true }), /prompt hash mismatch/u);
  await writeFile(jobPath, JSON.stringify(job));
  await prepareArtwork(root, { definitions: [{ ...fixtureDefinition, subject: 'A completely different form' }] });
  await assert.rejects(importArtwork(root, { key: fixtureDefinition.key, source, job: jobPath, visualApproved: true }), /changed after preparation/u);
});

test('validation detects bytes changed after acceptance', async t => {
  const root = await fixture(t);
  await prepareArtwork(root, { definitions: [fixtureDefinition] });
  await importArtwork(root, { key: fixtureDefinition.key, source: await syntheticSource(root), visualApproved: true });
  const manifest = await readJson(join(root, 'public', 'app-art', 'manifest.json')), url = Object.values(manifest.assets)[0].renditions[0].url;
  await writeFile(join(root, 'public', url.slice(1)), Buffer.from('broken-image'));
  await assert.rejects(validateArtwork(root), /file hash mismatch/u);
});

test('queue shows genuinely pending jobs and an edited definition is prepared again', async t => {
  const root = await fixture(t);
  await prepareArtwork(root, { definitions: [fixtureDefinition] });
  assert.equal((await queueArtwork(root)).queue[0].status, 'needs-generation');
  await importArtwork(root, { key: fixtureDefinition.key, source: await syntheticSource(root), visualApproved: true });
  assert.equal((await queueArtwork(root)).pending, 0);
  assert.equal((await queueArtwork(root, { includeAccepted: true })).queue[0].status, 'accepted');
  const registryPath = join(root, 'scripts', 'app-art', 'registry.json'), registry = await readJson(registryPath);
  registry.assets[fixtureDefinition.key].subject = 'A refined new glass object';
  await writeFile(registryPath, JSON.stringify(registry));
  assert.equal((await queueArtwork(root)).queue[0].status, 'needs-prepare');
  assert.equal((await prepareArtwork(root, { all: true })).prepared.length, 1);
  assert.equal((await queueArtwork(root)).queue[0].status, 'needs-generation');
});

test('parallel mutations fail with a bounded lock diagnostic and release the owned lock', async t => {
  const root = await fixture(t);
  await withArtLock(root, async () => { await assert.rejects(prepareArtwork(root, { definitions: [fixtureDefinition] }), /Another artwork operation/u); });
  assert.equal((await prepareArtwork(root, { definitions: [fixtureDefinition] })).prepared.length, 1);
});

test('an output junction cannot redirect writes outside the declared workspace', async t => {
  const root = await fixture(t), outside = await fixture(t);
  await rename(join(root, 'scripts', 'app-art'), join(root, 'scripts', 'app-art-original'));
  try { await symlink(join(outside, 'scripts', 'app-art'), join(root, 'scripts', 'app-art'), process.platform === 'win32' ? 'junction' : 'dir'); }
  catch (error) { if (['EPERM', 'EACCES', 'ENOSYS'].includes(error.code)) { t.skip('Directory junction unavailable'); return; } throw error; }
  const before = await readdir(join(outside, 'scripts', 'app-art'));
  await assert.rejects(prepareArtwork(root, { definitions: [fixtureDefinition] }), /Path escapes/u);
  assert.deepEqual(await readdir(join(outside, 'scripts', 'app-art')), before);
});

test('private catalog preparation publishes no app name, description, alt, id or local key', async t => {
  const root = await fixture(t);
  const [definition] = definitionsFromCatalog({ apps: [{ appId: fixtureDefinition.appId, name: 'PRIVATE_NAME_SENTINEL', description: 'PRIVATE_DESCRIPTION_SENTINEL', art: { key: 'private-secret-project', subject: 'PRIVATE_SUBJECT_SENTINEL', alt: 'PRIVATE_ALT_SENTINEL' } }] });
  const prepared = await prepareArtwork(root, { definitions: [definition] });
  assert.match(prepared.prepared[0].publicKey, /^art-[a-f0-9]{32}$/u);
  const publicJson = await readFile(join(root, 'public', 'app-art', 'manifest.json'), 'utf8');
  for (const value of ['PRIVATE_', fixtureDefinition.appId, definition.key]) assert.ok(!publicJson.includes(value));
  assert.deepEqual(JSON.parse(publicJson), { schemaVersion: 1, bindings: {}, assets: {} });
  assert.ok((await readFile(join(root, prepared.prepared[0].job), 'utf8')).includes('PRIVATE_DESCRIPTION_SENTINEL'));
  assert.ok((await readFile(join(root, 'scripts', 'app-art', 'registry.json'), 'utf8')).includes('PRIVATE_ALT_SENTINEL'));
});

test('a real Vite build ships only WebP and safe display metadata for a private app', async t => {
  const root = await fixture(t), definition = { ...fixtureDefinition, name: 'PRIVATE_NAME_SENTINEL', purpose: 'PRIVATE_PURPOSE_SENTINEL', subject: 'PRIVATE_SUBJECT_SENTINEL', alt: 'PRIVATE_ALT_SENTINEL' };
  await prepareArtwork(root, { definitions: [definition] });
  const registryPath = join(root, 'scripts', 'app-art', 'registry.json'), rawRegistry = await readJson(registryPath);
  rawRegistry.assets[definition.key].palette.privateLabel = 'PRIVATE_NESTED_PALETTE';
  rawRegistry.assets[definition.key].focalPoint.privateLabel = 'PRIVATE_NESTED_FOCAL_POINT';
  rawRegistry.assets[definition.key].compactFocalPoint = { x: 0.745, y: 0.45, privateLabel: 'PRIVATE_NESTED_COMPACT_POINT' };
  await writeFile(registryPath, JSON.stringify(rawRegistry));
  const result = await importArtwork(root, { key: definition.key, source: await syntheticSource(root, 'PRIVATE_OUTPUT_BASENAME.png'), visualApproved: true });
  const manifest = await readJson(join(root, 'public', 'app-art', 'manifest.json'));
  assert.equal(manifest.assets[result.publicKey].alt, '');
  assert.deepEqual(Object.keys(manifest.assets[result.publicKey]).sort(), ['alt','compactFocalPoint','focalPoint','key','palette','renditions','version']);
  assert.ok(!Object.hasOwn(manifest.bindings, definition.appId));
  await writeFile(join(root, 'index.html'), '<html><body><script type="module" src="/entry.mjs"></script></body></html>');
  await writeFile(join(root, 'entry.mjs'), "import manifest from './src/world/app-art-manifest.json'; document.body.textContent = JSON.stringify(manifest);");
  const warnings = [], logger = createLogger('error', { allowClearScreen: false });
  logger.warn = message => { warnings.push(String(message)); };
  logger.warnOnce = message => { warnings.push(String(message)); };
  await viteBuild({ root, configFile: false, customLogger: logger, logLevel: 'error', build: { outDir: join(root, 'dist'), emptyOutDir: true } });
  assert.deepEqual(warnings, [], 'Generated source manifest must build without public-asset import warnings');
  const files = await readdir(join(root, 'dist'), { recursive: true });
  assert.ok(files.some(name => name.endsWith('.webp')));
  for (const name of files) {
    assert.ok(!/provenance\.json|source\.|app-art-history|registry\.json|jobs[\\/]/u.test(name));
    if (/\.(?:json|html|js)$/u.test(name)) {
      const content = await readFile(join(root, 'dist', name), 'utf8');
      for (const value of ['PRIVATE_', definition.appId, definition.key, 'outputBasename', 'promptHash', 'definitionHash', 'provenance', 'app-art-history']) assert.ok(!content.includes(value), `Private metadata reached ${name}`);
    }
  }
  const internal = await readJson(join(root, 'output', 'app-art-history', 'index.json'));
  const receipt = await readJson(join(root, internal.assets[definition.key].provenance.path));
  assert.ok(receipt.job.prompt.includes('PRIVATE_PURPOSE_SENTINEL'));
  assert.equal(receipt.generation.outputBasename, 'PRIVATE_OUTPUT_BASENAME.png');
  manifest.assets[result.publicKey].privateDescription = 'PRIVATE_INJECTED_FIELD';
  await writeFile(join(root, 'public', 'app-art', 'manifest.json'), JSON.stringify(manifest));
  await assert.rejects(validateArtwork(root), /private or stale fields/u);
});

test('legacy source and receipts move privately with exact bytes and unchanged rendition URLs', async t => {
  const producer = await fixture(t), root = await fixture(t), definition = { ...fixtureDefinition, kind: 'example' };
  await prepareArtwork(producer, { definitions: [definition] });
  await importArtwork(producer, { key: definition.key, source: await syntheticSource(producer), visualApproved: true });
  const history = await readJson(join(producer, 'output', 'app-art-history', 'index.json')), accepted = history.assets[definition.key];
  const receipt = await readJson(join(producer, accepted.provenance.path));
  const directory = accepted.provenance.path.split('/').at(-2), urlBase = `/app-art/${definition.key}/${directory}`;
  const originalUrl = `${urlBase}/${basename(accepted.original.path)}`;
  const legacyAsset = { ...Object.fromEntries(Object.entries(receipt.asset).filter(([field]) => field !== 'publicKey')), original: { url: originalUrl, ...Object.fromEntries(Object.entries(receipt.asset.original).filter(([field]) => field !== 'path')) } };
  const legacyReceipt = Buffer.from(JSON.stringify({ ...receipt, asset: legacyAsset }, null, 2) + '\n');
  const destination = join(root, 'public', 'app-art', definition.key, directory); await mkdir(destination, { recursive: true });
  await copyFile(join(producer, accepted.original.path), join(root, 'public', originalUrl.slice(1)));
  for (const item of accepted.renditions) await copyFile(join(producer, 'public', item.url.slice(1)), join(root, 'public', item.url.slice(1)));
  await writeFile(join(destination, 'provenance.json'), legacyReceipt);
  await copyFile(join(producer, 'scripts', 'app-art', 'registry.json'), join(root, 'scripts', 'app-art', 'registry.json'));
  await writeFile(join(root, 'public', 'app-art', 'manifest.json'), JSON.stringify({ schemaVersion: 1, styleVersion: 'test-1', pending: {}, bindings: { [definition.appId]: definition.key }, assets: { [definition.key]: { ...legacyAsset, provenance: { url: `${urlBase}/provenance.json`, sha256: sha256(legacyReceipt) } } } }));
  const moved = await migrateArtworkHistory(root); assert.equal(moved.moved, 2);
  const migrated = await readJson(join(root, 'output', 'app-art-history', 'index.json'));
  assert.equal(sha256(await readFile(join(root, migrated.assets[definition.key].original.path))), accepted.original.sha256);
  assert.equal(sha256(await readFile(join(root, migrated.assets[definition.key].provenance.path))), sha256(legacyReceipt));
  assert.deepEqual((await readJson(join(root, 'public', 'app-art', 'manifest.json'))).assets[definition.key].renditions.map(item => item.url), accepted.renditions.map(item => item.url));
  assert.equal((await validateArtwork(root)).valid, true);
  assert.equal((await migrateArtworkHistory(root)).moved, 0);
});

test('missing operator history fails safely without dropping already shipped covers', async t => {
  const root = await fixture(t); await prepareArtwork(root, { definitions: [fixtureDefinition] });
  await importArtwork(root, { key: fixtureDefinition.key, source: await syntheticSource(root), visualApproved: true });
  const before = await readFile(join(root, 'public', 'app-art', 'manifest.json'));
  await rename(join(root, 'output', 'app-art-history', 'index.json'), join(root, 'output', 'app-art-history', 'index-not-loaded.json'));
  await assert.rejects(prepareArtwork(root, { definitions: [{ ...fixtureDefinition, key: 'another-private-key' }] }), /Private artwork history missing/u);
  assert.deepEqual(await readFile(join(root, 'public', 'app-art', 'manifest.json')), before);
});

test('Docker build context excludes full private pipeline data and runtime copies only built artifacts', async () => {
  const ignore = await readFile(join(REPO_ROOT, '.dockerignore'), 'utf8'), docker = await readFile(join(REPO_ROOT, 'Dockerfile'), 'utf8');
  assert.match(ignore, /^\/scripts\/app-art\/$/mu);
  assert.ok(ignore.split(/\r?\n/u).some(line => /^(?:\/?output\/?|\/output\/app-art-history\/)$/u.test(line.trim())), 'All output or private art history must be excluded from the Docker context');
  const runtime = docker.slice(docker.lastIndexOf('FROM node:24-trixie-slim'));
  assert.match(runtime, /COPY --from=build \/app\/dist \.\/dist/u);
  assert.ok(!/COPY[^\n]*(?:\/app\/(?:output|scripts)|\. \.|public\/app-art)/u.test(runtime));
});

test('compact layout changes append a private receipt while preserving image versions and bytes', async t => {
  const root = await fixture(t); await prepareArtwork(root, { definitions: [fixtureDefinition] });
  const imported = await importArtwork(root, { key: fixtureDefinition.key, source: await syntheticSource(root), visualApproved: true });
  const before = await readJson(join(root, 'output', 'app-art-history', 'index.json'));
  const accepted = before.assets[fixtureDefinition.key], generationBytes = await readFile(join(root, accepted.provenance.path));
  const updated = await prepareArtwork(root, { definitions: [{ ...fixtureDefinition, compactFocalPoint: { x: 0.745, y: 0.45 } }] });
  assert.equal(updated.prepared.length, 0); assert.equal(updated.layoutUpdated.length, 1);
  assert.equal(updated.layoutUpdated[0].version, 1);
  const after = await readJson(join(root, 'output', 'app-art-history', 'index.json')), asset = after.assets[fixtureDefinition.key];
  assert.equal(asset.version, accepted.version); assert.deepEqual(asset.original, accepted.original); assert.deepEqual(asset.renditions, accepted.renditions);
  assert.deepEqual(await readFile(join(root, asset.provenance.path)), generationBytes);
  const manifest = await readJson(join(root, 'public', 'app-art', 'manifest.json'));
  assert.deepEqual(manifest.assets[imported.publicKey].compactFocalPoint, { x: 0.745, y: 0.45 });
  const layout = await readJson(join(root, asset.layout.provenance.path));
  assert.equal(layout.artVersion, 1); assert.equal(layout.sourceSha256, accepted.original.sha256);
  assert.equal(layout.generationProvenanceSha256, accepted.provenance.sha256);
  assert.equal((await queueArtwork(root)).pending, 0); assert.equal((await validateArtwork(root)).valid, true);
  assert.equal((await prepareArtwork(root, { definitions: [{ ...fixtureDefinition, compactFocalPoint: { x: 0.745, y: 0.45 } }] })).layoutUpdated.length, 0);
  await writeFile(join(root, asset.layout.provenance.path), 'tampered-layout-receipt');
  await assert.rejects(validateArtwork(root), /file hash mismatch/u);
});

test('generated source and public manifests have identical bytes and SHA-256; drift fails validation', async t => {
  const root = await fixture(t); await prepareArtwork(root, { definitions: [fixtureDefinition] });
  await importArtwork(root, { key: fixtureDefinition.key, source: await syntheticSource(root), visualApproved: true });
  const publicPath = join(root, 'public', 'app-art', 'manifest.json'), sourcePath = join(root, 'src', 'world', 'app-art-manifest.json');
  const publicBytes = await readFile(publicPath), sourceBytes = await readFile(sourcePath);
  assert.deepEqual(sourceBytes, publicBytes);
  assert.equal((await validateArtwork(root)).manifestSha256, sha256(publicBytes));
  await writeFile(sourcePath, sourceBytes.toString('utf8').replace('"schemaVersion": 1', '"schemaVersion": 2'));
  await assert.rejects(validateArtwork(root), /Source and public artwork manifests differ/u);
});

test('two definitions cannot silently share one opaque public key', async t => {
  const root = await fixture(t), publicKey = 'art-' + 'a'.repeat(32);
  const first = { ...fixtureDefinition, publicKey };
  await prepareArtwork(root, { definitions: [first] });
  const before = await readFile(join(root, 'scripts', 'app-art', 'registry.json'));
  await assert.rejects(prepareArtwork(root, { definitions: [{ ...first, key: 'other-private-app', appId: 'app-' + '1'.repeat(32) }] }), /Public artwork key already belongs/u);
  assert.deepEqual(await readFile(join(root, 'scripts', 'app-art', 'registry.json')), before);
});

test('idempotent import refuses an accepted rendition that went missing or became corrupt', async t => {
  for (const corruption of ['missing', 'corrupt']) {
    const root = await fixture(t); await prepareArtwork(root, { definitions: [fixtureDefinition] });
    const source = await syntheticSource(root);
    await importArtwork(root, { key: fixtureDefinition.key, source, visualApproved: true });
    const history = await readJson(join(root, 'output', 'app-art-history', 'index.json'));
    const cover = join(root, 'public', history.assets[fixtureDefinition.key].renditions[0].url.slice(1));
    if (corruption === 'missing') await rm(cover); else await writeFile(cover, 'corrupt');
    const before = await readFile(join(root, 'public', 'app-art', 'manifest.json'));
    await assert.rejects(importArtwork(root, { key: fixtureDefinition.key, source, visualApproved: true }), /ENOENT|file hash mismatch/u);
    assert.deepEqual(await readFile(join(root, 'public', 'app-art', 'manifest.json')), before);
  }
});

test('edit preparation verifies the immutable accepted reference before creating a job', async t => {
  const root = await fixture(t); await prepareArtwork(root, { definitions: [fixtureDefinition] });
  await importArtwork(root, { key: fixtureDefinition.key, source: await syntheticSource(root), visualApproved: true });
  const history = await readJson(join(root, 'output', 'app-art-history', 'index.json'));
  const reference = history.assets[fixtureDefinition.key].original.path;
  const jobs = await readdir(join(root, 'scripts', 'app-art', 'jobs', fixtureDefinition.key));
  await writeFile(join(root, reference), 'tampered immutable source');
  await assert.rejects(prepareArtwork(root, { keys: [fixtureDefinition.key], referenceImage: reference, prompt: 'Remove one extra object. Preserve everything else. No text.' }), /file hash mismatch/u);
  assert.deepEqual(await readdir(join(root, 'scripts', 'app-art', 'jobs', fixtureDefinition.key)), jobs);
});

test('rollback rejects invalid acceptance provenance before changing the selected cover', async t => {
  for (const field of ['prompt', 'provider', 'noTextOrUi']) {
    const root = await fixture(t); await prepareArtwork(root, { definitions: [fixtureDefinition] });
    await importArtwork(root, { key: fixtureDefinition.key, source: await syntheticSource(root), visualApproved: true });
    const history = await readJson(join(root, 'output', 'app-art-history', 'index.json'));
    const receiptPath = join(root, history.assets[fixtureDefinition.key].provenance.path), receipt = await readJson(receiptPath);
    await importArtwork(root, { key: fixtureDefinition.key, source: await syntheticSource(root, 'next.png', '#112233'), visualApproved: true });
    if (field === 'prompt') receipt.job.prompt = 'tampered prompt';
    if (field === 'provider') receipt.generation.provider = 'unreviewed-provider';
    if (field === 'noTextOrUi') receipt.review.noTextOrUi = false;
    await writeFile(receiptPath, JSON.stringify(receipt));
    const before = await readFile(join(root, 'public', 'app-art', 'manifest.json'));
    await assert.rejects(rollbackArtwork(root, { key: fixtureDefinition.key, version: 1 }), /Provenance prompt mismatch|recorded visual acceptance/u);
    assert.deepEqual(await readFile(join(root, 'public', 'app-art', 'manifest.json')), before);
    assert.equal((await validateArtwork(root)).checked[0].version, 2);
  }
});

test('a cold Git checkout validates public covers and refuses mutations without deleting them', async t => {
  const operator = await fixture(t), cold = await fixture(t);
  await prepareArtwork(operator, { definitions: [fixtureDefinition] });
  await importArtwork(operator, { key: fixtureDefinition.key, source: await syntheticSource(operator), visualApproved: true });
  const publicRoot = join(operator, 'public', 'app-art');
  for (const name of await readdir(publicRoot, { recursive: true })) {
    const source = join(publicRoot, name);
    if (!(await stat(source)).isFile()) continue;
    const target = join(cold, 'public', 'app-art', name); await mkdir(dirname(target), { recursive: true }); await copyFile(source, target);
  }
  await mkdir(join(cold, 'src', 'world'), { recursive: true });
  await copyFile(join(operator, 'src', 'world', 'app-art-manifest.json'), join(cold, 'src', 'world', 'app-art-manifest.json'));
  await rm(join(cold, 'scripts', 'app-art', 'registry.json'));
  const before = await readFile(join(cold, 'public', 'app-art', 'manifest.json'));
  assert.equal((await validatePublicArtwork(cold)).valid, true);
  const checked = await execute(process.execPath, [cli, 'validate', '--public', '--root', cold]);
  assert.equal(JSON.parse(checked.stdout).operatorHistoryVerified, false);
  for (const action of [() => prepareArtwork(cold, { definitions: [fixtureDefinition] }), () => bindArtwork(cold, { appId: fixtureDefinition.appId, key: fixtureDefinition.key }), () => rollbackArtwork(cold, { key: fixtureDefinition.key, version: 1 })]) {
    await assert.rejects(action(), /Local artwork registry missing/u);
    assert.deepEqual(await readFile(join(cold, 'public', 'app-art', 'manifest.json')), before);
  }
  await copyFile(join(operator, 'scripts', 'app-art', 'registry.json'), join(cold, 'scripts', 'app-art', 'registry.json'));
  await assert.rejects(prepareArtwork(cold, { all: true }), /Private artwork history missing/u);
  assert.deepEqual(await readFile(join(cold, 'public', 'app-art', 'manifest.json')), before);
  const manifest = JSON.parse(before), key = Object.keys(manifest.assets)[0];
  manifest.assets[key].palette.privateLabel = 'PRIVATE_DATA';
  await writeFile(join(cold, 'public', 'app-art', 'manifest.json'), JSON.stringify(manifest));
  await writeFile(join(cold, 'src', 'world', 'app-art-manifest.json'), JSON.stringify(manifest));
  await assert.rejects(validatePublicArtwork(cold), /private or unexpected fields/u);
});

test('a style change with identical source bytes still records the newly prepared generation identity', async t => {
  const root = await fixture(t); await prepareArtwork(root, { definitions: [fixtureDefinition] });
  const source = await syntheticSource(root);
  await importArtwork(root, { key: fixtureDefinition.key, source, visualApproved: true });
  const path = join(root, 'scripts', 'app-art', 'registry.json'), registry = await readJson(path);
  registry.styleVersion = 'test-2'; await writeFile(path, JSON.stringify(registry));
  const next = await prepareArtwork(root, { all: true });
  assert.equal(next.prepared.length, 1);
  const imported = await importArtwork(root, { key: fixtureDefinition.key, source, visualApproved: true });
  assert.equal(imported.version, 2); assert.equal(imported.unchanged, false);
  assert.equal((await queueArtwork(root)).pending, 0);
  assert.equal((await validateArtwork(root)).valid, true);
});

test('a new edit-reference identity stays pending even when prompt and selected source bytes match', async t => {
  const root = await fixture(t), prompt = 'One glass plate. No text.';
  await prepareArtwork(root, { definitions: [fixtureDefinition], prompt });
  const source = await syntheticSource(root);
  await importArtwork(root, { key: fixtureDefinition.key, source, visualApproved: true });
  const first = await readJson(join(root, 'output', 'app-art-history', 'index.json'));
  const job = await prepareArtwork(root, { keys: [fixtureDefinition.key], prompt, referenceImage: first.assets[fixtureDefinition.key].original.path });
  assert.equal((await queueArtwork(root)).queue[0].status, 'needs-generation');
  const imported = await importArtwork(root, { key: fixtureDefinition.key, source, job: job.prepared[0].job, visualApproved: true });
  assert.equal(imported.version, 2); assert.equal(imported.unchanged, false);
  assert.equal((await queueArtwork(root)).pending, 0);
  assert.equal((await validateArtwork(root)).valid, true);
});

test('rollback preserves the configured compact crop as separate immutable layout metadata', async t => {
  const root = await fixture(t); await prepareArtwork(root, { definitions: [fixtureDefinition] });
  await importArtwork(root, { key: fixtureDefinition.key, source: await syntheticSource(root), visualApproved: true });
  const first = await readJson(join(root, 'output', 'app-art-history', 'index.json'));
  const before = first.assets[fixtureDefinition.key];
  await prepareArtwork(root, { definitions: [{ ...fixtureDefinition, compactFocalPoint: { x: .745, y: .45 } }] });
  await importArtwork(root, { key: fixtureDefinition.key, source: await syntheticSource(root, 'next.png', '#112233'), visualApproved: true });
  await rollbackArtwork(root, { key: fixtureDefinition.key, version: 1 });
  const manifest = await readJson(join(root, 'public', 'app-art', 'manifest.json'));
  assert.deepEqual(Object.values(manifest.assets)[0].compactFocalPoint, { x: .745, y: .45 });
  const after = (await readJson(join(root, 'output', 'app-art-history', 'index.json'))).assets[fixtureDefinition.key];
  assert.deepEqual(after.original, before.original); assert.deepEqual(after.renditions, before.renditions); assert.deepEqual(after.provenance, before.provenance);
  assert.equal((await validateArtwork(root)).valid, true);
});

test('cold-checkout public validation rejects malformed collection shapes even when both manifests agree', async t => {
  const root = await fixture(t); await prepareArtwork(root, { definitions: [fixtureDefinition] });
  for (const fields of [{ assets: [], bindings: {} }, { assets: {}, bindings: [] }, { assets: 'invalid', bindings: {} }]) {
    const bytes = JSON.stringify({ schemaVersion: 1, ...fields });
    await writeFile(join(root, 'public', 'app-art', 'manifest.json'), bytes);
    await writeFile(join(root, 'src', 'world', 'app-art-manifest.json'), bytes);
    await assert.rejects(validatePublicArtwork(root), /Unsupported public artwork manifest/u);
  }
});
