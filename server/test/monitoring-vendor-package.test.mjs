import test from 'node:test';
import assert from 'node:assert/strict';
import { readFile, readdir, mkdtemp, mkdir, copyFile, writeFile, rm, realpath } from 'node:fs/promises';
import { existsSync } from 'node:fs';
import { createHash, randomUUID } from 'node:crypto';
import { createRequire } from 'node:module';
import { tmpdir } from 'node:os';
import { basename, dirname, join, resolve } from 'node:path';
import { fileURLToPath, pathToFileURL } from 'node:url';

const actual = fileURLToPath(new URL('../../vendor/uplot/', import.meta.url));
const runtime = ['dist/uPlot.cjs.js', 'dist/uPlot.d.ts', 'dist/uPlot.esm.js', 'dist/uPlot.min.css'];
const pristine = ['LICENSE', 'package.json', ...runtime].sort();

export async function checkMonitoringVendorPackage(directory) {
  const root = resolve(directory);
  for (const file of runtime) assert.ok(existsSync(join(root, file)), 'required-dist-missing:' + file);
  const top = await readdir(root, { withFileTypes: true });
  assert.deepEqual(top.map(value => value.name).sort(), ['LICENSE', 'PROVENANCE.json', 'dist', 'package.json'], 'unlisted-vendor-runtime:package');
  assert.ok(top.every(value => value.name === 'dist' ? value.isDirectory() : value.isFile()), 'unlisted-vendor-runtime:link');
  const provenance = JSON.parse(await readFile(join(root, 'PROVENANCE.json'), 'utf8'));
  assert.equal(provenance.schema, 'soty.vendored-package.v1');
  assert.equal(provenance.name, 'uplot'); assert.equal(provenance.version, '1.6.32'); assert.equal(provenance.license, 'MIT');
  assert.deepEqual(Object.keys(provenance.files).sort(), pristine);
  const packageInfo = JSON.parse(await readFile(join(root, 'package.json'), 'utf8'));
  assert.equal(packageInfo.version, provenance.version); assert.equal(packageInfo.license, 'MIT');
  assert.equal(packageInfo.main, './dist/uPlot.cjs.js'); assert.equal(packageInfo.module, './dist/uPlot.esm.js');
  assert.equal(packageInfo.typings, './dist/uPlot.d.ts');
  for (const file of pristine) {
    const bytes = await readFile(join(root, file));
    assert.equal(createHash('sha256').update(bytes).digest('hex'), provenance.files[file], 'pristine npm bytes:' + file);
  }
  const entries = (await readdir(join(root, 'dist'), { withFileTypes: true }));
  assert.ok(entries.every(value => value.isFile()), 'unlisted-vendor-runtime:directory');
  assert.deepEqual(entries.map(value => 'dist/' + value.name).sort(), runtime, 'unlisted-vendor-runtime');
  const cjs = createRequire(import.meta.url)(join(root, packageInfo.main));
  const esm = await import(pathToFileURL(join(root, packageInfo.module)).href);
  assert.equal(typeof cjs, 'function'); assert.equal(typeof esm.default, 'function');
  for (const library of [cjs, esm.default]) {
    assert.equal(typeof library.fmtDate, 'function');
    assert.equal(library.fmtDate('{YYYY}')(new Date('2026-11-05T00:00:00Z')), '2026');
  }
  return { version: packageInfo.version, pristineFiles: pristine.length, runtimeFiles: runtime.length };
}

async function temporaryPackage(t) {
  const parent = await realpath(tmpdir()), root = await realpath(await mkdtemp(join(parent, 'soty-uplot-package-'))), marker = randomUUID();
  await writeFile(join(root, 'test-owner'), marker, { flag: 'wx' });
  t.after(async () => {
    assert.equal(await realpath(root), root); assert.equal(dirname(root), parent); assert.match(basename(root), /^soty-uplot-package-/u);
    assert.equal(await readFile(join(root, 'test-owner'), 'utf8'), marker); await rm(root, { recursive: true, force: true });
  });
  const packageRoot = join(root, 'package'); await mkdir(packageRoot); return packageRoot;
}

if (process.argv[2] === '--check-package') {
  try { const result = await checkMonitoringVendorPackage(process.argv[3]); console.log(JSON.stringify(result)); }
  catch (error) { console.error(`${error.code ?? error.name}:${error.message}`); process.exitCode = 1; }
} else {
  test('monitoring vendor preserves all six pristine npm files and actual CJS/ESM exports', async () => {
    assert.deepEqual(await checkMonitoringVendorPackage(actual), { version: '1.6.32', pristineFiles: 6, runtimeFiles: 4 });
  });

  test('the same package gate detects the actual old metadata-only inventory before any runtime import', async t => {
    const root = await temporaryPackage(t);
    for (const file of ['LICENSE', 'PROVENANCE.json', 'package.json']) await copyFile(join(actual, file), join(root, file));
    await assert.rejects(checkMonitoringVendorPackage(root), error => error.code === 'ERR_ASSERTION' && error.message.startsWith('required-dist-missing:'));
  });

  test('an unlisted runtime asset cannot be smuggled into the exact vendored distribution', async t => {
    const root = await temporaryPackage(t); await mkdir(join(root, 'dist'));
    for (const file of [...pristine, 'PROVENANCE.json']) await copyFile(join(actual, file), join(root, file));
    await writeFile(join(root, 'dist/unlisted.js'), 'throw new Error("synthetic unlisted asset");\n');
    await assert.rejects(checkMonitoringVendorPackage(root), error => error.code === 'ERR_ASSERTION' && error.message.includes('unlisted-vendor-runtime'));
  });
}
