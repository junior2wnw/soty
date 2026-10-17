import assert from 'node:assert/strict';
import { createHash } from 'node:crypto';
import { mkdtempSync, readFileSync, writeFileSync, symlinkSync, rmSync } from 'node:fs';
import { dirname, join, resolve, basename } from 'node:path';
import { tmpdir } from 'node:os';
import { pathToFileURL, fileURLToPath } from 'node:url';

// Reuse the exact maintained Native test fixture without changing its checkout
// or copying Native domain/auth implementations into Root. The fixture's issuer,
// connector and host DB binding are synthetic; the Native BFF/SDK, SQLite store,
// Root identity provisioning, ACL and repository operations are actual code.
const FIXTURE_SHA256 = '65551e110f4d3a96ad6527e3a4f60c70ba6fb2ee1a5d1d5e09f43004b829f7c4';
const root = resolve(fileURLToPath(new URL('../../../..', import.meta.url)));
export async function actualHiveWorkspace(t, packageRoot = process.env.SOTY_HIVE_NATIVE_SOURCE_ROOT) {
  assert.ok(packageRoot, 'Explicit owned HIVE Native source checkout required');
  const nativeRoot = resolve(packageRoot), source = readFileSync(join(nativeRoot, 'tests/workspace-root-identity.test.mjs'), 'utf8');
  assert.equal(createHash('sha256').update(source).digest('hex'), FIXTURE_SHA256, 'Review a changed Native fixture before using it');
  const boundary = source.indexOf("test('Root workspace SSO"); assert.ok(boundary > 0);
  const anchor = "const root = resolve(fileURLToPath(new URL('..', import.meta.url)));";
  const prefix = source.slice(0, boundary); assert.ok(prefix.includes(anchor));
  const folder = mkdtempSync(join(tmpdir(), 'soty-hive-workspace-fixture-')), moduleFile = join(folder, 'fixture.mjs');
  symlinkSync(join(root, 'node_modules'), join(folder, 'node_modules'), process.platform === 'win32' ? 'junction' : 'dir');
  writeFileSync(moduleFile, prefix.replace(anchor, 'const root = ' + JSON.stringify(nativeRoot) + ';') + '\nexport { fixture };\n');
  let f;
  try { f = await (await import(pathToFileURL(moduleFile))).fixture(t); }
  finally {
    t.after(() => {
      assert.equal(dirname(resolve(folder)), resolve(tmpdir())); assert.ok(basename(folder).startsWith('soty-hive-workspace-fixture-'));
      rmSync(folder, { recursive: true, force: true, maxRetries: 5, retryDelay: 30 });
    });
  }
  return f;
}
