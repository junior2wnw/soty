import test from 'node:test';
import assert from 'node:assert/strict';
import { mkdtemp, readFile, rm, mkdir, symlink } from 'node:fs/promises';
import { tmpdir } from 'node:os';
import { join, dirname } from 'node:path';
import { spawn } from 'node:child_process';
import { createRequire } from 'node:module';
import { pathToFileURL } from 'node:url';
import { buildSourceAppPackage } from '../scripts/build.mjs';
import { STANDARD_SELECTED_SOURCE } from '../server/standard-profile.mjs';

test('portable server/browser bundles load outside Root checkout with pinned protocol dependency and exact compiled profile', async t => {
  const directory = await mkdtemp(join(tmpdir(), 'soty-source-package-')); t.after(() => rm(directory, { recursive: true, force: true, maxRetries: 5, retryDelay: 30 }));
  const manifest = await buildSourceAppPackage({ outdir: directory }); assert.equal(manifest.protocolDependency.version, '6.8.4');
  assert.ok(manifest.sourceFiles.every(file => !file.path.includes(':') && !file.path.startsWith('/') && /^[a-f0-9]{64}$/u.test(file.sha256)));
  const require = createRequire(import.meta.url), protocolDir = dirname(dirname(require.resolve('openid-client')));
  await mkdir(join(directory, 'node_modules')); await symlink(protocolDir, join(directory, 'node_modules/openid-client'), process.platform === 'win32' ? 'junction' : 'dir');
  const server = await readFile(join(directory, 'server.mjs'), 'utf8'), browser = await readFile(join(directory, 'browser.mjs'), 'utf8');
  assert.equal(/from ['"]\.\.\//u.test(server), false); assert.equal(/node:|openid-client/u.test(browser), false);
  const script = `import {STANDARD_SELECTED_SOURCE,createSourceNativeAuthorityPort,isSourceNativeAuthorityPort} from ${JSON.stringify(pathToFileURL(join(directory, 'server.mjs')).href)};
    const p=createSourceNativeAuthorityPort({capture:async()=>({}),withCurrent:(_p,_s,f)=>f()});
    if(!isSourceNativeAuthorityPort(p))throw new Error('brand');process.stdout.write(JSON.stringify(STANDARD_SELECTED_SOURCE));`;
  const result = await new Promise((resolve, reject) => {
    const child = spawn(process.execPath, ['--input-type=module', '-e', script], { cwd: directory, windowsHide: true, stdio: ['ignore', 'pipe', 'pipe'] });
    let out = '', err = ''; child.stdout.on('data', part => { out += part; }); child.stderr.on('data', part => { err += part; }); child.on('error', reject);
    child.on('exit', code => resolve({ code, out, err }));
  });
  assert.equal(result.code, 0, result.err); assert.deepEqual(JSON.parse(result.out), STANDARD_SELECTED_SOURCE);
});
