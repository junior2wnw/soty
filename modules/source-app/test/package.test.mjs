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
    import{createOrdinaryAppServer}from${JSON.stringify(pathToFileURL(join(directory, 'example.mjs')).href)};
    import{createServer}from'node:http';import{randomBytes}from'node:crypto';
    const p=createSourceNativeAuthorityPort({capture:async()=>({}),withCurrent:(_p,_s,f)=>f()});
    if(!isSourceNativeAuthorityPort(p))throw new Error('brand');
    const reserve=createServer();await new Promise(done=>reserve.listen(0,'127.0.0.1',done));const port=reserve.address().port;await new Promise(done=>reserve.close(done));
    const appId='app-'+'a'.repeat(32),parentOrigin='https://fixture.invalid',nativeOrigin='http://localhost:'+port;
    const profile={schema:'soty.selected-human-embed.v2',appId,connector:{linkId:'synthetic-link',hostDeviceId:'synthetic-host',connectorId:'synthetic-connector'},
      target:{revision:1,digest:'a'.repeat(64)},sourceProfile:STANDARD_SELECTED_SOURCE,resource:{registryId:'soty',environmentId:'fixture',tenantId:'synthetic-owner',appId,resourceId:'selected',selection:{kind:'soty.resource.v1',nativeId:'selected',incarnationId:'one'}},
      issuer:parentOrigin+'/human-identity',clientId:'synthetic-client',embedOrigin:'https://'+appId+'.localhost:24444',nativeOrigin,parentOrigin};
    const app=await createOrdinaryAppServer({profile,transportKey:randomBytes(32),connectorPort:49424,rp:{issuer:profile.issuer,clientId:profile.clientId,clientSecret:'a'.repeat(43),redirectUri:nativeOrigin+'/soty/callback'},
      databasePath:${JSON.stringify(join(directory, 'example.sqlite'))},realmId:'standalone',cipherKey:randomBytes(32),keyId:'synthetic',initialize:true,newResource:{title:'Empty synthetic resource',guestEmpty:true},allowEmptyGuest:true});
    try{await app.listen();for(const path of ['/embed','/assets/ordinary-app.js','/assets/ordinary-app.css','/assets/source-sdk.js']){const r=await fetch('http://127.0.0.1:'+port+path);if(r.status!==200)throw new Error('public asset');}
      const denied=await fetch('http://127.0.0.1:'+port+'/api/embed/context');if(denied.status!==403)throw new Error('anonymous read');
      process.stdout.write(JSON.stringify(STANDARD_SELECTED_SOURCE));}finally{await app.close();}`;
  const result = await new Promise((resolve, reject) => {
    const child = spawn(process.execPath, ['--input-type=module', '-e', script], { cwd: directory, windowsHide: true, stdio: ['ignore', 'pipe', 'pipe'] });
    let out = '', err = ''; child.stdout.on('data', part => { out += part; }); child.stderr.on('data', part => { err += part; }); child.on('error', reject);
    child.on('exit', code => resolve({ code, out, err }));
  });
  assert.equal(result.code, 0, result.err); assert.deepEqual(JSON.parse(result.out), STANDARD_SELECTED_SOURCE);
});
