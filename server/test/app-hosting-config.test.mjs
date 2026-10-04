import test from 'node:test';
import assert from 'node:assert/strict';
import { mkdtempSync, writeFileSync, rmSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { readAppHostingConfig } from '../app-hosting-config.mjs';
import { validateNamedAppZone } from '../app-domain-policy.mjs';
import { createHttpApp } from '../http-app.js';
import { createServer, request } from 'node:http';
test('operator settings are nonsecret and strict; absence keeps the current production settings', () => {
  const dir = mkdtempSync(join(tmpdir(), 'soty-hosting-config-')), file = join(dir, 'hosting.json');
  try {
    assert.deepEqual(readAppHostingConfig(file), {});
    const value = { schema: 'soty.app-hosting.v1', domainProfile: 'shell-subdomains-v1', namedAppZone: 'https://xn--n1afe0b.online', discoveryOrigin: 'https://soty.pochinit.online' };
    writeFileSync(file, JSON.stringify(value)); assert.equal(readAppHostingConfig(file).namedAppZone, value.namedAppZone);
    writeFileSync(file,JSON.stringify({...value,retainedNamedAppZones:['https://4-2.xn--p1ai']}));
    assert.deepEqual(readAppHostingConfig(file).retainedNamedAppZones,['https://4-2.xn--p1ai']);
    for(const retainedNamedAppZones of [null,'https://4-2.xn--p1ai',[''],[123],Array(9).fill('https://4-2.xn--p1ai')]) {
      writeFileSync(file,JSON.stringify({...value,retainedNamedAppZones}));assert.throws(()=>readAppHostingConfig(file),/apps_hosting_config_invalid/);
    }
    for (const invalid of [{ ...value, extra: true }, { ...value, domainProfile: 'anything' }, [], null]) { writeFileSync(file, JSON.stringify(invalid)); assert.throws(() => readAppHostingConfig(file), /apps_hosting_config_invalid/); }
  } finally { rmSync(dir, { recursive: true }); }
});

test('HTTP subdomain profile keeps the shell root while unknown app hosts cannot reach shell APIs, including after restart', async () => {
  const dir = mkdtempSync(join(tmpdir(), 'soty-hosting-http-'));
  const config = { dataDir: dir, connectOrigins: ['https://xn--n1afe0b.online', 'https://soty.pochinit.online'],
    appOriginTemplate: 'https://{appId}.soty.pochinit.online',
    appHosting: { domainProfile: 'shell-subdomains-v1', namedAppZone: 'https://xn--n1afe0b.online', discoveryOrigin: 'https://soty.pochinit.online' } };
  try {
    for (const namedAppZone of ['https://xn--n1afe0b.online', '']) {
      const app = createHttpApp(dir, { ...config, namedAppZone }), server = createServer(app);
      await new Promise(resolve => server.listen(0, '127.0.0.1', resolve));
      try {
        const get = (host, path) => new Promise((resolve, reject) => {
          const req = request({ hostname: '127.0.0.1', port: server.address().port, path, headers: { Host: host } }, res => {
            let body = ''; res.setEncoding('utf8'); res.on('data', chunk => { body += chunk; });
            res.on('end', () => resolve({ status: res.statusCode, body }));
          }); req.on('error', reject); req.end();
        });
        for (const host of ['xn--n1afe0b.online', 'soty.pochinit.online']) {
          const response = await get(host, '/api/apps/catalog');
          assert.equal(response.status, 200); assert.deepEqual(JSON.parse(response.body), { schema: 'soty.app-catalog.v1', apps: [] });
        }
        for (const host of ['unknown.xn--n1afe0b.online', 'child.unknown.xn--n1afe0b.online', 'xn--n1afe0b.online:444']) {
          for (const path of ['/health', '/api/apps/catalog', '/api/connectors/storage-ready', '/oauth/authorize']) {
            const response = await get(host, path);
            assert.notEqual(response.status, 200, `${host}${path}`);
          }
        }
      } finally { await new Promise(resolve => server.close(resolve)); await app.locals.closeServices(); }
    }
    assert.throws(() => createHttpApp(dir, { ...config, appHosting: {}, namedAppZone: '' }), /apps_zone_separate_site_required|apps_named_zone_shell_overlap/);
  } finally { rmSync(dir, { recursive: true }); }
});
test('the explicit subdomain profile admits only the exact HTTPS shell root and keeps sibling sites separate', () => {
  const settings = { namedAppZone: 'https://xn--n1afe0b.online', shellOrigins: ['https://xn--n1afe0b.online', 'https://soty.pochinit.online'],
    appOriginTemplate: 'https://{appId}.soty.pochinit.online', domainProfile: 'shell-subdomains-v1' };
  assert.equal(validateNamedAppZone(settings), settings.namedAppZone);
  assert.throws(() => validateNamedAppZone({ ...settings, domainProfile: 'separate-site' }), /apps_zone_separate_site_required/);
  assert.throws(() => validateNamedAppZone({ ...settings, namedAppZone: 'https://apps.xn--n1afe0b.online' }), /apps_zone_shell_root_required/);
  assert.throws(() => validateNamedAppZone({ ...settings, shellOrigins: [...settings.shellOrigins, 'https://admin.xn--n1afe0b.online'] }), /apps_zone_separate_site_required/);
});
