import test from 'node:test';
import assert from 'node:assert/strict';
import { mkdtemp, readdir, readFile, rm } from 'node:fs/promises';
import { execFile } from 'node:child_process';
import { promisify } from 'node:util';
import { tmpdir } from 'node:os';
import { basename, dirname, join, resolve } from 'node:path';
import { validateNamedAppZone } from '../app-domain-policy.mjs';
import { createHttpApp } from '../http-app.js';

const publicShells = ['https://shell.pochinit.online'];
const admit = (namedAppZone, shellOrigins = publicShells, appOriginTemplate = '') =>
  validateNamedAppZone({ namedAppZone, shellOrigins, appOriginTemplate });

test('legacy variable-label spelling cannot hide overlap with a named namespace', () => {
  for (const appOriginTemplate of [
    'https://{appId}.apps.soty.online',
    'https://prefix-{appId}.apps.soty.online',
    'https://{appId}-suffix.apps.soty.online',
    'https://fixed.{appId}.apps.soty.online',
  ]) {
    for (const zone of ['https://apps.soty.online', 'https://named.apps.soty.online', 'https://soty.online']) {
      assert.throws(() => admit(zone, publicShells, appOriginTemplate), /apps_zone_legacy_overlap/u,
        `${appOriginTemplate} must reserve its actual suffix against ${zone}`);
    }
    assert.equal(admit('https://named.other.online', publicShells, appOriginTemplate), 'https://named.other.online');
  }
  assert.throws(() => admit('http://named.apps.localhost:5301', ['http://127.0.0.1:5300'],
    'http://fixed.prefix-{appId}.apps.localhost:5300'), /apps_zone_legacy_overlap/u);
});

test('rejected named-zone configuration cannot open or migrate any application data', async () => {
  const directory = await mkdtemp(join(tmpdir(), 'soty-zone-acceptance-'));
  const dataDir = join(directory, 'data');
  const cases = [
    { namedAppZone: 'https://named.apps.soty.online', appOriginTemplate: 'https://prefix-{appId}.apps.soty.online' },
    { namedAppZone: 'https://named.apps.soty.online', appOriginTemplate: 'https://fixed.{appId}.apps.soty.online' },
    { namedAppZone: 'https://apps.soty.online', appOriginTemplate: 'https://{appId}.{appId}.legacy.other.online' },
    { namedAppZone: 'https://apps.soty.online', connectOrigins: [...publicShells, 'http://login.soty.online:8080'] },
  ];
  try {
    for (const configuration of cases) {
      let app;
      try {
        assert.throws(() => {
          app = createHttpApp(directory, { dataDir, connectOrigins: publicShells, appOriginTemplate: '',
            gonka: { apiKey: '', baseUrl: 'http://127.0.0.1:1' }, ...configuration });
        }, /apps_zone_|invalid_apps_origin/u);
        assert.deepEqual(await readdir(directory), [], 'a rejected configuration must not create even a data directory');
      } finally {
        await app?.locals.closeServices();
      }
    }
  } finally {
    assert.equal(dirname(resolve(directory)), resolve(tmpdir()));
    assert.ok(basename(directory).startsWith('soty-zone-acceptance-'));
    await rm(directory, { recursive: true, force: true });
  }
});

test('IDNA aliases, PSL exception rules and private suffix tenants preserve the trusted-site boundary', () => {
  assert.throws(() => admit('https://apps.соты.online', ['https://login.xn--n1afe0b.online']),
    /apps_zone_separate_site_required/u);
  // city.kawasaki.jp is an exception to the *.kawasaki.jp PSL wildcard.
  assert.throws(() => admit('https://apps.city.kawasaki.jp', ['https://shell.city.kawasaki.jp']),
    /apps_zone_separate_site_required/u);
  assert.equal(admit('https://apps.foo.kawasaki.jp', ['https://shell.bar.kawasaki.jp']),
    'https://apps.foo.kawasaki.jp');
  assert.throws(() => admit('https://apps.tenant.github.io', ['https://login.tenant.github.io']),
    /apps_zone_separate_site_required/u);
  assert.equal(admit('https://apps.tenant-b.github.io', ['https://shell.tenant-a.github.io']),
    'https://apps.tenant-b.github.io');
});

test('the development exception checks every shell and never treats a loopback-looking public name as local', () => {
  const zone = 'http://named.localhost:5300';
  const allLocal = ['http://localhost:5300', 'http://127.0.0.2:5301', 'http://[::1]:5302'];
  assert.equal(admit(zone, allLocal), zone);
  for (const external of ['https://shell.pochinit.online', 'http://localhost.pochinit.online', 'http://127.0.0.1.pochinit.online']) {
    for (const shells of [[external, ...allLocal], [...allLocal, external]]) {
      assert.throws(() => admit(zone, shells), /apps_zone_local_shell_required/u);
    }
  }
  assert.throws(() => admit('https://apps.soty.online', [...publicShells, 'http://[::1]:5300']),
    /apps_zone_trusted_sites_required/u);
});

// Failure-only diagnostics: no error/reason, argv, configuration or payload.
function observeStartupPhase(signal, emit = value => console.log(JSON.stringify(value))) {
  const phases = ['directory', 'initial_create', 'initial_close', 'snapshot', 'child_a', 'child_b', 'verify_child_a', 'verify_child_b', 'cleanup'];
  let phase = 'directory', finished = false, abortReported = false;
  const report = code => {
    try {
      emit({ schema: 'soty.http-startup-test-diagnostic.v1', code, phase });
    } catch { /* Diagnostics must preserve the original failure. */ }
  };
  const aborted = () => {
    if (finished || abortReported) return;
    abortReported = true; report('test_aborted');
  };
  signal.addEventListener('abort', aborted, { once: true });
  if (signal.aborted) aborted();
  return Object.freeze({
    at(next) {
      if (finished) return;
      if (!phases.includes(next)) throw new Error('invalid_startup_diagnostic_phase');
      phase = next;
    },
    childFailed() {
      if (finished) return;
      if (phase !== 'child_a' && phase !== 'child_b') throw new Error('invalid_startup_child_phase');
      report('child_failed');
    },
    finish() {
      finished = true; phase = 'finished'; signal.removeEventListener('abort', aborted);
    },
  });
}
test('HTTP startup revalidates a retained named site even when new claims are disabled', { timeout: 20_000 }, async t => {
  const diagnostic = observeStartupPhase(t.signal);
  try {
  const directory = await mkdtemp(join(tmpdir(), 'soty-zone-acceptance-'));
  const configuration = { dataDir: directory, connectOrigins: publicShells,
    appOriginTemplate: 'https://{appId}.legacy.other.online', namedAppZone: 'https://apps.soty.online',
    gonka: { apiKey: '', baseUrl: 'http://127.0.0.1:1' } };
  try {
    diagnostic.at('initial_create'); const first = createHttpApp(directory, configuration);
    diagnostic.at('initial_close'); await first.locals.closeServices();
    diagnostic.at('snapshot'); const before = await readFile(join(directory, 'apps', 'registry.sqlite'));
    // A separate process also proves that refused startup releases its open
    // services. It receives only synthetic configuration, never host credentials.
    const source = `
      import assert from 'node:assert/strict';
      import { createHttpApp } from ${JSON.stringify(new URL('../http-app.js', import.meta.url).href)};
      const configuration = JSON.parse(process.argv[1]);
      assert.throws(() => createHttpApp(configuration.dataDir, configuration),
        /apps_zone_separate_site_required|apps_named_zone_shell_overlap/u);
      console.log('retained-zone-rejected');
    `;
    const env = Object.fromEntries(['SystemRoot', 'WINDIR', 'TEMP', 'TMP']
      .filter(name => process.env[name] !== undefined).map(name => [name, process.env[name]]));
    for (const [childIndex, newlyTrustedShell] of ['https://login.soty.online', 'https://child.apps.soty.online'].entries()) {
      diagnostic.at(childIndex === 0 ? 'child_a' : 'child_b');
      let stdout;
      try {
      ({ stdout } = await promisify(execFile)(process.execPath, ['--input-type=module', '-e', source,
        JSON.stringify({ ...configuration, namedAppZone: '', connectOrigins: [...publicShells, newlyTrustedShell] })],
      { env, timeout: 8_000, maxBuffer: 128 * 1024 }));
      } catch (error) { diagnostic.childFailed(); throw error; }
      diagnostic.at(childIndex === 0 ? 'verify_child_a' : 'verify_child_b');
      assert.equal(stdout.trim(), 'retained-zone-rejected');
      assert.deepEqual(await readFile(join(directory, 'apps', 'registry.sqlite')), before);
    }
  } finally {
    diagnostic.at('cleanup');
    assert.equal(dirname(resolve(directory)), resolve(tmpdir()));
    assert.ok(basename(directory).startsWith('soty-zone-acceptance-'));
    await rm(directory, { recursive: true, force: true });
  }
  } finally { diagnostic.finish(); }
});
