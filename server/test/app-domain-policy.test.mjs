import test from 'node:test';
import assert from 'node:assert/strict';
import { validateNamedAppZone, hasSingleHostHeader, legacyAppFrameSource } from '../app-domain-policy.mjs';

const policy = (namedAppZone, shellOrigins = ['https://soty.pochinit.online'], appOriginTemplate = '') =>
  validateNamedAppZone({ namedAppZone, shellOrigins, appOriginTemplate });

test('named claims are off by default; a distinct public site can be configured', () => {
  assert.equal(policy(''), '');
  assert.equal(policy('https://apps.soty.online/'), 'https://apps.soty.online');
  assert.equal(policy('https://соты.online'), 'https://xn--n1afe0b.online');
});

test('every trusted shell site must be isolated, including aliases and scheme changes', () => {
  for (const zone of ['https://apps.pochinit.online', 'https://pochinit.online', 'https://soty.pochinit.online']) {
    assert.throws(() => policy(zone), /apps_zone_separate_site_required/u);
  }
  assert.throws(() => policy('https://apps.pochinit.online', ['http://soty.pochinit.online']), /apps_zone_separate_site_required/u);
  assert.throws(() => policy('https://apps.soty.online', ['https://soty.pochinit.online', 'https://login.soty.online']), /apps_zone_separate_site_required/u);
});

test('PSL boundaries include multi-label and private suffixes, not the last two labels', () => {
  assert.equal(policy('https://apps.other.co.uk', ['https://soty.company.co.uk']), 'https://apps.other.co.uk');
  assert.throws(() => policy('https://apps.company.co.uk', ['https://soty.company.co.uk']), /apps_zone_separate_site_required/u);
  assert.equal(policy('https://apps.team-b.github.io', ['https://team-a.github.io']), 'https://apps.team-b.github.io');
  assert.throws(() => policy('https://github.io'), /apps_zone_public_domain_required/u);
});

test('invalid, non-public and ambiguous configured origins fail closed', () => {
  for (const zone of ['http://apps.soty.online', 'https://apps.soty.online:8443', 'https://127.0.0.1', 'https://[::1]',
    'https://apps.invalid', 'https://example.com', 'https://com', 'https://apps.unknownsuffixzz', 'https://foo_bar.com',
    ' https://apps.soty.online', 'https://apps.soty.online/path', 'https://apps.soty.online?a=1', 'https://apps.soty.online#x',
    'https://user:password@apps.soty.online', 'https://apps.soty.online.', 'https://apps.soty.online\\a', 'https://%61pps.soty.online']) {
    assert.throws(() => policy(zone), /apps_zone_/u, zone);
  }
});

test('localhost development is explicit and cannot mix with trusted public shells', () => {
  assert.equal(policy('http://named.localhost:5360', ['http://127.0.0.1:5360'], 'http://{appId}.legacy.localhost:5360'), 'http://named.localhost:5360');
  assert.throws(() => policy('http://named.localhost:5360'), /apps_zone_local_shell_required/u);
  assert.throws(() => policy('https://apps.soty.online', ['http://127.0.0.1:5360']), /apps_zone_trusted_sites_required/u);
  assert.throws(() => policy('http://named.localhost:5360', ['http://named.localhost:5360']), /apps_zone_shell_overlap/u);
});

test('named and legacy zones cannot overlap in either direction, regardless of ports', () => {
  for (const zone of ['https://apps.soty.online', 'https://named.apps.soty.online', 'https://soty.online']) {
    assert.throws(() => policy(zone, undefined, 'https://{appId}.apps.soty.online'), /apps_zone_legacy_overlap/u);
  }
  assert.throws(() => policy('http://named.localhost:5361', ['http://127.0.0.1:5360'], 'http://{appId}.localhost:5360'), /apps_zone_legacy_overlap/u);
});

test('raw Host duplicates are rejected even when Node has selected the first header', () => {
  const headers = { host: 'shell.localhost' };
  assert.equal(hasSingleHostHeader({ headers, rawHeaders: ['Host', 'shell.localhost'] }), true);
  assert.equal(hasSingleHostHeader({ headers, rawHeaders: ['Host', 'shell.localhost', 'hOSt', 'app.localhost'] }), false);
  assert.equal(hasSingleHostHeader({ headers, rawHeaders: [] }), false);
  assert.equal(hasSingleHostHeader({ headers: { host: ['shell.localhost'] }, rawHeaders: ['Host', 'shell.localhost'] }), false);
});

test('every supported legacy template yields a valid CSP wildcard for its reserved namespace', () => {
  assert.equal(legacyAppFrameSource(''), '');
  for (const template of ['https://{appId}.apps.soty.online', 'https://prefix-{appId}.apps.soty.online', 'https://fixed.{appId}.apps.soty.online']) {
    assert.equal(legacyAppFrameSource(template), 'https://*.apps.soty.online');
  }
  assert.equal(legacyAppFrameSource('http://{appId}.legacy.localhost:5360'), 'http://*.legacy.localhost:5360');
});
